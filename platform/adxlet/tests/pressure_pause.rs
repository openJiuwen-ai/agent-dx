//! Pressure-triggered pause behavior on the controller actor: the
//! conjunction matrix, the rate and in-flight budget, the attribution,
//! and the cooldown after a resume.
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Result};
use adxlet::checkpoint::{
    CheckpointCooperation, LocalCheckpointStore, PauseRequest, ResumeRequest,
};
use adxlet::resources::{NodePressureState, PressureLevel, ResourcePressure};
use adxlet::{
    Adxlet, Durability, EscalationMode, PressurePauseOptions, Readiness, Routes, RuntimeDriver,
    StateSink,
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Hand-built board rounds. The observation stamp deliberately moves into
/// the future so two consecutive rounds always land in different rate
/// windows without sleeping across a real clock second.
fn board(level: PressureLevel, failures: u32, observed_at: u64) -> NodePressureState {
    NodePressureState {
        memory: ResourcePressure {
            level,
            percent: if level == PressureLevel::High { 95 } else { 10 },
            last_transition_unix_seconds: observed_at,
        },
        disk: ResourcePressure::default(),
        observed_at_unix_seconds: observed_at,
        consecutive_failures: failures,
    }
}

fn publish(node: &Adxlet, state: NodePressureState) {
    // A normal round reopens admission; a high round closes it, matching
    // the historical admission formula.
    let accepting = state.memory.level == PressureLevel::Normal;
    node.set_pressure_state(&adxlet::resources::PressureUpdate { accepting, state });
}

fn now_plus(seconds: u64) -> u64 {
    adxlet::checkpoint::now().unwrap_or(0) + seconds
}

/// Options with pressure pauses enabled and a configurable idle-window age
/// so the window gate passes immediately after the window opens.
fn immediate(min_idle_seconds: u64) -> PressurePauseOptions {
    PressurePauseOptions {
        enabled: true,
        min_idle_seconds,
        freshness_seconds: 600,
        budget: 1,
        in_flight_cap: 1,
        escalate_checkpoint_storage: EscalationMode::Off,
    }
}

/// Enabled options with explicit admission limits for reason-attribution
/// scenarios: a wide in-flight cap isolates the budget denial, and a wide
/// budget isolates the in-flight denial.
fn limited(min_idle_seconds: u64, budget: u32, in_flight_cap: usize) -> PressurePauseOptions {
    PressurePauseOptions {
        budget,
        in_flight_cap,
        ..immediate(min_idle_seconds)
    }
}

fn skips(node: &Adxlet, reason: &str) -> u64 {
    let series = format!("adx_node_pressure_pause_skipped_total{{reason=\"{reason}\"}}");
    node.metrics()
        .lines()
        .find(|line| line.starts_with(&series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|token| token.parse().ok())
        .unwrap_or_else(|| panic!("series {series} missing from metrics"))
}

/// Unmet node or eligibility conditions never count as skips: only a denied
/// claim or a too-young window does.
fn assert_no_skips(node: &Adxlet) {
    for reason in [
        "budget_exhausted",
        "in_flight_exhausted",
        "window_too_young",
    ] {
        assert_eq!(skips(node, reason), 0, "reason {reason} counted a skip");
    }
}

#[derive(Default)]
struct Backend {
    running: Mutex<BTreeSet<String>>,
    events: Mutex<Vec<String>>,
    prepare_ids: Mutex<Vec<String>>,
    activity: Mutex<Option<(u64, u64)>>,
    checkpoint_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    checkpoint_entered: tokio::sync::Notify,
    committed_records: Mutex<BTreeMap<String, EnvironmentRecord>>,
    // The steady Paused record clears its intent on transition; the
    // attribution is observable on the reservation commit during the
    // pause, so keep the last observed intent per environment.
    seen_intents: Mutex<BTreeMap<String, adx_core::observation::OperationIntent>>,
    fail_prepare: Mutex<bool>,
    // Fails routes.reopen to reproduce a temporarily unreachable relay
    // after a quiesced pause attempt.
    fail_reopen: Mutex<bool>,
    // Test rendezvous for the mid-tick demand race: the first armed
    // observation reports itself and parks until released, so a test
    // can land an ensure-running demand bump between the tick's opening
    // window refresh and its trailing pressure evaluation. yield_now
    // cannot order this: the runtime may resume the observation without
    // ever polling the concurrent command.
    gate_observe: std::sync::atomic::AtomicBool,
    observe_reached: tokio::sync::Notify,
    observe_released: tokio::sync::Notify,
}
impl Backend {
    fn event(&self, name: &str) {
        self.events.lock().unwrap().push(name.into());
    }
    fn intent(&self, id: &str) -> Option<adx_core::observation::OperationIntent> {
        self.seen_intents.lock().unwrap().get(id).copied()
    }
    fn operation_ids(&self) -> Vec<String> {
        self.prepare_ids.lock().unwrap().clone()
    }
    fn count_events(&self, name: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == name)
            .count()
    }
    fn reopen_attempts(&self) -> usize {
        self.count_events("reopen")
    }
}

#[async_trait]
impl RuntimeDriver for Backend {
    async fn stats(&self, _: &str) -> Result<adxlet::metrics::RuntimeUsage> {
        Ok(adxlet::metrics::RuntimeUsage {
            cpu_usage_ns: 1_000_000,
            memory_usage_bytes: 100,
            memory_limit_bytes: 1024,
        })
    }
    fn retains_checkpoint_files(&self, _: &str) -> bool {
        true
    }
    fn retains_capture_files(&self, _: &str) -> bool {
        true
    }
    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        Ok(self
            .running
            .lock()
            .unwrap()
            .iter()
            .map(|id| adxlet::RuntimeObservation {
                environment_id: "environment".into(),
                tenant_id: "tenant".into(),
                generation: 1,
                runtime_id: id.clone(),
                running: true,
            })
            .collect())
    }
    async fn start(
        &self,
        _: &EnvironmentSpec,
        id: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.event("start");
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains(id))
    }
    async fn observe(&self, id: &str) -> Result<adxlet::RuntimeStatus> {
        if self
            .gate_observe
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.observe_reached.notify_one();
            self.observe_released.notified().await;
        }
        Ok(adxlet::RuntimeStatus {
            running: self.running.lock().unwrap().contains(id),
            failure: None,
        })
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.event("remove");
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, id: &str, path: &Path, _: Duration) -> Result<()> {
        self.event("checkpoint");
        self.checkpoint_entered.notify_one();
        let gate = self.checkpoint_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        std::fs::write(path.join("memory"), b"saved memory").unwrap();
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
    async fn restore_from(
        &self,
        _: &EnvironmentSpec,
        id: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        _: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        self.event("restore");
        assert_eq!(std::fs::read(path.join("memory")).unwrap(), b"saved memory");
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.3".parse().unwrap())
    }
}

#[async_trait]
impl CheckpointCooperation for Backend {
    async fn prepare(&self, _: &EnvironmentRecord, operation_id: &str) -> Result<()> {
        self.prepare_ids.lock().unwrap().push(operation_id.into());
        self.event("prepare");
        if *self.fail_prepare.lock().unwrap() {
            return Err(adx_core::Error::Unavailable("prepare reply lost".into()));
        }
        Ok(())
    }
    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        self.event("abort");
        Ok(())
    }
    async fn resumed(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Readiness for Backend {
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(u64, u64)> {
        self.activity
            .lock()
            .unwrap()
            .ok_or_else(|| adx_core::Error::Unavailable("activity unknown".into()))
    }
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("ready");
        Ok(())
    }
}

#[async_trait]
impl Routes for Backend {
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        Ok(("proxy".into(), 1, 0))
    }
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("activate");
        Ok(())
    }
    async fn reopen(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("reopen");
        if *self.fail_reopen.lock().unwrap() {
            return Err(adx_core::Error::Unavailable(
                "relay temporarily unreachable".into(),
            ));
        }
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("retire");
        Ok(())
    }
}

#[async_trait]
impl StateSink for Backend {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        self.event(&format!("commit:{:?}", record.state));
        if let Some(intent) = record.observation.as_ref().and_then(|o| o.intent) {
            self.seen_intents
                .lock()
                .unwrap()
                .insert(record.spec.id.clone(), intent);
        }
        self.committed_records
            .lock()
            .unwrap()
            .insert(record.spec.id.clone(), record.clone());
        Ok(Durability::Published)
    }
}

fn spec(id: &str, action: adx_core::lifecycle::IdleAction) -> EnvironmentSpec {
    EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: adx_core::lifecycle::LifecyclePolicy {
            idle_timeout_seconds: 3600,
            idle_action: action,
            pause: None,
            restart: None,
        },
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "firecracker".into(),
        resources: adx_core::Resources {
            cpu_millis: 100,
            memory_bytes: 1024,
            disk_bytes: 1024,
        },
        priority: 0,
        env: Default::default(),
        scheduling: Default::default(),
        execution: Default::default(),
    }
}

fn assignment(id: &str) -> Assignment {
    Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    }
}

/// One node with pressure pauses configured and one idle-pause environment
/// created, Running, and holding an idle window before the first pressure
/// round is published.
async fn fixture(
    options: PressurePauseOptions,
    action: adx_core::lifecycle::IdleAction,
) -> (
    tempfile::TempDir,
    Arc<Backend>,
    Adxlet,
    adxlet::EnvironmentHandle,
    adxlet::OperationResult,
) {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    *backend.activity.lock().unwrap() = Some((1, 0));
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap()
    .with_pressure_pause_options(options)
    .unwrap();
    node.update_capacity(
        adx_core::Resources {
            // Room for two environments so rate-limit scenarios can run
            // a second candidate on the same node.
            cpu_millis: 200,
            memory_bytes: 2048,
            disk_bytes: 2048,
        },
        Duration::from_secs(300),
    )
    .unwrap();
    let handle = node
        .environment(spec("environment", action), assignment("environment"))
        .unwrap();
    let created = handle.create().await.unwrap();
    assert_eq!(created.record.state, EnvironmentState::Running);
    // One monitoring round opens the idle window while the board is still
    // at its never-observed default.
    handle.tick().await.unwrap();
    (temp, backend, node, handle, created)
}

/// A normal board never pauses even with every other condition satisfied.
#[tokio::test]
async fn normal_board_does_not_trigger() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::Normal, 0, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// A high level with failed observation rounds stays unreliable and never
/// pauses.
#[tokio::test]
async fn failed_observations_do_not_trigger() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 2, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// A high level whose observation is older than the freshness window is
/// refused: stale evidence cannot stop an environment.
#[tokio::test]
async fn stale_board_does_not_trigger() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 0, 1));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// A running environment without an idle window is not a candidate: an
/// unknown activity observation drops the window and blocks the pause.
#[tokio::test]
async fn missing_idle_window_does_not_trigger() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    *backend.activity.lock().unwrap() = None;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// Environments whose idle action deletes stay out of pressure scope.
#[tokio::test]
async fn delete_action_specs_are_out_of_scope() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Delete).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// The idle window must be old enough: a 300-second cooldown keeps a freshly
/// observed window ineligible.
#[tokio::test]
async fn young_idle_window_does_not_trigger() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(300), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    // The exemption is counted so the cooldown can be tuned from metrics.
    // Both evaluation paths of the same round are counted: the signal wake
    // and the monitoring tick each evaluate the young window once.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(skips(&node, "window_too_young"), 2);
    assert_eq!(skips(&node, "budget_exhausted"), 0);
    assert_eq!(skips(&node, "in_flight_exhausted"), 0);
}

/// An ensure-running arriving while a pressure evaluation is already
/// underway has bumped demand before its command is observed: the stale
/// idle window must be refreshed against that bump instead of riding
/// through the eligibility checks on a polluted baseline.
#[tokio::test]
async fn a_late_ensure_running_cannot_ride_a_stale_idle_window() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    backend
        .gate_observe
        .store(true, std::sync::atomic::Ordering::Release);
    // The tick enters first: its opening window refresh sees the
    // pre-bump demand, then its runtime observation parks at the gate
    // with the window still in place.
    let ticking = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.tick().await })
    };
    backend.observe_reached.notified().await;
    // The ensure-running bumps demand and enqueues its command while the
    // controller is parked mid-tick; the start rendezvous witnesses the
    // bump before the gate opens, so the trailing pressure evaluation
    // runs with the stale window and the bumped counter.
    let ensure_started = std::sync::Arc::new(tokio::sync::Notify::new());
    let ensured = {
        let handle = handle.clone();
        let ensure_started = ensure_started.clone();
        tokio::spawn(async move {
            ensure_started.notify_one();
            handle.ensure_running("late".into()).await
        })
    };
    ensure_started.notified().await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    backend.observe_released.notify_one();
    ticking.await.unwrap().unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running,
        "the interleaved command must not be overtaken by a pause"
    );
    assert!(
        backend.operation_ids().is_empty(),
        "no pressure pause may start on a stale window: {:?}",
        backend.operation_ids()
    );
    assert_no_skips(&node);
    ensured.await.unwrap().unwrap();
}

/// An ensure-running arriving together with the pressure signal is
/// processed first: the command clears the window before the pressure
/// wake evaluates, and no pause may start from the interleaving.
#[tokio::test]
async fn ensure_running_arriving_with_the_signal_is_processed_first() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    let ensured = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.ensure_running("interleaved".into()).await })
    };
    tokio::task::yield_now().await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    ensured.await.unwrap().unwrap();
    // The pressure wake follows the command; the cleared window keeps
    // it silent.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.operation_ids().is_empty());
    assert_no_skips(&node);
}

/// A resume migrates the identity and resets the runtime observers; the
/// pressure arm stays live across it: with the window re-opened by one
/// monitoring round and no further tick, a High round alone wakes the
/// environment and counts the cooldown exemption.
#[tokio::test]
async fn a_resumed_environment_wakes_on_pressure_without_a_tick() {
    let (_temp, backend, node, handle, created) =
        fixture(immediate(300), adx_core::lifecycle::IdleAction::Pause).await;
    handle
        .pause(PauseRequest {
            operation_id: "pause-user".into(),
            expected_revision: created.record.revision,
            retention: adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 600 },
            timeout_seconds: 60,
            storage: None,
        })
        .await
        .unwrap();
    let paused = handle.sync().await.unwrap().record;
    handle
        .resume(ResumeRequest {
            run_timeout_seconds: None,
            operation_id: "resume-arm".into(),
            expected_revision: paused.revision,
        })
        .await
        .unwrap();
    // One monitoring round re-opens the idle window while the board is
    // still calm; its trailing evaluation stays silent.
    handle.tick().await.unwrap();
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(
        skips(&node, "window_too_young"),
        1,
        "no monitoring round follows the signal: only the pressure wake evaluates"
    );
    // The user pause from the setup stays; no pressure pause may start.
    assert!(backend
        .operation_ids()
        .iter()
        .all(|id| !id.starts_with("pressure-")));
}

/// A pressure pause lands long before the 3600-second idle deadline, carries
/// the pressure attribution, and prefixes its operation ids.
#[tokio::test]
async fn pressure_pause_triggers_early_with_pressure_attribution() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    let paused = handle.sync().await.unwrap().record;
    assert_eq!(paused.state, EnvironmentState::Paused);
    assert_eq!(
        backend.intent("environment"),
        Some(adx_core::observation::OperationIntent::Pressure)
    );
    let ids = backend.operation_ids();
    assert!(
        !ids.is_empty() && ids.iter().all(|id| id.starts_with("pressure-")),
        "operation ids must carry the pressure prefix: {ids:?}"
    );
}

/// One budget slot per observation window: the second environment of the
/// same round is refused, and the next round admits it again.
#[tokio::test]
async fn one_pause_per_round_and_a_new_round_admits_the_rest() {
    let (_temp, _backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    let other = node
        .environment(
            spec("other", adx_core::lifecycle::IdleAction::Pause),
            assignment("other"),
        )
        .unwrap();
    assert_eq!(
        other.create().await.unwrap().record.state,
        EnvironmentState::Running
    );
    other.tick().await.unwrap();
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    node.monitor_environments().await.unwrap();
    let states = [
        handle.sync().await.unwrap().record.state,
        other.sync().await.unwrap().record.state,
    ];
    assert_eq!(
        states
            .iter()
            .filter(|s| **s == EnvironmentState::Paused)
            .count(),
        1,
        "exactly one environment pauses per round: {states:?}"
    );
    // A new observation round resets the rate budget.
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    node.monitor_environments().await.unwrap();
    for h in [&handle, &other] {
        assert_eq!(
            h.sync().await.unwrap().record.state,
            EnvironmentState::Paused
        );
    }
}

/// The claim runs last: an ineligible candidate of the same round (a
/// delete-action spec) never consumes the single budget slot, so the real
/// candidate still pauses.
#[tokio::test]
async fn ineligible_candidates_never_consume_the_budget() {
    let (_temp, _backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Delete).await;
    let other = node
        .environment(
            spec("other", adx_core::lifecycle::IdleAction::Pause),
            assignment("other"),
        )
        .unwrap();
    assert_eq!(
        other.create().await.unwrap().record.state,
        EnvironmentState::Running
    );
    other.tick().await.unwrap();
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    node.monitor_environments().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(
        other.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    assert_no_skips(&node);
}

/// A denied claim counts one budget-exhausted skip. The next observation
/// round resets the budget and admits the refused environment without
/// counting another skip.
#[tokio::test]
async fn budget_exhaustion_counts_denied_evaluations() {
    let (_temp, _backend, node, handle, _) =
        fixture(limited(0, 1, 2), adx_core::lifecycle::IdleAction::Pause).await;
    let other = node
        .environment(
            spec("other", adx_core::lifecycle::IdleAction::Pause),
            assignment("other"),
        )
        .unwrap();
    other.create().await.unwrap();
    other.tick().await.unwrap();
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    node.monitor_environments().await.unwrap();
    let states = [
        handle.sync().await.unwrap().record.state,
        other.sync().await.unwrap().record.state,
    ];
    assert_eq!(
        states
            .iter()
            .filter(|s| **s == EnvironmentState::Paused)
            .count(),
        1,
        "the wide in-flight cap leaves the budget as the only limit: {states:?}"
    );
    // Both evaluation paths of the round are counted: the signal wake and
    // the monitoring tick each deny the refused environment once.
    assert_eq!(skips(&node, "budget_exhausted"), 2);
    assert_eq!(skips(&node, "in_flight_exhausted"), 0);
    // A new round is a fresh budget: the refused environment pauses and no
    // further skip is counted.
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    node.monitor_environments().await.unwrap();
    for h in [&handle, &other] {
        assert_eq!(
            h.sync().await.unwrap().record.state,
            EnvironmentState::Paused
        );
    }
    assert_eq!(skips(&node, "budget_exhausted"), 2);
}

/// A failed pressure pause consumes its rate slot: the same round cannot
/// retry another environment, and only a new round admits an attempt again.
#[tokio::test]
async fn failed_pause_keeps_the_rate_slot_consumed() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    let other = node
        .environment(
            spec("other", adx_core::lifecycle::IdleAction::Pause),
            assignment("other"),
        )
        .unwrap();
    other.create().await.unwrap();
    other.tick().await.unwrap();
    *backend.fail_prepare.lock().unwrap() = true;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    // The failing prepare surfaces as a monitoring error; the budget slot
    // is still consumed for the round.
    let _ = node.monitor_environments().await;
    for h in [&handle, &other] {
        assert_eq!(
            h.sync().await.unwrap().record.state,
            EnvironmentState::Running
        );
    }
    assert!(
        !backend.operation_ids().is_empty(),
        "the failing attempt still claimed the budget slot"
    );
    // A new round is a fresh budget; clearing the failure lets one
    // environment pause, proving the release comes from the round boundary
    // rather than a rollback.
    *backend.fail_prepare.lock().unwrap() = false;
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    node.monitor_environments().await.unwrap();
    let states = [
        handle.sync().await.unwrap().record.state,
        other.sync().await.unwrap().record.state,
    ];
    assert_eq!(
        states
            .iter()
            .filter(|s| **s == EnvironmentState::Paused)
            .count(),
        1,
        "the new round admits exactly one pause: {states:?}"
    );
}

/// A failed reopen after a failed pressure pause arms a retrying
/// recovery instead of stranding a closed admission: the armed
/// recovery retries on its own deadline without waiting for the next
/// monitoring round, later monitoring ticks keep the recovery armed
/// while the relay stays unreachable, the route restores even after
/// the pressure has fully subsided (no further High signal ever
/// rings), and the recovered environment still pauses on a later high
/// round.
#[tokio::test]
async fn failed_reopen_is_retried_after_pressure_relieves() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    // The pause fails and the route reopen fails with it: the admission
    // stays closed while the environment keeps running.
    *backend.fail_prepare.lock().unwrap() = true;
    *backend.fail_reopen.lock().unwrap() = true;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    let _ = handle.tick().await;
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    // The signal-driven evaluation runs ahead of the queued tick (the
    // submission queue yields before the command lands), and its failed
    // reopen arms the recovery: the tick's own admission-recovery hook
    // retries once — two attempts land inside the tick.
    assert_eq!(
        backend.reopen_attempts(),
        2,
        "the failed pause reopened once and the armed recovery retried once"
    );
    // The retry failure backs off: no further attempt rings within the
    // backoff window against the still-unreachable relay.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        backend.reopen_attempts(),
        2,
        "the backoff holds: no third attempt within the window"
    );
    // The pressure fully subsides: only normal boards follow, so no
    // further High signal rings. The monitoring tick retries the
    // recovery: the relay still fails, so the recovery stays armed.
    publish(&node, board(PressureLevel::Normal, 0, now_plus(200)));
    let _ = handle.tick().await;
    assert_eq!(
        backend.reopen_attempts(),
        3,
        "the recovery retried the reopen on the next tick"
    );
    // The relay recovers and the pause failure clears: the next
    // monitoring tick restores the route.
    *backend.fail_reopen.lock().unwrap() = false;
    *backend.fail_prepare.lock().unwrap() = false;
    node.monitor_environments().await.unwrap();
    assert_eq!(backend.reopen_attempts(), 4, "the reopen finally lands");
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    // The recovered environment still pauses on a later high round, and
    // the cleared recovery state never reopens the route again.
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    node.monitor_environments().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    assert_eq!(
        backend.reopen_attempts(),
        4,
        "a successful pause keeps the route retired without further reopens"
    );
}

/// After a resume the window restarts: a 300-second cooldown keeps the
/// fresh window ineligible even under sustained high pressure, while an
/// aged window still pauses before the idle deadline.
#[tokio::test(start_paused = true)]
async fn cooldown_after_resume_blocks_immediate_repause() {
    let (_temp, _backend, node, handle, _) =
        fixture(immediate(300), adx_core::lifecycle::IdleAction::Pause).await;
    // Age the idle window past the cooldown, but stay far below the
    // 3600-second idle deadline.
    tokio::time::advance(Duration::from_secs(301)).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    let paused = handle.sync().await.unwrap().record;
    assert_eq!(paused.state, EnvironmentState::Paused);
    // Relief reopens admission before the resume, matching how an
    // operator resumes after the node pressure clears.
    publish(&node, board(PressureLevel::Normal, 0, now_plus(150)));
    handle
        .resume(ResumeRequest {
            run_timeout_seconds: None,
            operation_id: "resume-a".into(),
            expected_revision: paused.revision,
        })
        .await
        .unwrap();
    // Sustained high pressure returns while the cooldown is armed.
    publish(&node, board(PressureLevel::High, 0, now_plus(200)));
    for _ in 0..2 {
        handle.tick().await.unwrap();
        assert_eq!(
            handle.sync().await.unwrap().record.state,
            EnvironmentState::Running
        );
    }
    // The resumed environment opens a fresh window; the cooldown keeps it
    // ineligible for two consecutive monitoring rounds.
}

/// With a zero cooldown the resumed window is eligible again after one
/// monitoring round.
#[tokio::test]
async fn zero_cooldown_allows_repause_after_resume() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    let paused = handle.sync().await.unwrap().record;
    // Relief reopens admission before the resume.
    publish(&node, board(PressureLevel::Normal, 0, now_plus(150)));
    handle
        .resume(ResumeRequest {
            run_timeout_seconds: None,
            operation_id: "resume-b".into(),
            expected_revision: paused.revision,
        })
        .await
        .unwrap();
    // One round re-opens the window; the returned high board pauses again
    // immediately under the zero cooldown.
    publish(&node, board(PressureLevel::High, 0, now_plus(200)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    assert_eq!(
        backend.intent("environment"),
        Some(adx_core::observation::OperationIntent::Pressure)
    );
}

/// The in-flight cap serializes pressure pauses: while one pause hangs in
/// its checkpoint, a second environment of the same round is refused, and
/// it proceeds once the first lands and a new round arrives.
#[tokio::test]
async fn in_flight_cap_blocks_a_second_pause_until_the_first_lands() {
    let (_temp, backend, node, handle, _) =
        fixture(limited(0, 2, 1), adx_core::lifecycle::IdleAction::Pause).await;
    let other = node
        .environment(
            spec("other", adx_core::lifecycle::IdleAction::Pause),
            assignment("other"),
        )
        .unwrap();
    other.create().await.unwrap();
    other.tick().await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    // The high round rings both controllers; whichever claims first hangs
    // inside its checkpoint while the in-flight cap refuses the other.
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    backend.checkpoint_entered.notified().await;
    let occupied_is_first = backend.intent("environment").is_some();
    let (occupied, refused) = if occupied_is_first {
        (&handle, &other)
    } else {
        (&other, &handle)
    };
    let occupied_id = if occupied_is_first {
        "environment"
    } else {
        "other"
    };
    assert_eq!(
        backend.intent(occupied_id),
        Some(adx_core::observation::OperationIntent::Pressure)
    );
    // The refused candidate of the same round stays Running: its controller
    // never started a pause.
    assert_eq!(
        refused.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    // Landing the first pause releases the in-flight registration.
    gate.notify_waiters();
    // Further checkpoints proceed without the gate.
    *backend.checkpoint_gate.lock().unwrap() = None;
    assert_eq!(
        occupied.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    // A new round admits the refused environment.
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    refused.tick().await.unwrap();
    assert_eq!(
        refused.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    // The wide budget isolates the cap as the denial cause: exactly one
    // in-flight-exhausted skip, no budget skip.
    assert_eq!(skips(&node, "in_flight_exhausted"), 1);
    assert_eq!(skips(&node, "budget_exhausted"), 0);
}

/// Paused environments stay paused: relief and further high rounds never
/// resume them, and a paused environment reacts to a high signal silently.
#[tokio::test]
async fn paused_environments_stay_paused_when_pressure_relieves() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    publish(&node, board(PressureLevel::Normal, 0, now_plus(200)));
    handle.tick().await.unwrap();
    // Another high round rings the signal again; the paused environment
    // stays out of scope without failing its controller.
    publish(&node, board(PressureLevel::High, 0, now_plus(300)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.tick().await.unwrap();
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
    assert!(
        !backend
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == "restore"),
        "pressure relief must not resume a paused environment"
    );
}

/// The High-round signal alone pauses an eligible environment without any
/// monitoring tick: the actor's pressure wake path drives the same verdict
/// as the tick path, with the same attribution.
#[tokio::test]
async fn signal_wake_triggers_without_a_tick() {
    let (_temp, backend, node, handle, _) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    // Publishing a high board rings the signal; the actor wakes and pauses
    // the environment without a tick command.
    publish(&node, board(PressureLevel::High, 0, now_plus(100)));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state = handle.sync().await.unwrap().record.state;
        if state == EnvironmentState::Paused {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pressure signal never paused the environment"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        backend.intent("environment"),
        Some(adx_core::observation::OperationIntent::Pressure)
    );
    let ids = backend.operation_ids();
    assert!(
        !ids.is_empty() && ids.iter().all(|id| id.starts_with("pressure-")),
        "signal-path operation ids must carry the pressure prefix: {ids:?}"
    );
}

/// An explicit user pause keeps its own attribution: the pressure machinery
/// never rewrites the intent of an operator-issued pause.
#[tokio::test]
async fn user_pause_keeps_its_own_intent() {
    let (_temp, backend, _node, handle, created) =
        fixture(immediate(0), adx_core::lifecycle::IdleAction::Pause).await;
    handle
        .pause(PauseRequest {
            operation_id: "pause-user".into(),
            expected_revision: created.record.revision,
            retention: adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 600 },
            timeout_seconds: 60,
            storage: None,
        })
        .await
        .unwrap();
    let settled = handle.sync().await.unwrap().record;
    assert_eq!(settled.state, EnvironmentState::Paused);
    assert_eq!(
        backend.intent("environment"),
        Some(adx_core::observation::OperationIntent::User)
    );
    assert!(backend
        .operation_ids()
        .iter()
        .all(|id| !id.starts_with("pressure-")));
}

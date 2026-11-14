use adx_core::{Resources, Result};
use adxlet::resources::{NodePressureState, Pressure, PressureGate, PressureLevel, PressurePolicy};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// How long a silent signal must stay silent. Well below the smallest
/// reporting period a real deployment uses, so a missed ring shows up as a
/// flake of this bound, not as an accidental pass.
const QUIET: Duration = Duration::from_millis(120);

struct NullBackend;

#[async_trait]
impl RuntimeDriver for NullBackend {
    async fn start(
        &self,
        _spec: &adx_core::EnvironmentSpec,
        _runtime_id: &str,
        _generation: u64,
        _devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        Err(adx_core::Error::Unavailable(
            "runtime start is unused".into(),
        ))
    }
    async fn is_running(&self, _runtime_id: &str) -> Result<bool> {
        Ok(false)
    }
    async fn remove(&self, _runtime_id: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Readiness for NullBackend {
    async fn wait_ready(&self, _record: &adx_core::EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Routes for NullBackend {
    async fn activate(&self, _record: &adx_core::EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _record: &adx_core::EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl StateSink for NullBackend {
    async fn commit(&self, _record: &adx_core::EnvironmentRecord) -> Result<Durability> {
        Ok(Durability::Published)
    }
}

fn node() -> Adxlet {
    let backend = Arc::new(NullBackend);
    Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend,
    )
}

fn gate() -> PressureGate {
    PressureGate::new(PressurePolicy::default()).expect("default policy is valid")
}

/// The board starts at the never-observed state: consumers can treat
/// `observed_at == 0` as "no evidence yet" before the first sample, and the
/// node still accepts work because no pressure fact exists.
#[tokio::test]
async fn unpublished_node_reports_the_never_observed_state() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    let state = manager.pressure_state();
    assert_eq!(state, NodePressureState::default());
    assert_eq!(state.observed_at_unix_seconds, 0);
    assert!(manager.accepting_allocations());
}

/// One observation round reaches both planes of the manager: the admission
/// bit keeps its historical meaning (true = accepting new work), and the
/// board exposes the same structured state to every reader.
#[tokio::test]
async fn publishing_one_round_updates_admission_and_board_together() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    let mut gate = gate();
    let pressured = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    assert!(!pressured.accepting);
    manager.set_pressure_state(&pressured);
    assert_eq!(manager.pressure_state().memory.level, PressureLevel::High);
    assert!(!manager.accepting_allocations());
    let relieved = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 70,
    });
    assert!(relieved.accepting);
    manager.set_pressure_state(&relieved);
    assert_eq!(manager.pressure_state(), relieved.state);
    assert!(manager.accepting_allocations());
}

/// The board always shows the present: a reader that arrives between or
/// after rounds sees the latest published snapshot without replaying
/// history, and a failed round keeps the frozen levels visible while the
/// failure counter grows.
#[tokio::test]
async fn board_reads_always_return_the_latest_published_value() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    let mut gate = gate();
    let first = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    manager.set_pressure_state(&first);
    // Both readings sit inside the hysteresis band: the memory level stays
    // high from the previous round while the disk level stays normal.
    let second = gate.update(Pressure {
        memory_percent: 85,
        disk_percent: 85,
    });
    manager.set_pressure_state(&second);
    assert_eq!(manager.pressure_state().memory.level, PressureLevel::High);
    assert_eq!(manager.pressure_state().disk.level, PressureLevel::Normal);
    let failed = gate.fail();
    manager.set_pressure_state(&failed);
    let state = manager.pressure_state();
    assert_eq!(state.consecutive_failures, 1);
    assert_eq!(state.memory.level, PressureLevel::High);
    assert!(!manager.accepting_allocations());
}

/// The signal rings only for rounds that end at a high level: a normal
/// round stays silent, a high round rings, a failed round frozen at high
/// keeps ringing, and a failed round on a normal gate stays silent because
/// a broken probe is not pressure.
#[tokio::test]
async fn signal_rings_only_for_high_rounds() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    let mut probe = gate();
    let mut signal = manager.pressure_signal();

    let normal = probe.update(Pressure {
        memory_percent: 10,
        disk_percent: 10,
    });
    manager.set_pressure_state(&normal);
    assert!(tokio::time::timeout(QUIET, signal.changed()).await.is_err());

    let high = probe.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    manager.set_pressure_state(&high);
    assert!(signal.changed().await.is_ok());

    let failed_high = probe.fail();
    manager.set_pressure_state(&failed_high);
    assert!(signal.changed().await.is_ok());

    let mut fresh = gate();
    let failed_normal = fresh.fail();
    manager.set_pressure_state(&failed_normal);
    assert!(tokio::time::timeout(QUIET, signal.changed()).await.is_err());
}

/// The heartbeat report and the board are two readings of one source: the
/// producer-side conversion of the round just published agrees with what a
/// board reader sees, and the wire field stays absent before the first
/// successful sample. The heartbeat caller in the binary fills its field
/// from this same conversion.
#[tokio::test]
async fn heartbeat_report_matches_the_published_board_value() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    // Before any successful sample the report is omitted entirely.
    assert_eq!(manager.pressure_state().observed_at_unix_seconds, 0);
    let mut gate = gate();
    let pressured = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    manager.set_pressure_state(&pressured);
    let report = adxlet::resources::report_from(&pressured.state).unwrap();
    let board = manager.pressure_state();
    assert_eq!(
        report.observed_at_unix_seconds,
        board.observed_at_unix_seconds
    );
    assert_eq!(report.consecutive_failures, board.consecutive_failures);
    let memory = report.memory.unwrap();
    assert_eq!(memory.percent, u32::from(board.memory.percent));
    // A failed round keeps the report present with frozen levels and a
    // grown failure count: the local view stays complete.
    let failed = gate.fail();
    manager.set_pressure_state(&failed);
    let failed_report = adxlet::resources::report_from(&failed.state).unwrap();
    assert_eq!(failed_report.consecutive_failures, 1);
    assert_eq!(
        failed_report.observed_at_unix_seconds,
        board.observed_at_unix_seconds
    );
}

fn gauge_value(text: &str, series: &str) -> u64 {
    text.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|token| token.parse().ok())
        .unwrap_or_else(|| panic!("series {series} missing from metrics text"))
}

/// The gauge family exists exactly when pressure sampling is configured:
/// an unconfigured node omits every pressure gauge (the skip counters are
/// event counters with startup baselines and stay), while a configured node
/// exports the full family even before the first successful sample.
#[tokio::test]
async fn pressure_gauges_exist_exactly_when_sampling_is_configured() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    let unconfigured = manager.metrics();
    assert!(
        !unconfigured.contains("adx_node_pressure_level"),
        "{unconfigured}"
    );
    assert!(
        !unconfigured.contains("adx_node_pressure_observed_at_unix_seconds"),
        "{unconfigured}"
    );
    manager.mark_pressure_configured();
    let configured = manager.metrics();
    for series in [
        "adx_node_pressure_level{resource=\"memory\"}",
        "adx_node_pressure_level{resource=\"disk\"}",
        "adx_node_pressure_percent{resource=\"memory\"}",
        "adx_node_pressure_percent{resource=\"disk\"}",
        "adx_node_pressure_last_transition_unix_seconds{resource=\"memory\"}",
        "adx_node_pressure_last_transition_unix_seconds{resource=\"disk\"}",
        "adx_node_pressure_observed_at_unix_seconds",
        "adx_node_pressure_consecutive_failures",
    ] {
        assert!(configured.contains(series), "{configured}");
    }
}

/// The gauges report the board values: a high memory round shows up as
/// level 1 with its percent, a failing round grows the failure gauge while
/// the frozen level stays high.
#[tokio::test]
async fn pressure_gauges_report_the_board_values() {
    let manager = node();
    manager
        .update_capacity(Resources::default(), Duration::from_secs(300))
        .expect("capacity update succeeds");
    manager.mark_pressure_configured();
    let mut gate = gate();
    let pressured = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    manager.set_pressure_state(&pressured);
    let text = manager.metrics();
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_level{resource=\"memory\"}"),
        1
    );
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_level{resource=\"disk\"}"),
        0
    );
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_percent{resource=\"memory\"}"),
        95
    );
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_percent{resource=\"disk\"}"),
        10
    );
    let observed_at = gauge_value(&text, "adx_node_pressure_observed_at_unix_seconds");
    assert_eq!(
        observed_at,
        manager.pressure_state().observed_at_unix_seconds
    );
    let transition = gauge_value(
        &text,
        "adx_node_pressure_last_transition_unix_seconds{resource=\"memory\"}",
    );
    assert!(transition > 0);
    assert_eq!(
        gauge_value(
            &text,
            "adx_node_pressure_last_transition_unix_seconds{resource=\"disk\"}"
        ),
        0
    );
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_consecutive_failures"),
        0
    );
    // A failed round freezes the levels and grows the failure count.
    let failed = gate.fail();
    manager.set_pressure_state(&failed);
    let text = manager.metrics();
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_consecutive_failures"),
        1
    );
    assert_eq!(
        gauge_value(&text, "adx_node_pressure_level{resource=\"memory\"}"),
        1
    );
}

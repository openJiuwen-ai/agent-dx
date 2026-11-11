//! Admission control for pressure-triggered pauses: the rate and in-flight
//! ledger plus its static options. Sampling state lives on the pressure
//! board instead of here.
use std::collections::BTreeMap;

/// Static admission configuration for pressure-triggered pauses. Sampling
/// state flows through the pressure board instead; these fields are
/// lifetime-fixed facts.
#[derive(Clone, Debug)]
pub struct PressurePauseOptions {
    /// Master switch: pressure pauses are only considered when enabled.
    pub enabled: bool,
    /// Minimum idle-window age before a pressure pause may trigger.
    pub min_idle_seconds: u64,
    /// Maximum age of the pressure observation the decision may rely on.
    pub freshness_seconds: u64,
    /// Pressure-pause starts allowed per observation window.
    pub budget: u32,
    /// Concurrent pressure pauses allowed in flight across windows.
    pub in_flight_cap: usize,
    /// Scope of the checkpoint-storage escalation attempted on pause.
    pub escalate_checkpoint_storage: EscalationMode,
}
impl Default for PressurePauseOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            min_idle_seconds: 300,
            freshness_seconds: 90,
            budget: 1,
            in_flight_cap: 1,
            escalate_checkpoint_storage: EscalationMode::Off,
        }
    }
}

/// Scope of the checkpoint-storage escalation attempted by pressure pauses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EscalationMode {
    /// Mechanism only: every pressure pause keeps the Spec-resolved storage.
    #[default]
    Off,
    /// Escalate only when the Spec already asks for a durable location.
    ConfiguredOnly,
    /// Escalate every pause whose node can accept shared storage.
    All,
}

/// Claims older than this are reclaimed as a defensive fuse: no pause may
/// legitimately stay in flight past the checkpoint timeout ceiling plus a
/// reporting-interval margin.
const STALE_IN_FLIGHT_SECONDS: u64 = 3_660;

/// Outcome of claiming one admission slot. The denial cause is reported so
/// the skip counter can distinguish an exhausted budget from an exhausted
/// in-flight cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    Granted,
    BudgetExhausted,
    InFlightExhausted,
}

/// Rate limit (budget per observation window) plus in-flight cap for
/// pressure-triggered pauses. `used` measures starts per window and is not
/// rolled back on failure; `in_flight` measures concurrently awaited
/// pauses and is always released on completion.
#[derive(Debug, Default)]
pub struct PressurePauseLedger {
    window_key: u64,
    used: u32,
    in_flight: BTreeMap<String, u64>,
}
impl PressurePauseLedger {
    /// Claims one admission slot for `env_id`. The window key is the board's
    /// `observed_at_unix_seconds`; a new window resets `used` while
    /// `in_flight` persists across windows. A denial reports which limit was
    /// exhausted; the budget check keeps its historical short-circuit
    /// priority.
    pub fn claim(
        &mut self,
        window_key: u64,
        env_id: &str,
        budget: u32,
        cap: usize,
        now: u64,
    ) -> ClaimOutcome {
        if self.window_key != window_key {
            self.window_key = window_key;
            self.used = 0;
        }
        self.reclaim_stale(now);
        if self.used >= budget {
            return ClaimOutcome::BudgetExhausted;
        }
        if self.in_flight.len() >= cap {
            return ClaimOutcome::InFlightExhausted;
        }
        self.used += 1;
        self.in_flight.insert(env_id.into(), now);
        ClaimOutcome::Granted
    }
    /// Releases the in-flight entry for one env after its pause completed
    /// (successfully or not).
    pub fn release(&mut self, env_id: &str) {
        self.in_flight.remove(env_id);
    }
    fn reclaim_stale(&mut self, now: u64) {
        self.in_flight
            .retain(|_, claimed| now.saturating_sub(*claimed) <= STALE_IN_FLIGHT_SECONDS);
    }
    #[cfg(test)]
    fn in_flight_of(&self, env_id: &str) -> Option<u64> {
        self.in_flight.get(env_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_limits_claims_per_observation_window() {
        let mut ledger = PressurePauseLedger::default();
        assert_eq!(ledger.claim(100, "env-a", 1, 2, 100), ClaimOutcome::Granted);
        assert_eq!(
            ledger.claim(100, "env-b", 1, 2, 100),
            ClaimOutcome::BudgetExhausted,
            "second claim in the same window is rejected"
        );
    }

    #[test]
    fn new_window_resets_used_but_keeps_in_flight() {
        let mut ledger = PressurePauseLedger::default();
        assert_eq!(ledger.claim(100, "env-a", 1, 2, 100), ClaimOutcome::Granted);
        // The used counter resets with the window: the fresh window admits
        // a claim even though the previous window's budget was spent.
        assert_eq!(ledger.claim(200, "env-b", 1, 2, 200), ClaimOutcome::Granted);
        // The in-flight entry does not reset with the window.
        assert!(ledger.in_flight_of("env-a").is_some());
        ledger.release("env-a");
        ledger.release("env-b");
    }

    #[test]
    fn failed_pause_keeps_the_rate_slot_consumed() {
        let mut ledger = PressurePauseLedger::default();
        assert_eq!(ledger.claim(100, "env-a", 1, 2, 100), ClaimOutcome::Granted);
        // A failed auto_pause: the claim is released (not in flight any more)
        // but the budget slot stays consumed.
        ledger.release("env-a");
        assert_eq!(
            ledger.claim(100, "env-c", 1, 2, 100),
            ClaimOutcome::BudgetExhausted,
            "the window budget is not rolled back after a failure"
        );
    }

    #[test]
    fn in_flight_cap_blocks_parallel_claims_independently_of_budget() {
        let mut ledger = PressurePauseLedger::default();
        assert_eq!(ledger.claim(100, "env-a", 2, 1, 100), ClaimOutcome::Granted);
        // Budget 2 with used 1: the denial names the in-flight cap, not the
        // budget.
        assert_eq!(
            ledger.claim(100, "env-b", 2, 1, 100),
            ClaimOutcome::InFlightExhausted,
            "cap=1 blocks a second claim while the first is in flight"
        );
        ledger.release("env-a");
        assert_eq!(ledger.claim(100, "env-b", 2, 1, 100), ClaimOutcome::Granted);
    }

    #[test]
    fn stale_in_flight_claims_are_reclaimed_as_a_fuse() {
        let mut ledger = PressurePauseLedger::default();
        assert_eq!(ledger.claim(100, "env-a", 1, 1, 100), ClaimOutcome::Granted);
        // Claimed at 100; at 100 + STALE_IN_FLIGHT_SECONDS + 1 the entry is
        // reclaimed before evaluating the next claim.
        let now = 100 + STALE_IN_FLIGHT_SECONDS + 1;
        assert_eq!(ledger.claim(200, "env-b", 1, 1, now), ClaimOutcome::Granted);
        assert!(ledger.in_flight_of("env-b").is_some());
        assert!(ledger.in_flight_of("env-a").is_none());
    }

    #[test]
    fn default_options_keep_pressure_pause_disabled() {
        let options = PressurePauseOptions::default();
        assert!(!options.enabled);
        assert_eq!(options.escalate_checkpoint_storage, EscalationMode::Off);
    }
}

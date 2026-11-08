//! Who deletes an external generation, and handing it over (S4-9).
use std::{collections::BTreeSet, sync::Arc};

use crate::dfs::GcState;

/// How long the single external deleter has held its generation (S4-9).
///
/// An external object has one copy that any Node can delete, so a deleter
/// that stays registered but never finishes (its object store hangs) must not
/// hold collection forever: past the deadline the next live participant that
/// calls takes the generation over. Deleting an object twice is harmless; its
/// metadata left with the plan. Process-local: a Meta restart restarts it.
#[derive(Clone)]
pub(crate) struct DeleterWatch {
    handoff: std::time::Duration,
    held: Arc<std::sync::Mutex<Option<(u64, String, std::time::Instant)>>>,
    /// A deleter that lost a generation to the handoff, and that generation.
    /// Later generations and listing skip it until it reports that generation
    /// done, which proves it is deleting again.
    overdue: Arc<std::sync::Mutex<Option<(String, u64)>>>,
}

/// A batch of up to 512 deletions; well past a healthy store's latency.
const GC_DELETER_HANDOFF: std::time::Duration = std::time::Duration::from_secs(120);

impl Default for DeleterWatch {
    fn default() -> Self {
        Self::new(GC_DELETER_HANDOFF)
    }
}

impl DeleterWatch {
    pub(super) fn new(handoff: std::time::Duration) -> Self {
        Self {
            handoff,
            held: Arc::default(),
            overdue: Arc::default(),
        }
    }

    /// The deleter (and lister) of the next generation: the first live
    /// participant, passing over one that just lost a generation.
    pub(super) fn choose(&self, participants: &BTreeSet<String>) -> Option<String> {
        let overdue = self
            .overdue
            .lock()
            .ok()
            .and_then(|value| value.as_ref().map(|(node, _)| node.clone()));
        participants
            .iter()
            .find(|node| overdue.as_ref() != Some(*node))
            .or_else(|| participants.first())
            .cloned()
    }

    /// `node` reported `completed`; past the generation it lost, it is
    /// deleting again.
    pub(super) fn finished(&self, node: &str, completed: u64) {
        if let Ok(mut overdue) = self.overdue.lock()
            && overdue
                .as_ref()
                .is_some_and(|(value, generation)| value == node && completed >= *generation)
        {
            *overdue = None;
        }
    }

    /// Moves an overdue generation to `caller`; true if it did.
    pub(super) fn hand_over(
        &self,
        gc: &mut GcState,
        caller: &str,
        participants: &BTreeSet<String>,
    ) -> bool {
        let [deleter] = gc.waiting_nodes.as_slice() else {
            return false;
        };
        let Ok(mut held) = self.held.lock() else {
            return false;
        };
        let now = std::time::Instant::now();
        let since = match held.as_ref() {
            Some((generation, node, since)) if *generation == gc.generation && node == deleter => {
                *since
            }
            _ => {
                *held = Some((gc.generation, deleter.clone(), now));
                now
            }
        };
        if deleter == caller
            || !participants.contains(caller)
            || now.duration_since(since) < self.handoff
        {
            return false;
        }
        afs_logging::warn!("dfs.gc_deleter_handoff"; "generation" => gc.generation,
            "from" => deleter.as_str(), "to" => caller,
            "held_seconds" => now.duration_since(since).as_secs());
        if let Ok(mut overdue) = self.overdue.lock() {
            *overdue = Some((deleter.clone(), gc.generation));
        }
        gc.waiting_nodes = vec![caller.to_owned()];
        *held = Some((gc.generation, caller.to_owned(), now));
        true
    }
}

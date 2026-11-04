//! Watch history index accounting, preparation and application.

use super::*;

impl State {
    pub(in crate::meta::store) fn history_stats(&self) -> super::super::history::Retained {
        self.history.stats()
    }

    pub(in crate::meta::store) fn ensure_history_index(&mut self) -> Result<()> {
        if !self.history_indexed {
            self.rebuild_history_index()?;
        }
        Ok(())
    }

    pub(in crate::meta::store) fn rebuild_history_index(&mut self) -> Result<()> {
        use super::super::history;
        self.ensure_authority_index()?;
        if self.request_completion.len() != self.requests.len() {
            return Err(invalid_contract(
                "history outcome completion coverage mismatch",
            ));
        }
        let mut command_history = im::OrdMap::new();
        let mut command_history_bytes = 0;
        let mut index = history::Index::default();
        for (key, outcome) in self.requests.iter() {
            let completion = self
                .request_completion
                .get(key)
                .ok_or_else(|| invalid_contract("history request completion missing"))?;
            if key != &outcome.request
                || completion.revision == StoreRevision::ZERO
                || completion.revision > self.revision
            {
                return Err(invalid_contract(
                    "history original request identity/revision invalid",
                ));
            }
            account_command_result(
                &mut command_history,
                &mut command_history_bytes,
                &outcome.result,
                true,
            )?;
            index.track_request(
                key.clone(),
                completion.clone(),
                history::request_charge(outcome)?,
            )?;
        }
        let mut events = self.events.iter().peekable();
        let mut previous = self.compacted_to;
        while let Some(event) = events.peek() {
            let revision = event.revision;
            if revision <= previous || revision > self.revision {
                return Err(invalid_contract("history event revision order invalid"));
            }
            let group = std::iter::from_fn(|| events.next_if(|event| event.revision == revision));
            let (count, bytes) = history::events_charge(group)?;
            index.track_events(
                revision,
                self.event_completion.get(&revision).copied(),
                count,
                bytes,
            )?;
            previous = revision;
        }
        self.history = index;
        self.command_history = command_history;
        self.command_history_bytes = command_history_bytes;
        self.history_indexed = true;
        Ok(())
    }

    pub(super) fn account_history_changes(
        &mut self,
        previous: &Self,
        keys: impl Iterator<Item = RequestKey>,
        events_from: usize,
        completed_at: u64,
    ) -> Result<()> {
        use super::super::history;
        for key in keys {
            let outcome = self
                .requests
                .get(&key)
                .ok_or_else(|| invalid_contract("history new outcome missing"))?;
            let completion = self
                .request_completion
                .get(&key)
                .ok_or_else(|| invalid_contract("history new completion missing"))?;
            if let Some(old) = previous.requests.get(&key) {
                account_command_result(
                    &mut self.command_history,
                    &mut self.command_history_bytes,
                    &old.result,
                    false,
                )?;
            }
            account_command_result(
                &mut self.command_history,
                &mut self.command_history_bytes,
                &outcome.result,
                true,
            )?;
            self.history.track_request(
                key,
                completion.clone(),
                history::request_charge(outcome)?,
            )?;
        }
        if events_from > self.events.len() {
            return Err(invalid_contract("history new event range is invalid"));
        }
        // im::Vector's iterator has no indexed nth; skip would walk the entire
        // old history on every small commit. Visit only this decision's tail.
        let (count, bytes) = history::events_charge(
            (events_from..self.events.len()).map(|index| self.events.0[index].as_ref()),
        )?;
        if count > 0 {
            self.history
                .track_events(self.revision, Some(completed_at), count, bytes)?;
        }
        self.history_indexed = true;
        Ok(())
    }

    pub(in crate::meta::store) fn prepare_history(
        &self,
        now: u64,
        limits: super::super::history::Limits,
        protected: &std::collections::BTreeSet<RequestKey>,
    ) -> Result<Option<(Self, super::super::history::Delta)>> {
        // Before the first maintenance tick, legacy outcomes may be the last
        // surviving evidence of a used epoch. Capture that floor durably before
        // retention can retire it. Normal resource admission remains unchanged.
        if !self.entities.contains_key(&MetaKey::RootEpochWatermark)
            && self.root_epoch_watermark() != 0
        {
            return Ok(None);
        }
        let mut prepared = self.clone();
        prepared.ensure_history_index()?;
        let Some(delta) = prepared.history.select(
            prepared.revision,
            prepared.compacted_to,
            now,
            limits,
            protected,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((prepared.apply_history(&delta)?, delta)))
    }

    pub(in crate::meta::store) fn history_fits(
        &self,
        limits: super::super::history::Limits,
    ) -> bool {
        self.history_indexed && self.history.fits(limits)
    }

    pub(super) fn next_node_event_index(&self) -> Result<u32> {
        self.events_removed
            .checked_add(self.events.len() as u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| invalid_contract("node event index overflow"))
    }

    pub(in crate::meta::store) fn apply_history(
        &self,
        history: &super::super::history::Delta,
    ) -> Result<Self> {
        history.validate()?;
        if history.revision != self.revision || history.compacted_from != self.compacted_to {
            return Err(invalid_contract(
                "history cleanup does not follow published revision/watermark",
            ));
        }
        for request in &history.requests {
            if !self.requests.contains_key(&request.key)
                || self.request_completion.get(&request.key) != Some(&request.completion)
            {
                return Err(invalid_contract(
                    "history cleanup request generation mismatch",
                ));
            }
        }
        // Validate the exact complete prefix before changing this private view.
        // The persisted watermark, not this process's clock, chooses removals.
        let mut removed = 0usize;
        let mut last = self.compacted_to;
        for event in self.events.iter() {
            if event.revision <= self.compacted_to {
                return Err(invalid_contract(
                    "retained event is below compaction watermark",
                ));
            }
            if event.revision > history.compacted_to {
                break;
            }
            removed = removed
                .checked_add(1)
                .ok_or_else(|| invalid_contract("history event removal count overflow"))?;
            last = event.revision;
        }
        if last != history.compacted_to {
            return Err(invalid_contract(
                "history cleanup watermark is not a retained revision boundary",
            ));
        }
        let events_removed = self
            .events_removed
            .checked_add(removed as u64)
            .ok_or_else(|| invalid_contract("history event ordinal overflow"))?;
        let mut next = self.clone();
        next.ensure_history_index()?;
        next.history = next
            .history
            .apply(history, self.revision, self.compacted_to)?;
        for request in &history.requests {
            let outcome = next
                .requests
                .get(&request.key)
                .ok_or_else(|| invalid_contract("retiring command outcome missing"))?;
            account_command_result(
                &mut next.command_history,
                &mut next.command_history_bytes,
                &outcome.result,
                false,
            )?;
            next.requests.remove(&request.key);
            next.request_completion.remove(&request.key);
        }
        for _ in 0..removed {
            next.events.0.pop_front();
        }
        while let Some(revision) = next
            .event_completion
            .iter()
            .next()
            .map(|(revision, _)| *revision)
        {
            if revision > history.compacted_to {
                break;
            }
            next.event_completion.remove(&revision);
        }
        next.events_removed = events_removed;
        next.compacted_to = history.compacted_to;
        Ok(next)
    }
}

use crate::*;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

/// Explicit test fixture, never selected automatically after a Redis failure.
#[derive(Default)]
pub struct MemoryRepository {
    state: Mutex<MemoryState>,
}
#[derive(Default)]
struct MemoryState {
    records: BTreeMap<Key, Record>,
    indexes: BTreeMap<Index, BTreeSet<String>>,
}

#[async_trait]
impl Repository for MemoryRepository {
    async fn get(&self, key: &Key) -> Result<Option<Record>> {
        Ok(self.state.lock().await.records.get(key).cloned())
    }
    async fn commit(&self, tx: &Transaction) -> Result<bool> {
        let mut state = self.state.lock().await;
        for check in &tx.checks {
            if state.records.get(&check.key).map(|r| &r.revision) != check.expected.as_ref() {
                return Ok(false);
            }
        }
        for put in &tx.puts {
            state.records.insert(put.key.clone(), put.record.clone());
            if let Some(index) = &put.key.1 {
                state
                    .indexes
                    .entry(index.clone())
                    .or_default()
                    .insert(put.key.0.clone());
            }
        }
        for key in &tx.deletes {
            state.records.remove(key);
            if let Some(index) = &key.1 {
                if let Some(members) = state.indexes.get_mut(index) {
                    members.remove(&key.0);
                }
            }
        }
        Ok(true)
    }
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Record)>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.state.lock().await;
        let Some(members) = state.indexes.get(index) else {
            return Ok(Vec::new());
        };
        let start = after.map_or(Unbounded, |v| Excluded(v.to_owned()));
        members
            .range((start, Unbounded))
            .take(limit)
            .map(|member| {
                state
                    .records
                    .get(&Key(member.clone(), Some(index.clone())))
                    .cloned()
                    .map(|record| (member.clone(), record))
                    .ok_or_else(|| Error::Corrupt("Environment index member missing".into()))
            })
            .collect()
    }
}

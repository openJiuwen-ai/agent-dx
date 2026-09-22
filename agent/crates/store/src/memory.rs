use crate::*;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

/// Explicit test fixture, never selected automatically after a Redis failure.
#[derive(Default)]
pub struct MemoryRepository {
    records: Mutex<BTreeMap<Key, Record>>,
}

#[async_trait]
impl Repository for MemoryRepository {
    async fn get(&self, key: &Key) -> Result<Option<Record>> {
        Ok(self.records.lock().await.get(key).cloned())
    }
    async fn commit(&self, tx: &Transaction) -> Result<bool> {
        let mut records = self.records.lock().await;
        for check in &tx.checks {
            if records.get(&check.key).map(|r| &r.revision) != check.expected.as_ref() {
                return Ok(false);
            }
        }
        for put in &tx.puts {
            records.insert(put.key.clone(), put.record.clone());
        }
        for key in &tx.deletes {
            records.remove(key);
        }
        Ok(true)
    }
}

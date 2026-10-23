//! Process-local operation coordination. The Coordinator catalog owns recovery.
use adx_core::{Error, Result};
use std::{collections::BTreeMap, net::IpAddr, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Phase {
    Creating,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug)]
pub(super) struct Record {
    pub runtime_id: String,
    pub environment_id: String,
    pub tenant_id: String,
    pub generation: u64,
    pub sandbox_id: String,
    pub backend: String,
    pub epoch: String,
    pub fingerprint: String,
    pub phase: Phase,
    pub ip: Option<IpAddr>,
}

#[derive(Clone, Default)]
pub(super) struct State(Arc<Mutex<BTreeMap<String, Record>>>);

impl State {
    pub async fn get(&self, id: &str) -> Result<Option<Record>> {
        Ok(self.0.lock().await.get(id).cloned())
    }
    pub async fn insert(&self, record: Record) -> Result<()> {
        let mut records = self.0.lock().await;
        if records.contains_key(&record.runtime_id)
            || records.values().any(|r| r.sandbox_id == record.sandbox_id)
        {
            return Err(Error::Conflict);
        }
        records.insert(record.runtime_id.clone(), record);
        Ok(())
    }
    pub async fn update(&self, id: &str, change: impl FnOnce(&mut Record)) -> Result<Record> {
        let mut records = self.0.lock().await;
        let record = records.get_mut(id).ok_or(Error::NotFound)?;
        change(record);
        Ok(record.clone())
    }
    pub async fn all(&self) -> Result<BTreeMap<String, Record>> {
        Ok(self.0.lock().await.clone())
    }
}

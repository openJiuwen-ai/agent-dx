//! Current-state storage. Conditional reads and writes form one atomic transaction.
//! Production has no memory fallback. Redis Cluster cross-slot transactions are unsupported.
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, fmt};

mod redis_store;
pub use redis_store::RedisRepository;
mod state;
pub use state::AgentState;
#[cfg(feature = "test-memory")]
mod memory;
#[cfg(feature = "test-memory")]
pub use memory::MemoryRepository;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid(String),
    Conflict(String),
    Corrupt(String),
    Unavailable(String),
    /// A write may have committed. Read the same identities/revisions before retrying.
    OutcomeUnknown(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(String, Option<Index>);

/// Derived ordered index; updated with its records in the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Index(String);
impl Index {
    pub fn environments(tenant: &str, template: &str, version: &str) -> Result<Self> {
        for value in [tenant, template, version] {
            adx_agent_core::identifier(value, "index scope").map_err(Error::Invalid)?;
        }
        Ok(Self(format!(
            "environments:{}",
            adx_agent_core::encode_key(&[tenant, template, version])
        )))
    }
}
impl Key {
    pub fn new(kind: &str, parts: &[&str]) -> Result<Self> {
        if !matches!(kind, "template" | "environment")
            || parts.is_empty()
            || parts.iter().any(|s| s.is_empty() || s.len() > 2048)
        {
            return Err(Error::Invalid("invalid record key".into()));
        }
        let index = if kind == "environment" && parts.len() == 4 {
            Some(Index::environments(parts[0], parts[1], parts[2])?)
        } else {
            None
        };
        Ok(Self(
            format!("{kind}:{}", adx_agent_core::encode_key(parts)),
            index,
        ))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque client-generated revisions avoid ABA and Lua's double integer precision limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision(String);
impl Default for Revision {
    fn default() -> Self {
        Self::new()
    }
}
impl Revision {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub revision: Revision,
    pub value: Value,
}
impl Record {
    pub fn new<T: Serialize>(value: &T) -> Result<Self> {
        Ok(Self {
            revision: Revision::new(),
            value: serde_json::to_value(value).map_err(|e| Error::Invalid(e.to_string()))?,
        })
    }
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.value.clone()).map_err(|e| Error::Corrupt(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub key: Key,
    pub expected: Option<Revision>,
}
#[derive(Debug, Clone)]
pub struct Put {
    pub key: Key,
    pub record: Record,
}

/// Every written key must be checked, and every new revision differs from its predecessor.
/// Read-only checks fence concurrent product lifecycle changes.
#[derive(Debug, Clone)]
pub struct Transaction {
    checks: Vec<Check>,
    puts: Vec<Put>,
    deletes: Vec<Key>,
}
impl Transaction {
    pub fn new(checks: Vec<Check>, puts: Vec<Put>) -> Result<Self> {
        Self::with_deletes(checks, puts, Vec::new())
    }
    pub fn with_deletes(checks: Vec<Check>, puts: Vec<Put>, deletes: Vec<Key>) -> Result<Self> {
        if checks.is_empty()
            || (puts.is_empty() && deletes.is_empty())
            || checks.len() > adx_agent_core::limits::TRANSACTION_CHECKS
        {
            return Err(Error::Invalid(
                "transaction requires 1..128 checks and at least one write".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for check in &checks {
            if !seen.insert(&check.key) {
                return Err(Error::Invalid("duplicate check".into()));
            }
        }
        seen.clear();
        for put in &puts {
            if !seen.insert(&put.key) {
                return Err(Error::Invalid("duplicate write".into()));
            }
            let check = checks
                .iter()
                .find(|c| c.key == put.key)
                .ok_or_else(|| Error::Invalid("unchecked write".into()))?;
            if check.expected.as_ref() == Some(&put.record.revision) {
                return Err(Error::Invalid("new revision must differ".into()));
            }
        }
        for key in &deletes {
            if !seen.insert(key) || !checks.iter().any(|c| &c.key == key && c.expected.is_some()) {
                return Err(Error::Invalid(
                    "deletion requires a unique checked existing key".into(),
                ));
            }
        }
        Ok(Self {
            checks,
            puts,
            deletes,
        })
    }
}

#[async_trait]
pub trait Repository: Send + Sync {
    async fn get(&self, key: &Key) -> Result<Option<Record>>;
    /// False means a definite conflict with no writes. Errors may mean unknown outcome.
    async fn commit(&self, transaction: &Transaction) -> Result<bool>;
    /// Exclusive lexicographic cursor. Reads at most `limit` records from one index.
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Record)>>;
}

impl From<Error> for adx_agent_core::error::Error {
    fn from(value: Error) -> Self {
        use Error as E;
        match value {
            E::Invalid(s) => Self::Invalid(s),
            E::Conflict(s) => Self::Conflict(s),
            E::Unavailable(s) => Self::Unavailable(s),
            E::OutcomeUnknown(s) => Self::OutcomeUnknown(s),
            E::Corrupt(_) => Self::Unavailable("invalid stored Agent state".into()),
        }
    }
}

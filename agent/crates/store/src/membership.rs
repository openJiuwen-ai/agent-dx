//! Dispatcher discovery leases are not Session ownership or autoscaling leases.
use crate::*;
use std::time::Duration;

pub use adx_agent_core::routing::DispatcherMember;

fn validate_member(member: &DispatcherMember) -> Result<()> {
    if member.node_id.is_empty()
        || member.node_id.len() > adx_agent_core::limits::IDENTIFIER_BYTES
        || uuid::Uuid::parse_str(&member.boot_id).is_err()
        || !(member.address.starts_with("http://") || member.address.starts_with("https://"))
        || member.address.contains(['\r', '\n', '\0'])
    {
        return Err(Error::Invalid(
            "invalid Dispatcher identity or address".into(),
        ));
    }
    Ok(())
}

const REGISTER: &str = r#"
local old = redis.call('GET', KEYS[1])
if old and old ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
"#;
const RENEW: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"#;
const REMOVE: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"#;

impl RedisRepository {
    async fn membership_write(
        &self,
        script: &str,
        member: &DispatcherMember,
        ttl: Duration,
    ) -> Result<bool> {
        validate_member(member)?;
        if ttl.as_millis() == 0 || ttl.as_millis() > 300_000 {
            return Err(Error::Invalid(
                "membership TTL must be positive and at most 5 minutes".into(),
            ));
        }
        let key = format!(
            "{}dispatcher:{}",
            self.prefix,
            adx_agent_core::encode_key(&[&member.node_id])
        );
        let value = serde_json::to_string(member).map_err(|e| Error::Invalid(e.to_string()))?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(script)
            .arg(1)
            .arg(key)
            .arg(value)
            .arg(ttl.as_millis() as u64);
        let result: i64 = self.execute(command, true).await?;
        Ok(result == 1)
    }

    /// A different boot cannot replace a still-live registration with the same node ID.
    pub async fn register_dispatcher(
        &self,
        member: &DispatcherMember,
        ttl: Duration,
    ) -> Result<bool> {
        self.membership_write(REGISTER, member, ttl).await
    }
    pub async fn renew_dispatcher(&self, member: &DispatcherMember, ttl: Duration) -> Result<bool> {
        self.membership_write(RENEW, member, ttl).await
    }
    pub async fn unregister_dispatcher(&self, member: &DispatcherMember) -> Result<bool> {
        self.membership_write(REMOVE, member, Duration::from_secs(1))
            .await
    }

    /// Rebuildable snapshot using current TTL keys, never a persisted membership index.
    /// Callers refresh periodically and after routing failures, including TTL expiry.
    pub async fn dispatchers(&self) -> Result<Vec<DispatcherMember>> {
        let mut cursor = 0u64;
        let mut found = std::collections::BTreeMap::new();
        loop {
            let mut command = redis::cmd("SCAN");
            command
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}dispatcher:*", self.prefix))
                .arg("COUNT")
                .arg(100);
            let (next, keys): (u64, Vec<String>) = self.execute(command, false).await?;
            if !keys.is_empty() {
                let mut command = redis::cmd("MGET");
                command.arg(&keys);
                let values: Vec<Option<String>> = self.execute(command, false).await?;
                for value in values.into_iter().flatten() {
                    let member: DispatcherMember =
                        serde_json::from_str(&value).map_err(|e| Error::Corrupt(e.to_string()))?;
                    validate_member(&member)?;
                    found.insert(member.node_id.clone(), member);
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(found.into_values().collect())
    }
}

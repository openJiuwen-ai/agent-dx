//! Credential metadata and permanent revocation tombstones. Bootstrap replay
//! cannot resurrect a key explicitly removed by an administrator.
use super::*;
use crate::auth::Credential;

impl Session {
    pub async fn list_credentials(&self) -> Result<BTreeMap<String, Credential>> {
        let header = self.store.fields(&[HEADER.into()]).await?;
        self.header(&header[0])?;
        let mut command = redis::cmd("EVAL");
        command.arg("if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return false end; return redis.call('HGETALL', KEYS[2])")
            .arg(2).arg(&self.store.key).arg(format!("{}:credentials", self.store.key))
            .arg(header[0].as_deref().ok_or(Error::Conflict)?);
        let values: Option<BTreeMap<String, String>> = self.store.query(command).await?;
        values
            .ok_or(Error::Conflict)?
            .into_iter()
            .map(|(id, value)| {
                let credential: Credential = decode(&value)?;
                credential.validate()?;
                Ok((id, credential))
            })
            .collect()
    }

    pub async fn revoke_credential(&self, id: &str) -> Result<()> {
        if id.len() != 64 || !id.bytes().all(|v| v.is_ascii_hexdigit()) {
            return Err(Error::Invalid("invalid credential ID".into()));
        }
        let header = self.store.fields(&[HEADER.into()]).await?;
        self.header(&header[0])?;
        // Compare the inspected opaque record as well as the Master header.
        // Administrator bootstrap credentials are managed by deployment config.
        let credential = match self.credential(id).await {
            Ok(c) if c.administrator => {
                return Err(Error::Invalid("only tenant keys can be revoked".into()))
            }
            Ok(c) => Some(c),
            Err(Error::NotFound) => None,
            Err(e) => return Err(e),
        };
        let encoded = credential
            .map(|c| serde_json::to_string(&c))
            .transpose()
            .map_err(|_| Error::Invalid("credential encoding failed".into()))?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            local old = redis.call('HGET', KEYS[2], ARGV[2])
            if not old then return 1 end
            if old ~= ARGV[3] then return 0 end
            redis.call('HSET', KEYS[3], ARGV[2], 'revoked')
            redis.call('HDEL', KEYS[2], ARGV[2])
            return 1
        "#,
            )
            .arg(3)
            .arg(&self.store.key)
            .arg(format!("{}:credentials", self.store.key))
            .arg(format!("{}:revoked-credentials", self.store.key))
            .arg(header[0].as_deref().ok_or(Error::Conflict)?)
            .arg(id)
            .arg(encoded.unwrap_or_default());
        let accepted: u64 = self.store.query(command).await?;
        if accepted != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

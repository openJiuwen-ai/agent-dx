//! Credential metadata and permanent revocation tombstones. Bootstrap replay
//! cannot resurrect a key explicitly removed by an administrator.
use super::*;
use crate::auth::Credential;

impl Session {
    /// Reconcile the deployment's complete administrator key set atomically.
    /// Removed keys are permanently revoked. Invalid sets, revoked keys and
    /// concurrent credential changes fail without changing the active keys.
    pub async fn reconcile_administrators(&self, keys: &[(String, Credential)]) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Unavailable("clock unavailable".into()))?
            .as_secs();
        let mut desired = BTreeMap::new();
        for (key, credential) in keys {
            credential.validate()?;
            if !credential.administrator
                || (credential.expires_at_unix_seconds != 0
                    && credential.expires_at_unix_seconds <= now)
            {
                return Err(Error::Invalid(
                    "active administrator credential required".into(),
                ));
            }
            let digest = crate::auth::digest(key)?;
            if desired.insert(digest, encode(credential)?).is_some() {
                return Err(Error::Invalid("duplicate administrator key".into()));
            }
        }
        if desired.is_empty() {
            return Err(Error::Invalid(
                "at least one administrator key required".into(),
            ));
        }
        let current = self.list_credentials().await?;
        let mut expected = BTreeMap::new();
        let mut removed = Vec::new();
        for (id, credential) in current {
            if desired.contains_key(&id) && !credential.administrator {
                return Err(Error::Conflict);
            }
            if credential.administrator && !desired.contains_key(&id) {
                removed.push(id.clone());
            }
            expected.insert(id, encode(&credential)?);
        }
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            local expected = cjson.decode(ARGV[2])
            local desired = cjson.decode(ARGV[3])
            local removed = cjson.decode(ARGV[4])
            local count = 0
            for id, value in pairs(expected) do
                count = count + 1
                if redis.call('HGET', KEYS[2], id) ~= value then return 0 end
            end
            if redis.call('HLEN', KEYS[2]) ~= count then return 0 end
            for id, _ in pairs(desired) do
                if redis.call('HEXISTS', KEYS[3], id) == 1 then return 2 end
            end
            for _, id in ipairs(removed) do
                redis.call('HSET', KEYS[3], id, 'revoked')
                redis.call('HDEL', KEYS[2], id)
            end
            for id, value in pairs(desired) do
                redis.call('HSET', KEYS[2], id, value)
            end
            return 1
        "#,
            )
            .arg(3)
            .arg(&self.store.key)
            .arg(format!("{}:credentials", self.store.key))
            .arg(format!("{}:revoked-credentials", self.store.key))
            .arg(header_value.as_deref().ok_or(Error::Conflict)?)
            .arg(encode(&expected)?)
            .arg(encode(&desired)?)
            .arg(encode(&removed)?);
        match self.store.query::<u64>(command).await? {
            1 => Ok(()),
            2 => Err(Error::Invalid(
                "revoked administrator key cannot be reused".into(),
            )),
            _ => Err(Error::Conflict),
        }
    }

    pub async fn list_credentials(&self) -> Result<BTreeMap<String, Credential>> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let expected_header = header_value.as_deref().ok_or(Error::Conflict)?;
        let mut command = redis::cmd("EVAL");
        command.arg("if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return false end; return redis.call('HGETALL', KEYS[2])")
            .arg(2).arg(&self.store.key).arg(format!("{}:credentials", self.store.key))
            .arg(expected_header);
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
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let expected_header = header_value.as_deref().ok_or(Error::Conflict)?;
        // Compare the inspected opaque record as well as the Coordinator header.
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
            .arg(expected_header)
            .arg(id)
            .arg(encoded.unwrap_or_default());
        let accepted: u64 = self.store.query(command).await?;
        if accepted != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

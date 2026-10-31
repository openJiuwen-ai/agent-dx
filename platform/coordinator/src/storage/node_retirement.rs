//! Retire empty offline registrations, retaining only registration fencing/policy.
use super::*;

pub(super) fn departure_field(id: &str, session: &str) -> String {
    let digest = Sha256::digest(format!("{}:{id}{session}", id.len()).as_bytes());
    format!("node-departed:{digest:x}")
}

#[derive(Serialize, Deserialize)]
struct RetiredNode {
    shard_id: usize,
    session: Option<NodeSession>,
    scheduling_paused: bool,
    unregistered: bool,
}
impl Session {
    pub async fn departed_node_sessions(
        &self,
    ) -> Result<std::collections::BTreeSet<(String, String)>> {
        let raw = self.store.raw().await?;
        self.header(&raw.get(HEADER).cloned())?;
        raw.iter()
            .filter(|(key, _)| key.starts_with("node-departed:"))
            .map(|(_, value)| decode(value))
            .collect()
    }

    /// Nodes referenced by retained Environments remain until those objects retire.
    /// The membership version and object CAS fence a concurrent assignment/registration.
    pub async fn retire_expired_node(
        &self,
        id: &str,
        session_id: &str,
        now: u64,
        retention: u64,
    ) -> Result<bool> {
        let field = format!("node:{id}");
        let membership = format!("node-ownership:{id}");
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let header = self.header(&raw.get(HEADER).cloned())?;
            let Some(value) = raw.get(&field) else {
                return Ok(false);
            };
            let node: StoredNode = decode(value)?;
            let Some(retirement) = node.retirement.as_ref() else {
                return Ok(false);
            };
            if node
                .session
                .as_ref()
                .is_none_or(|s| s.id != session_id || s.routable)
                || node.node.available
                || (!retirement.unregistered
                    && now.saturating_sub(retirement.since_unix_seconds) < retention)
                || snapshot(&raw)?
                    .environments
                    .values()
                    .any(|e| e.assignment.node_id == id)
            {
                return Ok(false);
            }
            let tombstone = RetiredNode {
                shard_id: node.shard_id,
                session: node.session.clone(),
                scheduling_paused: node.scheduling_paused,
                unregistered: retirement.unregistered,
            };
            let mut counters = CounterUpdate::load(&self.store, field.clone()).await?;
            counters.prepare_retirement(&self.store).await?;
            let mut cmd = redis::cmd("EVAL");
            cmd.arg(
                r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
                if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
                if (redis.call('HGET',KEYS[1],ARGV[4]) or '') ~= ARGV[5] then return 0 end
                local facts=cjson.decode(ARGV[8])
                if (redis.call('HGET',KEYS[2],facts[1]) or '') ~= facts[2] then return 0 end
                if (redis.call('HGET',KEYS[2],facts[4]) or '') ~= facts[5] then return 0 end
                redis.call('HSET',KEYS[2],facts[4],facts[6]); redis.call('HDEL',KEYS[2],facts[1])
                redis.call('HDEL',KEYS[1],ARGV[2],ARGV[4],'node-session:' .. ARGV[6])
                redis.call('HSET',KEYS[1],'retired-node:' .. ARGV[6],ARGV[7]); return 1
            "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(format!("{}:business", self.store.key))
            .arg(header.epoch.to_string())
            .arg(&field)
            .arg(value)
            .arg(&membership)
            .arg(raw.get(&membership).map_or("", String::as_str))
            .arg(id)
            .arg(encode(&tombstone)?)
            .arg(counters.argument()?);
            let write = self
                .store
                .publication_write(self.epoch, vec![field.clone()])
                .await;
            let applied = self.store.query::<u8>(cmd).await? == 1;
            write.finish(applied, vec![PublicationValue::Node(id.into(), None)]);
            if applied {
                return Ok(true);
            }
        }
        Err(Error::Unavailable("node retirement changed; retry".into()))
    }

    pub async fn is_unregistered_node(&self, id: &str, session_id: &str) -> Result<bool> {
        let [header, value] = self
            .store
            .fields([HEADER.into(), format!("retired-node:{id}")])
            .await?;
        self.header(&header)?;
        let retired = value.as_deref().map(decode::<RetiredNode>).transpose()?;
        Ok(retired.is_some_and(|r| {
            r.unregistered && r.session.as_ref().is_some_and(|s| s.id == session_id)
        }))
    }

    pub(super) async fn register_retired_node(
        &self,
        node: &Node,
        address: &str,
        proxy: &str,
        session: Option<&NodeSession>,
        checkpoint_scopes: &BTreeSet<String>,
        expected: &str,
    ) -> Result<Option<StoredNode>> {
        let retired: RetiredNode = decode(expected)?;
        if let Some((old, new)) = retired.session.as_ref().zip(session) {
            if old.id == new.id && (retired.unregistered || new.sequence <= old.sequence) {
                return Err(Error::Conflict);
            }
        }
        let mut node = node.clone();
        node.available &= !retired.scheduling_paused;
        let record = StoredNode {
            retirement: None,
            last_incident_id: None,
            node,
            shard_id: retired.shard_id,
            address: address.into(),
            proxy_address: proxy.into(),
            session: session.cloned(),
            scheduling_paused: retired.scheduling_paused,
            checkpoint_scopes: checkpoint_scopes.clone(),
            // Retirement follows ownership invalidation. A returning node remains
            // fenced until this authenticated process finishes reconciliation.
            checkpoint_gc_fenced: !session.is_some_and(|session| session.routable),
        };
        let id = &record.node.id;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(
            r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            if redis.call('HEXISTS',KEYS[1],'node:' .. ARGV[2]) ~= 0 then return 0 end
            if redis.call('HGET',KEYS[1],'retired-node:' .. ARGV[2]) ~= ARGV[3] then return 0 end
            redis.call('HSET',KEYS[1],'node:' .. ARGV[2],ARGV[4])
            redis.call('HDEL',KEYS[1],'retired-node:' .. ARGV[2])
            if ARGV[5] ~= '' then redis.call('HSET',KEYS[1],'node-session:' .. ARGV[2],ARGV[5]) end
            return 1
        "#,
        )
        .arg(1)
        .arg(&self.store.key)
        .arg(self.epoch.to_string())
        .arg(id)
        .arg(expected)
        .arg(encode(&record)?)
        .arg(session.map_or("", |s| s.id.as_str()));
        let write = self
            .store
            .publication_write(self.epoch, vec![format!("node:{id}")])
            .await;
        let applied = self.store.query::<u8>(cmd).await? == 1;
        write.finish(
            applied,
            vec![PublicationValue::Node(
                id.into(),
                Some(Arc::new(record.clone())),
            )],
        );
        if applied {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}

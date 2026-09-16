//! Persist node expiry and all retired execution results in one Redis script.
use super::*;

impl Session {
    pub async fn invalidate_node(&self, id: &str, session_id: &str) -> Result<StoredSnapshot> {
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let mut saved = snapshot(&raw)?;
            let node = saved.nodes.get_mut(id).ok_or(Error::NotFound)?;
            if node.session.as_ref().is_none_or(|s| s.id != session_id) {
                return Err(Error::Conflict);
            }
            node.node.available = false;
            node.session.as_mut().unwrap().routable = false;
            let mut writes = vec![(format!("node:{id}"), encode(node)?)];
            for (instance_id, instance) in &mut saved.instances {
                if instance.assignment.node_id != id
                    || instance.invalidated
                    || instance
                        .result
                        .as_ref()
                        .is_some_and(|r| r.state == InstanceState::Deleted)
                {
                    continue;
                }
                let mut result = instance.result.clone().unwrap_or_else(|| InstanceRecord {
                    spec: instance.spec.clone(),
                    assignment: instance.assignment.clone(),
                    runtime_id: format!("{}-{}", instance_id, instance.assignment.generation),
                    revision: 0,
                    state: InstanceState::Pending,
                    resources_held: true,
                    runtime_ip: None,
                    checkpoint: None,
                    last_operation: None,
                    restart_attempts: 0,
                    restart_pending: false,
                });
                result.state = InstanceState::Failed;
                result.revision = result.revision.checked_add(1).ok_or(Error::Conflict)?;
                result.resources_held = false;
                result.runtime_ip = None;
                result.restart_pending = false;
                result.last_operation = None;
                instance.result = Some(result);
                instance.invalidated = true;
                instance.validate()?;
                writes.push((format!("instance:{instance_id}"), encode(instance)?));
            }
            if writes
                .iter()
                .all(|(field, value)| raw.get(field) == Some(value))
            {
                return Ok(saved);
            }
            header.advance()?;
            saved.revision = header.revision;
            saved.validate()?;
            // Validate/encode all data before the script. No fallible decoding or
            // floating point version comparisons occur between its writes.
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
                for i = 3, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1]) end
                redis.call('HSET', KEYS[1], 'header', ARGV[2]); return 1
            "#,
                )
                .arg(1)
                .arg(&self.store.key)
                .arg(raw.get(HEADER).ok_or(Error::Conflict)?)
                .arg(encode(&header)?);
            for (field, value) in writes {
                command.arg(field).arg(value);
            }
            if self.store.query::<u8>(command).await? == 1 {
                return Ok(saved);
            }
        }
        Err(Error::Unavailable(
            "concurrent node invalidation; retry request".into(),
        ))
    }
}

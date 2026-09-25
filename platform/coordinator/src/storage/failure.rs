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
            node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
            let mut writes = vec![(format!("node:{id}"), encode(node)?)];
            for (environment_id, environment) in &mut saved.environments {
                if environment.assignment.node_id != id
                    || environment.invalidated
                    || environment
                        .result
                        .as_ref()
                        .is_some_and(|r| r.state == EnvironmentState::Deleted)
                {
                    continue;
                }
                let mut result = environment
                    .result
                    .clone()
                    .unwrap_or_else(|| EnvironmentRecord {
                        spec: environment.spec.clone(),
                        assignment: environment.assignment.clone(),
                        runtime: adx_core::Runtime {
                            id: format!("{}-{}", environment_id, environment.assignment.generation),
                            ip: None,
                        },
                        revision: 0,
                        state: EnvironmentState::Pending,
                        resources_held: true,
                        checkpoint: None,
                        last_operation: None,
                        restart_attempts: 0,
                        restart_pending: false,
                    });
                result.state = EnvironmentState::Failed;
                result.revision = result.revision.checked_add(1).ok_or(Error::Conflict)?;
                result.resources_held = false;
                result.runtime.ip = None;
                result.restart_pending = false;
                result.last_operation = None;
                environment.result = Some(result);
                environment.invalidated = true;
                environment.validate()?;
                writes.push((
                    format!("environment:{environment_id}"),
                    encode(environment)?,
                ));
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
                self.store.notify_committed_revision(header.revision);
                return Ok(saved);
            }
        }
        Err(Error::Unavailable(
            "concurrent node invalidation; retry request".into(),
        ))
    }
}

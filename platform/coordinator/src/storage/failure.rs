//! Persist node expiry and all retired execution results in one Redis script.
use super::*;

impl Session {
    pub async fn invalidate_node(&self, id: &str, session_id: &str) -> Result<StoredSnapshot> {
        for _ in 0..ATTEMPTS {
            let snapshots = self.retained_snapshots().await?;
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let mut saved = snapshot(&raw)?;
            let node = saved.nodes.get_mut(id).ok_or(Error::NotFound)?;
            if node.session.as_ref().is_none_or(|s| s.id != session_id) {
                return Err(Error::Conflict);
            }
            node.node.available = false;
            node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
            let mut holds = Vec::new();
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
                        execution_artifacts: vec![],
                        snapshot_source: None,
                        failure: None,
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
                let mut inputs = result.execution_artifacts.clone();
                if result.resources_held {
                    if let Some(snapshot) = snapshots
                        .iter()
                        .find(|s| environment.spec.snapshot_id.as_deref() == Some(s.id.as_str()))
                    {
                        if !inputs.contains(&snapshot.artifact) {
                            inputs.push(snapshot.artifact.clone());
                        }
                    }
                    for point in result
                        .checkpoint
                        .iter()
                        .chain(result.snapshot_source.iter())
                    {
                        if !inputs.contains(&point.artifact) {
                            inputs.push(point.artifact.clone());
                        }
                    }
                }
                if !inputs.is_empty() {
                    holds.push((
                        artifacts::hold_key(&environment.assignment),
                        encode(&artifacts::ExecutionHold {
                            assignment: environment.assignment.clone(),
                            artifacts: inputs,
                        })?,
                    ));
                }
                // Losing a node retires execution ownership, not the user's
                // dormancy intent. A paused restore point is awakened on demand.
                let autonomous_recovery = environment.resume.is_none()
                    && environment.recovery.as_ref().is_some_and(|r| r.pending);
                if result.state != EnvironmentState::Paused || autonomous_recovery {
                    result.state = EnvironmentState::Failed;
                }
                result.revision = result.revision.checked_add(1).ok_or(Error::Conflict)?;
                result.resources_held = false;
                result.runtime.ip = None;
                result.restart_pending = false;
                result.last_operation = None;
                environment.result = Some(result);
                environment.invalidated = true;
                environment.track_failure(super::collection::now()?);
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
                local count=tonumber(ARGV[3]); for i=4,3+count*2,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
                for i=4+count*2,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end
                redis.call('HSET', KEYS[1], 'header', ARGV[2]); return 1
            "#,
                )
                .arg(2)
                .arg(&self.store.key)
                .arg(format!("{}:input-holds",self.store.key))
                .arg(raw.get(HEADER).ok_or(Error::Conflict)?)
                .arg(encode(&header)?)
                .arg(writes.len());
            let changed = writes.iter().map(|(field, _)| field.clone()).collect();
            for (field, value) in writes {
                command.arg(field).arg(value);
            }
            let cached_holds = holds
                .iter()
                .map(|(field, value)| {
                    Ok((field.clone(), decode::<artifacts::ExecutionHold>(value)?))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            for (field, value) in holds {
                command.arg(field).arg(value);
            }
            let mut inventory = self.inspection.lock().await;
            let mut committed = inventory.take();
            let applied = match self.store.query::<u8>(command).await {
                Ok(applied) => applied,
                Err(error) => {
                    *inventory = None;
                    return Err(error);
                }
            };
            if applied == 1 {
                if let Some(inventory) = committed.as_mut() {
                    inventory.holds.extend(cached_holds);
                }
                *inventory = committed;
                self.store.notify_committed_change(header.revision, changed);
                return Ok(saved);
            }
            *inventory = committed;
        }
        Err(Error::Unavailable(
            "concurrent node invalidation; retry request".into(),
        ))
    }
}

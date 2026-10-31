//! Initial success is the durable creation boundary. Both public identities are
//! checked in the same transaction; unfinished executions remain node-owned.
use super::*;
use sha2::{Digest, Sha256};

const CREATE_RECEIPT_TTL: Duration = Duration::from_secs(600);

impl Session {
    fn receipt_key(&self, tenant: &str, request: &str) -> Result<String> {
        let identity = encode(&(tenant, request))?;
        Ok(format!(
            "{}:create-receipt:{:x}",
            self.store.key,
            Sha256::digest(identity.as_bytes())
        ))
    }

    pub async fn published_request(
        &self,
        tenant: &str,
        request: &str,
    ) -> Result<Option<EnvironmentRecord>> {
        match self.request_receipt(tenant, request).await? {
            Some(receipt) => self.resolve_receipt(receipt).await.map(Some),
            None => Ok(None),
        }
    }

    async fn request_receipt(
        &self,
        tenant: &str,
        request: &str,
    ) -> Result<Option<StoredEnvironment>> {
        if request.is_empty() {
            return Ok(None);
        }
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("GET");
        command.arg(self.receipt_key(tenant, request)?);
        let Some(raw) = self.store.query::<Option<String>>(command).await? else {
            return Ok(None);
        };
        decode(&raw).map(Some)
    }

    async fn resolve_receipt(&self, receipt: StoredEnvironment) -> Result<EnvironmentRecord> {
        receipt.validate()?;
        let mut record = receipt.result.ok_or(Error::Conflict)?;
        match self.get(&record.spec.id).await {
            Ok(current)
                if current.creation_generation == receipt.creation_generation
                    && current.spec.tenant_id == record.spec.tenant_id =>
            {
                record = current.result.ok_or(Error::Conflict)?;
            }
            Ok(_) | Err(Error::NotFound) => {
                record.state = EnvironmentState::Deleted;
                record.resources_held = false;
                record.runtime.ip = None;
            }
            Err(error) => return Err(error),
        }
        Ok(record)
    }

    /// Remove only an unfinished optional Claim belonging to a losing execution.
    /// Successful or replacement records cannot be removed by a delayed loser.
    pub(crate) async fn abandon_initial(&self, assignment: &Assignment) -> Result<()> {
        let field = format!("environment:{}", assignment.environment_id);
        let [header, old] = self.store.fields([HEADER.into(), field.clone()]).await?;
        self.header(&header)?;
        let Some(old) = old else {
            return Ok(());
        };
        let stored: StoredEnvironment = decode(&old)?;
        if volumes::has_volumes(&stored.spec)
            || stored.assignment != *assignment
            || stored.result.is_some()
            || stored.recovery.is_some()
        {
            return Ok(());
        }
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
            redis.call('HDEL',KEYS[1],ARGV[2]); return 1
        "#,
            )
            .arg(1)
            .arg(&self.store.key)
            .arg(self.epoch.to_string())
            .arg(&field)
            .arg(old);
        let values = RedisStore::publication_values([(field.as_str(), None)])?;
        let write = self.store.publication_write(self.epoch, vec![field]).await;
        let applied = self.store.query::<u8>(command).await? == 1;
        write.finish(applied, values);
        Ok(())
    }

    /// Publish a completed initial execution, or return the previously published
    /// identity. Request receipts expire ten minutes after first publication;
    /// replay does not refresh their deadline. Live Environment identity is retained.
    /// Callers must retire their execution when another result wins.
    pub async fn publish_initial(
        &self,
        record: EnvironmentRecord,
        request: &str,
        node_session: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_initial_requests(record, &[request.to_owned()], node_session)
            .await
    }

    pub(crate) async fn publish_initial_requests(
        &self,
        record: EnvironmentRecord,
        requests: &[String],
        node_session: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_initial_observed(record, requests, node_session, None)
            .await
    }

    pub(crate) async fn publish_initial_observed(
        &self,
        record: EnvironmentRecord,
        requests: &[String],
        node_session: &str,
        creation: Option<CreationSummary>,
    ) -> Result<EnvironmentRecord> {
        self.publish_success(record, requests, node_session, false, creation)
            .await
    }

    pub(crate) async fn publish_existing(
        &self,
        record: EnvironmentRecord,
        request: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_success(record, &[request.to_owned()], "", true, None)
            .await
    }

    async fn publish_success(
        &self,
        record: EnvironmentRecord,
        requests: &[String],
        node_session: &str,
        require_existing: bool,
        creation: Option<CreationSummary>,
    ) -> Result<EnvironmentRecord> {
        validate_result(&record)?;
        if record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        let field = format!("environment:{}", record.spec.id);
        let request_keys = requests
            .iter()
            .filter(|request| !request.is_empty())
            .map(|request| self.receipt_key(&record.spec.tenant_id, request))
            .collect::<Result<Vec<_>>>()?;
        let next = StoredEnvironment {
            creation_generation: record.assignment.generation,
            creation: Some(match creation {
                Some(creation) => creation,
                None => CreationSummary::new(requests.first().map_or("", String::as_str))?,
            }),
            last_interruption: None,
            volume_execution: None,
            resume: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec: record.spec.clone(),
            assignment: record.assignment.clone(),
            result: None,
        };
        let artifacts = artifacts::record_artifacts(&record);
        let attaches_artifacts = record.checkpoint.is_some()
            || record.snapshot_source.is_some()
            || !record.execution_artifacts.is_empty();
        let _access = self.store.lock_artifacts(&artifacts)?;
        for _ in 0..ATTEMPTS {
            let catalog = if volumes::has_volumes(&record.spec) {
                Some(self.volume_catalog().await?)
            } else {
                None
            };
            let [header, old] = self.store.fields([HEADER.into(), field.clone()]).await?;
            if catalog
                .as_ref()
                .is_some_and(|catalog| catalog.control.get(&field) != old.as_ref())
            {
                continue;
            }
            let mut counters = CounterUpdate::load(&self.store, field.clone()).await?;
            let current_header = self.header(&header)?;
            let previous = old
                .as_deref()
                .map(decode::<StoredEnvironment>)
                .transpose()?;
            if let Some(previous) = &previous {
                previous.validate()?;
            }
            let published = previous.as_ref().is_some_and(|e| e.result.is_some());
            // Keep receipt replay ahead of this guard inside the atomic script:
            // a successful Request ID still wins even when this name is pending elsewhere.
            let pending_conflict = previous.as_ref().is_some_and(|e| {
                e.result.is_none()
                    && (e.assignment != record.assignment || e.spec != record.spec || e.invalidated)
            });

            if require_existing && !published {
                return Err(Error::NotFound);
            }
            let mut next = next.clone();
            if let Some(previous) = &previous {
                next.creation = previous.creation.clone();
                next.volume_execution = previous.volume_execution.clone();
            }
            let mut catalog = catalog;
            if !published && !pending_conflict {
                if let Some(catalog) = &mut catalog {
                    catalog.apply_result(&record, node_session, &[])?;
                }
            }
            next.observe_result(&record, &mut counters.value)?;
            next.result = Some(record.clone());
            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return {-1,''} end
                local function aliases(value)
                    for i=5,#KEYS do
                        redis.call('SET',KEYS[i],value,'NX','EX',ARGV[5])
                    end
                end
                for i=5,#KEYS do
                    local receipt = redis.call('GET',KEYS[i])
                    if receipt then
                        aliases(receipt)
                        return {2,receipt}
                    end
                end
                local old = redis.call('HGET',KEYS[1],ARGV[2])
                if ARGV[3] == '' then if old then return {0,''} end
                elseif old ~= ARGV[3] then return {0,''} end
                if ARGV[6] == 'published' then
                    aliases(old)
                    return {2,old}
                end
                if ARGV[6] == 'conflict' then return {-1,''} end
                if ARGV[7] ~= '' and redis.call('HGET',KEYS[1],ARGV[8]) ~= ARGV[7] then return {-1,''} end
                if ARGV[10] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[10] then return {0,''} end
                local facts = cjson.decode(ARGV[9])
                if (redis.call('HGET',KEYS[3],facts[1]) or '') ~= facts[2] then return {0,''} end
                redis.call('HSET',KEYS[3],facts[1],facts[3])
                redis.call('HSET',KEYS[1],ARGV[2],ARGV[4])
                redis.call('HINCRBY',KEYS[1],'node-ownership:' .. string.sub(ARGV[8],14),1)
                aliases(ARGV[4])
                local cursor=13
                for _=1,tonumber(ARGV[12]) do redis.call('HSET',KEYS[4],ARGV[cursor],ARGV[cursor+1]); cursor=cursor+2 end
                if ARGV[10] ~= '' then redis.call('HSET',KEYS[1],'header',ARGV[11]) end
                for i=cursor,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end
                return {1,ARGV[4]}
            "#).arg(4 + request_keys.len()).arg(&self.store.key)
                .arg(format!("{}:artifacts", self.store.key))
                .arg(format!("{}:business", self.store.key))
                .arg(self.volumes_key())
                .arg(&request_keys)
                .arg(self.epoch.to_string()).arg(&field).arg(old.as_deref().unwrap_or(""))
                .arg(encode(&next)?).arg(CREATE_RECEIPT_TTL.as_secs())
                .arg(if published { "published" } else if pending_conflict { "conflict" } else { "initial" }).arg(node_session)
                .arg(format!("node-session:{}", record.assignment.node_id))
                .arg(counters.argument()?);
            if let Some(catalog) = &catalog {
                command
                    .arg(&catalog.header_value)
                    .arg(encode(&catalog.advanced_header()?)?);
                catalog.append_writes(&mut command)?;
            } else if attaches_artifacts {
                let mut advanced = current_header.clone();
                advanced.advance()?;
                command
                    .arg(header.as_deref().ok_or(Error::Conflict)?)
                    .arg(encode(&advanced)?)
                    .arg(0);
            } else {
                command.arg("").arg("").arg(0);
            }
            for artifact in &artifacts {
                command
                    .arg(artifacts::artifact_key(artifact))
                    .arg(encode(artifact)?);
            }
            let values = vec![PublicationValue::Environment(
                record.spec.id.clone(),
                Some(Arc::new(next.clone())),
            )];
            let write = self
                .store
                .publication_write(self.epoch, vec![field.clone()])
                .await;
            let (code, raw): (i8, String) = self.store.query(command).await?;
            write.finish_with_fields(
                code == 1,
                values,
                catalog
                    .iter()
                    .flat_map(volumes::Catalog::change_fields)
                    .collect(),
            );
            match code {
                1 | 2 => {
                    let winner: StoredEnvironment = decode(&raw)?;
                    if code == 2 {
                        return self.resolve_receipt(winner).await;
                    }
                    return winner.result.ok_or(Error::Conflict);
                }
                0 => (),
                _ => return Err(Error::Conflict),
            }
        }
        Err(Error::Unavailable(
            "initial result publication contended; retry original request".into(),
        ))
    }
}

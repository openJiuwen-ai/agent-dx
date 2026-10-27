//! Durable preparation authority in the same Redis hash slot as Environment
//! ownership. Every write is fenced by the Coordinator epoch and compared by
//! opaque bytes; Redis/Lua never rounds Rust u64 revisions through doubles.
use super::*;
use adx_core::prewarm::{self, PreparationContext};

pub use adx_core::prewarm::management::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    request_digest: String,
    accepted: Accepted,
}
#[derive(Serialize, Deserialize)]
struct ArtifactCursor {
    tenant: String,
    pool_id: String,
    state: Option<prewarm::SlotState>,
    last_id: String,
}

pub fn fingerprint(value: &impl Serialize) -> Result<String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(encode(value)?.as_bytes())
    ))
}
pub fn template_pool_id(source: &prewarm::SourceRef) -> Result<String> {
    let digest = fingerprint(&(
        source.tenant.as_str(),
        source.template.as_str(),
        source.version.as_str(),
    ))?;
    Ok(format!("template-{}", digest.trim_start_matches("sha256:")))
}

impl Session {
    pub(crate) async fn preparation_is_assigned(
        &self,
        id: &str,
        assignment: &Assignment,
    ) -> Result<bool> {
        let Some(raw) = self.preparation_read(&format!("attempt:{id}")).await? else {
            return Ok(false);
        };
        let attempt: PreparationAttempt = decode(&raw)?;
        match attempt.assignment {
            Some(owner) if owner == *assignment => Ok(true),
            Some(_) => Err(Error::Conflict),
            None => Ok(false),
        }
    }
    pub(super) async fn guard_preparation_generation(
        &self,
        id: &str,
        generation: Option<u64>,
    ) -> Result<()> {
        if let Some(raw) = self.preparation_read(&format!("attempt:{id}")).await? {
            let attempt: PreparationAttempt = decode(&raw)?;
            if attempt
                .assignment
                .is_some_and(|a| Some(a.generation) != generation)
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    /// Opaque random cursors are server stored and expire independently of
    /// preparation ownership/tombstones. A cursor never grants tenant access.
    pub async fn prewarm_artifact_page(
        &self,
        tenant: &str,
        id: &str,
        page_size: u32,
        state: Option<prewarm::SlotState>,
        token: &str,
    ) -> PoolResult<ArtifactPage> {
        if !(1..=100).contains(&page_size) {
            return Err(Error::Invalid("artifact page_size must be 1-100".into()).into());
        }
        let pool = self.get_pool(tenant, id).await?;
        if pool.policy.mode != prewarm::PoolMode::ImageRootfs {
            return Err(PoolError::Unsupported);
        }
        let after = if token.is_empty() {
            String::new()
        } else {
            uuid::Uuid::parse_str(token)
                .map_err(|_| Error::Invalid("invalid page token".into()))?;
            let mut cmd = redis::cmd("GET");
            cmd.arg(format!("{}:cursor:{token}", self.preparation_key()));
            let raw: String = self
                .store
                .query::<Option<String>>(cmd)
                .await?
                .ok_or_else(|| Error::Invalid("invalid or expired page token".into()))?;
            let cursor: ArtifactCursor = decode(&raw)?;
            if cursor.tenant != tenant || cursor.pool_id != id || cursor.state != state {
                return Err(Error::Invalid("page token scope mismatch".into()).into());
            }
            cursor.last_id
        };
        let mut items: Vec<_> = pool
            .artifacts
            .into_iter()
            .filter(|a| {
                a.id > after
                    && a.state != prewarm::SlotState::Deleted
                    && state.is_none_or(|state| state == a.state)
            })
            .collect();
        items.sort_by(|a, b| a.id.cmp(&b.id));
        let next_page_token = if items.len() > page_size as usize {
            items.truncate(page_size as usize);
            let last_id = items.last().ok_or(Error::Conflict)?.id.clone();
            let token = uuid::Uuid::new_v4().to_string();
            let mut cmd = redis::cmd("SET");
            cmd.arg(format!("{}:cursor:{token}", self.preparation_key()))
                .arg(encode(&ArtifactCursor {
                    tenant: tenant.into(),
                    pool_id: id.into(),
                    state,
                    last_id,
                })?)
                .arg("EX")
                .arg(900);
            self.store.query::<String>(cmd).await?;
            Some(token)
        } else {
            None
        };
        Ok(ArtifactPage {
            items,
            next_page_token,
        })
    }
    pub async fn prewarm_mutation_replay(
        &self,
        tenant: &str,
        request: &Mutation,
    ) -> PoolResult<Option<Accepted>> {
        let receipt_key = format!("receipt:{}", fingerprint(&(tenant, &request.request_id))?);
        let Some(raw) = self.preparation_read(&receipt_key).await? else {
            return Ok(None);
        };
        let receipt: Receipt = decode(&raw)?;
        if receipt.request_digest != fingerprint(&(tenant, request))? {
            return Err(PoolError::IdempotencyConflict);
        }
        Ok(Some(receipt.accepted))
    }
    fn preparation_key(&self) -> String {
        format!("{}:prewarm", self.store.key)
    }

    async fn preparation_read(&self, field: &str) -> Result<Option<String>> {
        // Read the epoch and record atomically. A separate epoch read would
        // allow a retired controller to observe data from its replacement.
        // Redis enforces that this query cannot mutate lifecycle authority.
        let mut command = redis::cmd("EVAL_RO");
        command.arg("return {redis.call('HGET', KEYS[1], 'header'), redis.call('HGET', KEYS[2], ARGV[1])}")
            .arg(2).arg(&self.store.key).arg(self.preparation_key()).arg(field);
        let values: (Option<String>, Option<String>) = self.store.query(command).await?;
        self.header(&values.0)?;
        Ok(values.1)
    }

    async fn preparation_cas(
        &self,
        field: &str,
        old: Option<&str>,
        value: &str,
        receipt: Option<(&str, &str)>,
    ) -> Result<bool> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            local old = redis.call('HGET', KEYS[2], ARGV[2])
            if ARGV[3] == '' then
                if old then return 0 end
            elseif old ~= ARGV[3] then return 0 end
            if ARGV[5] ~= '' and redis.call('HEXISTS', KEYS[2], ARGV[5]) == 1 then return 0 end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
            if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], ARGV[5], ARGV[6]) end
            return 1
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.preparation_key())
            .arg(header.as_deref().ok_or(Error::Conflict)?)
            .arg(field)
            .arg(old.unwrap_or(""))
            .arg(value)
            .arg(receipt.map_or("", |(key, _)| key))
            .arg(receipt.map_or("", |(_, value)| value));
        Ok(self.store.query::<u8>(command).await? == 1)
    }

    /// Create/update policy and its durable replay receipt in one transaction.
    /// Without a node qualification this entrypoint stages disabled policies.
    /// The service supplies the immutable recipe when enabling EROFS preparation.
    pub async fn mutate_pool(
        &self,
        tenant: &str,
        request: &Mutation,
        now: u64,
    ) -> PoolResult<Accepted> {
        self.mutate_qualified_pool(tenant, request, now, None).await
    }

    /// `qualified` is computed from authenticated node capability RPCs, never
    /// from a public request. Runtime slots remain unsupported on every backend.
    pub async fn mutate_qualified_pool(
        &self,
        tenant: &str,
        request: &Mutation,
        now: u64,
        qualified_recipe: Option<String>,
    ) -> PoolResult<Accepted> {
        prewarm::validate_identifier(tenant)?;
        prewarm::validate_identifier(&request.pool_id)?;
        if request.request_id.is_empty()
            || request.request_id.len() > 256
            || request.request_id.chars().any(char::is_control)
        {
            return Err(Error::Invalid(
                "X-ADX-Request-ID must contain 1-256 non-control bytes".into(),
            )
            .into());
        }
        let request_digest = fingerprint(&(tenant, request))?;
        let receipt_key = format!("receipt:{}", fingerprint(&(tenant, &request.request_id))?);
        let field = format!("pool:{}", request.pool_id);
        for _ in 0..ATTEMPTS {
            if let Some(raw) = self.preparation_read(&receipt_key).await? {
                let receipt: Receipt = decode(&raw)?;
                return if receipt.request_digest == request_digest {
                    Ok(receipt.accepted)
                } else {
                    Err(PoolError::IdempotencyConflict)
                };
            }
            let raw = self.preparation_read(&field).await?;
            let old: Option<PoolRecord> = raw.as_deref().map(decode).transpose()?;
            if old.as_ref().is_some_and(|pool| pool.tenant != tenant) {
                return Err(PoolError::NotFound);
            }
            match (&request.condition, &old) {
                (Condition::Missing, _) => return Err(PoolError::PreconditionRequired),
                (Condition::Absent, None) if matches!(request.command, Command::Put { .. }) => (),
                (Condition::Revision(expected), Some(old))
                    if *expected == old.revision && old.phase != PoolPhase::Deleted => {}
                _ => return Err(PoolError::PreconditionFailed),
            }
            let mut pool = match &request.command {
                Command::Put { profile, policy } => {
                    profile.validate()?;
                    profile.source_ref.validate(tenant)?;
                    policy.validate(profile.resources)?;
                    let recipe = qualified_recipe
                        .clone()
                        .or_else(|| old.as_ref().map(|p| p.artifact_recipe_digest.clone()))
                        .unwrap_or_default();
                    if old.as_ref().is_some_and(|p| {
                        !p.artifact_recipe_digest.is_empty() && p.artifact_recipe_digest != recipe
                    }) {
                        return Err(PoolError::ImmutableProfile);
                    }
                    let digest = fingerprint(&(profile, &recipe))?;
                    if old.as_ref().is_some_and(|old| {
                        old.profile != **profile || old.policy.mode != policy.mode
                    }) {
                        return Err(PoolError::ImmutableProfile);
                    }
                    if policy.enabled
                        && (qualified_recipe.is_none()
                            || policy.mode != prewarm::PoolMode::ImageRootfs)
                    {
                        return Err(PoolError::Unsupported);
                    }
                    if policy.enabled && request.pool_id != template_pool_id(&profile.source_ref)? {
                        return Err(Error::Invalid(
                            "template pools require the canonical template pool ID".into(),
                        )
                        .into());
                    }
                    if old.as_ref().is_some_and(|p| p.phase == PoolPhase::Deleting) {
                        return Err(PoolError::ImmutableProfile);
                    }
                    PoolRecord {
                        pool_id: request.pool_id.clone(),
                        tenant: tenant.into(),
                        revision: old.as_ref().map_or(Ok(1), |old| {
                            old.revision.checked_add(1).ok_or(Error::Conflict)
                        })?,
                        profile_digest: digest,
                        artifact_recipe_digest: recipe,
                        profile: profile.as_ref().clone(),
                        policy: policy.clone(),
                        phase: if policy.enabled {
                            PoolPhase::Active
                        } else {
                            PoolPhase::Suspended
                        },
                        created_at: old.as_ref().map_or(now, |old| old.created_at),
                        updated_at: now,
                        artifacts: old.as_ref().map_or_else(Vec::new, |p| p.artifacts.clone()),
                        observed_revision: 0,
                        observed_at: 0,
                        refill_failures: old.as_ref().map_or(0, |p| p.refill_failures),
                        refill_retry_at: old.as_ref().map_or(0, |p| p.refill_retry_at),
                    }
                }
                Command::Action { action } => {
                    let mut pool = old.clone().ok_or(PoolError::NotFound)?;
                    if pool.phase == PoolPhase::Deleting && *action != Action::Reconcile {
                        return Err(Error::Conflict.into());
                    }
                    match action {
                        Action::Resume
                            if qualified_recipe.is_some()
                                && pool.policy.mode == prewarm::PoolMode::ImageRootfs
                                && pool.phase != PoolPhase::Deleting =>
                        {
                            let recipe = qualified_recipe.clone().ok_or(PoolError::Unsupported)?;
                            if !pool.artifact_recipe_digest.is_empty()
                                && pool.artifact_recipe_digest != recipe
                            {
                                return Err(PoolError::ImmutableProfile);
                            }
                            pool.artifact_recipe_digest = recipe;
                            pool.profile_digest =
                                fingerprint(&(&pool.profile, &pool.artifact_recipe_digest))?;
                            pool.phase = PoolPhase::Active;
                            pool.policy.enabled = true;
                        }
                        Action::Resume => return Err(PoolError::Unsupported),
                        Action::Reconcile => (),
                        Action::Suspend => {
                            pool.phase = PoolPhase::Suspended;
                            pool.policy.enabled = false;
                        }
                        // No qualified backend can have created inventory yet.
                        Action::Drain => {
                            pool.phase = PoolPhase::Draining;
                            pool.policy.enabled = false;
                        }
                    }
                    pool.revision = pool.revision.checked_add(1).ok_or(Error::Conflict)?;
                    pool
                }
                Command::Delete => {
                    let mut pool = old.clone().ok_or(PoolError::NotFound)?;
                    pool.phase = if pool.artifacts.iter().any(|a| a.counts_as_live()) {
                        PoolPhase::Deleting
                    } else {
                        PoolPhase::Deleted
                    };
                    pool.policy.enabled = false;
                    pool.revision = pool.revision.checked_add(1).ok_or(Error::Conflict)?;
                    pool
                }
            };
            pool.updated_at = now;
            let accepted = Accepted {
                operation: ManagementOperation {
                    operation_id: format!(
                        "prewarm-{}",
                        request_digest.trim_start_matches("sha256:")
                    ),
                    pool_id: pool.pool_id.clone(),
                    tenant: tenant.into(),
                    accepted_revision: pool.revision,
                    state: if pool.phase == PoolPhase::Deleting
                        || (matches!(
                            request.command,
                            Command::Action {
                                action: Action::Drain
                            }
                        ) && pool.artifacts.iter().any(|a| a.counts_as_live()))
                    {
                        "accepted"
                    } else {
                        "succeeded"
                    }
                    .into(),
                    outcome: "confirmed".into(),
                    created_at: now,
                },
                pool,
            };
            let receipt = encode(&Receipt {
                request_digest: request_digest.clone(),
                accepted: accepted.clone(),
            })?;
            if self
                .preparation_cas(
                    &field,
                    raw.as_deref(),
                    &encode(&accepted.pool)?,
                    Some((&receipt_key, &receipt)),
                )
                .await?
            {
                return Ok(accepted);
            }
        }
        Err(
            Error::Unavailable("prewarm update contended; retry the same logical request".into())
                .into(),
        )
    }

    pub async fn get_pool(&self, tenant: &str, id: &str) -> PoolResult<PoolRecord> {
        let raw = self
            .preparation_read(&format!("pool:{id}"))
            .await?
            .ok_or(PoolError::NotFound)?;
        let pool: PoolRecord = decode(&raw)?;
        if pool.tenant != tenant || pool.phase == PoolPhase::Deleted {
            return Err(PoolError::NotFound);
        }
        Ok(pool)
    }

    async fn preparation_records(&self) -> Result<BTreeMap<String, String>> {
        let mut command = redis::cmd("EVAL_RO");
        command
            .arg("return {redis.call('HGET', KEYS[1], 'header'), redis.call('HGETALL', KEYS[2])}")
            .arg(2)
            .arg(&self.store.key)
            .arg(self.preparation_key());
        let (header, records): (Option<String>, BTreeMap<String, String>) =
            self.store.query(command).await?;
        self.header(&header)?;
        Ok(records)
    }

    pub async fn list_pools(&self, tenant: &str) -> PoolResult<Vec<PoolRecord>> {
        let mut pools = vec![];
        for (key, raw) in self.preparation_records().await? {
            if !key.starts_with("pool:") {
                continue;
            }
            let pool: PoolRecord = decode(&raw)?;
            if pool.tenant == tenant && pool.phase != PoolPhase::Deleted {
                pools.push(pool);
            }
        }
        pools.sort_by(|a, b| a.pool_id.cmp(&b.pool_id));
        Ok(pools)
    }

    pub async fn get_prewarm_operation(
        &self,
        tenant: &str,
        id: &str,
    ) -> PoolResult<ManagementOperation> {
        for (key, raw) in self.preparation_records().await? {
            if !key.starts_with("receipt:") {
                continue;
            }
            let receipt: Receipt = decode(&raw)?;
            if receipt.accepted.operation.tenant == tenant
                && receipt.accepted.operation.operation_id == id
            {
                let mut operation = receipt.accepted.operation;
                if operation.state == "accepted" {
                    let raw = self
                        .preparation_read(&format!("pool:{}", operation.pool_id))
                        .await?
                        .ok_or(PoolError::NotFound)?;
                    let current: PoolRecord = decode(&raw)?;
                    let complete = current.phase == PoolPhase::Deleted
                        || (receipt.accepted.pool.phase == PoolPhase::Draining
                            && !current.artifacts.iter().any(|a| a.counts_as_live()));
                    operation.state = if complete { "succeeded" } else { "reconciling" }.into();
                    operation.outcome = if complete { "confirmed" } else { "unknown" }.into();
                }
                return Ok(operation);
            }
        }
        Err(PoolError::NotFound)
    }

    /// Register one creation attempt by global EID, independent of HTTP request
    /// IDs. Before assignment its cold value is provisional; assign_preparation
    /// atomically fixes the final cold/artifact choice against placement.
    /// This registration does not allocate an Environment or report a hit.
    pub async fn select_preparation(
        &self,
        tenant: &str,
        environment_id: &str,
        context: &PreparationContext,
        request_digest: &str,
        now: u64,
    ) -> Result<PreparationAttempt> {
        context.validate(tenant)?;
        prewarm::validate_identifier(environment_id)?;
        if !prewarm::valid_digest(request_digest) {
            return Err(Error::Invalid("invalid create fingerprint".into()));
        }
        let field = format!("attempt:{environment_id}");
        for _ in 0..ATTEMPTS {
            if let Some(raw) = self.preparation_read(&field).await? {
                let old: PreparationAttempt = decode(&raw)?;
                if old.tenant != tenant
                    || old.context != *context
                    || old.request_digest != request_digest
                {
                    return Err(Error::Conflict);
                }
                if let Some(assignment) = &old.assignment {
                    let saved = self.get(environment_id).await.map_err(|e| {
                        if e == Error::NotFound {
                            Error::Conflict
                        } else {
                            e
                        }
                    })?;
                    if &saved.assignment != assignment {
                        return Err(Error::Conflict);
                    }
                }
                return Ok(old);
            }
            let next = PreparationAttempt {
                tenant: tenant.into(),
                environment_id: environment_id.into(),
                context: context.clone(),
                request_digest: request_digest.into(),
                selection: Selection::Cold,
                reason: "pool_miss".into(),
                created_at: now,
                assignment: None,
                artifact_id: None,
            };
            if self
                .preparation_cas(&field, None, &encode(&next)?, None)
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "preparation selection contended; retry the same Environment ID".into(),
        ))
    }
    pub async fn preparation_attempt(
        &self,
        tenant: &str,
        environment_id: &str,
    ) -> Result<PreparationAttempt> {
        let raw = self
            .preparation_read(&format!("attempt:{environment_id}"))
            .await?
            .ok_or(Error::NotFound)?;
        let attempt: PreparationAttempt = decode(&raw)?;
        if attempt.tenant != tenant {
            return Err(Error::NotFound);
        }
        Ok(attempt)
    }

    pub async fn retained_prewarm_pools(&self) -> Result<Vec<PoolRecord>> {
        let mut pools = vec![];
        for (key, raw) in self.preparation_records().await? {
            if key.starts_with("pool:") {
                let pool: PoolRecord = decode(&raw)?;
                if pool.phase != PoolPhase::Deleted {
                    pools.push(pool);
                }
            }
        }
        Ok(pools)
    }

    /// The entire inventory and its byte ledger are committed with one pool CAS.
    /// Desired policy revision is unchanged by inventory/health observations.
    pub async fn update_pool_inventory(
        &self,
        expected: &PoolRecord,
        next: &PoolRecord,
    ) -> Result<bool> {
        if next.pool_id != expected.pool_id
            || next.tenant != expected.tenant
            || next.revision != expected.revision
            || next.profile != expected.profile
            || next.policy != expected.policy
            || next.profile_digest != expected.profile_digest
            || next.artifact_recipe_digest != expected.artifact_recipe_digest
            || (next.phase != expected.phase
                && !(expected.phase == PoolPhase::Deleting && next.phase == PoolPhase::Deleted))
        {
            return Err(Error::Conflict);
        }
        let mut ids = std::collections::BTreeSet::new();
        for artifact in &next.artifacts {
            if !ids.insert(&artifact.id)
                || artifact.profile_digest != next.profile_digest
                || artifact.node_id.is_empty()
                || artifact.node_session.is_empty()
                || (artifact.counts_as_live() && artifact.size_bytes == 0)
                || !matches!(
                    artifact.state,
                    prewarm::SlotState::Preparing
                        | prewarm::SlotState::Ready
                        | prewarm::SlotState::Quarantined
                        | prewarm::SlotState::Retiring
                        | prewarm::SlotState::Deleted
                )
            {
                return Err(Error::Conflict);
            }
        }
        let bytes = |pool: &PoolRecord| {
            pool.artifacts
                .iter()
                .filter(|a| a.counts_as_live())
                .try_fold(0u64, |n, a| {
                    n.checked_add(a.size_bytes).ok_or(Error::NoCapacity)
                })
        };
        let old_live = expected
            .artifacts
            .iter()
            .filter(|a| a.counts_as_live())
            .count();
        let new_live = next.artifacts.iter().filter(|a| a.counts_as_live()).count();
        if (new_live > next.policy.capacity.max_live as usize && new_live > old_live)
            || (bytes(next)? > next.policy.idle_budget.disk_bytes
                && bytes(next)? > bytes(expected)?)
        {
            return Err(Error::NoCapacity);
        }
        self.preparation_cas(
            &format!("pool:{}", expected.pool_id),
            Some(&encode(expected)?),
            &encode(next)?,
            None,
        )
        .await
    }

    /// Fix the preparation choice against the already committed Environment
    /// assignment, atomically checking both the inventory and Environment bytes.
    /// A read-only artifact is shareable; it has no user ownership transfer.
    pub async fn assign_preparation(
        &self,
        spec: &EnvironmentSpec,
        assignment: &Assignment,
        now: u64,
    ) -> Result<Option<PreparationAttempt>> {
        let field = format!("attempt:{}", spec.id);
        for _ in 0..ATTEMPTS {
            let Some(raw) = self.preparation_read(&field).await? else {
                return Ok(None);
            };
            let mut attempt: PreparationAttempt = decode(&raw)?;
            if attempt.tenant != spec.tenant_id || attempt.request_digest != fingerprint(spec)? {
                return Err(Error::Conflict);
            }
            if let Some(old) = &attempt.assignment {
                return if old == assignment {
                    Ok(Some(attempt))
                } else {
                    Err(Error::Conflict)
                };
            }
            // A template/version has one canonical pool, regardless of request ID.
            let pool_id = template_pool_id(&attempt.context.source_ref)?;
            let pool_field = format!("pool:{pool_id}");
            let pool_raw = self.preparation_read(&pool_field).await?;
            if let Some(pool) = pool_raw.as_deref().map(decode::<PoolRecord>).transpose()? {
                if pool.tenant == spec.tenant_id
                    && pool.phase == PoolPhase::Active
                    && pool.policy.enabled
                    && pool.profile.source_ref == attempt.context.source_ref
                    && pool.profile.image == spec.image
                    && pool.profile.runtime_class == spec.runtime_class
                    && pool.profile.resources == spec.resources
                    && spec.snapshot_id.is_none()
                    && spec.execution.rootfs.as_ref().is_none_or(|rootfs|matches!(&rootfs.source,adx_core::execution::StorageSource::Image(image) if image==&spec.image))
                    && spec.lifecycle.pause.is_none()
                    && spec.lifecycle.idle_action != adx_core::lifecycle::IdleAction::Pause
                {
                    if let Some(artifact) = pool
                        .artifacts
                        .iter()
                        .find(|a| a.node_id == assignment.node_id && a.claimable(now, &pool.policy))
                    {
                        attempt.selection = Selection::Artifact;
                        attempt.artifact_id = Some(artifact.id.clone());
                        attempt.reason = "artifact_hit".into();
                    }
                }
            }
            let [header, environment, node_session] = self
                .store
                .fields([
                    HEADER.into(),
                    format!("environment:{}", spec.id),
                    format!("node-session:{}", assignment.node_id),
                ])
                .await?;
            self.header(&header)?;
            let saved: StoredEnvironment = decode(environment.as_deref().ok_or(Error::NotFound)?)?;
            let node_record = self.get_node(&assignment.node_id).await?;
            let active_session = node_session
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or(Error::Conflict)?;
            if node_record
                .session
                .as_ref()
                .is_none_or(|s| s.id != active_session)
            {
                return Err(Error::Conflict);
            }
            if saved.spec != *spec
                || saved.assignment != *assignment
                || saved.invalidated
                || saved.result.is_some()
            {
                return Err(Error::Conflict);
            }
            if let Some(id) = &attempt.artifact_id {
                let pool: PoolRecord = decode(pool_raw.as_deref().ok_or(Error::Conflict)?)?;
                if !pool.artifacts.iter().any(|a| {
                    &a.id == id
                        && node_record
                            .session
                            .as_ref()
                            .is_some_and(|s| s.id == a.node_session && s.routable)
                }) {
                    attempt.artifact_id = None;
                    attempt.selection = Selection::Cold;
                    attempt.reason = "node_session_miss".into();
                }
            }
            attempt.assignment = Some(assignment.clone());
            let mut cmd = redis::cmd("EVAL");
            cmd.arg(
                r#"
                if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
                if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
                if redis.call('HGET',KEYS[2],ARGV[4]) ~= ARGV[5] then return 0 end
                if redis.call('HGET',KEYS[1],ARGV[9]) ~= ARGV[10] then return 0 end
                local pool = redis.call('HGET',KEYS[2],ARGV[6]) or ''
                if pool ~= ARGV[7] then return 0 end
                redis.call('HSET',KEYS[2],ARGV[4],ARGV[8]);return 1
            "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.preparation_key())
            .arg(header.as_deref().ok_or(Error::Conflict)?)
            .arg(format!("environment:{}", spec.id))
            .arg(environment.as_deref().ok_or(Error::Conflict)?)
            .arg(&field)
            .arg(&raw)
            .arg(pool_field)
            .arg(pool_raw.as_deref().unwrap_or(""))
            .arg(encode(&attempt)?)
            .arg(format!("node-session:{}", assignment.node_id))
            .arg(active_session);
            if self.store.query::<u8>(cmd).await? == 1 {
                return Ok(Some(attempt));
            }
        }
        Err(Error::Unavailable(
            "preparation assignment contended; retry same Environment".into(),
        ))
    }
}

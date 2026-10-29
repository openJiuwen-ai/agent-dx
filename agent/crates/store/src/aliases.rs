//! Alias configuration and two-reference Binding transitions use ordinary revision CAS.
use crate::{
    state::{binding_key, check, put},
    *,
};
use adx_agent_core::{
    alias::AliasConfig, AgentBinding, BindingPhase, EnvironmentRef, Scope, SecondaryEnv,
    SecondaryIntent, TemplateRef,
};

fn alias_key(tenant: &str, template: &str, alias: &str) -> Result<Key> {
    for value in [tenant, template, alias] {
        adx_agent_core::identifier(value, "alias scope").map_err(Error::Invalid)?;
    }
    Key::new("alias", &[tenant, template, alias])
}
impl AgentState {
    pub async fn selected_version(&self, scope: &Scope) -> Result<String> {
        scope.validate().map_err(Error::Invalid)?;
        Ok(self.desired_version(scope).await?.0)
    }
    pub async fn alias(
        &self,
        tenant: &str,
        template: &str,
        alias: &str,
    ) -> Result<Option<(Revision, AliasConfig)>> {
        adx_agent_core::alias_name(alias).map_err(Error::Invalid)?;
        let Some(record) = self.store.get(&alias_key(tenant, template, alias)?).await? else {
            return Ok(None);
        };
        let config: AliasConfig = record.decode()?;
        config.validate().map_err(Error::Corrupt)?;
        if config.tenant != tenant || config.template != template || config.alias != alias {
            return Err(Error::Corrupt("alias identity mismatch".into()));
        }
        Ok(Some((record.revision, config)))
    }

    /// None creates an alias; updates require the exact last observed revision.
    pub async fn put_alias(
        &self,
        config: &AliasConfig,
        expected: Option<&Revision>,
    ) -> Result<Revision> {
        config.validate().map_err(Error::Invalid)?;
        let key = alias_key(&config.tenant, &config.template, &config.alias)?;
        if let Some(record) = self.store.get(&key).await? {
            let previous: AliasConfig = record.decode()?;
            if previous.seed != config.seed {
                return Err(Error::Invalid("alias seed is immutable".into()));
            }
        }
        for entry in &config.versions {
            if self
                .template(&config.tenant, &config.template, &entry.version)
                .await?
                .is_none()
            {
                return Err(Error::Invalid(
                    "alias references an unpublished template version".into(),
                ));
            }
        }
        let value = put(&key, config)?;
        let revision = value.record.revision.clone();
        if !self
            .store
            .commit(&Transaction::new(
                vec![Check {
                    key,
                    expected: expected.cloned(),
                }],
                vec![value],
            )?)
            .await?
        {
            return Err(Error::Conflict("alias revision changed".into()));
        }
        Ok(revision)
    }

    pub(crate) async fn desired_version(&self, scope: &Scope) -> Result<(String, Option<Check>)> {
        match &scope.template_ref {
            TemplateRef::Version(version) => Ok((version.clone(), None)),
            TemplateRef::Alias(alias) => {
                let (revision, config) =
                    self.alias(&scope.tenant, &scope.template, alias)
                        .await?
                        .ok_or_else(|| Error::Invalid("alias does not exist".into()))?;
                let version = config
                    .select(&scope.binding_id)
                    .map_err(Error::Invalid)?
                    .to_owned();
                Ok((
                    version,
                    Some(Check {
                        key: alias_key(&scope.tenant, &scope.template, alias)?,
                        expected: Some(revision),
                    }),
                ))
            }
        }
    }

    /// Persist a candidate before Platform side effects, or retire an obsolete candidate.
    /// Retiring references always remain until the caller confirms cleanup.
    pub async fn prepare_binding(&self, scope: &Scope) -> Result<AgentBinding> {
        let key = binding_key(scope)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("binding missing".into()))?;
            let mut binding: AgentBinding = record.decode()?;
            if binding.phase != BindingPhase::Active {
                return Ok(binding);
            }
            let (version, alias_check) = self.desired_version(scope).await?;
            match &mut binding.secondary {
                Some(secondary) if secondary.intent == SecondaryIntent::Retiring => {
                    return Ok(binding)
                }
                Some(secondary)
                    if secondary.environment.version == version
                        && binding.current.version != version =>
                {
                    return Ok(binding)
                }
                Some(secondary) => secondary.intent = SecondaryIntent::Retiring,
                None if binding.current.version == version => return Ok(binding),
                None => {
                    if self
                        .template(&scope.tenant, &scope.template, &version)
                        .await?
                        .is_none()
                    {
                        return Err(Error::Unavailable(format!(
                            "desired template version {version} does not exist"
                        )));
                    }
                    let environment =
                        EnvironmentRef::new(version, format!("adx-{}", uuid::Uuid::new_v4()));
                    binding.secondary = Some(SecondaryEnv {
                        environment,
                        intent: SecondaryIntent::Preparing,
                    });
                }
            }
            let mut checks = vec![check(&key, &record)];
            checks.extend(alias_check);
            if self
                .store
                .commit(&Transaction::new(checks, vec![put(&key, &binding)?])?)
                .await?
            {
                return Ok(binding);
            }
        }
        Err(Error::Conflict("binding preparation contention".into()))
    }

    /// Caller must observe the candidate Ready before requesting promotion.
    pub async fn switch_current(&self, prepared: &AgentBinding) -> Result<AgentBinding> {
        let key = binding_key(&prepared.scope)?;
        let record = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("binding missing".into()))?;
        let mut binding: AgentBinding = record.decode()?;
        let (version, alias_check) = self.desired_version(&binding.scope).await?;
        let secondary = binding
            .secondary
            .as_ref()
            .ok_or_else(|| Error::Conflict("candidate missing".into()))?;
        if binding.generation != prepared.generation
            || binding.phase != BindingPhase::Active
            || binding.current != prepared.current
            || binding.secondary != prepared.secondary
            || secondary.intent != SecondaryIntent::Preparing
            || secondary.environment.version != version
        {
            return Err(Error::Conflict(
                "candidate or desired version changed".into(),
            ));
        }
        let mut secondary = binding
            .secondary
            .take()
            .ok_or_else(|| Error::Conflict("candidate missing".into()))?;
        std::mem::swap(&mut binding.current, &mut secondary.environment);
        secondary.intent = SecondaryIntent::Retiring;
        binding.secondary = Some(secondary);
        let mut checks = vec![check(&key, &record)];
        checks.extend(alias_check);
        if !self
            .store
            .commit(&Transaction::new(checks, vec![put(&key, &binding)?])?)
            .await?
        {
            return Err(Error::Conflict("binding changed during switch".into()));
        }
        Ok(binding)
    }

    /// Persist cleanup intent for exactly the candidate observed by the caller.
    /// A changed generation, current or secondary returns Conflict without retiring another Env.
    pub async fn retire_secondary(&self, saved: &AgentBinding) -> Result<AgentBinding> {
        let key = binding_key(&saved.scope)?;
        let record = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("binding missing".into()))?;
        let mut binding: AgentBinding = record.decode()?;
        if binding.generation != saved.generation
            || binding.phase != BindingPhase::Active
            || binding.current != saved.current
            || binding.secondary != saved.secondary
        {
            return Err(Error::Conflict(
                "candidate changed before retirement".into(),
            ));
        }
        let secondary = binding
            .secondary
            .as_mut()
            .ok_or_else(|| Error::Conflict("candidate missing".into()))?;
        secondary.intent = SecondaryIntent::Retiring;
        if !self
            .store
            .commit(&Transaction::new(
                vec![check(&key, &record)],
                vec![put(&key, &binding)?],
            )?)
            .await?
        {
            return Err(Error::Conflict(
                "candidate changed before retirement".into(),
            ));
        }
        Ok(binding)
    }

    /// Clear exactly the retired reference whose deletion and associations were confirmed.
    pub async fn clear_secondary(&self, retired: &AgentBinding) -> Result<()> {
        if retired
            .secondary
            .as_ref()
            .is_none_or(|s| s.intent != SecondaryIntent::Retiring)
        {
            return Err(Error::Conflict("reference is not retiring".into()));
        }
        let key = binding_key(&retired.scope)?;
        let record = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("binding missing".into()))?;
        let mut binding: AgentBinding = record.decode()?;
        if binding.generation != retired.generation || binding.secondary != retired.secondary {
            return Err(Error::Conflict("retired reference changed".into()));
        }
        if let Some(secondary) = &binding.secondary {
            binding
                .creation_attempts
                .remove(&secondary.environment.environment_id);
        }
        binding.secondary = None;
        if !self
            .store
            .commit(&Transaction::new(
                vec![check(&key, &record)],
                vec![put(&key, &binding)?],
            )?)
            .await?
        {
            return Err(Error::Conflict("binding changed during cleanup".into()));
        }
        Ok(())
    }

    /// Defer the saved creation attempt. Only a confirmed pre-assignment cancellation
    /// may set `cancelled`; an active previous Env observation keeps the request identity.
    /// Changed lifecycles/references return Conflict; an advanced attempt is reused.
    pub async fn defer_creation(
        &self,
        saved: &AgentBinding,
        environment: &EnvironmentRef,
        expected: &adx_agent_core::CreationAttempt,
        cancelled: bool,
        now_unix_ms: u64,
    ) -> Result<AgentBinding> {
        let key = binding_key(&saved.scope)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("binding missing".into()))?;
            let mut binding: AgentBinding = record.decode()?;
            if binding.generation != saved.generation
                || binding.phase != BindingPhase::Active
                || !(binding.current == *environment
                    || binding.secondary.as_ref().is_some_and(|s| {
                        s.intent == SecondaryIntent::Preparing && s.environment == *environment
                    }))
            {
                return Err(Error::Conflict("creation reference changed".into()));
            }
            let current = binding
                .creation_attempts
                .get(&environment.environment_id)
                .cloned()
                .unwrap_or_default();
            if current != *expected {
                return Ok(binding);
            }
            let number = if cancelled {
                current
                    .number
                    .checked_add(1)
                    .ok_or_else(|| Error::Conflict("creation attempt exhausted".into()))?
            } else {
                current.number
            };
            let backoff_seconds = match current.backoff_seconds {
                0 => 5,
                n => n.saturating_mul(2).min(60),
            };
            let retry_after_unix_ms = now_unix_ms
                .checked_add(backoff_seconds * 1000)
                .ok_or_else(|| Error::Invalid("creation retry time overflow".into()))?;
            binding.creation_attempts.insert(
                environment.environment_id.clone(),
                adx_agent_core::CreationAttempt {
                    number,
                    retry_after_unix_ms,
                    backoff_seconds,
                },
            );
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &binding)?],
                )?)
                .await?
            {
                return Ok(binding);
            }
        }
        Err(Error::Conflict("creation attempt contention".into()))
    }

    pub async fn binding_records(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, AgentBinding)>> {
        self.store
            .page(index, after, limit)
            .await?
            .into_iter()
            .map(|(key, record)| Ok((key, record.decode()?)))
            .collect()
    }

    /// Fix Volume UIDs before creating an Environment; merge only within the same lifecycle.
    pub async fn fix_volumes(
        &self,
        saved: &AgentBinding,
        references: &std::collections::BTreeMap<String, String>,
    ) -> Result<AgentBinding> {
        let key = binding_key(&saved.scope)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("binding missing".into()))?;
            let mut binding: AgentBinding = record.decode()?;
            if binding.generation != saved.generation || binding.phase != BindingPhase::Active {
                return Err(Error::Conflict("binding lifecycle changed".into()));
            }
            for (id, uid) in references {
                if binding.volumes.get(id).is_some_and(|fixed| fixed != uid) {
                    return Err(Error::Conflict("fixed Volume UID changed".into()));
                }
                binding.volumes.insert(id.clone(), uid.clone());
            }
            let old: AgentBinding = record.decode()?;
            if binding.volumes == old.volumes {
                return Ok(binding);
            }
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &binding)?],
                )?)
                .await?
            {
                return Ok(binding);
            }
        }
        Err(Error::Conflict("Volume reference contention".into()))
    }
}

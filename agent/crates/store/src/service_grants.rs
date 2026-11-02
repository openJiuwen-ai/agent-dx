//! Service credential records refer to authoritative AgentBinding scope/generation only.
use crate::{state::binding_key, AgentState, Check, Error, Key, Put, Record, Result, Transaction};
use adx_agent_core::{
    limits,
    service_grants::{
        ServiceAccess, ServiceGrant, CREDENTIAL_AUDIENCE, MAX_GRANT_SECONDS, ORBIT_CONFIG_PURPOSE,
    },
    AgentBinding, BindingPhase, Scope,
};

fn grant_key(digest: &str) -> Result<Key> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(Error::Invalid("invalid service credential digest".into()));
    }
    Key::new("service-grant", &[digest])
}

fn access(binding: &AgentBinding, purpose: &str) -> Result<ServiceAccess> {
    let workspace = binding
        .parameters
        .get("workspace")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Invalid("AgentBinding has no workspace parameter".into()))?;
    let access = ServiceAccess {
        tenant: binding.scope.tenant.clone(),
        workspace: workspace.clone(),
        purpose: purpose.into(),
    };
    access.validate().map_err(Error::Invalid)?;
    Ok(access)
}

impl AgentState {
    /// Atomically fence issuance against deletion/replacement. Ownership comes from the stored Binding.
    pub async fn create_service_grant(
        &self,
        scope: &Scope,
        generation: &str,
        digest: &str,
        purpose: &str,
        expires: u64,
        now: u64,
    ) -> Result<ServiceAccess> {
        let key = grant_key(digest)?;
        if purpose != ORBIT_CONFIG_PURPOSE
            || expires <= now
            || expires > now.saturating_add(MAX_GRANT_SECONDS)
            || expires > 253_402_300_799
        {
            return Err(Error::Invalid(
                "invalid service grant purpose or expiry".into(),
            ));
        }
        let binding_key = binding_key(scope)?;
        let record = self
            .store
            .get(&binding_key)
            .await?
            .ok_or_else(|| Error::Conflict("AgentBinding missing".into()))?;
        let binding: AgentBinding = record.decode()?;
        if &binding.scope != scope
            || binding.phase != BindingPhase::Active
            || binding.generation != generation
        {
            return Err(Error::Conflict(
                "AgentBinding changed or is deleting".into(),
            ));
        }
        let context = access(&binding, purpose)?;
        let mut value = Record::new(&ServiceGrant {
            scope: scope.clone(),
            generation: generation.into(),
            audience: CREDENTIAL_AUDIENCE.into(),
            purpose: purpose.into(),
            expires_at_unix_seconds: expires,
        })?;
        value.expires_at_unix_seconds = Some(expires);
        let committed = self
            .store
            .commit(&Transaction::new(
                vec![
                    Check {
                        key: binding_key,
                        expected: Some(record.revision),
                    },
                    Check {
                        key: key.clone(),
                        expected: None,
                    },
                ],
                vec![Put { key, record: value }],
            )?)
            .await?;
        if !committed {
            return Err(Error::Conflict(
                "AgentBinding changed or service credential already exists".into(),
            ));
        }
        Ok(context)
    }

    pub async fn authorize_service_grant(
        &self,
        digest: &str,
        expected: &ServiceAccess,
        now: u64,
    ) -> Result<bool> {
        let Some(record) = self.store.get(&grant_key(digest)?).await? else {
            return Ok(false);
        };
        let grant: ServiceGrant = record.decode()?;
        if grant.audience != CREDENTIAL_AUDIENCE
            || grant.purpose != ORBIT_CONFIG_PURPOSE
            || grant.purpose != expected.purpose
            || grant.expires_at_unix_seconds <= now
        {
            return Ok(false);
        }
        let Some(binding) = self.binding(&grant.scope).await? else {
            return Ok(false);
        };
        if binding.scope != grant.scope
            || binding.phase != BindingPhase::Active
            || binding.generation != grant.generation
        {
            return Ok(false);
        }
        Ok(access(&binding, &grant.purpose)? == *expected)
    }

    pub async fn revoke_service_grant(&self, digest: &str) -> Result<()> {
        let key = grant_key(digest)?;
        for _ in 0..limits::CAS_ATTEMPTS {
            let Some(record) = self.store.get(&key).await? else {
                return Ok(());
            };
            if self
                .store
                .commit(&Transaction::with_deletes(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(record.revision),
                    }],
                    vec![],
                    vec![key.clone()],
                )?)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Conflict(
            "service credential changed during revocation".into(),
        ))
    }
}

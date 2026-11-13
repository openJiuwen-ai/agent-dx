//! Formal process assembly. API keys come only from the existing encrypted owner.
use super::super::{connector_owner::OwnerAuthority, database::DatabaseConfig};
use super::{
    client::Client, owner::M44Authority, Composio, Error, MailAuthConfigs, PostgresBudget,
    RequestBudgetLimits,
};
use crate::ingress::server::Ingress;
use adx_agent_api::managed::ManagedService;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComposioConfig {
    pub auth_configs: MailAuthConfigs,
    pub organization: String,
    pub database: DatabaseConfig,
    pub limits: RequestBudgetLimits,
}

impl ComposioConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.auth_configs.validate()?;
        self.limits.validate()?;
        if !self.database.valid()
            || self.organization.is_empty()
            || self.organization.len() > 256
            || self.organization.chars().any(char::is_control)
        {
            return Err(Error::NotConfigured);
        }
        Ok(())
    }

    pub(crate) async fn prepare(
        &self,
        key: String,
        owner: Arc<OwnerAuthority>,
        managed: Arc<ManagedService>,
    ) -> Result<PreparedComposio, Error> {
        let key = zeroize::Zeroizing::new(key);
        self.validate()?;
        let pool = self
            .database
            .open()
            .await
            .map_err(|_| Error::NotConfigured)?;
        // Operator applies budget.sql; startup never creates a schema or substitutes
        // an unlimited/per-replica budget when shared accounting is unavailable.
        let budget = Arc::new(
            PostgresBudget::from_pool(pool, self.organization.clone(), self.limits.clone()).await?,
        );
        Ok(PreparedComposio {
            client: Client::new(key.to_string(), self.auth_configs.clone(), budget)?,
            owner,
            managed,
            configs: self.auth_configs.clone(),
        })
    }
}

pub(crate) struct PreparedComposio {
    client: Client,
    owner: Arc<OwnerAuthority>,
    managed: Arc<ManagedService>,
    configs: MailAuthConfigs,
}

impl PreparedComposio {
    pub(crate) fn bind(&self, ingress: Arc<Ingress>) -> Composio {
        // The process client, HTTP pool and PG budget are shared. Bind only the
        // existing request's Relay handle; retaining it on the API would cycle.
        Composio {
            client: self.client.clone(),
            authority: Arc::new(M44Authority::new(
                self.owner.clone(),
                self.managed.clone(),
                ingress,
                self.configs.clone(),
            )),
        }
    }
}

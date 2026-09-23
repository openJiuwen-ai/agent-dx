//! Stateless Environment activation. Platform owns Sandbox lifecycle and readiness.
pub mod server;
pub mod transport;
pub use adx_agent_core::error::{Error, Result};
use adx_agent_core::sandbox::{CreateSandbox, Sandbox, SandboxObservation, SandboxPhase};
use adx_agent_core::{activator::Target, Environment, EnvironmentPhase, Scope, TemplateVersion};
use adx_agent_store::AgentState;
use std::sync::Arc;

pub struct Activator {
    state: AgentState,
    sandbox: Arc<dyn Sandbox>,
}
impl Activator {
    pub fn new(state: AgentState, sandbox: Arc<dyn Sandbox>) -> Self {
        Self { state, sandbox }
    }
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        self.sandbox.validate_execution(&template.into())?;
        self.state.publish(tenant, template).await?;
        Ok(())
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        self.state
            .template(tenant, name, version)
            .await?
            .ok_or(Error::NotFound)
    }
    pub async fn environment(&self, scope: &Scope) -> Result<Environment> {
        self.state.environment(scope).await?.ok_or(Error::NotFound)
    }
    pub async fn list_environments(
        &self,
        query: &adx_agent_core::activator::EnvironmentList,
    ) -> Result<adx_agent_core::activator::EnvironmentPage> {
        query.validate().map_err(Error::Invalid)?;
        self.template(&query.tenant, &query.template, &query.version)
            .await?;
        Ok(self.state.list_environments(query).await?)
    }
    fn validate_observation(
        environment: &Environment,
        observed: &SandboxObservation,
    ) -> Result<()> {
        if observed.id != environment.sandbox_id || observed.tenant != environment.scope.tenant {
            return Err(Error::Unavailable(
                "Sandbox response identity mismatch".into(),
            ));
        }
        Ok(())
    }
    pub async fn activate(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
    ) -> Result<Target> {
        let template = self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?;
        let environment = match self.state.environment(scope).await? {
            Some(environment) => environment,
            None if expected_generation.is_some() => {
                return Err(Error::Conflict(
                    "selected Environment no longer exists".into(),
                ))
            }
            None => self.state.create_environment(scope.clone()).await?,
        };
        if expected_generation.is_some_and(|generation| generation != environment.generation) {
            return Err(Error::Conflict(
                "selected Environment lifecycle changed".into(),
            ));
        }
        if environment.phase != EnvironmentPhase::Active {
            return Err(Error::Conflict("environment is deleting".into()));
        }
        let observed = match self
            .sandbox
            .get(&scope.tenant, &environment.sandbox_id)
            .await?
        {
            Some(observed) => observed,
            None => {
                self.sandbox
                    .create(&CreateSandbox {
                        id: environment.sandbox_id.clone(),
                        tenant: scope.tenant.clone(),
                        execution: (&template).into(),
                        deadline_unix_ms: Some(deadline_unix_ms),
                    })
                    .await?
            }
        };
        Self::validate_observation(&environment, &observed)?;
        // Reject a delayed result after product deletion/recreation. Platform must also fence
        // create/delete ordering: a metadata check alone cannot revoke an already-sent create.
        let current = self.environment(scope).await?;
        if current != environment {
            return Err(Error::Conflict(
                "environment changed during activation".into(),
            ));
        }
        match observed.phase {
            SandboxPhase::Running if observed.ready => Ok(Target {
                environment,
                service: template.service,
            }),
            SandboxPhase::Creating | SandboxPhase::Running => Err(Error::NotReady(
                "Sandbox is not ready; retry the same Environment".into(),
            )),
            SandboxPhase::Failed => Err(Error::Conflict(
                "Sandbox failed; inspect Platform state".into(),
            )),
            SandboxPhase::Deleted => Err(Error::Conflict(
                "Sandbox was deleted; create a new Environment lifecycle".into(),
            )),
        }
    }
    pub async fn delete_environment(&self, scope: &Scope) -> Result<()> {
        self.environment(scope).await?;
        let environment = self.state.begin_delete(scope).await?;
        let observed = self
            .sandbox
            .delete(&scope.tenant, &environment.sandbox_id)
            .await?;
        Self::validate_observation(&environment, &observed)?;
        if observed.phase != SandboxPhase::Deleted {
            return Err(Error::OutcomeUnknown(
                "Platform has not confirmed deletion; retry this Environment deletion".into(),
            ));
        }
        self.state.finish_delete(&environment).await?;
        Ok(())
    }
}

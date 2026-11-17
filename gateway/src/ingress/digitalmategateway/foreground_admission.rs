//! Business-session identity for foreground connectors, without model or activation work.
use super::application_ws::{Application, ApplicationWsConfig};
use super::entrypoint::workspace_parameters;
use crate::ingress::accounts::{self, BusinessAuth, SessionGuard};
use adx_agent_api::{managed::ManagedService, request::RequestContext};
use adx_agent_core::{BindingPhase, Scope};
use http::HeaderMap;
use std::{sync::Arc, time::Duration};

/// Created only by the configured DigitalMate Gateway entrypoint.
/// Account credentials and caller-supplied routing headers cannot select scope.
#[derive(Clone)]
pub struct ForegroundAdmission {
    tenant: String,
    application: ApplicationWsConfig,
    managed: Arc<ManagedService>,
    auth: Arc<dyn BusinessAuth>,
}

/// Issued after real account/session checks. Not a serializable permission or a
/// background grant; consumers must recheck before their foreground operations.
#[derive(Clone)]
pub struct ForegroundIdentity {
    guard: Arc<SessionGuard>,
    scope: Scope,
    workspace: String,
}

impl ForegroundAdmission {
    pub(super) fn new(
        tenant: String,
        application: ApplicationWsConfig,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
    ) -> accounts::Result<Self> {
        application
            .validate(&tenant, Application::DigitalMate)
            .map_err(|_| accounts::Error::Configuration)?;
        Ok(Self {
            tenant,
            application,
            managed,
            auth,
        })
    }

    /// Authenticate one existing business session and resolve only its configured
    /// DigitalMate Binding metadata. Rejects missing/ambiguous bearer credentials,
    /// revoked sessions, unavailable metadata and mismatched/deleting bindings.
    /// Does not fetch a model key, activate an Environment or accept a body scope.
    pub async fn authenticate(&self, headers: &HeaderMap) -> accounts::Result<ForegroundIdentity> {
        let token = accounts::bearer(headers)?;
        let principal = tokio::time::timeout(accounts::IO_TIMEOUT, self.auth.authenticate(token))
            .await
            .map_err(|_| accounts::Error::Unavailable)??;
        let scope = self
            .application
            .scope(&self.tenant, &principal.user_id, Application::DigitalMate)
            .map_err(|_| accounts::Error::Unauthorized)?;
        let workspace =
            workspace_parameters(&self.tenant, &self.application.template, &principal.user_id)
                .map_err(|_| accounts::Error::Configuration)?
                .remove("workspace")
                .ok_or(accounts::Error::Configuration)?;
        let guard = Arc::new(SessionGuard {
            auth: self.auth.clone(),
            principal,
        });
        guard.check().await?;
        let context = RequestContext::new(Duration::from_secs(
            self.application.request_timeout_seconds,
        ));
        let view = self
            .managed
            .binding_view(&context, &scope)
            .await
            .map_err(|_| accounts::Error::Unavailable)?;
        if let Some(binding) = view.binding {
            if binding.phase != BindingPhase::Active
                || binding.scope != scope
                || binding.parameters.get("workspace") != Some(&workspace)
            {
                return Err(accounts::Error::Forbidden);
            }
        }
        // Revocation during the metadata read must not issue a foreground identity.
        guard.check().await?;
        Ok(ForegroundIdentity {
            guard,
            scope,
            workspace,
        })
    }
}

impl ForegroundIdentity {
    pub(super) fn principal(&self) -> &accounts::Principal {
        &self.guard.principal
    }

    pub fn tenant(&self) -> &str {
        &self.scope.tenant
    }
    pub fn user_id(&self) -> &str {
        &self.guard.principal.user_id
    }
    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn binding_scope(&self) -> &Scope {
        &self.scope
    }

    /// Re-read the actual account/session owner, including revocation and expiry.
    /// A detached identity does not extend the authenticated session's lifetime.
    pub async fn check(&self) -> accounts::Result<()> {
        self.guard.check().await
    }
}

#[cfg(test)]
#[path = "connector_owner_tests.rs"]
mod owner_purpose_tests;

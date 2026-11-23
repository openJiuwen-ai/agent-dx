//! Shared managed target selection and authorized, unsent service connection.
use super::server::{Ingress, IngressOpenError, IngressStream};
use adx_agent_api::{managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{BindingPhase, Protocol, Scope};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub struct ServiceSelector {
    pub protocol: Protocol,
    pub port: Option<u16>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceAccessError {
    #[error(transparent)]
    Agent(#[from] Error),
    #[error(transparent)]
    Ingress(#[from] IngressOpenError),
}

/// Selected logical Binding plus the original request's one pre-write retry budget.
/// Constructed only after ManagedService validates the scope and declared service.
#[derive(Clone)]
pub struct ServiceSelection {
    context: Arc<RequestContext>,
    scope: Scope,
    protocol: Protocol,
    port: u16,
    requested_port: Option<u16>,
    environment_id: String,
    generation: String,
    pub(super) created_environment: bool,
    retried: bool,
    parameters: Option<Vec<u8>>,
}
impl ServiceSelection {
    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        self.context.deadline()
    }

    pub(crate) fn forwarding_path(&self, backend_uri: &http::Uri) -> String {
        format!("/{}/{}{}", self.environment_id, self.port, backend_uri)
    }

    pub(crate) fn rewrite_request<B>(
        &self,
        request: &mut http::Request<B>,
        backend_uri: &http::Uri,
    ) -> Result<(), Error> {
        super::server::replace_request_path(request, &self.forwarding_path(backend_uri))
            .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))
    }
}

#[derive(Clone)]
pub struct AgentV2Access {
    managed: Arc<ManagedService>,
}
impl AgentV2Access {
    pub fn new(managed: Arc<ManagedService>) -> Self {
        Self { managed }
    }

    /// `scope.tenant` must be the caller's authenticated tenant. Selection validates
    /// the binding and declared service, then activates within the caller's deadline.
    /// Product validation, lifecycle and deadline errors are returned unchanged.
    pub async fn select(
        &self,
        context: Arc<RequestContext>,
        scope: Scope,
        selector: ServiceSelector,
    ) -> Result<ServiceSelection, Error> {
        self.select_with_parameters(context, scope, selector, None)
            .await
    }
    pub async fn select_with_parameters(
        &self,
        context: Arc<RequestContext>,
        scope: Scope,
        selector: ServiceSelector,
        parameters: Option<&[u8]>,
    ) -> Result<ServiceSelection, Error> {
        let resolved = context
            .run(self.managed.resolve_with_parameters(
                &context,
                &scope,
                selector.protocol,
                selector.port,
                parameters,
                false,
            ))
            .await?;
        let target = resolved.target;
        Ok(ServiceSelection {
            context,
            scope,
            protocol: selector.protocol,
            port: resolved.port,
            created_environment: resolved.created_environment,
            requested_port: selector.port,
            environment_id: target.binding.current.environment_id,
            generation: target.binding.generation,
            retried: false,
            parameters: parameters.map(<[u8]>::to_vec),
        })
    }

    /// Refresh once before writes. WS admission may reselect a deleted/rebuilt Binding;
    /// HTTP/SSH retain the original generation. All paths retain the original deadline.
    pub(crate) async fn retry(&self, selection: &mut ServiceSelection) -> Result<bool, Error> {
        if selection.retried {
            return Ok(false);
        }
        selection.retried = true;
        let resolved = selection
            .context
            .run(self.managed.retry_resolve(
                &selection.context,
                &selection.scope,
                selection.protocol,
                selection.requested_port,
                &selection.generation,
            ))
            .await;
        let resolved = match resolved {
            Err(error @ Error::Conflict(_)) if selection.protocol == Protocol::Ws => {
                let changed = match selection
                    .context
                    .run(self.managed.binding(&selection.context, &selection.scope))
                    .await
                {
                    Ok(binding) => {
                        binding.scope == selection.scope
                            && binding.phase == BindingPhase::Active
                            && binding.generation != selection.generation
                    }
                    Err(Error::NotFound) => true,
                    Err(error) => return Err(error),
                };
                if !changed {
                    return Err(error);
                }
                // No WS handshake or business bytes have been sent. Reuse this
                // admission's initialization input only if the Binding is missing.
                selection
                    .context
                    .run(self.managed.resolve_with_parameters(
                        &selection.context,
                        &selection.scope,
                        selection.protocol,
                        selection.requested_port,
                        selection.parameters.as_deref(),
                        true,
                    ))
                    .await?
            }
            result => result?,
        };
        let target = resolved.target;
        selection.created_environment = resolved.created_environment;
        selection.environment_id = target.binding.current.environment_id;
        selection.generation = target.binding.generation;
        selection.port = resolved.port;
        Ok(true)
    }

    /// Connect a selected service through the existing authorized Relay path.
    /// Only route/open failures may refresh selection once. No application bytes
    /// are written here, and the returned stream never replays or reconnects itself.
    /// Returns typed lifecycle/deadline or route/authorization/connection errors.
    /// HTTP callers can instead retain selection while using their connection pool.
    pub async fn connect_service(
        &self,
        ingress: &Ingress,
        selection: ServiceSelection,
    ) -> Result<IngressStream, ServiceAccessError> {
        let mut selection = selection;
        loop {
            let result = selection
                .context
                .run(async {
                    Ok(ingress
                        .open_selected_service(
                            &selection.environment_id,
                            selection.port,
                            &selection.scope.tenant,
                            selection.created_environment,
                            selection.deadline(),
                        )
                        .await)
                })
                .await?;
            match result {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    let retryable = !matches!(
                        error,
                        IngressOpenError::Forbidden
                            | IngressOpenError::Auth(_)
                            | IngressOpenError::Draining
                    ) && !matches!(&error, IngressOpenError::Connect(error) if error.kind() == std::io::ErrorKind::PermissionDenied);
                    if retryable && self.retry(&mut selection).await? {
                        continue;
                    }
                    return Err(error.into());
                }
            }
        }
    }
}

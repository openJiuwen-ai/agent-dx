//! Shared managed target selection and authorized, unsent service connection.
use super::resolver::AccessKind;
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
    environment_id: String,
    generation: String,
    retried: bool,
    parameters: Option<Vec<u8>>,
}
impl ServiceSelection {
    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        self.context.deadline()
    }

    pub(crate) fn websocket_port(&self) -> Result<u16, Error> {
        if self.protocol != Protocol::Ws {
            return Err(Error::Invalid("selected service is not WebSocket".into()));
        }
        Ok(self.port)
    }

    pub(crate) fn forwarding_uri(&self, backend_uri: &http::Uri) -> Result<http::Uri, Error> {
        format!("/{}/{}{}", self.environment_id, self.port, backend_uri)
            .parse()
            .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))
    }
}

/// Preserve a business initializer's authentication errors without coupling shared activation to it.
pub enum InitializationError<E> {
    Agent(Error),
    Source(E),
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
        let (target, port) = context
            .run(self.managed.resolve_with_parameters(
                &context,
                &scope,
                selector.protocol,
                selector.port,
                parameters,
                false,
            ))
            .await?;
        Ok(ServiceSelection {
            context,
            scope,
            protocol: selector.protocol,
            port,
            environment_id: target.binding.environment_id,
            generation: target.binding.generation,
            retried: false,
            parameters: parameters.map(<[u8]>::to_vec),
        })
    }

    /// Business adapters provide parameters lazily; Binding decisions stay in the shared flow.
    pub async fn select_with_initializer<F, E>(
        &self,
        context: Arc<RequestContext>,
        scope: Scope,
        selector: ServiceSelector,
        initializer: F,
    ) -> Result<ServiceSelection, InitializationError<E>>
    where
        F: std::future::Future<Output = Result<adx_agent_core::mounts::Parameters, E>>,
    {
        let parameters = match context.run(self.managed.binding(&context, &scope)).await {
            Ok(_) => None,
            Err(Error::NotFound) => {
                let values = tokio::time::timeout_at(context.deadline(), initializer)
                    .await
                    .map_err(|_| {
                        InitializationError::Agent(Error::Unavailable(
                            "initialization deadline exceeded".into(),
                        ))
                    })?
                    .map_err(InitializationError::Source)?;
                Some(adx_agent_core::parameters::encode(&values))
            }
            Err(error) => return Err(InitializationError::Agent(error)),
        };
        self.select_with_parameters(context, scope, selector, parameters.as_deref())
            .await
            .map_err(InitializationError::Agent)
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
                selection.port,
                &selection.generation,
            ))
            .await;
        let (target, port) = match resolved {
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
                        Some(selection.port),
                        selection.parameters.as_deref(),
                        true,
                    ))
                    .await?
            }
            result => result?,
        };
        selection.environment_id = target.binding.environment_id;
        selection.generation = target.binding.generation;
        selection.port = port;
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
        self.connect_service_inner(ingress, selection, false).await
    }

    /// Jiuwen cold starts can publish a runtime before its business port listens.
    /// Wait only for TCP refusal, before sending any application bytes, using the
    /// original admission deadline. Handshake failures are never replayed.
    pub(crate) async fn connect_service_when_ready(
        &self,
        ingress: &Ingress,
        selection: ServiceSelection,
    ) -> Result<IngressStream, ServiceAccessError> {
        self.connect_service_inner(ingress, selection, true).await
    }

    async fn connect_service_inner(
        &self,
        ingress: &Ingress,
        mut selection: ServiceSelection,
        wait_for_listener: bool,
    ) -> Result<IngressStream, ServiceAccessError> {
        loop {
            let result = selection
                .context
                .run(async {
                    let attempt = async {
                        let route = ingress
                            .resolve_route(
                                &selection.environment_id,
                                selection.port,
                                AccessKind::Direct,
                                String::new(),
                            )
                            .await?;
                        loop {
                            let opened = ingress
                                .open_resolved_stream(route.clone(), &selection.scope.tenant)
                                .await;
                            if wait_for_listener
                                && matches!(&opened, Err(IngressOpenError::Connect(error))
                                    if error.kind() == std::io::ErrorKind::ConnectionRefused)
                            {
                                // No route refresh or activation for a port that is merely
                                // starting. The surrounding context also bounds this sleep.
                                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                                continue;
                            }
                            return opened;
                        }
                    }
                    .await;
                    Ok(attempt)
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

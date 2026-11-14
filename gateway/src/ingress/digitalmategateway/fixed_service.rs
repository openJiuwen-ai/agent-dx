//! DigitalMate's existing-Binding selection through the published Agent owner.
//! No missing Binding creation, admission reselection or application-byte retry.
use crate::ingress::{
    agent_service::ServiceAccessError,
    resolver::AccessKind,
    server::{Ingress, IngressOpenError, IngressStream},
};
use adx_agent_api::{managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{BindingPhase, Protocol, Scope};
use std::{sync::Arc, time::Duration};

pub(super) struct FixedServiceAccess {
    managed: Arc<ManagedService>,
}

pub(super) struct FixedSelection {
    context: Arc<RequestContext>,
    scope: Scope,
    generation: String,
    environment_id: String,
    port: u16,
}

impl FixedSelection {
    pub(super) fn matches_target(&self, environment_id: &str, port: u16) -> bool {
        self.environment_id == environment_id && self.port == port
    }
}

impl FixedServiceAccess {
    pub(super) fn new(managed: Arc<ManagedService>) -> Self {
        Self { managed }
    }

    pub(super) async fn select(
        &self,
        context: Arc<RequestContext>,
        scope: Scope,
        port: u16,
        expected_generation: &str,
    ) -> Result<FixedSelection, Error> {
        if expected_generation.is_empty() || port == 0 {
            return Err(Error::Invalid(
                "existing HTTP Binding target required".into(),
            ));
        }
        let (target, port) = context
            .run(self.managed.retry_resolve(
                &context,
                &scope,
                Protocol::Http,
                Some(port),
                expected_generation,
            ))
            .await?;
        if target.binding.generation != expected_generation {
            return Err(Error::Conflict(
                "DigitalMate Binding generation changed".into(),
            ));
        }
        Ok(FixedSelection {
            context,
            scope,
            generation: target.binding.generation,
            environment_id: target.binding.current.environment_id,
            port,
        })
    }

    /// Connect only to the selected route. TCP-refused readiness waits consume
    /// the original deadline; no application bytes or alternate Binding are used.
    pub(super) async fn connect(
        &self,
        ingress: &Ingress,
        selection: &FixedSelection,
    ) -> Result<IngressStream, ServiceAccessError> {
        selection
            .context
            .run(async {
                let opened = async {
                    let route = ingress
                        .resolve_route(
                            &selection.environment_id,
                            selection.port,
                            AccessKind::Direct,
                            String::new(),
                        )
                        .await?;
                    loop {
                        let stream = ingress
                            .open_resolved_stream(route.clone(), &selection.scope.tenant)
                            .await;
                        match &stream {
                            Err(IngressOpenError::Connect(error))
                                if error.kind() == std::io::ErrorKind::ConnectionRefused =>
                            {
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                            _ => return stream,
                        }
                    }
                }
                .await;
                Ok(opened)
            })
            .await?
            .map_err(ServiceAccessError::from)
    }

    /// Callers explicitly fence the target after readiness and before HTTP.
    pub(super) async fn revalidate(&self, selection: &FixedSelection) -> Result<(), Error> {
        let current = selection
            .context
            .run(self.managed.binding(&selection.context, &selection.scope))
            .await?;
        if current.scope != selection.scope
            || current.generation != selection.generation
            || current.phase != BindingPhase::Active
            || current.current.environment_id != selection.environment_id
        {
            return Err(Error::Conflict(
                "DigitalMate Binding changed during readiness".into(),
            ));
        }
        Ok(())
    }
}

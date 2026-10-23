//! Session admission for a fixed template; transport stays in shared Gateway forwarding.
use super::entrypoint::{agent_error, auth_error, error};
use crate::ingress::{
    accounts::{BusinessAuth, Principal, SessionGuard},
    agent_service::{AgentV2Access, InitializationError, ServiceAccessError, ServiceSelector},
    server::{proxy_http, wait_for_route_cancellation, Ingress, ProxyBody},
};
use adx_agent_api::{managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{Protocol, Scope};
use http::{Request, Response, StatusCode, Uri};
use hyper::body::Incoming;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrbitWsConfig {
    #[serde(default = "super::entrypoint::request_timeout")]
    pub request_timeout_seconds: u64,
    pub template: String,
    pub version: String,
    pub backend_path: String,
    pub service_port: u16,
}

fn invalid() -> Error {
    Error::Invalid("invalid Orbit configuration or user identity".into())
}

impl OrbitWsConfig {
    pub(super) fn scope(&self, tenant: &str, user_id: &str) -> Result<Scope, Error> {
        for value in [tenant, user_id] {
            adx_agent_core::identifier(value, "Orbit WebSocket identity")
                .map_err(Error::Invalid)?;
            if value.len() > 256 {
                return Err(invalid());
            }
        }
        let tuple = serde_json::to_vec(&[tenant, user_id]).map_err(|_| invalid())?;
        let scope = Scope {
            tenant: tenant.into(),
            template: self.template.clone(),
            version: self.version.clone(),
            binding_id: format!("orbit-{}", hex::encode(Sha256::digest(tuple))),
        };
        scope.validate().map_err(Error::Invalid)?;
        Ok(scope)
    }

    pub(super) fn validate(&self, tenant: &str) -> Result<(), Error> {
        if !(1..=86400).contains(&self.request_timeout_seconds) {
            return Err(invalid());
        }
        self.scope(tenant, "validation")?;
        let uri: Uri = self.backend_path.parse().map_err(|_| invalid())?;
        if self.service_port == 0
            || !self.backend_path.starts_with('/')
            || self.backend_path.starts_with("//")
            || uri.scheme().is_some()
            || uri.authority().is_some()
            || uri.query().is_some()
            || uri.path() != self.backend_path
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn destination(&self, query: Option<&str>) -> Result<Uri, Error> {
        let destination = match query {
            Some(query) => format!("{}?{query}", self.backend_path),
            None => self.backend_path.clone(),
        };
        destination.parse().map_err(|_| invalid())
    }

    pub(super) async fn handle(
        &self,
        mut request: Request<Incoming>,
        ingress: Arc<Ingress>,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
        principal: Principal,
        tenant: &str,
    ) -> Response<ProxyBody> {
        if request.method() != http::Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        let destination = match self.destination(request.uri().query()) {
            Ok(value) => value,
            Err(e) => return agent_error(e),
        };
        let scope = match self.scope(tenant, &principal.user_id) {
            Ok(value) => value,
            Err(e) => return agent_error(e),
        };
        let mut handshake = Request::new(());
        *handshake.method_mut() = request.method().clone();
        *handshake.uri_mut() = request.uri().clone();
        *handshake.version_mut() = request.version();
        *handshake.headers_mut() = request.headers().clone();
        if tokio_tungstenite::tungstenite::handshake::server::create_response(&handshake).is_err() {
            return error(StatusCode::BAD_REQUEST, "INVALID_WEBSOCKET_HANDSHAKE");
        }
        let context = Arc::new(RequestContext::new(Duration::from_secs(
            self.request_timeout_seconds,
        )));
        let deadline = context.deadline();
        let guard = Arc::new(SessionGuard {
            auth: auth.clone(),
            principal: principal.clone(),
        });
        let mut session = Box::pin({
            let guard = guard.clone();
            async move { guard.watch().await }
        });
        let access = AgentV2Access::new(managed);
        let admission = async {
            let selection = access
                .select_with_initializer(
                    context,
                    scope,
                    ServiceSelector {
                        protocol: Protocol::Ws,
                        port: Some(self.service_port),
                    },
                    async {
                        let workspace = super::entrypoint::workspace_parameters(
                            tenant,
                            &self.template,
                            &principal.user_id,
                        )
                        .map_err(|_| crate::ingress::accounts::Error::Invalid)?;
                        let mut parameters = auth.model_parameters(&principal).await?;
                        parameters.extend(workspace);
                        Ok(parameters)
                    },
                )
                .await
                .map_err(|e| match e {
                    InitializationError::Agent(e) => agent_error(e),
                    InitializationError::Source(e) => auth_error(e),
                })?;
            access
                .connect_service_when_ready(&ingress, selection)
                .await
                .map_err(|e| match e {
                    ServiceAccessError::Agent(e) => agent_error(e),
                    ServiceAccessError::Ingress(_) => {
                        error(StatusCode::BAD_GATEWAY, "AGENT_SERVER_UNAVAILABLE")
                    }
                })
        };
        let stream = tokio::select! {
            biased;
            _ = &mut session => return end_response(End::Session),
            _ = tokio::time::sleep_until(deadline) => return error(StatusCode::GATEWAY_TIMEOUT, "AGENT_SERVER_UNAVAILABLE"),
            result = admission => match result { Ok(value) => value, Err(response) => return response },
        };
        let route = stream.cancellation();
        tokio::select! {
            biased;
            _ = wait_for_route_cancellation(Some(route.clone())) => return end_response(End::Route),
            _ = &mut session => return end_response(End::Session),
            _ = tokio::time::sleep_until(deadline) => return error(StatusCode::GATEWAY_TIMEOUT, "AGENT_SERVER_UNAVAILABLE"),
            result = guard.check() => if let Err(e) = result { return auth_error(e); },
        }
        let (ended, cancelled) = watch::channel(false);
        let (cause, reason) = watch::channel(End::Active);
        tokio::spawn(async move {
            let why = tokio::select! {
                biased;
                _ = wait_for_route_cancellation(Some(route)) => End::Route,
                _ = session => End::Session,
                _ = ended.closed() => return,
            };
            cause.send_replace(why);
            ended.send_replace(true);
        });
        *request.uri_mut() = destination;
        let names: Vec<_> = request
            .headers()
            .keys()
            .filter(|name| {
                name.as_str().starts_with("x-adx-")
                    || matches!(
                        name.as_str(),
                        "authorization" | "proxy-authorization" | "cookie" | "x-auth" | "x-user-id"
                    )
            })
            .cloned()
            .collect();
        for name in names {
            request.headers_mut().remove(name);
        }
        let response = tokio::select! {
            biased;
            why = wait_for_end(reason.clone()) => end_response(why),
            _ = tokio::time::sleep_until(deadline) => error(StatusCode::GATEWAY_TIMEOUT, "AGENT_SERVER_UNAVAILABLE"),
            response = proxy_http(request, stream, "environment", Some(cancelled)) => response,
        };
        if *reason.borrow() != End::Active {
            return end_response(*reason.borrow());
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OrbitWsConfig {
        OrbitWsConfig {
            request_timeout_seconds: 60,
            template: "orbit".into(),
            version: "1".into(),
            backend_path: "/ws".into(),
            service_port: 18092,
        }
    }

    #[test]
    fn orbit_binding_identity_reuses_and_isolates_users() {
        let config = config();
        let alice = config.scope("tenant", "alice").unwrap();
        assert_eq!(alice, config.scope("tenant", "alice").unwrap());
        for other in [
            config.scope("tenant", "bob").unwrap(),
            config.scope("other", "alice").unwrap(),
        ] {
            assert_ne!(alice.binding_id, other.binding_id);
        }
        assert_eq!(alice.template, "orbit");
        assert_eq!(alice.version, "1");
        let mut next = config.clone();
        next.version = "2".into();
        assert_eq!(
            alice.binding_id,
            next.scope("tenant", "alice").unwrap().binding_id
        );
        assert_ne!(alice, next.scope("tenant", "alice").unwrap());
    }

    #[test]
    fn orbit_workspace_uses_the_same_rule_with_its_own_template() {
        use super::super::entrypoint::{workspace_parameters, DigitalMateGatewayConfig};
        let jiuwen: DigitalMateGatewayConfig = serde_json::from_str(
            r#"{"tenant":"tenant","jiuwen":{"agent_type":"assistant","template":"demo","version":"1"}}"#,
        ).unwrap();
        let same_template = workspace_parameters("tenant", "demo", "alice").unwrap();
        assert_eq!(same_template, jiuwen.parameters("alice").unwrap());
        let orbit = workspace_parameters("tenant", "orbit", "alice").unwrap();
        assert_ne!(orbit, same_template);
        assert_ne!(
            orbit,
            workspace_parameters("tenant", "orbit", "bob").unwrap()
        );
        assert_ne!(
            orbit,
            workspace_parameters("other", "orbit", "alice").unwrap()
        );
    }

    #[test]
    fn orbit_configuration_rejects_nonlocal_destinations() {
        for path in [
            "http://example/ws",
            "//example/ws",
            "ws",
            "/ws?token=x",
            "/ws#fragment",
        ] {
            let mut config = config();
            config.backend_path = path.into();
            assert!(config.validate("tenant").is_err(), "{path}");
        }
        let mut config = config();
        config.service_port = 0;
        assert!(config.validate("tenant").is_err());
    }

    #[test]
    fn orbit_route_revocation_is_not_an_authentication_failure() {
        assert_eq!(end_response(End::Route).status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            end_response(End::Session).status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn orbit_watcher_shutdown_is_not_route_revocation() {
        let (sender, receiver) = watch::channel(End::Active);
        drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), wait_for_end(receiver))
                .await
                .is_err()
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Active,
    Session,
    Route,
}

fn end_response(reason: End) -> Response<ProxyBody> {
    match reason {
        End::Session => auth_error(crate::ingress::accounts::Error::Unauthorized),
        End::Active | End::Route => error(StatusCode::BAD_GATEWAY, "ROUTE_REVOKED"),
    }
}

async fn wait_for_end(mut reason: watch::Receiver<End>) -> End {
    loop {
        let current = *reason.borrow_and_update();
        if current != End::Active {
            return current;
        }
        if reason.changed().await.is_err() {
            // A completed forwarding future drops its cancellation receivers.
            // Watcher shutdown alone is not evidence that a route was revoked.
            std::future::pending::<()>().await;
        }
    }
}

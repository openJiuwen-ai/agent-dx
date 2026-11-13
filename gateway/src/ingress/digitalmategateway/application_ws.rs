//! Session admission for a configured template reference; transport stays in shared Gateway forwarding.
use super::entrypoint::{agent_error, auth_error, error};
use crate::ingress::{
    accounts::SessionGuard,
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
pub struct ApplicationWsConfig {
    #[serde(default = "super::entrypoint::request_timeout")]
    pub request_timeout_seconds: u64,
    pub template: String,
    pub template_ref: String,
    pub backend_path: String,
    pub service_port: u16,
}

fn invalid() -> Error {
    Error::Invalid("invalid application WebSocket configuration or user identity".into())
}

#[derive(Clone, Copy)]
pub(super) enum Application {
    Orbit,
    DigitalMate,
}

impl Application {
    fn name(self) -> &'static str {
        match self {
            Self::Orbit => "orbit",
            Self::DigitalMate => "digitalmate",
        }
    }
}

impl ApplicationWsConfig {
    pub(super) fn scope(
        &self,
        tenant: &str,
        user_id: &str,
        application: Application,
    ) -> Result<Scope, Error> {
        for value in [tenant, user_id] {
            adx_agent_core::identifier(value, "application WebSocket identity")
                .map_err(Error::Invalid)?;
            if value.len() > 256 {
                return Err(invalid());
            }
        }
        let tuple = serde_json::to_vec(&[tenant, user_id]).map_err(|_| invalid())?;
        let scope = Scope {
            tenant: tenant.into(),
            template: self.template.clone(),
            template_ref: self.template_ref.parse().map_err(Error::Invalid)?,
            binding_id: format!(
                "{}-{}",
                application.name(),
                hex::encode(Sha256::digest(tuple))
            ),
        };
        scope.validate().map_err(Error::Invalid)?;
        Ok(scope)
    }

    pub(super) fn validate(&self, tenant: &str, application: Application) -> Result<(), Error> {
        if !(1..=86400).contains(&self.request_timeout_seconds) {
            return Err(invalid());
        }
        self.scope(tenant, "validation", application)?;
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
        guard: Arc<SessionGuard>,
        tenant: &str,
        #[cfg(unix)] privacy: Option<std::sync::Arc<super::privacy_runtime::PrivacyAccess>>,
    ) -> Response<ProxyBody> {
        let auth = guard.auth.clone();
        let principal = guard.principal.clone();
        if request.method() != http::Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        let destination = match self.destination(request.uri().query()) {
            Ok(value) => value,
            Err(e) => return agent_error(e),
        };
        let application = if request.uri().path() == "/digitalmate/ws" {
            Application::DigitalMate
        } else {
            Application::Orbit
        };
        let scope = match self.scope(tenant, &principal.user_id, application) {
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
        let mut session = Box::pin({
            let guard = guard.clone();
            async move { guard.watch().await }
        });
        #[cfg(unix)]
        let privacy_scope = scope.clone();
        let access = AgentV2Access::new(managed);
        #[cfg(unix)]
        let model_configuration = privacy
            .as_ref()
            .filter(|p| p.identity.targets(&self.template))
            .and_then(|p| p.model_configuration.clone());
        #[cfg(not(unix))]
        let model_mode = false;
        #[cfg(unix)]
        let model_mode = model_configuration.is_some();
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
                        let mut parameters = if model_mode {
                            Default::default()
                        } else {
                            auth.model_parameters(&principal).await?
                        };
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
        #[cfg(unix)]
        if let Some(identity) = privacy
            .as_ref()
            .filter(|access| access.identity.targets(&self.template))
        {
            let workspace = match super::entrypoint::workspace_parameters(
                tenant,
                &self.template,
                &principal.user_id,
            ) {
                Ok(values) => values,
                Err(error) => return agent_error(error),
            };
            let Some(workspace) = workspace.get("workspace") else {
                return error(StatusCode::BAD_GATEWAY, "PRIVACY_UNAVAILABLE");
            };
            if let Some((name, registry)) = &model_configuration {
                let registration = async {
                    let scope = super::privacy_center::ConfigurationScope {
                        tenant,
                        workspace,
                        user: &principal.user_id,
                    };
                    if registry.get(scope, name).await?.is_some() {
                        return auth.check(&principal).await;
                    }
                    let parameters = auth.model_parameters(&principal).await?;
                    let value = std::collections::BTreeMap::from([
                        (
                            "API_KEY".into(),
                            parameters
                                .get("model_api_key")
                                .ok_or(crate::ingress::accounts::Error::Unavailable)?
                                .clone(),
                        ),
                        (
                            "API_BASE".into(),
                            parameters
                                .get("model_api_base")
                                .ok_or(crate::ingress::accounts::Error::Unavailable)?
                                .clone(),
                        ),
                    ]);
                    registry.register(scope, name, &value).await?;
                    auth.check(&principal).await
                };
                let registration = tokio::select! {
                    biased;
                    why = wait_for_end(reason.clone()) => return end_response(why),
                    result = tokio::time::timeout_at(deadline, registration) => result,
                };
                match registration {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => return auth_error(error),
                    Err(_) => return error(StatusCode::GATEWAY_TIMEOUT, "PRIVACY_UNAVAILABLE"),
                }
            }
            let token = match tokio::time::timeout_at(
                deadline,
                identity
                    .identity
                    .admit(&privacy_scope, workspace, &principal.user_id),
            )
            .await
            {
                Ok(Ok(token)) => token,
                Ok(Err(error)) => return auth_error(error),
                Err(_) => return error(StatusCode::GATEWAY_TIMEOUT, "PRIVACY_UNAVAILABLE"),
            };
            let mut header = match format!("Bearer {}", token.as_str()).parse::<http::HeaderValue>()
            {
                Ok(header) => header,
                Err(_) => return error(StatusCode::BAD_GATEWAY, "PRIVACY_UNAVAILABLE"),
            };
            header.set_sensitive(true);
            request
                .headers_mut()
                .insert(http::header::AUTHORIZATION, header);
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

    fn config() -> ApplicationWsConfig {
        ApplicationWsConfig {
            request_timeout_seconds: 60,
            template: "orbit".into(),
            template_ref: "1".into(),
            backend_path: "/ws".into(),
            service_port: 18092,
        }
    }

    #[test]
    fn orbit_alias_reference_preserves_user_binding_and_workspace_identity() {
        let fixed = config()
            .scope("tenant", "alice", Application::Orbit)
            .unwrap();
        let config: ApplicationWsConfig = serde_json::from_value(serde_json::json!({
            "template": "orbit", "template_ref": "production",
            "backend_path": "/ws", "service_port": 18092
        }))
        .unwrap();
        config.validate("tenant", Application::Orbit).unwrap();
        let alias = config.scope("tenant", "alice", Application::Orbit).unwrap();
        assert_eq!(alias.binding_id, fixed.binding_id);
        assert_eq!(alias.template, fixed.template);
        assert_eq!(alias.tenant, fixed.tenant);
        assert_eq!(
            alias.template_ref,
            adx_agent_core::TemplateRef::Alias("production".into())
        );
        assert_ne!(
            alias.binding_id,
            config
                .scope("tenant", "bob", Application::Orbit)
                .unwrap()
                .binding_id
        );
        assert_ne!(
            alias.binding_id,
            config
                .scope("other", "alice", Application::Orbit)
                .unwrap()
                .binding_id
        );
        let workspace = super::super::entrypoint::workspace_parameters;
        assert_eq!(
            workspace(&alias.tenant, &alias.template, "alice").unwrap(),
            workspace(&fixed.tenant, &fixed.template, "alice").unwrap()
        );
    }

    #[test]
    fn application_template_reference_requires_one_valid_version_or_alias() {
        for reference in [serde_json::json!("1"), serde_json::json!("production")] {
            let config: ApplicationWsConfig = serde_json::from_value(serde_json::json!({
                "template": "orbit", "template_ref": reference,
                "backend_path": "/ws", "service_port": 18092
            }))
            .unwrap();
            for application in [Application::Orbit, Application::DigitalMate] {
                config.validate("tenant", application).unwrap();
            }
        }
        for reference in [
            serde_json::json!({}),
            serde_json::json!({"version": "1", "alias": "production"}),
            serde_json::json!(""),
            serde_json::json!("\n"),
        ] {
            let config = serde_json::from_value::<ApplicationWsConfig>(serde_json::json!({
                "template": "orbit", "template_ref": reference,
                "backend_path": "/ws", "service_port": 18092
            }));
            if let Ok(config) = config {
                for application in [Application::Orbit, Application::DigitalMate] {
                    assert!(config.validate("tenant", application).is_err());
                }
            }
        }
    }

    #[test]
    fn orbit_volume_template_keeps_existing_workspace_directory() {
        use adx_agent_core::{mounts::Parameters, volumes::VolumeSource};
        let draft: adx_agent_core::TemplateDraft = serde_json::from_str(include_str!(
            "../../../../agent/activator/examples/orbit-sfs-template.json"
        ))
        .unwrap();
        draft.validate().unwrap();
        let template = draft.with_version("1".into());
        let parameters = Parameters::from([
            ("workspace".into(), "w-existing-user".into()),
            ("model_api_base".into(), "https://model.example/v1".into()),
            ("model_api_key".into(), "synthetic-key".into()),
        ]);
        let volumes = template.render_volumes(&parameters).unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id, "orbit-workspace-w-existing-user");
        let VolumeSource::Directory {
            storage_domain,
            path,
        } = &volumes[0].source;
        assert_eq!(storage_domain, "sfs");
        assert_eq!(
            format!("/mnt/sfs/{path}"),
            "/mnt/sfs/workspaces/w-existing-user"
        );
        assert!(volumes[0].create_if_missing);
        let options = volumes[0].create_options.as_ref().unwrap();
        assert_eq!(
            (options.uid, options.gid, options.mode.as_str()),
            (10001, 10001, "0700")
        );
        let mounts = template.render_mounts(&parameters).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].volume_id.as_deref(), Some(volumes[0].id.as_str()));
        assert!(mounts[0].host_path.is_empty());
        assert_eq!(mounts[0].target, "/data/state");
        let mut next = template.clone();
        next.version = "2".into();
        assert_eq!(volumes, next.render_volumes(&parameters).unwrap());
    }

    #[test]
    fn orbit_binding_identity_reuses_and_isolates_users() {
        let config = config();
        let alice = config.scope("tenant", "alice", Application::Orbit).unwrap();
        assert_eq!(
            alice,
            config.scope("tenant", "alice", Application::Orbit).unwrap()
        );
        for other in [
            config.scope("tenant", "bob", Application::Orbit).unwrap(),
            config.scope("other", "alice", Application::Orbit).unwrap(),
        ] {
            assert_ne!(alice.binding_id, other.binding_id);
        }
        assert_eq!(alice.template, "orbit");
        let digitalmate = config
            .scope("tenant", "alice", Application::DigitalMate)
            .unwrap();
        assert_ne!(alice.binding_id, digitalmate.binding_id);
        assert!(digitalmate.binding_id.starts_with("digitalmate-"));
        assert_eq!(
            alice.template_ref,
            adx_agent_core::TemplateRef::Version("1".into())
        );
        let mut next = config.clone();
        next.template_ref = "2".into();
        assert_eq!(
            alice.binding_id,
            next.scope("tenant", "alice", Application::Orbit)
                .unwrap()
                .binding_id
        );
        assert_ne!(
            alice,
            next.scope("tenant", "alice", Application::Orbit).unwrap()
        );
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
    fn application_configuration_rejects_nonlocal_destinations() {
        for path in [
            "http://example/ws",
            "//example/ws",
            "ws",
            "/ws?token=x",
            "/ws#fragment",
        ] {
            let mut config = config();
            config.backend_path = path.into();
            for application in [Application::Orbit, Application::DigitalMate] {
                assert!(config.validate("tenant", application).is_err(), "{path}");
            }
        }
        let mut config = config();
        config.service_port = 0;
        for application in [Application::Orbit, Application::DigitalMate] {
            assert!(config.validate("tenant", application).is_err());
        }
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

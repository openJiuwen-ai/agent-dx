//! HTTP contract for the opt-in E2B API-key compatibility profile.
#[path = "support/http.rs"]
mod http;

use adx_apiserver::{clients::Clients, config::Config, http::Api};
use adx_protocol::control as pb;
use http::request as send_http_request;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tonic::{Request, Response, Status};

const TEAM: &str = "tenant-a";
const PUBLIC_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
const SECOND_PUBLIC_ID: &str = "550e8400-e29b-41d4-a716-446655440001";

#[derive(Default)]
struct State {
    admin_enabled: AtomicBool,
    revoked: AtomicBool,
    create_calls: AtomicUsize,
    list_calls: AtomicUsize,
    revoke_calls: AtomicUsize,
}

#[derive(Clone)]
struct Service(Arc<State>);

fn metadata(id: &str, name: &str) -> pb::CredentialMetadata {
    pb::CredentialMetadata {
        public_id: id.into(),
        name: name.into(),
        created_at_unix_ms: 1_700_000_000_000,
        prefix: "adx_".into(),
        value_length: 64,
        masked_value_prefix: "rr".into(),
        masked_value_suffix: "rrrr".into(),
    }
}

fn managed_key(id: &str, name: &str) -> pb::TenantKey {
    pb::TenantKey {
        id: "d".repeat(64),
        tenant_id: TEAM.into(),
        expires_at_unix_seconds: 0,
        metadata: Some(metadata(id, name)),
    }
}

#[tonic::async_trait]
impl pb::auth_service_server::AuthService for Service {
    async fn verify_api_key(
        &self,
        request: Request<pb::VerifyApiKeyRequest>,
    ) -> Result<Response<pb::VerifyApiKeyResponse>, Status> {
        let key = request.into_inner().api_key;
        let administrator = key == "a".repeat(32) && self.0.admin_enabled.load(Ordering::SeqCst);
        if administrator {
            return Ok(Response::new(pb::VerifyApiKeyResponse {
                caller: Some(pb::CallerContext {
                    tenant_id: TEAM.into(),
                    administrator: true,
                }),
                expires_at_unix_seconds: 0,
            }));
        }
        if key == "u".repeat(32) {
            return Ok(Response::new(pb::VerifyApiKeyResponse {
                caller: Some(pb::CallerContext {
                    tenant_id: TEAM.into(),
                    administrator: false,
                }),
                expires_at_unix_seconds: 0,
            }));
        }
        Err(Status::unauthenticated("invalid key"))
    }
}

#[tonic::async_trait]
impl pb::credential_service_server::CredentialService for Service {
    async fn create_tenant_key(
        &self,
        request: Request<pb::CreateTenantKeyRequest>,
    ) -> Result<Response<pb::CreateTenantKeyResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.tenant_id, TEAM);
        assert_eq!(request.name.as_deref(), Some("ci-key"));
        self.0.create_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(pb::CreateTenantKeyResponse {
            key: Some(managed_key(PUBLIC_ID, "ci-key")),
            api_key: format!("adx_{}", "r".repeat(64)),
        }))
    }

    async fn list_tenant_keys(
        &self,
        request: Request<pb::ListTenantKeysRequest>,
    ) -> Result<Response<pb::ListTenantKeysResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.tenant_id, TEAM);
        let call = self.0.list_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            assert!(request.page_token.is_empty());
            return Ok(Response::new(pb::ListTenantKeysResponse {
                keys: vec![
                    pb::TenantKey {
                        id: "e".repeat(64),
                        tenant_id: TEAM.into(),
                        expires_at_unix_seconds: 0,
                        metadata: None,
                    },
                    managed_key(PUBLIC_ID, "ci-key"),
                ],
                next_page_token: "page-2".into(),
            }));
        }
        assert_eq!(request.page_token, "page-2");
        Ok(Response::new(pb::ListTenantKeysResponse {
            keys: vec![managed_key(SECOND_PUBLIC_ID, "second-key")],
            next_page_token: String::new(),
        }))
    }

    async fn revoke_tenant_key(
        &self,
        request: Request<pb::RevokeTenantKeyRequest>,
    ) -> Result<Response<pb::RevokeTenantKeyResponse>, Status> {
        let request = request.into_inner();
        assert!(request.id.is_empty());
        assert_eq!(request.tenant_id, TEAM);
        assert_eq!(request.public_id, PUBLIC_ID);
        self.0.revoke_calls.fetch_add(1, Ordering::SeqCst);
        if self.0.revoked.swap(true, Ordering::SeqCst) {
            return Err(Status::not_found("key not found"));
        }
        Ok(Response::new(pb::RevokeTenantKeyResponse {}))
    }
}

struct Fixture {
    api: Arc<Api>,
    state: Arc<State>,
    coordinator: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.coordinator.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        adx_transport::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        state.admin_enabled.store(true, Ordering::SeqCst);
        let endpoint = Service(state.clone());
        let coordinator = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::auth_service_server::AuthServiceServer::new(
                    endpoint.clone(),
                ))
                .add_service(pb::credential_service_server::CredentialServiceServer::new(
                    endpoint,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0",
            "coordinator_address":address,
            "internal_security":"network",
            "ingress_mode":"standalone",
            "rpc_timeout_seconds":2,
            "cache_entries":8,
            "auth_cache_ttl_seconds":60,
            "create_unknown_retention_seconds":600,
            "e2b": {
                "domain":"sandbox.test",
                "envdVersion":"0.9.0",
                "templates":{"base":{"name":"base","version":"1"}}
            },
            "runtime_profile": {
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":true},
                "bootstrap":{"type":"erofs","root":"/rootfs","target":"/rootfs","entrypoint":["/rootfs/init"]},
                "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/workspace"}
            }
        }))
        .unwrap();
        let api = Api::new(Clients::new(config).unwrap()).unwrap();
        Self {
            api,
            state,
            coordinator,
        }
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, String)],
        body: &str,
    ) -> hyper::Response<Vec<u8>> {
        send_http_request(self.api.clone(), method, path, headers, body).await
    }

    fn management_headers() -> [(&'static str, String); 3] {
        [
            ("X-Admin-Token", "a".repeat(32)),
            ("X-Team-ID", TEAM.into()),
            ("Content-Type", "application/json".into()),
        ]
    }
}

fn json_body(reply: &hyper::Response<Vec<u8>>) -> Value {
    serde_json::from_slice(reply.body()).unwrap()
}

#[tokio::test]
async fn e2b_keys_create_list_delete_and_rejections_follow_the_http_contract() {
    let fixture = Fixture::new().await;
    let headers = Fixture::management_headers();

    let created = fixture
        .request("POST", "/api-keys", &headers, r#"{"name":"ci-key"}"#)
        .await;
    assert_eq!(created.status().as_u16(), 201);
    assert_eq!(
        created
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let created = json_body(&created);
    assert_eq!(created["id"], PUBLIC_ID);
    assert_eq!(created["name"], "ci-key");
    assert_eq!(created["createdAt"], "2023-11-14T22:13:20.000Z");
    assert_eq!(created["mask"]["prefix"], "adx_");
    assert_eq!(created["mask"]["valueLength"], 64);
    assert_eq!(created["mask"]["maskedValuePrefix"], "rr");
    assert_eq!(created["mask"]["maskedValueSuffix"], "rrrr");
    assert!(created["key"].as_str().is_some_and(|key| key.len() >= 32));
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);

    let listed = fixture.request("GET", "/api-keys", &headers, "").await;
    assert_eq!(listed.status().as_u16(), 200);
    let listed = json_body(&listed);
    let items = listed.as_array().expect("E2B list body is a raw array");
    assert_eq!(
        items.len(),
        2,
        "legacy key must not be represented as E2B metadata"
    );
    assert!(items.iter().all(|item| item.get("key").is_none()));
    assert_eq!(items[0]["id"], PUBLIC_ID);
    assert_eq!(items[1]["id"], SECOND_PUBLIC_ID);
    assert_eq!(
        fixture.state.list_calls.load(Ordering::SeqCst),
        2,
        "all credential pages must be consumed"
    );

    let deleted = fixture
        .request("DELETE", &format!("/api-keys/{PUBLIC_ID}"), &headers, "")
        .await;
    assert_eq!(deleted.status().as_u16(), 204);
    let again = fixture
        .request("DELETE", &format!("/api-keys/{PUBLIC_ID}"), &headers, "")
        .await;
    assert_eq!(again.status().as_u16(), 404);
    assert_eq!(fixture.state.revoke_calls.load(Ordering::SeqCst), 2);

    let patch = fixture
        .request(
            "PATCH",
            &format!("/api-keys/{PUBLIC_ID}"),
            &headers,
            r#"{"name":"renamed"}"#,
        )
        .await;
    assert_eq!(patch.status().as_u16(), 501);

    let wrong_team = [
        ("X-Admin-Token", "a".repeat(32)),
        ("X-Team-ID", "other-tenant".into()),
        ("Content-Type", "application/json".into()),
    ];
    assert_eq!(
        fixture
            .request("GET", "/api-keys", &wrong_team, "")
            .await
            .status()
            .as_u16(),
        403
    );

    let ordinary = [("X-API-Key", "u".repeat(32))];
    let ordinary = fixture.request("GET", "/api-keys", &ordinary, "").await;
    assert!(matches!(ordinary.status().as_u16(), 401 | 403));
}

#[tokio::test]
async fn every_e2b_request_revalidates_after_a_cached_admin_key_is_revoked() {
    let fixture = Fixture::new().await;
    let key = "a".repeat(32);
    // Prime the native cache directly. E2B requests must not reuse this result.
    fixture.api.clients.authenticate(&key).await.unwrap();
    fixture.state.admin_enabled.store(false, Ordering::SeqCst);

    let headers = Fixture::management_headers();
    assert_eq!(
        fixture
            .request("GET", "/api-keys", &headers, "")
            .await
            .status()
            .as_u16(),
        401
    );
    let sandbox_headers = [("X-API-Key", key)];
    assert_eq!(
        fixture
            .request("GET", "/v2/sandboxes", &sandbox_headers, "")
            .await
            .status()
            .as_u16(),
        401
    );
}

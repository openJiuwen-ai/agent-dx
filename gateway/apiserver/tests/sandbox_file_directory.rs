//! The new creation contract is sufficient for embedded file authorization.
use adx_apiserver::{clients::Clients, config::Config, contract, sandbox_service::SandboxService};
use adx_protocol::control as pb;
use data_plane_gateway::ingress::sandbox_files::{ReadError, SandboxDirectory};
use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::{pin::Pin, time::Duration};
use tonic::{Request, Response, Status};

struct Directory(Vec<pb::PublishedEnvironment>);
#[tonic::async_trait]
impl pb::environment_directory_service_server::EnvironmentDirectoryService for Directory {
    type WatchEnvironmentsStream =
        Pin<Box<dyn Stream<Item = Result<pb::EnvironmentDirectoryFrame, Status>> + Send>>;
    async fn watch_environments(
        &self,
        _: Request<pb::WatchEnvironmentsRequest>,
    ) -> Result<Response<Self::WatchEnvironmentsStream>, Status> {
        let reset = pb::EnvironmentDirectoryFrame {
            epoch: 1,
            revision: 1,
            reset: true,
            upserts: self.0.clone(),
            ..Default::default()
        };
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async { Ok(reset) }).chain(futures_util::stream::pending()),
        )))
    }
}

#[tokio::test]
async fn embedded_file_directory_accepts_new_specs_without_legacy_credentials() {
    let profile = serde_json::from_value(json!({
        "rootfs":{"runtime_class":"runc","type":"image","image":format!("runtime@sha256:{}", "0".repeat(64)),"readonly":false},
        "bootstrap":{"type":"image","image":format!("runtime@sha256:{}", "0".repeat(64)),"target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]},
        "env":{"EXECD_HTTP_PORT":"50090","EXECD_HTTP_TOKEN":"deployment-only"}
    })).unwrap();
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let mut records = vec![];
    for (id, state) in [
        ("new-running", pb::EnvironmentState::Running),
        ("new-paused", pb::EnvironmentState::Paused),
    ] {
        let spec = contract::create_spec_with_environment(json!({"namespace":"new","name":id.strip_prefix("new-").unwrap(),"image":"app:1","runtime":"runc","cpu":1000,"memory":512,"inheritEntrypoint":true}), &caller, Some(&profile)).unwrap();
        assert!(!spec.env.contains_key("EXECD_HTTP_TOKEN"));
        assert!(spec
            .runtime_profile
            .as_ref()
            .unwrap()
            .env
            .contains_key("EXECD_HTTP_TOKEN"));
        records.push(pb::PublishedEnvironment {
            record: Some(pb::EnvironmentRecord {
                spec: Some(spec),
                state: state as i32,
                assignment: Some(pb::Assignment {
                    environment_id: id.into(),
                    node_id: "node".into(),
                    generation: 1,
                    ..Default::default()
                }),
                revision: 1,
                ..Default::default()
            }),
            node_address: "127.0.0.1:1".into(),
            relay_address: "127.0.0.1:2".into(),
        });
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let incoming = futures_util::stream::unfold(listener, |listener| async {
            let stream = listener.accept().await.map(|(socket, _)| socket);
            Some((stream, listener))
        });
        tonic::transport::Server::builder()
            .add_service(
                pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                    Directory(records),
                ),
            )
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let config: Config = serde_json::from_value(json!({"listen":"127.0.0.1:0","coordinator_address":format!("http://{address}"),"internal_security":"network","ingress_mode":"standalone","rpc_timeout_seconds":1,"cache_entries":16,"auth_cache_ttl_seconds":1})).unwrap();
    let service = SandboxService::new(Clients::new(config).unwrap());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if service.authorize("tenant", "new-running").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        service.authorize("other", "new-running").await,
        Err(ReadError::NotFound)
    ));
    assert!(matches!(
        service.authorize("tenant", "absent").await,
        Err(ReadError::NotFound)
    ));
    assert!(matches!(
        service.authorize("tenant", "new-paused").await,
        Err(ReadError::Runtime)
    ));
    server.abort();
}

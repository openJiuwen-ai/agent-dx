use super::*;
use serde_json::{json, Value};

type ImageClient = pb::managed_image_service_client::ManagedImageServiceClient<Channel>;

async fn mutate(
    client: &mut ImageClient,
    id: &str,
    mutation: Value,
    admin: bool,
) -> std::result::Result<Value, tonic::Status> {
    let mut caller = caller().unwrap();
    caller.administrator = admin;
    let value = client
        .mutate_managed_image(pb::ManagedImageMutationRequest {
            tenant_id: "tenant".into(),
            id: id.into(),
            mutation_json: serde_json::to_vec(&mutation).unwrap(),
            caller: Some(caller),
        })
        .await?
        .into_inner();
    Ok(serde_json::from_slice(&value.view_json).unwrap())
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn managed_images_protect_retained_snapshots_and_fence_concurrent_acquisition() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap()
    .with_managed_image_reclamation(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc.clone()))
            .add_service(pb::managed_image_service_server::ManagedImageServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut api = ImageClient::new(channel(address, "apiserver").await);
    let begun = mutate(&mut api, "", json!({"operation":"begin","request_id":"one","retention":{"mode":"ephemeral","grace_seconds":0}}), false).await.unwrap();
    let id = begun["image"]["id"].as_str().unwrap().to_string();
    let repository = format!("registry.test/managed/publications/{id}/service-0");
    let reference = format!("{repository}@sha256:{}", "a".repeat(64));
    mutate(&mut api, &id, json!({"operation":"plan","execution_id":"attempt","outputs":{"main":format!("{repository}:build")}}), false).await.unwrap();
    assert!(mutate(&mut api, &id, json!({"operation":"plan","execution_id":"different-attempt","outputs":{"main":format!("{repository}:build")}}), false).await.is_err());
    mutate(
        &mut api,
        &id,
        json!({"operation":"publish","digests":{"main":format!("sha256:{}", "a".repeat(64))}}),
        false,
    )
    .await
    .unwrap();
    mutate(
        &mut api,
        &id,
        json!({"operation":"release","lease_id":"publisher"}),
        false,
    )
    .await
    .unwrap();
    let mut template = spec("source");
    // Compose dependencies live in the outer Environment's persisted specification.
    template.execution.extra_config = json!({"managedImagePublications":[id]}).to_string();
    session
        .publish_snapshot(
            adx_core::snapshots::Snapshot::new(
                "saved".into(),
                vec![],
                template.clone(),
                "node".into(),
                "source-1".into(),
                adx_core::CheckpointArtifact {
                    scope: String::new(),
                    storage: "shared".into(),
                    location: "owned-checkpoint".into(),
                    size_bytes: 1,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let protected = mutate(&mut api, &id, json!({"operation":"observe"}), false)
        .await
        .unwrap();
    assert_eq!(protected["image"]["references"], json!(["snapshot:saved"]));
    assert_eq!(protected["reclaimable"], false);
    // A caller restoring without extra_config must still protect captured
    // Compose publications while queued, before a runtime can be assigned.
    let mut restores = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let mut restored = spec("restored");
    restored.snapshot_id = Some("saved".into());
    let pending = tokio::spawn(async move {
        restores
            .create_environment(pb::CreateEnvironmentRequest {
                spec: Some(restored.into()),
                caller: caller(),
                schedule_timeout_seconds: 1,
                create_timeout_seconds: 5,
                ..Default::default()
            })
            .await
    });
    let mut saw_restoring_reference = false;
    for _ in 0..50 {
        let observed = mutate(&mut api, &id, json!({"operation":"observe"}), false)
            .await
            .unwrap();
        if observed["image"]["references"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "environment:restored")
        {
            saw_restoring_reference = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        saw_restoring_reference,
        "queued restore lost captured managed image dependencies"
    );
    assert!(pending.await.unwrap().is_err());
    assert!(mutate(
        &mut api,
        &id,
        json!({"operation":"claim_deletion","manifests":[reference]}),
        true
    )
    .await
    .is_err());
    let deleting = session.delete_snapshot("saved", "tenant").await.unwrap();
    session
        .finish_snapshot_deletion("saved", deleting.revision)
        .await
        .unwrap();
    let candidate = mutate(&mut api, &id, json!({"operation":"observe"}), false)
        .await
        .unwrap();
    assert_eq!(candidate["reclaimable"], true);
    assert_eq!(
        mutate(
            &mut api,
            &id,
            json!({"operation":"claim_deletion","manifests":[reference]}),
            false
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
    let mut other = api.clone();
    let (held, deleted) = tokio::join!(
        mutate(
            &mut api,
            &id,
            json!({"operation":"retain","lease_id":"new-client"}),
            false
        ),
        mutate(
            &mut other,
            &id,
            json!({"operation":"claim_deletion","manifests":[reference]}),
            true
        ),
    );
    assert_ne!(held.is_ok(), deleted.is_ok());
    if deleted.is_ok() {
        assert!(session.acquire_managed_images(&template).await.is_err());
    }
    task.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn managed_image_deletion_requires_explicit_operator_configuration() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session,
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::managed_image_service_server::ManagedImageServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut api = ImageClient::new(channel(address, "apiserver").await);
    // Observation must work without granting permission to delete anything.
    let begun = mutate(&mut api, "", json!({"operation":"begin","request_id":"disabled-observation","retention":{"mode":"ephemeral","grace_seconds":0}}), false).await.unwrap();
    let id = begun["image"]["id"].as_str().unwrap();
    let repository = format!("registry.test/managed/publications/{id}/service-0");
    mutate(&mut api, id, json!({"operation":"plan","execution_id":"attempt","outputs":{"single":format!("{repository}:build")}}), false).await.unwrap();
    mutate(
        &mut api,
        id,
        json!({"operation":"publish","digests":{"single":format!("sha256:{}", "a".repeat(64))}}),
        false,
    )
    .await
    .unwrap();
    mutate(
        &mut api,
        id,
        json!({"operation":"release","lease_id":"publisher"}),
        false,
    )
    .await
    .unwrap();
    let candidate = mutate(&mut api, id, json!({"operation":"observe"}), false)
        .await
        .unwrap();
    assert_eq!(candidate["eligible_for_reclamation"], true);
    assert_eq!(candidate["reclaimable"], false);
    assert_eq!(candidate["reclamation_enabled"], false);
    let result = mutate(
        &mut api,
        &"a".repeat(64),
        json!({"operation":"claim_deletion","manifests":[]}),
        true,
    )
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    let mut node = ImageClient::new(channel(address, "node").await);
    assert_eq!(mutate(&mut node, "", json!({"operation":"begin","request_id":"unauthorized","retention":{"mode":"retained"}}), false).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    task.abort();
}

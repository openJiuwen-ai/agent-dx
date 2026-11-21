use super::*;
use serde_json::{json, Value};

fn fixture_policy() -> adx_coordinator::rpc::ManagedImageConfig {
    serde_json::from_value(json!({
        "registry": {"authority":"registry.test","repository_prefix":"managed"}
    }))
    .unwrap()
}

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
    .with_managed_image_config(fixture_policy())
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
    let repository = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{id}/service-0"
    );
    let reference = format!("{repository}@sha256:{}", "a".repeat(64));
    let mut creates = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let mut unregistered = spec("unregistered-consumer");
    unregistered.image = format!("https://{reference}");
    assert_eq!(
        creates
            .create_environment(pb::CreateEnvironmentRequest {
                spec: Some(unregistered.into()),
                caller: caller(),
                schedule_timeout_seconds: 1,
                create_timeout_seconds: 5,
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
        "a future publication must not acquire an untracked consumer before registering its outputs"
    );
    let registration = json!({
        "operation": "register_outputs",
        "attempt_id": "external-writer",
        "outputs": {"main": format!("{repository}:build")},
    });
    let foreign = api
        .mutate_managed_image(pb::ManagedImageMutationRequest {
            tenant_id: "another-tenant".into(),
            id: id.clone(),
            mutation_json: serde_json::to_vec(&registration).unwrap(),
            caller: caller(),
        })
        .await
        .unwrap_err();
    assert_eq!(foreign.code(), tonic::Code::PermissionDenied);
    // A tenant can register and complete a publication without a builder Environment.
    mutate(&mut api, &id, registration.clone(), false)
        .await
        .unwrap();
    mutate(&mut api, &id, registration, false).await.unwrap();
    let mut completion = json!({
        "operation": "complete",
        "attempt_id": "another-writer",
        "digests": {"main": format!("sha256:{}", "a".repeat(64))},
    });
    assert!(mutate(&mut api, &id, completion.clone(), false)
        .await
        .is_err());
    completion["attempt_id"] = json!("external-writer");
    mutate(&mut api, &id, completion, false).await.unwrap();
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
    template
        .execution
        .managed_image_publications
        .insert(id.clone());
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
    let protected = mutate(&mut api, &id, json!({"operation":"observe"}), true)
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
        let observed = mutate(&mut api, &id, json!({"operation":"observe"}), true)
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
    let candidate = mutate(&mut api, &id, json!({"operation":"observe"}), true)
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
        let resumed = mutate(
            &mut api,
            &id,
            json!({"operation":"claim_deletion","manifests":[reference]}),
            true,
        )
        .await
        .unwrap();
        assert_eq!(resumed["references_current"], false);
        assert_eq!(resumed["image"]["deletion_manifests"], json!([reference]));
    }
    task.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn managed_image_deletion_requires_explicit_operator_configuration() {
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
    .with_managed_image_config(fixture_policy())
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
    let repository = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{id}/service-0"
    );
    mutate(
        &mut api,
        id,
        json!({"operation":"register_outputs","attempt_id":"fixture","outputs":{"single":format!("{repository}:build")}}),
        true,
    )
    .await
    .unwrap();
    mutate(
        &mut api,
        id,
        json!({"operation":"complete","attempt_id":"fixture","digests":{"single":format!("sha256:{}", "a".repeat(64))}}),
        true,
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
    let candidate = mutate(&mut api, id, json!({"operation":"observe"}), true)
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

    // Four maximum-sized lease histories used to overflow the default gRPC
    // response when returned as full receipts on a single valid page.
    for index in 0..4 {
        let request_id = format!("busy-publication-{index}");
        let mutation =
            json!({"operation":"begin","request_id":request_id,"retention":{"mode":"retained"}});
        let view = mutate(&mut api, "", mutation, false).await.unwrap();
        let large_id = view["image"]["id"].as_str().unwrap();
        session
            .mutate_managed_image("tenant", large_id, |image| {
                for index in 0..4095 {
                    image.retain(&format!("{index:04}{}", "x".repeat(252)))?;
                }
                Ok(())
            })
            .await
            .unwrap();
    }
    let page = api
        .get_managed_images(pb::ManagedImageQuery {
            tenant_id: "tenant".into(),
            caller: caller(),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .view_json;
    assert!(page.len() < 4096);
    let page: Value = serde_json::from_slice(&page).unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 5);
    assert_eq!(
        items
            .iter()
            .filter(|item| item["image"]["lease_count"] == 4096)
            .count(),
        4
    );
    assert!(items
        .iter()
        .all(|item| item["summary"] == true && item["references_current"] == false));
    let mut node = ImageClient::new(channel(address, "node").await);
    assert_eq!(mutate(&mut node, "", json!({"operation":"begin","request_id":"unauthorized","retention":{"mode":"retained"}}), false).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    task.abort();
}

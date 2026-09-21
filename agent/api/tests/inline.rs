use adx_agent_api::{management::*, Error};
use adx_agent_core::{inline::CreateRequest, sandbox::*, *};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};
#[derive(Default)]
struct Backend {
    instances: Mutex<BTreeMap<String, SandboxObservation>>,
    creates: AtomicUsize,
    deletes: AtomicUsize,
    pause_create: AtomicBool,
    pause_delete: AtomicBool,
    lose_create_response: AtomicBool,
    unknown_delete: AtomicBool,
    create_entered: Notify,
    create_resume: Notify,
    delete_entered: Notify,
    delete_resume: Notify,
}
#[async_trait]
impl Sandbox for Backend {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        if self.pause_create.load(Ordering::SeqCst) {
            self.create_entered.notify_one();
            self.create_resume.notified().await;
        }
        let observed = self
            .instances
            .lock()
            .await
            .entry(encode_key(&[&r.tenant, &r.id]))
            .or_insert_with(|| SandboxObservation {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                phase: SandboxPhase::Running,
                ready: true,
                runtime_id: Some(format!("{}-1", r.id)),
                message: None,
            })
            .clone();
        if self.lose_create_response.swap(false, Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown("lost create reply".into()));
        }
        Ok(observed)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(self
            .instances
            .lock()
            .await
            .get(&encode_key(&[tenant, id]))
            .cloned())
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        if self.unknown_delete.load(Ordering::SeqCst) {
            return Err(SandboxError::OutcomeUnknown("delete unconfirmed".into()));
        }
        if self.pause_delete.load(Ordering::SeqCst) {
            self.delete_entered.notify_one();
            self.delete_resume.notified().await;
        }
        let observed = SandboxObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        };
        self.instances
            .lock()
            .await
            .insert(encode_key(&[tenant, id]), observed.clone());
        Ok(observed)
    }
}
fn request() -> CreateRequest {
    serde_json::from_value(serde_json::json!({"name":"demo","namespace":"default","urn":"ignored-inline-wins","runtime_spec":{"runtime":"Python3.11","sandbox_type":"docker","rootfs":{"imageurl":"app:1","ports":["tcp:8080"]},"cmds":[["/app/start"]]}})).unwrap()
}
fn service(backend: Arc<Backend>) -> Arc<InlineService> {
    Arc::new(
        InlineService::new(
            backend,
            Options {
                profiles: vec![InlineProfile {
                    sandbox_type: adx_agent_core::inline::SandboxType::Docker,
                    request_image: Some("app:1".into()),
                    image: "app:1".into(),
                    isolation_runtime: "runc".into(),
                    request_user: None,
                    working_dir: "/".into(),
                    default_entrypoint: vec![],
                    service: vec![],
                    preinstalled_workspace: None,
                    preinstalled_mounts: vec![],
                }],
                backend_timeout: Duration::from_secs(2),
                max_inflight: 16,
            },
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn inline_uses_sandbox_identity_and_survives_adapter_replacement_without_store() {
    let backend = Arc::new(Backend::default());
    let first = service(backend.clone());
    let created = first.create("tenant", request()).await.unwrap();
    assert!(uuid::Uuid::parse_str(&created.instance_id).is_ok());
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    assert!(backend
        .instances
        .lock()
        .await
        .contains_key(&encode_key(&["tenant", &created.instance_id])));
    drop(first);
    let second = service(backend.clone());
    let detail = second.get("tenant", &created.instance_id).await.unwrap();
    assert_eq!(detail.status, "RUNNING");
    assert!(matches!(
        second.get("other", &created.instance_id).await,
        Err(Error::NotFound)
    ));
    second.kill("tenant", &created.instance_id).await.unwrap();
    assert!(matches!(
        second.get("tenant", &created.instance_id).await,
        Err(Error::NotFound)
    ));
    second.kill("tenant", &created.instance_id).await.unwrap(); // Sandbox confirms the tombstone.
}

#[tokio::test]
async fn create_returns_success_after_sandbox_acceptance() {
    let backend = Arc::new(Backend::default());
    backend.pause_create.store(true, Ordering::SeqCst);
    let service = service(backend.clone());
    let task = tokio::spawn({
        let service = service.clone();
        async move { service.create("tenant", request()).await }
    });
    backend.create_entered.notified().await;
    assert!(!task.is_finished());
    backend.create_resume.notify_one();
    let created = task.await.unwrap().unwrap();
    assert_eq!(created.code, 200);
}

#[tokio::test]
async fn lost_reply_reports_original_id_without_retrying_create() {
    let backend = Arc::new(Backend::default());
    backend.lose_create_response.store(true, Ordering::SeqCst);
    let first = service(backend.clone());
    let error = first.create("tenant", request()).await.unwrap_err();
    let id = backend
        .instances
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .id
        .clone();
    assert!(matches!(&error, Error::OutcomeUnknown(message) if message.contains(&id)));
    drop(first);
    let replacement = service(backend.clone());
    assert_eq!(
        replacement.get("tenant", &id).await.unwrap().status,
        "RUNNING"
    );
    replacement.kill("tenant", &id).await.unwrap();
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn delete_requires_sandbox_confirmation_and_can_be_retried() {
    let backend = Arc::new(Backend::default());
    let first = service(backend.clone());
    let id = first.create("tenant", request()).await.unwrap().instance_id;
    backend.unknown_delete.store(true, Ordering::SeqCst);
    assert!(matches!(
        first.kill("tenant", &id).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(first.get("tenant", &id).await.unwrap().status, "RUNNING");
    backend.unknown_delete.store(false, Ordering::SeqCst);
    service(backend.clone()).kill("tenant", &id).await.unwrap();
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn create_timeout_reports_unknown_outcome() {
    let backend = Arc::new(Backend::default());
    backend.pause_create.store(true, Ordering::SeqCst);
    let service = service(backend.clone());
    assert!(matches!(
        service.create("tenant", request()).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_identity_is_rejected_before_backend_operations() {
    let backend = Arc::new(Backend::default());
    let service = service(backend.clone());
    assert!(service.create("", request()).await.is_err());
    assert!(service.kill("tenant", "adx-managed-id").await.is_err());
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn delete_waits_for_backend_confirmation() {
    let backend = Arc::new(Backend::default());
    let service = service(backend.clone());
    let id = service
        .create("tenant", request())
        .await
        .unwrap()
        .instance_id;
    backend.pause_delete.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let service = service.clone();
        let id = id.clone();
        async move { service.kill("tenant", &id).await }
    });
    backend.delete_entered.notified().await;
    assert!(!task.is_finished());
    backend.delete_resume.notify_one();
    task.await.unwrap().unwrap();
    assert!(matches!(
        service.get("tenant", &id).await,
        Err(Error::NotFound)
    ));
}

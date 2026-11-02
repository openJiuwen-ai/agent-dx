use adx_activator::{Activator, Error};
use adx_agent_core::{activator::Target, environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
struct Platform(AtomicUsize, Option<std::path::PathBuf>);

#[async_trait::async_trait]
impl Environment for Platform {
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some(identity) = &self.1 {
            assert!(std::fs::read_to_string(identity).is_ok());
        }
        Ok(EnvironmentObservation {
            id: request.id.clone(),
            tenant: request.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        })
    }

    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(None)
    }

    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        Err(EnvironmentError::NotFound)
    }
}

async fn activate_remote_app(delivery_template: Option<&str>) -> (Result<Target, Error>, usize) {
    let platform = Arc::new(Platform::default());
    let service = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    );
    let directory = tempfile::tempdir().unwrap();
    let delivery =
        delivery_template.map(
            |template| adx_activator::credential_delivery::CredentialDelivery {
                template: template.into(),
                root: directory.path().join("unused-identities"),
                ca_file: directory.path().join("unused-ca.pem"),
                uid: 10001,
                gid: 10001,
                ttl_seconds: 3600,
                renew_before_seconds: 600,
            },
        );
    let service = service.with_credential_delivery(delivery).unwrap();
    let draft: TemplateDraft = serde_json::from_value(serde_json::json!({
        "name":"manual-app", "image":"registry.example.com/app:1", "isolation_runtime":"runc",
        "env":{"DIGITALMATE_SECRET_STORE":"remote"},
        "resources":{"cpu_millis":1000,"memory_mib":512}
    }))
    .unwrap();
    service.publish("tenant", &draft).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "manual-app".into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: "user".into(),
    };
    let result = service
        .activate(&scope, None, unix_time_millis() + 60_000)
        .await;
    assert!(!directory.path().join("unused-identities").exists());
    (result, platform.0.load(Ordering::SeqCst))
}

#[tokio::test]
async fn manual_remote_credentials_do_not_require_automatic_delivery() {
    let (result, creates) = activate_remote_app(None).await;
    assert!(
        result.is_ok(),
        "manual provisioning was blocked: {result:?}"
    );
    assert_eq!(creates, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn delivery_does_not_intercept_another_template() {
    let (result, creates) = activate_remote_app(Some("managed-app")).await;
    assert!(
        result.is_ok(),
        "another template was intercepted: {result:?}"
    );
    assert_eq!(creates, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn configured_template_still_rejects_invalid_remote_context_before_creation() {
    let (result, creates) = activate_remote_app(Some("manual-app")).await;
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert_eq!(creates, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn digitalmate_remote_identity_is_delivered_before_environment_creation() {
    use adx_agent_core::service_grants::{ServiceAccess, VerifyServiceGrant};
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir().unwrap();
    let identity = directory.path().join("identities/users/w-user/identity");
    let platform = Arc::new(Platform(AtomicUsize::new(0), Some(identity.clone())));
    let ca = directory.path().join("ca.pem");
    std::fs::write(&ca, "synthetic-ca").unwrap();
    let metadata = std::fs::metadata(directory.path()).unwrap();
    let service = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    )
    .with_credential_delivery(Some(
        adx_activator::credential_delivery::CredentialDelivery {
            template: "orbit".into(),
            root: directory.path().join("identities"),
            ca_file: ca,
            uid: metadata.uid(),
            gid: metadata.gid(),
            ttl_seconds: 3600,
            renew_before_seconds: 600,
        },
    ))
    .unwrap();
    let draft: TemplateDraft = serde_json::from_value(serde_json::json!({
        "name":"orbit", "image":"registry.example.com/digitalmate:1", "isolation_runtime":"runc",
        "env":{"DIGITALMATE_SECRET_STORE":"remote", "DIGITALMATE_CRYPTO_TENANT":"tenant",
               "DIGITALMATE_CRYPTO_WORKSPACE":"${workspace}", "DIGITALMATE_CRYPTO_PURPOSE":"orbit-config"},
        "resources":{"cpu_millis":1000,"memory_mib":512}
    }))
    .unwrap();
    service.publish("tenant", &draft).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "orbit".into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: "user".into(),
    };
    service
        .activate_with_parameters(
            &scope,
            None,
            unix_time_millis() + 60_000,
            false,
            Some(b"workspace=w-user"),
        )
        .await
        .unwrap();
    assert_eq!(platform.0.load(Ordering::SeqCst), 1);
    assert!(
        service
            .verify_service_grant(&VerifyServiceGrant {
                credential: std::fs::read_to_string(identity).unwrap(),
                expected: ServiceAccess {
                    tenant: "tenant".into(),
                    workspace: "w-user".into(),
                    purpose: "orbit-config".into()
                },
            })
            .await
            .unwrap()
            .allowed
    );
}

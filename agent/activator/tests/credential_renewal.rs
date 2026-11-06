#![cfg(unix)]
use adx_activator::{credential_delivery::CredentialDelivery, Activator};
use adx_agent_core::{environment::*, service_grants::*, *};
use adx_agent_store::{AgentState, Index, Key, MemoryRepository, Record, Repository, Transaction};
use std::{
    collections::BTreeMap,
    os::unix::fs::MetadataExt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

struct NoPlatform;
#[async_trait::async_trait]
impl Environment for NoPlatform {
    async fn create(
        &self,
        _: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        unreachable!()
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

#[derive(Default)]
struct FaultRepository {
    inner: MemoryRepository,
    fail_get: Mutex<Option<(String, bool)>>,
    unknown_commit: AtomicBool,
}
#[async_trait::async_trait]
impl Repository for FaultRepository {
    async fn get(&self, key: &Key) -> adx_agent_store::Result<Option<Record>> {
        {
            let mut fault = self.fail_get.lock().unwrap();
            if fault
                .as_ref()
                .is_some_and(|(target, _)| target == key.as_str())
            {
                let (_, unknown) = fault.take().unwrap();
                if unknown {
                    self.unknown_commit.store(true, Ordering::SeqCst);
                } else {
                    return Err(adx_agent_store::Error::Unavailable(
                        "synthetic revocation read failure".into(),
                    ));
                }
            }
        }
        self.inner.get(key).await
    }
    async fn commit(&self, tx: &Transaction) -> adx_agent_store::Result<bool> {
        let committed = self.inner.commit(tx).await?;
        if self.unknown_commit.swap(false, Ordering::SeqCst) {
            return Err(adx_agent_store::Error::OutcomeUnknown(
                "synthetic lost revocation response".into(),
            ));
        }
        Ok(committed)
    }
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> adx_agent_store::Result<Vec<(String, Record)>> {
        self.inner.page(index, after, limit).await
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    state: AgentState,
    binding: AgentBinding,
    config: CredentialDelivery,
    repository: Arc<FaultRepository>,
}
impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.pem");
        std::fs::write(&ca, "synthetic-ca").unwrap();
        let metadata = std::fs::metadata(directory.path()).unwrap();
        let config = CredentialDelivery {
            template: "orbit".into(),
            root: directory.path().join("identities"),
            ca_file: ca,
            uid: metadata.uid(),
            gid: metadata.gid(),
            ttl_seconds: 3600,
            renew_before_seconds: 600,
        };
        let repository = Arc::new(FaultRepository::default());
        let state = AgentState::new(repository.clone());
        let draft: TemplateDraft = serde_json::from_value(serde_json::json!({
            "name":"orbit", "image":"registry.example.com/digitalmate:1", "isolation_runtime":"runc",
            "resources":{"cpu_millis":1000,"memory_mib":512}
        })).unwrap();
        state.publish("tenant", &draft).await.unwrap();
        let binding = state
            .create_binding_with_parameters(
                Scope {
                    tenant: "tenant".into(),
                    template: "orbit".into(),
                    template_ref: TemplateRef::Version("1".into()),
                    binding_id: "user".into(),
                },
                &BTreeMap::from([("workspace".into(), "w-user".into())]),
            )
            .await
            .unwrap();
        Self {
            _directory: directory,
            state,
            binding,
            config,
            repository,
        }
    }
    fn activator(&self) -> Activator {
        Activator::new(self.state.clone(), Arc::new(NoPlatform))
    }
    fn identity(&self) -> String {
        std::fs::read_to_string(self.config.root.join("users/w-user/identity")).unwrap()
    }
    fn reference(&self) -> std::path::PathBuf {
        self.config.root.join("metadata/w-user.json")
    }
    fn renewal_config(&self) -> CredentialDelivery {
        // Enter the old grant's renewal window without waiting or modifying its Redis expiry.
        CredentialDelivery {
            ttl_seconds: 7200,
            renew_before_seconds: 3601,
            ..self.config.clone()
        }
    }
    async fn allowed(&self, credential: &str) -> bool {
        self.activator()
            .verify_service_grant(&VerifyServiceGrant {
                credential: credential.into(),
                expected: ServiceAccess {
                    tenant: "tenant".into(),
                    workspace: "w-user".into(),
                    purpose: "orbit-config".into(),
                },
            })
            .await
            .unwrap()
            .allowed
    }
}

async fn recovery_retries_revocation(unknown: bool) {
    let fixture = Fixture::new().await;
    let a = fixture.activator();
    a.deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .unwrap();
    let old = fixture.identity();
    let key = Key::new("service-grant", &[&credential_digest(&old).unwrap()]).unwrap();
    *fixture.repository.fail_get.lock().unwrap() = Some((key.as_str().into(), unknown));
    let config = fixture.renewal_config();
    assert!(a
        .deliver_credential_identity(&fixture.binding, &config)
        .await
        .is_err());
    let new = fixture.identity();
    assert_ne!(old, new);
    assert!(fixture.allowed(&new).await);
    if !unknown {
        assert!(fixture.allowed(&old).await);
    }
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.reference()).unwrap()).unwrap();
    assert!(saved["pending_revocations"]
        .as_array()
        .is_some_and(|pending| pending
            .iter()
            .any(|digest| digest == &credential_digest(&old).unwrap())));
    // A fresh process must finish the durable obligation, without issuing another healthy identity.
    fixture
        .activator()
        .renew_delivered_credentials(&config)
        .await
        .unwrap();
    assert_eq!(fixture.identity(), new);
    assert!(!fixture.allowed(&old).await);
    assert!(fixture.allowed(&new).await);
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.reference()).unwrap()).unwrap();
    assert!(saved["pending_revocations"]
        .as_array()
        .is_none_or(Vec::is_empty));
}

#[tokio::test]
async fn restart_retries_a_failed_old_credential_revocation() {
    recovery_retries_revocation(false).await;
}

#[tokio::test]
async fn restart_retries_an_unknown_revocation_idempotently() {
    recovery_retries_revocation(true).await;
}

#[tokio::test]
async fn restart_recovers_metadata_published_before_the_identity() {
    let fixture = Fixture::new().await;
    let a = fixture.activator();
    a.deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .unwrap();
    let old = fixture.identity();
    let issued = a
        .issue_service_grant(&IssueServiceGrant {
            scope: fixture.binding.scope.clone(),
            expected_generation: fixture.binding.generation.clone(),
            purpose: "orbit-config".into(),
            ttl_seconds: 3600,
        })
        .await
        .unwrap();
    // Simulate a restart after journaling the replacement but before publishing its bearer file.
    std::fs::write(fixture.reference(), serde_json::to_vec(&serde_json::json!({
        "scope":fixture.binding.scope, "generation":fixture.binding.generation,
        "credential_digest":issued.credential_digest, "expires_at_unix_seconds":issued.expires_at_unix_seconds,
        "pending_revocations":[credential_digest(&old).unwrap()]
    })).unwrap()).unwrap();
    fixture
        .activator()
        .renew_delivered_credentials(&fixture.config)
        .await
        .unwrap();
    let current = fixture.identity();
    assert_ne!(current, old);
    assert!(fixture.allowed(&current).await);
    assert!(!fixture.allowed(&old).await);
    assert!(!fixture.allowed(&issued.credential).await);
}

#[tokio::test]
async fn unreadable_metadata_does_not_discard_revocation_obligations() {
    let fixture = Fixture::new().await;
    let a = fixture.activator();
    a.deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .unwrap();
    let old = fixture.identity();
    std::fs::write(fixture.reference(), "{").unwrap();
    assert!(a
        .deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .is_err());
    assert_eq!(fixture.identity(), old);
}

#[tokio::test]
async fn legacy_delivery_metadata_without_pending_revocations_remains_reusable() {
    let fixture = Fixture::new().await;
    let a = fixture.activator();
    a.deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .unwrap();
    let old = fixture.identity();
    let mut saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.reference()).unwrap()).unwrap();
    saved.as_object_mut().unwrap().remove("pending_revocations");
    std::fs::write(fixture.reference(), serde_json::to_vec(&saved).unwrap()).unwrap();
    a.deliver_credential_identity(&fixture.binding, &fixture.config)
        .await
        .unwrap();
    assert_eq!(fixture.identity(), old);
    assert!(fixture.allowed(&old).await);
}

#[tokio::test]
async fn a_one_second_renewal_window_has_a_subsecond_polling_cadence() {
    let fixture = Fixture::new().await;
    let config = CredentialDelivery {
        ttl_seconds: 2,
        renew_before_seconds: 1,
        ..fixture.config.clone()
    };
    config.validate().unwrap();
    assert!(!config.renewal_interval().is_zero());
    assert!(config.renewal_interval() <= Duration::from_millis(500));
}

#[tokio::test]
async fn short_grants_are_checked_well_inside_their_renewal_window() {
    let fixture = Fixture::new().await;
    let config = CredentialDelivery {
        ttl_seconds: 60,
        renew_before_seconds: 10,
        ..fixture.config.clone()
    };
    config.validate().unwrap();
    assert!(config.renewal_interval() <= Duration::from_secs(5));
}

#[tokio::test]
async fn ordinary_grants_keep_the_bounded_five_minute_polling_cadence() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.config.renewal_interval(), Duration::from_secs(300));
}

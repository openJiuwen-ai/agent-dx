#![cfg(unix)]
use adx_activator::{credential_delivery::CredentialDelivery, Activator};
use adx_agent_core::{environment::*, service_grants::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use std::{collections::BTreeMap, sync::Arc};

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

#[tokio::test]
async fn delivery_is_private_reuses_valid_identity_and_replaces_revoked_identity() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("source-ca.pem");
    std::fs::write(&ca, "test-ca").unwrap();
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let draft: TemplateDraft = serde_json::from_value(serde_json::json!({"name":"orbit", "image":"registry/orbit:1", "isolation_runtime":"runsc", "resources":{"cpu_millis":1000,"memory_mib":512}})).unwrap();
    state.publish("tenant", &draft).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "orbit".into(),
        template_ref: TemplateRef::Version("1".into()),
        binding_id: "alice".into(),
    };
    let binding = state
        .create_binding_with_parameters(
            scope.clone(),
            &BTreeMap::from([("workspace".into(), "w-alice".into())]),
        )
        .await
        .unwrap();
    let delivery = CredentialDelivery {
        template: "orbit".into(),
        root: dir.path().join("delivery"),
        ca_file: ca,
        uid: std::fs::metadata(dir.path()).unwrap().uid(),
        gid: std::fs::metadata(dir.path()).unwrap().gid(),
        ttl_seconds: 3600,
        renew_before_seconds: 600,
    };
    let a = Activator::new(state, Arc::new(NoPlatform));
    a.deliver_credential_identity(&binding, &delivery)
        .await
        .unwrap();
    let identity = delivery.root.join("users/w-alice/identity");
    let first = std::fs::read_to_string(&identity).unwrap();
    assert_eq!(std::fs::metadata(&identity).unwrap().mode() & 0o777, 0o400);
    a.deliver_credential_identity(&binding, &delivery)
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&identity).unwrap(), first);
    a.revoke_service_grant(&RevokeServiceGrant {
        credential_digest: credential_digest(&first).unwrap(),
    })
    .await
    .unwrap();
    a.deliver_credential_identity(&binding, &delivery)
        .await
        .unwrap();
    let second = std::fs::read_to_string(&identity).unwrap();
    assert_ne!(first, second);
    let expected = ServiceAccess {
        tenant: "tenant".into(),
        workspace: "w-alice".into(),
        purpose: "orbit-config".into(),
    };
    assert!(
        a.verify_service_grant(&VerifyServiceGrant {
            credential: second.clone(),
            expected: expected.clone()
        })
        .await
        .unwrap()
        .allowed
    );
    assert!(
        !a.verify_service_grant(&VerifyServiceGrant {
            credential: second,
            expected: ServiceAccess {
                workspace: "w-bob".into(),
                ..expected
            }
        })
        .await
        .unwrap()
        .allowed
    );
    a.delete_binding(&scope).await.unwrap();
    assert!(a
        .deliver_credential_identity(&binding, &delivery)
        .await
        .is_err());
}

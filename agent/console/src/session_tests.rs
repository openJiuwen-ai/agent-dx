use crate::session::{LoginInput, Role, SessionRegistry};
use std::collections::BTreeMap;

fn registry() -> SessionRegistry {
    use sha2::{Digest, Sha256};
    SessionRegistry::new(vec![1; 32], 3600,
        BTreeMap::from([("operator".into(), hex::encode(Sha256::digest(b"admin-secret")))]),
        BTreeMap::from([("tenant".into(), hex::encode(Sha256::digest(b"tenant-secret")))])).unwrap()
}

#[test]
fn platform_login_has_global_role_and_no_bound_tenant() {
    let registry = registry();
    let (session, cookie) = registry.login(&LoginInput::PlatformAdmin {
        admin_id: "operator".into(), token: "admin-secret".into()
    }).unwrap();
    assert_eq!(session.role, Role::PlatformAdmin);
    assert!(session.tenant_id.is_none());
    assert_eq!(registry.verify(&cookie).unwrap().subject_id, "operator");
    assert!(registry.verify(&(cookie + "x")).is_none());
}

#[test]
fn tenant_login_is_scoped_and_logout_invalidates_cookie() {
    let registry = registry();
    let (session, cookie) = registry.login(&LoginInput::TenantAdmin {
        tenant_id: "tenant".into(), token: "tenant-secret".into()
    }).unwrap();
    assert_eq!(session.role, Role::TenantAdmin);
    assert_eq!(session.tenant_id.as_deref(), Some("tenant"));
    registry.logout(&session.session_id);
    assert!(registry.verify(&cookie).is_none());
}

#[test]
fn login_rejects_wrong_or_unknown_credentials_uniformly() {
    let registry = registry();
    for id in ["operator", "missing"] {
        assert!(registry.login(&LoginInput::PlatformAdmin { admin_id: id.into(), token: "bad".into() }).is_err());
    }
    assert!(registry.verify("garbage.signature").is_none());
}

#[test]
fn verify_rejects_expired_session_and_another_registry_cookie() {
    use sha2::{Digest,Sha256};
    let short = SessionRegistry::new(vec![1;32],1,BTreeMap::from([("operator".into(),hex::encode(Sha256::digest(b"admin-secret")))]),BTreeMap::new()).unwrap();
    let (record,cookie) = short.login(&LoginInput::PlatformAdmin { admin_id:"operator".into(),token:"admin-secret".into() }).unwrap();
    assert!(registry().verify(&cookie).is_none());
    assert!(short.verify_at(&cookie,record.expires_at-1).is_some());
    assert!(short.verify_at(&cookie,record.expires_at).is_none());
    assert!(short.verify_at(&cookie,record.created_at).is_none());
}

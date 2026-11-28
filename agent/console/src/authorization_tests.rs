use crate::{authorization::{authorize_tenant, require_digitalmate}, config::TenantType, session::{Role, SessionRecord}};

fn session(role: Role) -> SessionRecord {
    SessionRecord { session_id: "session".into(), subject_id:"actor".into(), role,
        tenant_id: (role == Role::TenantAdmin).then(|| "one".into()), created_at:0, expires_at:u64::MAX }
}

#[test]
fn tenant_authorization_does_not_rebind_actor_or_allow_other_targets() {
    let tenant = session(Role::TenantAdmin);
    assert!(authorize_tenant(&tenant, "one", TenantType::Standard).is_ok());
    assert_eq!(authorize_tenant(&tenant, "two", TenantType::Digitalmate).err(), Some("tenant_forbidden"));
    let platform = session(Role::PlatformAdmin);
    for id in ["one", "two"] {
        let target = authorize_tenant(&platform, id, TenantType::Digitalmate).unwrap();
        assert_eq!(target.tenant_id, id);
        assert!(require_digitalmate(&target).is_ok());
    }
    assert_eq!(platform.subject_id, "actor");
    assert!(platform.tenant_id.is_none());
}

#[test]
fn standard_business_gate_is_enforced_after_authorization() {
    let target = authorize_tenant(&session(Role::PlatformAdmin), "one", TenantType::Standard).unwrap();
    assert_eq!(require_digitalmate(&target), Err("feature_not_enabled"));
}

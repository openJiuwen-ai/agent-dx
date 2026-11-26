use crate::{audit::{AuditEvent, AuditTarget, Outcome}, session::{Role, SessionRecord}};

#[test]
fn download_audit_keeps_fixed_scope_and_relative_path() {
    let target=AuditTarget {slot:Some("secondary".into()),environment_id:Some("env-old".into()),mount_id:Some("fixed".into()),path:Some("docs/readme".into()),..Default::default()};
    let value=serde_json::to_value(target).unwrap();
    assert_eq!(value["slot"],"secondary");
    assert_eq!(value["path"],"docs/readme");
    assert!(value.get("host_path").is_none());
}

#[test]
fn reveal_audit_identifies_user_key_and_purpose_without_plaintext() {
    let actor=SessionRecord {session_id:"s".into(),subject_id:"admin".into(),role:Role::PlatformAdmin,tenant_id:None,created_at:0,expires_at:100};
    let event=AuditEvent::new(&actor,"tenant","console.key.reveal",AuditTarget {
        user_id:Some("alice".into()),key_id:Some("record-1".into()),purpose:Some("copy".into()),..Default::default()
    },Outcome::Success);
    let value=serde_json::to_value(event).unwrap();
    assert_eq!(value["target"]["user_id"],"alice");
    assert_eq!(value["target"]["key_id"],"record-1");
    assert_eq!(value["target"]["purpose"],"copy");
}

#[test]
fn audit_preserves_platform_actor_and_target_tenant_without_credentials() {
    let actor = SessionRecord { session_id:"session".into(), subject_id:"operator".into(), role:Role::PlatformAdmin,
        tenant_id:None, created_at:0, expires_at:100 };
    let event = AuditEvent::new(&actor, "second", "console.binding.delete",
        AuditTarget { template:Some("app".into()), binding_id:Some("user".into()), generation:Some("7".into()), ..Default::default() }, Outcome::Unknown);
    let value = serde_json::to_value(event).unwrap();
    assert_eq!(value["actor"]["subject_id"], "operator");
    assert_eq!(value["actor"]["role"], "platform_admin");
    assert_eq!(value["target_tenant_id"], "second");
    assert_eq!(value["outcome"], "unknown");
    assert!(value["actor"].get("expires_at").is_none());
    assert!(value["actor"].get("token").is_none());
}

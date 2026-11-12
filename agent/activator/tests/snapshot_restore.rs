use adx_activator::environment_request;
use adx_agent_core::{
    environment::{CreateEnvironment, ExecutionSpec},
    network::SandboxNetworkPolicy,
    TemplateDraft,
};
use serde_json::json;
use std::time::Duration;

fn draft() -> serde_json::Value {
    json!({
        "name":"digitalmate",
        "image":"registry.test/digitalmate:fixture",
        "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":2048},
        "env":{"DIGITALMATE_BOOT_MODE":"clone","DIGITALMATE_ACTIVATION_ID":"user-fixture"},
        "mounts":[{"type":"bind","target":"/data/state","volume_id":"workspace","volume_uid":"workspace-uid"}],
        "service":[{"protocol":"ws","port":18092}]
    })
}

fn create_input(template: serde_json::Value, version: &str) -> serde_json::Value {
    let template: TemplateDraft = serde_json::from_value(template).unwrap();
    template.validate().unwrap();
    let published = template.with_version(version.into());
    assert_eq!(template, TemplateDraft::from(&published));
    environment_request::create_input(
        &CreateEnvironment {
            id: "adx-restore-fixture".into(),
            tenant: "test".into(),
            execution: ExecutionSpec::from(&published),
            deadline_unix_ms: None,
            creation_attempt: 0,
        },
        Duration::from_secs(90),
        &serde_json::from_value::<SandboxNetworkPolicy>(json!({
            "sandbox_cidrs":["10.236.64.0/20"],
            "internal_cidrs":[],"public_egress":false,"egress_allow":[]
        }))
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn pinned_template_versions_restore_their_own_snapshot_with_user_configuration() {
    for (version, id) in [("1", "snapshot-one"), ("2", "snapshot-two")] {
        let mut template = draft();
        template["snapshot_id"] = json!(id);
        let input = create_input(template, version);
        assert_eq!(input["snapshotId"], id);
        assert_eq!(input["env"]["DIGITALMATE_ACTIVATION_ID"], "user-fixture");
        assert_eq!(input["mounts"][0]["volume_uid"], "workspace-uid");
        assert!(input.get("inheritEntrypoint").is_none());
        assert!(input.get("readinessProbe").is_none());
    }
}

#[test]
fn ordinary_template_still_inherits_its_image_entrypoint() {
    let input = create_input(draft(), "1");
    assert_eq!(input["inheritEntrypoint"], true);
    assert!(input.get("snapshotId").is_none());
}

#[test]
fn snapshot_template_rejects_captured_probe_override_and_empty_identity() {
    for id in ["", " snapshot-leading-space", "\0"] {
        let mut value = draft();
        value["snapshot_id"] = json!(id);
        let template: TemplateDraft = serde_json::from_value(value).unwrap();
        assert!(template.validate().is_err(), "invalid snapshot accepted");
    }
    let mut value = draft();
    value["snapshot_id"] = json!("snapshot-one");
    value["readiness_probe"] = json!({
        "http_get":{"port":18092,"path":"/health/ready"},
        "initial_delay_seconds":0,"period_seconds":1,"timeout_seconds":1
    });
    let template: TemplateDraft = serde_json::from_value(value).unwrap();
    assert!(
        template.validate().is_err(),
        "captured probe can be overridden"
    );
}

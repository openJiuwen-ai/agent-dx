use adx_agent_core::{TemplateDraft, TemplateVersion};
use serde_json::json;

#[test]
fn publish_draft_rejects_a_caller_supplied_prewarm_recipe() {
    let digest = format!("sha256:{}", "a".repeat(64));
    let mut input = json!({
        "name":"assistant", "image":format!("assistant@{digest}"), "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":2048},
        "prewarm_recipe":{"snapshot_id":"existing", "image_digest":digest,
            "restore_profile_digest":digest, "handoff_contract_version":"2"}
    });
    for enabled in [false, true] {
        input["auto_prepare_snapshot"] = json!(enabled);
        let error = serde_json::from_value::<TemplateDraft>(input.clone()).unwrap_err();
        assert!(error.to_string().contains("unknown field `prewarm_recipe`"));
    }
}

#[test]
fn automatic_preparation_is_opt_in_and_requires_a_fixed_image() {
    let mut input = json!({"name":"assistant", "image":"assistant:1", "isolation_runtime":"runsc",
       "resources":{"cpu_millis":1000,"memory_mib":2048}});
    let disabled: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(!disabled.auto_prepare_snapshot);
    input["auto_prepare_snapshot"] = json!(true);
    let bad: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(bad.validate().is_err());
    let digest = format!("sha256:{}", "a".repeat(64));
    input["image"] = json!(format!("assistant@{digest}"));
    let automatic: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(automatic.validate().is_ok());
    let version = automatic.with_version("1".into());
    let mut obsolete = input.clone();
    obsolete["golden_seed_profile"] = json!("shared-profile");
    assert!(serde_json::from_value::<TemplateDraft>(obsolete).is_err());
    let mut spoofed = input.clone();
    spoofed["env"] = json!({"GOLDEN_PHASE": "seed"});
    assert!(serde_json::from_value::<TemplateDraft>(spoofed)
        .unwrap()
        .validate()
        .is_err());
    assert_eq!(TemplateDraft::from(&version), automatic);
}

#[test]
fn prewarm_requires_a_fixed_digest_and_a_restore_only_environment() {
    let hex = "a".repeat(64);
    let digest = format!("sha256:{hex}");
    let version: TemplateVersion = serde_json::from_value(json!({
        "name":"assistant",
        "version":"1",
        "image":format!("example.test/assistant@{digest}"),
        "isolation_runtime":"runsc",
        "auto_prepare_snapshot":true,
        "snapshot_preparation":{"status":"ready"},
        "resources":{"cpu_millis":1000,"memory_mib":2048},
        "prewarm_recipe": {
            "snapshot_id":"snapshot-fixed", "image_digest":digest,
            "restore_profile_digest":format!("sha256:{hex}"),
            "handoff_contract_version":"2",
            "restore_env":{"GOLDEN_PHASE":"restore"}
        }
    }))
    .unwrap();
    assert!(version.validate().is_ok());
    assert!(!version.env.contains_key("GOLDEN_PHASE"));
    let draft = TemplateDraft::from(&version);
    assert_eq!(
        serde_json::to_value(&draft).unwrap().get("prewarm_recipe"),
        None
    );
    assert!(draft.with_version("2".into()).prewarm_recipe.is_none());
    let mut bad = version.clone();
    bad.image = "example.test/assistant:latest".into();
    assert!(bad.validate().is_err());
    let mut bad = version;
    bad.prewarm_recipe
        .as_mut()
        .unwrap()
        .restore_env
        .insert("ADX_RUNTIME_ID".into(), "x".into());
    assert!(bad.validate().is_err());
}

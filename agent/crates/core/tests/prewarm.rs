use adx_agent_core::prewarm::{EligibleStart, PrewarmPolicy, PrewarmRecipe};
use adx_agent_core::TemplateDraft;
use serde_json::json;

#[test]
fn automatic_preparation_is_opt_in_and_excludes_a_manual_recipe() {
    let mut input = json!({"name":"assistant", "image":"assistant:1", "isolation_runtime":"runsc",
       "resources":{"cpu_millis":1000,"memory_mib":2048}});
    let manual: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(!manual.auto_prepare_snapshot);
    input["auto_prepare_snapshot"] = json!(true);
    let bad: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(bad.validate().is_err());
    let digest = format!("sha256:{}", "a".repeat(64));
    input["image"] = json!(format!("assistant@{digest}"));
    let automatic: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(automatic.validate().is_ok());
    input["prewarm_recipe"] = json!({"snapshot_id":"existing", "image_digest":digest,
        "restore_profile_digest":digest, "handoff_contract_version":"1"});
    let conflicting: TemplateDraft = serde_json::from_value(input).unwrap();
    assert!(conflicting.validate().is_err());
}

#[test]
fn prewarm_requires_a_fixed_digest_and_a_restore_only_environment() {
    let hex = "a".repeat(64);
    let digest = format!("sha256:{hex}");
    let draft: TemplateDraft = serde_json::from_value(json!({
        "name":"assistant",
        "image":format!("example.test/assistant@{digest}"),
        "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":2048},
        "prewarm_recipe": {
            "snapshot_id":"snapshot-fixed", "image_digest":digest,
            "restore_profile_digest":format!("sha256:{hex}"),
            "handoff_contract_version":"1",
            "restore_env":{"APP_PHASE":"restore"}
        }
    }))
    .unwrap();
    assert!(draft.validate().is_ok());
    assert!(!draft.env.contains_key("APP_PHASE"));
    let mut bad = draft.clone();
    bad.image = "example.test/assistant:latest".into();
    assert!(bad.validate().is_err());
    let mut bad = draft;
    bad.prewarm_recipe
        .as_mut()
        .unwrap()
        .restore_env
        .insert("ADX_RUNTIME_ID".into(), "x".into());
    assert!(bad.validate().is_err());
}

#[test]
fn enable_cut_is_strict_and_missing_registration_never_qualifies() {
    let policy = PrewarmPolicy {
        enabled: true,
        first_enabled_at_ms: Some(100),
        assignment_tracking_since_ms: Some(100),
        template: "assistant".into(),
        version: "1".into(),
        validation_ref: "verified".into(),
    };
    for (registered_at_ms, eligible) in [
        (None, false),
        (Some(99), false),
        (Some(100), false),
        (Some(101), true),
    ] {
        let facts = EligibleStart {
            product: "assistant".into(),
            registered_at_ms,
        };
        assert_eq!(policy.allows(&facts), eligible);
    }
    let _: Option<PrewarmRecipe> = None;
}

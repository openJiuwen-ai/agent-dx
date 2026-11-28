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
fn automatic_preparation_is_opt_in_and_accepts_a_tag() {
    let mut input = json!({"name":"assistant", "image":"assistant:1", "isolation_runtime":"runsc",
       "resources":{"cpu_millis":1000,"memory_mib":2048}});
    let disabled: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(!disabled.auto_prepare_snapshot);
    input["auto_prepare_snapshot"] = json!(true);
    let tagged: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    assert!(tagged.validate().is_ok());
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
fn persisted_version_two_recipe_is_validated_but_not_selected_for_new_allocations() {
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
    assert!(adx_agent_core::prewarm::template_golden(&version).is_none());
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

fn phase_free_template() -> TemplateVersion {
    let digest = format!("sha256:{}", "a".repeat(64));
    serde_json::from_value(json!({
        "name":"assistant", "version":"1", "image":format!("assistant@{digest}"),
        "isolation_runtime":"runsc", "auto_prepare_snapshot":true,
        "snapshot_preparation":{"status":"ready"},
        "resources":{"cpu_millis":1000,"memory_mib":2048},
        "prewarm_recipe": {
            "snapshot_id":"snapshot-fixed", "image_digest":digest,
            "restore_profile_digest":digest, "handoff_contract_version":"3"
        }
    }))
    .unwrap()
}

#[test]
fn prepared_recipe_validates_an_optional_digest_against_the_effective_image() {
    let existing = phase_free_template();
    let legacy_json = serde_json::to_value(&existing).unwrap();
    let decoded: TemplateVersion = serde_json::from_value(legacy_json.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), legacy_json);
    let pinned_recipe = existing.prewarm_recipe.as_ref().unwrap();
    assert!(pinned_recipe.validate(&existing.image).is_ok());
    assert!(pinned_recipe
        .validate(&format!("assistant@sha256:{}", "b".repeat(64)))
        .is_err());

    let mut tagged = legacy_json;
    tagged["image"] = json!("assistant:v1");
    let projected: TemplateVersion = serde_json::from_value(tagged.clone()).unwrap();
    assert!(projected.validate().is_ok());
    tagged["prewarm_recipe"]
        .as_object_mut()
        .unwrap()
        .remove("image_digest");
    let unpinned: TemplateVersion = serde_json::from_value(tagged).unwrap();
    assert!(unpinned.validate().is_ok());
    let tag_recipe = unpinned.prewarm_recipe.as_ref().unwrap();
    assert!(tag_recipe.validate(&unpinned.image).is_ok());
    assert!(tag_recipe.validate(&existing.image).is_err());
}

#[test]
fn phase_free_handoff_uses_a_distinct_version_for_new_allocations() {
    let template = phase_free_template();
    assert!(template.validate().is_ok());
    assert!(adx_agent_core::prewarm::template_golden(&template).is_some());
    for version in ["1", "2", "4"] {
        let mut other = template.clone();
        other
            .prewarm_recipe
            .as_mut()
            .unwrap()
            .handoff_contract_version = version.into();
        assert!(adx_agent_core::prewarm::template_golden(&other).is_none());
    }
}

#[test]
fn phase_free_handoff_rejects_restore_only_environment_values() {
    for name in ["GOLDEN_PHASE", "APP_RESTORE_HINT"] {
        let mut template = phase_free_template();
        template
            .prewarm_recipe
            .as_mut()
            .unwrap()
            .restore_env
            .insert(name.into(), "restore".into());
        assert!(template.validate().is_err());
        assert!(adx_agent_core::prewarm::template_golden(&template).is_none());
    }
}

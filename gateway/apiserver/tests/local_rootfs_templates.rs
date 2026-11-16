use adx_apiserver::contract::create_spec_with_templates;
use adx_core::{execution::StorageSource, runtime_profile::RuntimeProfile};
use adx_protocol::control::CallerContext;
use serde_json::json;
use std::collections::BTreeMap;

fn profile() -> RuntimeProfile {
    serde_json::from_value(json!({
        "rootfs":{"type":"local","path":"/opt/adx/runtime/root.img","runtime_class":"runsc"},
        "bootstrap":{"type":"erofs","root":"/opt/adx/runtime/root.img","target":"/__adx",
            "entrypoint":["/__adx/usr/local/bin/adx-execd"]}
    }))
    .unwrap()
}

#[test]
fn named_local_rootfs_preserves_metadata_and_request_overrides_without_registry_source() {
    let templates = serde_json::from_value(json!({"tools-v1": {
        "path":"/var/lib/agentos/local-images/objects/abc/rootfs",
        "env":{"PATH":"/image/bin", "IMAGE_VALUE":"original"},
        "process":{"commands":[["/bin/app","--serve"]], "cwd":"/app", "user":"1000:1000"}
    }}))
    .unwrap();
    let native = create_spec_with_templates(
        json!({"rootfs":{"type":"template","name":"tools-v1"},
        "env":{"IMAGE_VALUE":"override"}, "inheritEntrypoint":true}),
        &CallerContext {
            tenant_id: "tenant".into(),
            ..Default::default()
        },
        Some(&profile()),
        &templates,
    )
    .unwrap();
    let spec = adx_core::EnvironmentSpec::try_from(native).unwrap();
    assert!(spec.image.is_empty());
    assert_eq!(
        spec.scheduling.required_node[0].expressions[0].key,
        "ADX_LOCAL_ROOTFS_tools-v1"
    );
    assert!(!spec.execution.inherit_entrypoint);
    assert_eq!(
        spec.execution.rootfs.unwrap().source,
        StorageSource::Local("/var/lib/agentos/local-images/objects/abc/rootfs".into())
    );
    assert_eq!(spec.env["PATH"], "/image/bin");
    assert_eq!(spec.env["IMAGE_VALUE"], "override");
    assert_eq!(
        spec.execution.process.unwrap().commands.unwrap()[0],
        ["/bin/app", "--serve"]
    );
}

#[test]
fn local_template_rejects_missing_names_and_arbitrary_paths() {
    for root in [
        json!({"type":"template","name":"missing"}),
        json!({"type":"template","name":"tools-v1","path":"/etc"}),
        json!({"type":"local","path":"/etc"}),
    ] {
        assert!(create_spec_with_templates(
            json!({"rootfs":root}),
            &CallerContext {
                tenant_id: "tenant".into(),
                ..Default::default()
            },
            Some(&profile()),
            &BTreeMap::new()
        )
        .is_err());
    }
}

#[test]
fn explicit_process_can_start_on_a_template_without_image_entrypoint_metadata() {
    let templates =
        serde_json::from_value(json!({"plain": {"path":"/var/lib/agentos/plain/rootfs"}})).unwrap();
    let spec = create_spec_with_templates(
        json!({"rootfs":{"type":"template","name":"plain"},
        "inheritEntrypoint":true, "process":{"commands":[["/bin/app"]]}}),
        &CallerContext {
            tenant_id: "tenant".into(),
            ..Default::default()
        },
        Some(&profile()),
        &templates,
    )
    .unwrap();
    let spec = adx_core::EnvironmentSpec::try_from(spec).unwrap();
    assert_eq!(
        spec.execution.process.unwrap().commands.unwrap()[0],
        ["/bin/app"]
    );
    assert!(!spec.execution.inherit_entrypoint);
}

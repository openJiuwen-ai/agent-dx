use adx_agent_core::TemplateVersion;
use serde_json::json;

#[test]
fn template_renders_stable_volume_definitions_and_references_without_host_paths() {
    let template: TemplateVersion = serde_json::from_value(json!({
        "name":"app","version":"1","image":"image","isolation_runtime":"runc",
        "resources":{"cpu_millis":1000,"memory_mib":128},
        "volumes":[{"id":"workspace-${user}","source":{"type":"directory","storage_domain":"sfs","path":"workspaces/tenant/${user}"},"create_if_missing":true}],
        "mounts":[{"type":"bind","volume_id":"workspace-${user}","target":"/workspace","options":["rw"]}]
    })).unwrap();
    template.validate().unwrap();
    let parameters = [("user".into(), "alice".into())].into();
    let definitions = template.render_volumes(&parameters).unwrap();
    let value = serde_json::to_value(definitions).unwrap();
    assert_eq!(value[0]["id"], "workspace-alice");
    assert_eq!(value[0]["access_mode"], "shared");
    assert_eq!(value[0]["source"]["path"], "workspaces/tenant/alice");
    let mounts = serde_json::to_value(template.render_mounts(&parameters).unwrap()).unwrap();
    assert_eq!(mounts[0]["volume_id"], "workspace-alice");
    assert!(mounts[0].get("host_path").is_none());
}

#[test]
fn concrete_volume_ids_reject_url_metacharacters_and_non_ascii() {
    use adx_agent_core::volumes::{VolumeSource, VolumeSpec};
    let mut spec = VolumeSpec {
        id: "workspace.A_0-z".into(),
        source: VolumeSource::Directory {
            storage_domain: "sfs".into(),
            path: "workspaces/user".into(),
        },
        access_mode: Default::default(),
        create_if_missing: false,
        create_options: None,
    };
    spec.validate().unwrap();
    for id in [
        "",
        "a/b",
        "a\\b",
        "$id",
        "{id}",
        "a b",
        "a%2Fb",
        "中文",
        "a?b",
        "a#b",
        ".",
        "..",
        &"a".repeat(257),
    ] {
        spec.id = id.into();
        assert!(spec.validate().is_err(), "accepted {id:?}");
    }
    spec.id = "a".repeat(256);
    spec.validate().unwrap();
}

use adx_agent_core::*;
use serde_json::json;

fn template() -> serde_json::Value {
    json!({"name":"test","version":"1","image":"registry/app:v1",
        "isolation_runtime":"runsc","entrypoint":["/app/start", ""],
        "resources":{"cpu_millis":1000,"memory_mib":1024},
        "service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})
}

#[test]
fn template_preserves_argv_and_shared_http_ws_port() {
    let t: TemplateVersion = serde_json::from_value(template()).unwrap();
    t.validate().unwrap();
    assert_eq!(t.entrypoint[1], "");
    assert_eq!(t.service.len(), 2);
}

#[test]
fn template_requires_an_explicit_valid_entrypoint() {
    let mut t = template();
    t.as_object_mut().unwrap().remove("entrypoint");
    assert!(serde_json::from_value::<TemplateVersion>(t).is_err());
    for argv in [json!([]), json!([""]), json!(["start", "a\u{0000}b"])] {
        let mut t = template();
        t["entrypoint"] = argv;
        assert!(serde_json::from_value::<TemplateVersion>(t)
            .unwrap()
            .validate()
            .is_err());
    }
}

#[test]
fn keys_include_every_scope_component_without_delimiter_collisions() {
    assert_ne!(encode_key(&["a:b", "c"]), encode_key(&["a", "b:c"]));
    assert_ne!(
        encode_key(&["t", "n", "1", "c"]),
        encode_key(&["t", "n", "2", "c"])
    );
}

#[test]
fn isolation_and_reserved_environment_are_not_silently_changed() {
    let mut t: TemplateVersion = serde_json::from_value(template()).unwrap();
    for key in [
        "ADX_ENVIRONMENT_ID",
        "ADX_RUNTIME_ID",
        "ADX_OWNERSHIP_GENERATION",
    ] {
        t.env.insert(key.into(), "forged".into());
        assert!(t.validate().is_err(), "{key} must be owned by the platform");
        t.env.clear();
    }
    t.resources.memory_mib = u64::MAX;
    assert!(t.validate().is_err());
}

#[test]
fn internal_response_additions_are_allowed_but_write_specs_remain_strict() {
    let observation = serde_json::json!({"id":"i", "tenant":"t", "phase":"running", "ready":true, "runtime_id":null, "message":null, "future_field":42});
    assert!(
        serde_json::from_value::<adx_agent_core::sandbox::SandboxObservation>(observation).is_ok()
    );
    let target = serde_json::json!({"environment":{"sandbox_id":"s", "scope":{"tenant":"t","template":"a","version":"1","environment_id":"e"}, "generation":"g", "phase":"active"}, "service":[], "future_field":42});
    assert!(serde_json::from_value::<adx_agent_core::activator::Target>(target).is_ok());
    let execution = serde_json::json!({"image":"app", "isolation_runtime":"runc", "entrypoint":[], "working_dir":"/", "user":null, "env":{}, "resources":{"cpu_millis":1,"memory_mib":1}, "service":[], "future_field":42});
    assert!(serde_json::from_value::<adx_agent_core::sandbox::ExecutionSpec>(execution).is_err());
}

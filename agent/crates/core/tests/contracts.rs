use adx_agent_core::*;
use serde_json::json;

fn template() -> serde_json::Value {
    json!({"name":"test","version":"1","image":"registry/app:v1",
        "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":1024},
        "service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080}]})
}

#[test]
fn template_preserves_shared_http_ws_port() {
    let t: TemplateVersion = serde_json::from_value(template()).unwrap();
    t.validate().unwrap();
    assert_eq!(t.service.len(), 2);
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
fn binding_identity_round_trips_between_wire_scope_and_target() {
    let scope_json = json!({
        "tenant": "tenant-a",
        "template": "assistant",
        "version": "v1",
        "binding_id": "user:a/中文"
    });
    let scope: Scope = serde_json::from_value(scope_json.clone()).unwrap();
    scope.validate().unwrap();
    assert_eq!(serde_json::to_value(&scope).unwrap(), scope_json);

    let wire = json!({
        "binding": {
            "scope": scope_json,
            "generation": "generation-a",
            "environment_id": "environment-a",
            "phase": "active"
        },
        "service": [{"protocol": "ws", "port": 8080}]
    });
    let resolved: adx_agent_core::activator::Target = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(resolved).unwrap(), wire);

    let urn = "urn:adx:binding:assistant:v1:user%3Aa%2F%E4%B8%AD%E6%96%87";
    let target: adx_agent_core::target::Target = urn.parse().unwrap();
    assert_eq!(target.to_string(), urn);
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
        serde_json::from_value::<adx_agent_core::environment::EnvironmentObservation>(observation)
            .is_ok()
    );
    let target = serde_json::json!({"binding":{"environment_id":"s", "scope":{"tenant":"t","template":"a","version":"1","binding_id":"e"}, "generation":"g", "phase":"active"}, "service":[], "future_field":42});
    assert!(serde_json::from_value::<adx_agent_core::activator::Target>(target).is_ok());
    let execution = serde_json::json!({"image":"app", "isolation_runtime":"runc", "entrypoint":[], "working_dir":"/", "user":null, "env":{}, "resources":{"cpu_millis":1,"memory_mib":1}, "service":[], "future_field":42});
    assert!(
        serde_json::from_value::<adx_agent_core::environment::ExecutionSpec>(execution).is_err()
    );
}

#[test]
fn managed_template_inherits_image_process_without_overrides() {
    let mut wire = template();
    wire.as_object_mut().unwrap().remove("entrypoint");
    wire.as_object_mut().unwrap().remove("working_dir");
    let template: TemplateVersion = serde_json::from_value(wire.clone()).unwrap();
    template.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&template).unwrap().get("entrypoint"),
        None
    );
    assert_eq!(
        serde_json::to_value(&template).unwrap().get("working_dir"),
        None
    );
    let execution = adx_agent_core::environment::ExecutionSpec::from(&template);
    execution.validate().unwrap();
    assert_eq!(
        serde_json::to_value(execution).unwrap()["inherit_entrypoint"],
        true
    );
    for (field, value) in [
        ("entrypoint", json!(["/override"])),
        ("working_dir", json!("/override")),
    ] {
        let mut changed = wire.clone();
        changed[field] = value;
        assert!(serde_json::from_value::<TemplateVersion>(changed).is_err());
    }
}

#[test]
fn template_resource_limits_allow_bursting_and_reject_invalid_budgets() {
    let mut wire = template();
    wire["resources"] = json!({"cpu_millis":500,"memory_mib":1024,
        "cpu_limit_millis":2000,"memory_limit_mib":4096});
    let t: TemplateVersion = serde_json::from_value(wire.clone()).unwrap();
    t.validate().unwrap();
    let execution = adx_agent_core::environment::ExecutionSpec::from(&t);
    assert_eq!(
        serde_json::to_value(execution).unwrap()["resources"],
        wire["resources"]
    );
    for (field, invalid) in [
        ("cpu_limit_millis", 0),
        ("cpu_limit_millis", 499),
        ("memory_limit_mib", 0),
        ("memory_limit_mib", 1023),
        ("memory_limit_mib", u64::MAX),
    ] {
        let mut invalid_wire = wire.clone();
        invalid_wire["resources"][field] = json!(invalid);
        let t: TemplateVersion = serde_json::from_value(invalid_wire).unwrap();
        assert!(t.validate().is_err(), "{field}={invalid}");
    }
}

#[test]
fn template_readiness_probe_is_typed_and_validated() {
    let mut wire = template();
    wire["init_timeout_seconds"] = json!(30);
    wire["readiness_probe"] = json!({
        "tcp_socket":{"port":18092},
        "initial_delay_seconds":3,
        "period_seconds":3,
        "timeout_seconds":2
    });
    let template: TemplateVersion = serde_json::from_value(wire.clone()).unwrap();
    template.validate().unwrap();
    assert_eq!(
        serde_json::to_value(adx_agent_core::environment::ExecutionSpec::from(&template)).unwrap()
            ["readiness_probe"],
        wire["readiness_probe"]
    );

    for invalid in [
        json!({"tcp_socket":{"port":0}}),
        json!({"tcp_socket":{"port":18092},"http_get":{"port":18092,"path":"/ready"}}),
        json!({"http_get":{"port":18092,"path":"ready"}}),
        json!({"exec":{"command":[]}}),
        json!({"tcp_socket":{"port":18092},"period_seconds":0}),
    ] {
        let mut invalid_wire = wire.clone();
        invalid_wire["readiness_probe"] = invalid;
        let template: TemplateVersion = serde_json::from_value(invalid_wire).unwrap();
        assert!(template.validate().is_err());
    }
}

#[test]
fn healthcheck_rejects_zero_failure_threshold() {
    let mut wire = template();
    wire["healthcheck"] = json!({"tcp_socket":{"port":18092},"failure_threshold":0});
    let template: TemplateVersion = serde_json::from_value(wire).unwrap();
    assert!(template.validate().is_err());
}

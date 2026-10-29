mod support;

use adx_activator::environment_request::{create_input, operation_id};
use adx_agent_core::environment::CreateEnvironment;
use serde_json::json;
use std::time::Duration;
use support::network_policy;

fn request() -> CreateEnvironment {
    serde_json::from_value(json!({
        "tenant":"tenant", "id":"adx-generation", "execution": {
            "image":"app:1", "isolation_runtime":"runc", "inherit_entrypoint":true,
            "entrypoint":[], "working_dir":"", "user":null,
            "env":{"MODEL_KEY":"test-value"},
            "resources":{"cpu_millis":1500,"memory_mib":768}, "service":[]
        }
    }))
    .unwrap()
}
#[test]
fn both_transports_submit_the_public_environment_contract_without_agent_metadata() {
    let mut request = request();
    let first = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(
        first,
        json!({
            "namespace":"adx", "name":"generation", "image":"app:1", "runtime":"runc",
            "cpu":1500, "memory":768, "inheritEntrypoint":true,
            "env":{"MODEL_KEY":"test-value"}, "createTimeoutSeconds":60,"ports":[],
            "network":adx_activator::environment_request::network_input(&network_policy()).unwrap()
        })
    );
    request.deadline_unix_ms = Some(1);
    assert_eq!(
        create_input(&request, Duration::from_secs(60), &network_policy()).unwrap(),
        first
    );
    assert_ne!(
        operation_id("create", "tenant", &request.id).unwrap(),
        operation_id("delete", "tenant", &request.id).unwrap()
    );
}
#[test]
fn platform_owned_credentials_and_process_overrides_are_rejected() {
    let mut request = request();
    request
        .execution
        .env
        .insert("EXECD_HTTP_TOKEN".into(), "user-value".into());
    assert!(create_input(&request, Duration::from_secs(60), &network_policy()).is_err());
    request.execution.env.clear();
    request.execution.entrypoint = vec!["/override".into()];
    assert!(create_input(&request, Duration::from_secs(60), &network_policy()).is_err());
}

#[test]
fn burst_limits_reach_the_public_sandbox_contract_independently_of_requests() {
    let mut wire = serde_json::to_value(request()).unwrap();
    wire["execution"]["resources"] = json!({"cpu_millis":500,"memory_mib":1024,
        "cpu_limit_millis":2000,"memory_limit_mib":4096});
    let request: CreateEnvironment = serde_json::from_value(wire.clone()).unwrap();
    let input = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(input["cpu"], 500);
    assert_eq!(input["memory"], 1024);
    assert_eq!(input["cpu_limit"], 2000);
    assert_eq!(input["mem_limit"], 4096);
    for (field, value) in [
        ("cpu_limit_millis", i64::MAX as u64 + 1),
        ("memory_limit_mib", i64::MAX as u64 / 1048576 + 1),
    ] {
        let mut invalid = wire.clone();
        invalid["execution"]["resources"][field] = json!(value);
        let request = serde_json::from_value(invalid).unwrap();
        assert!(create_input(&request, Duration::from_secs(60), &network_policy()).is_err());
    }
}

#[test]
fn rendered_mounts_reach_the_shared_public_request() {
    let mut wire = serde_json::to_value(request()).unwrap();
    let mounts = json!([{"type":"bind","host_path":"/mnt/sfs/w-user","create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"},"target":"/home/jiuwen","options":["rw"]}]);
    wire["execution"]["mounts"] = mounts.clone();
    let request = serde_json::from_value(wire).unwrap();
    let input = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(input["mounts"], mounts);
}

#[test]
fn template_idle_pause_policy_reaches_the_public_environment_request() {
    let template: adx_agent_core::TemplateVersion = serde_json::from_value(json!({
        "name":"app", "version":"1", "image":"app:1", "isolation_runtime":"runsc",
        "resources":{"cpu_millis":500,"memory_mib":1024},
        "lifecycle":{"idle_timeout_seconds":600,"idle_action":"pause",
            "pause":{"retention":{"kind":"forever"},"timeout_seconds":300,"storage":"shared"}}
    }))
    .unwrap();
    template.validate().unwrap();
    let mut request = request();
    request.execution = (&template).into();
    let input = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(input["idleTimeoutSeconds"], 600);
    assert_eq!(input["idleAction"], "pause");
    assert_eq!(
        input["pauseOptions"],
        json!({"retention":{"kind":"forever"},"timeoutSeconds":300,"storage":"shared"})
    );
}

#[test]
fn template_readiness_reaches_the_public_environment_request() {
    let template: adx_agent_core::TemplateVersion = serde_json::from_value(json!({
        "name":"jiuwen", "version":"1", "image":"jiuwen:1", "isolation_runtime":"runsc",
        "resources":{"cpu_millis":1000,"memory_mib":1024},
        "service":[{"protocol":"ws","port":18092}],
        "init_timeout_seconds":30,
        "readiness_probe":{"tcp_socket":{"port":18092},"initial_delay_seconds":3,
            "period_seconds":3,"timeout_seconds":2}
    }))
    .unwrap();
    template.validate().unwrap();
    let mut request = request();
    request.execution = (&template).into();
    let input = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(input["initCallTimeoutSeconds"], 30);
    assert_eq!(
        input["readinessProbe"],
        json!({"tcpSocket":{"port":18092},"initialDelaySeconds":3,
            "periodSeconds":3,"timeoutSeconds":2})
    );
}

#[test]
fn template_healthcheck_reaches_platform_with_failure_threshold() {
    let template: adx_agent_core::TemplateVersion = serde_json::from_value(json!({
        "name":"jiuwen", "version":"1", "image":"jiuwen:1", "isolation_runtime":"runc",
        "resources":{"cpu_millis":1000,"memory_mib":1024},
        "healthcheck":{"tcp_socket":{"port":18092}, "initial_delay_seconds":5,
            "period_seconds":10,"timeout_seconds":2,"failure_threshold":3}
    }))
    .unwrap();
    template.validate().unwrap();
    let mut request = request();
    request.execution = (&template).into();
    let input = create_input(&request, Duration::from_secs(60), &network_policy()).unwrap();
    assert_eq!(
        input["healthcheckProbe"],
        json!({"tcpSocket":{"port":18092},
        "initialDelaySeconds":5,"periodSeconds":10,"timeoutSeconds":2,"failureThreshold":3})
    );
}

#[test]
fn runtime_network_policy_requires_an_explicit_deployment_opt_out() {
    let policy: adx_agent_core::network::SandboxNetworkPolicy = serde_json::from_value(json!({
        "sandbox_cidrs":["100.64.0.0/10", "10.83.0.0/24"], "internal_cidrs":[],
        "public_egress":true, "egress_allow":[],
        "runtime_policies":{"supervisor":"unrestricted", "conch":"deployment"}
    }))
    .unwrap();
    let mut request = request();
    request.execution.isolation_runtime = "supervisor".into();
    let input = create_input(&request, Duration::from_secs(180), &policy).unwrap();
    assert!(input.get("network").is_none());
    for runtime in ["conch", "runsc", "unknown-runtime"] {
        request.execution.isolation_runtime = runtime.into();
        let input = create_input(&request, Duration::from_secs(180), &policy).unwrap();
        assert_eq!(input["network"]["traffic"]["egressDefaultAction"], "deny");
    }
    assert!(
        serde_json::from_value::<adx_agent_core::network::SandboxNetworkPolicy>(json!({
            "sandbox_cidrs":["100.64.0.0/10"], "internal_cidrs":[],
            "public_egress":true, "egress_allow":[],
            "runtime_policies":{"supervisor":"automatic-fallback"}
        }))
        .is_err()
    );
}

#[test]
fn unrestricted_runtime_does_not_bypass_deployment_configuration_validation() {
    for (pools, overrides) in [
        (
            json!(["invalid-cidr"]),
            json!({"supervisor":"unrestricted"}),
        ),
        (json!(["100.64.0.0/10"]), json!({"":"unrestricted"})),
    ] {
        let policy = serde_json::from_value(json!({
            "sandbox_cidrs":pools,"internal_cidrs":[],"public_egress":true,
            "egress_allow":[],"runtime_policies":overrides
        }))
        .unwrap();
        let mut request = request();
        request.execution.isolation_runtime = "supervisor".into();
        assert!(create_input(&request, Duration::from_secs(180), &policy).is_err());
    }
}

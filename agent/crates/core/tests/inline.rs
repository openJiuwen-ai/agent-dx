use adx_agent_core::inline::CreateRequest;
use serde_json::json;

#[test]
fn later_report_without_image_accepts_commands_and_default_resources() {
    let input = json!({"name":"p1", "namespace":"default", "runtime_spec":{
        "runtime":"Python3.11", "sandbox_type":"supervisor", "cmds":[["sh","-c","sleep 3600"]]},
        "env_vars":{"HOME":"/home/agentos"}});
    let request: CreateRequest = serde_json::from_value(input).unwrap();
    request.validate().unwrap();
    assert_eq!(request.resources().cpu_millis, 1000);
    assert_eq!(request.resources().memory_mib, 2048);
    assert_eq!(request.runtime_spec.cmds[0][2], "sleep 3600");
}

#[test]
fn platform_dependent_fields_are_preserved_for_capability_checks() {
    let input = json!({"name":"agent", "namespace":"dev", "runtime_spec":{
        "runtime":"python3.11", "sandbox_type":"docker", "cpu":600,"memory":512,
        "rootfs":{"imageurl":"agent:v1", "user":"agentos", "ports":["tcp:22"]}},
        "workspace":"/host/workspace", "mounts":[{"source":"/host/data","target":"/mnt/data","readonly":true}]});
    let request: CreateRequest = serde_json::from_value(input).unwrap();
    request.validate().unwrap();
    assert_eq!(
        request.runtime_spec.rootfs.unwrap().user.as_deref(),
        Some("agentos")
    );
    assert!(request.mounts[0].readonly);
}

#[test]
fn inline_probe_configuration_is_preserved_for_admission() {
    let input = json!({"name":"p2", "namespace":"default", "runtime_spec":{
        "runtime":"Python3.11", "sandbox_type":"supervisor", "cmds":[["sleep","3600"]],
        "probes":{"startup":{"exec":{"command":["true"]}},"liveness":{"tcpSocket":{"port":19999},"failureThreshold":1}}}});
    let request: CreateRequest = serde_json::from_value(input).unwrap();
    request.validate().unwrap();
    assert_eq!(
        request
            .runtime_spec
            .probes
            .unwrap()
            .liveness
            .unwrap()
            .failure_threshold,
        Some(1)
    );
}

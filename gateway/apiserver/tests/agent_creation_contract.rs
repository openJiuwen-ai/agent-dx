//! Agent transports submit the same public request; API Server owns runtime injection.
use adx_activator::environment_request::create_input;
use adx_agent_core::environment::CreateEnvironment;
use adx_apiserver::{
    clients::Clients, config::Config, contract, environment_service::EnvironmentService,
};
use adx_protocol::control as pb;
use data_plane_gateway::ingress::agent_api::AgentConfig;
use serde_json::json;
#[tokio::test]
async fn embedded_and_http_creation_use_the_same_platform_contract() {
    let config: Config = serde_json::from_value(json!({
        "listen":"127.0.0.1:0", "coordinator_address":"http://127.0.0.1:1", "internal_security":"network", "ingress_mode":"standalone", "rpc_timeout_seconds":1,"cache_entries":16,"auth_cache_ttl_seconds":1,
        "runtime_profile": {
            "rootfs":{"runtime_class":"runc","type":"image","image":format!("runtime@sha256:{}","0".repeat(64)),"readonly":false},
            "bootstrap":{"type":"image","image":format!("runtime@sha256:{}","0".repeat(64)),"target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]},
            "env":{"EXECD_HTTP_TOKEN":"test-deployment-credential","EXECD_HTTP_PORT":"50090"}
        }
    })).unwrap();
    let runtime = config.runtime_profile.clone();
    let service = EnvironmentService::new(Clients::new(config).unwrap());
    let agent: AgentConfig = serde_json::from_value(json!({"embedded":{
        "redis_url":"redis://127.0.0.1:6379", "namespace":"agent",
        "request_timeout_seconds":75, "binding_cache":{"capacity":42},
        "egress_isolation":{"template_priority":200,"egress":{
            "deny":[{"cidr":"192.0.2.0/24","protocol":"any","priority":300}]
        }}
    }}))
    .unwrap();
    let settings = agent.embedded.unwrap();
    assert_eq!(settings.binding_cache.capacity, 42);
    let timeout = settings.request_timeout();
    let create: CreateEnvironment = serde_json::from_value(json!({"tenant":"tenant","id":"adx-generation","execution":{
        "network":{"egress":{"allow":[{"cidr":"192.0.2.1/32","protocol":"tcp","ports":{"first":443,"last":443}}]}},
        "image":"app:1","isolation_runtime":"runc","lifecycle":{"idle_timeout_seconds":600,"idle_action":"pause","pause":{"retention":{"kind":"forever"},"storage":"shared","timeout_seconds":300}},"inherit_entrypoint":true,"entrypoint":[],"working_dir":"","user":null,"env":{"MODEL_KEY":"test-model-value"},"mounts":[{"type":"bind","host_path":"/mnt/sfs/w-user","target":"/home/jiuwen","options":["rw"]}],"resources":{"cpu_millis":1500,"memory_mib":768},"service":[{"protocol":"ws","port":18092}],"init_timeout_seconds":30,"readiness_probe":{"tcp_socket":{"port":18092},"initial_delay_seconds":3,"period_seconds":3,"timeout_seconds":2}
    }})).unwrap();
    let input = create_input(&create, timeout, settings.egress_isolation.as_ref()).unwrap();
    assert_eq!(input["createTimeoutSeconds"], 75);
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let local = service.prepare_create(input.clone(), &caller).unwrap();
    let public = contract::create_spec_with_environment(input, &caller, runtime.as_ref()).unwrap();
    assert_eq!(local, public);
    assert_eq!(local.id, "adx-generation");
    assert_eq!(local.tenant_id, "tenant");
    let lifecycle = local.lifecycle.as_ref().unwrap();
    assert_eq!(lifecycle.idle_timeout_seconds, 600);
    assert_eq!(lifecycle.idle_action, pb::IdleAction::Pause as i32);
    assert_eq!(
        lifecycle.pause.as_ref().unwrap().storage,
        pb::CheckpointStorage::Shared as i32
    );
    let retention = lifecycle
        .pause
        .as_ref()
        .unwrap()
        .retention
        .as_ref()
        .unwrap();
    assert!(matches!(
        retention.policy,
        Some(pb::retention_policy::Policy::Forever(_))
    ));

    assert_eq!(
        local.resources.as_ref().unwrap().memory_bytes,
        768 * 1048576
    );
    let mount = &local.execution.as_ref().unwrap().mounts[0];
    assert_eq!(mount.target, "/home/jiuwen");
    assert_eq!(mount.options, ["rw"]);
    assert!(matches!(mount.mount_source.as_ref(),
        Some(pb::mount_spec::MountSource::Source(pb::StorageSource { source: Some(pb::storage_source::Source::Local(path)) })) if path == "/mnt/sfs/w-user"));
    assert!(local.execution.as_ref().unwrap().inherit_entrypoint);
    assert_eq!(local.execution.as_ref().unwrap().init_timeout_seconds, 30);
    let readiness = local
        .execution
        .as_ref()
        .unwrap()
        .readiness_probe
        .as_ref()
        .unwrap();
    assert!(matches!(
        readiness.action,
        Some(pb::probe_spec::Action::TcpSocket(ref probe)) if probe.port == 18092
    ));
    let execution = local.execution.as_ref().unwrap();
    let policy = execution.network.as_ref().unwrap();
    let rules = &policy.traffic.as_ref().unwrap().rules;
    assert_eq!(rules.len(), 2);
    assert_eq!((rules[0].priority, rules[1].priority), (300, 200));
    assert_eq!(execution.ports, vec![18092]);
    assert_eq!(local.env["MODEL_KEY"], "test-model-value");
    assert_eq!(
        local.runtime_profile.unwrap().env["EXECD_HTTP_TOKEN"],
        "test-deployment-credential"
    );
    let mut forbidden = create_input(&create, timeout, settings.egress_isolation.as_ref()).unwrap();
    forbidden["env"]["EXECD_HTTP_TOKEN"] = json!("user-supplied");
    assert!(service.prepare_create(forbidden, &caller).is_err());
}

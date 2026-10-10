//! Agent transports submit the same public request; API Server owns runtime injection.
use adx_activator::sandbox_request::create_input;
use adx_agent_core::sandbox::CreateSandbox;
use adx_apiserver::{clients::Clients, config::Config, contract, sandbox_service::SandboxService};
use adx_protocol::control as pb;
use serde_json::json;
use std::time::Duration;

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
    let service = SandboxService::new(Clients::new(config).unwrap());
    let create: CreateSandbox = serde_json::from_value(json!({"tenant":"tenant","id":"adx-generation","execution":{
        "image":"app:1","isolation_runtime":"runc","inherit_entrypoint":true,"entrypoint":[],"working_dir":"","user":null,"env":{"MODEL_KEY":"test-model-value"},"resources":{"cpu_millis":1500,"memory_mib":768},"service":[]
    }})).unwrap();
    let input = create_input(&create, Duration::from_secs(60)).unwrap();
    let caller = pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    };
    let local = service.prepare_create(input.clone(), &caller).unwrap();
    let public = contract::create_spec_with_environment(input, &caller, runtime.as_ref()).unwrap();
    assert_eq!(local, public);
    assert_eq!(local.id, "adx-generation");
    assert_eq!(local.tenant_id, "tenant");
    assert_eq!(
        local.resources.as_ref().unwrap().memory_bytes,
        768 * 1048576
    );
    assert!(local.sandbox.as_ref().unwrap().inherit_entrypoint);
    assert_eq!(local.env["MODEL_KEY"], "test-model-value");
    assert_eq!(
        local.runtime_profile.unwrap().env["EXECD_HTTP_TOKEN"],
        "test-deployment-credential"
    );
    let mut forbidden = create_input(&create, Duration::from_secs(60)).unwrap();
    forbidden["env"]["EXECD_HTTP_TOKEN"] = json!("user-supplied");
    assert!(service.prepare_create(forbidden, &caller).is_err());
}

use adx_activator::sandbox_request::{create_input, operation_id};
use adx_agent_core::sandbox::CreateSandbox;
use serde_json::json;
use std::time::Duration;

fn request() -> CreateSandbox {
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
fn both_transports_submit_the_public_sandbox_contract_without_agent_metadata() {
    let mut request = request();
    let first = create_input(&request, Duration::from_secs(60)).unwrap();
    assert_eq!(
        first,
        json!({
            "namespace":"adx", "name":"generation", "image":"app:1", "runtime":"runc",
            "cpu":1500, "memory":768, "inheritEntrypoint":true,
            "env":{"MODEL_KEY":"test-value"}, "createTimeoutSeconds":60
        })
    );
    request.deadline_unix_ms = Some(1);
    assert_eq!(
        create_input(&request, Duration::from_secs(60)).unwrap(),
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
    assert!(create_input(&request, Duration::from_secs(60)).is_err());
    request.execution.env.clear();
    request.execution.entrypoint = vec!["/override".into()];
    assert!(create_input(&request, Duration::from_secs(60)).is_err());
}

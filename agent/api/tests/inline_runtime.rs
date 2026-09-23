use adx_agent_api::inline_runtime::{command, exec_result, list_result};
use serde_json::json;

#[test]
fn inline_commands_preserve_argv_and_results() {
    assert_eq!(
        command(&json!(["printf", "%s", "a'b;$HOME"])).unwrap(),
        "exec 'printf' '%s' 'a'\"'\"'b;$HOME'"
    );
    assert_eq!(command(&json!("echo hi | cat")).unwrap(), "echo hi | cat");
    assert_eq!(
        exec_result(json!({"stdout":"hello\n","stderr":"warn","exit_code":3})).unwrap(),
        json!({"stdout":"hello\n","stderr":"warn","returncode":3})
    );
}

#[test]
fn inline_listing_maps_runtime_metadata_to_legacy_fields() {
    let value = list_result(json!({"error":null,"entries":[{"name":"file","path":"/tmp/file","type":"file","size":4,"modified_time":0.0}]})).unwrap();
    assert_eq!(
        value["items"][0],
        json!({"name":"file","path":"/tmp/file","size":4,"is_directory":false,"type":"file","modified_time":"1970-01-01T00:00:00+00:00"})
    );
    assert!(list_result(json!({"error":"Permission denied","entries":[]})).is_err());
}

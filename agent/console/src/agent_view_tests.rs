use adx_agent_core::{Scope, TemplateRef};

#[test]
fn catalog_redaction_preserves_pagination_cursor_and_null_end() {
    for cursor in [serde_json::Value::Null,serde_json::json!("opaque-cursor")] {
        let output=crate::agent_view::redact(serde_json::json!({"next_page_token":cursor,"config":{"api_token":"secret"}}));
        assert_eq!(output["next_page_token"],cursor);
        assert_eq!(output["config"]["api_token"],"********");
    }
}

#[test]
fn context_check_rejects_changed_generation_or_slot_without_retargeting() {
    let scope = Scope { tenant:"t".into(), template:"app".into(), template_ref:TemplateRef::Version("1".into()), binding_id:"user".into() };
    let value: adx_agent_core::AgentBinding = serde_json::from_value(serde_json::json!({
        "scope":scope,"generation":"7","phase":"active", "current":{"version":"1","environment_id":"env-a"},
        "secondary":null,"parameters":{},"volumes":{},"creation_attempts":{}})).unwrap();
    assert!(crate::agent_view::check_context(&value, &scope, "7", Some(("current","env-a"))).is_ok());
    assert_eq!(crate::agent_view::check_context(&value, &scope, "8", None).unwrap_err(), "generation_conflict");
    assert_eq!(crate::agent_view::check_context(&value, &scope, "7", Some(("current","env-b"))).unwrap_err(), "environment_context_conflict");
}

#[test]
fn typed_redaction_preserves_shape_without_plain_credentials() {
    let value = serde_json::json!({"name":"app","env":{"API_KEY":"sensitive","NORMAL":"hello"},"nested":[{"password":"hidden"}]});
    let masked = crate::agent_view::redact(value);
    assert_eq!(masked["env"]["NORMAL"],"hello");
    let raw = masked.to_string();
    assert!(!raw.contains("sensitive"));
    assert!(!raw.contains("hidden"));
    assert_eq!(masked["nested"][0]["password"],"********");
}

#[tokio::test]
async fn console_delete_rejects_stale_generation_without_upstream_delete() {
    use axum::{routing::get, Router, Json};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let deletes = Arc::new(AtomicUsize::new(0));
    let counter = deletes.clone();
    let upstream = Router::new().route("/templates/app/versions/1/bindings/user", get(|| async {
        Json(serde_json::json!({"binding":{"scope":{"tenant":"t","template":"app","template_ref":{"version":"1"},"binding_id":"user"},
            "generation":"8","phase":"active","current":{"version":"1","environment_id":"env-a"},"secondary":null,
            "parameters":{},"volumes":{},"creation_attempts":{}}}))
    }).delete(move || { let counter = counter.clone(); async move { counter.fetch_add(1, Ordering::SeqCst); Json(serde_json::json!({"status":"deleted"})) } }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap(); });
    let token_path = std::env::temp_dir().join(format!("console-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"upstream-secret").unwrap();
    let tenant: crate::config::Tenant = serde_json::from_value(serde_json::json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let target = crate::authorization::AuthorizedTenant { tenant_id:"t".into(), tenant_type:crate::config::TenantType::Standard };
    let scope = Scope { tenant:"t".into(), template:"app".into(), template_ref:TemplateRef::Version("1".into()), binding_id:"user".into() };
    let actor = crate::session::SessionRecord { session_id:"s".into(),subject_id:"admin".into(),role:crate::session::Role::PlatformAdmin,tenant_id:None,created_at:0,expires_at:u64::MAX };
    let endpoint = crate::config::Endpoint { base_url:format!("http://{address}"), timeout_ms:1000 };
    let clients = crate::clients::Clients::new().unwrap();
    let result = crate::agent_view::delete_binding(&clients, &endpoint, &tenant, &target, &actor, &scope, "7").await;
    assert_eq!(result.err().map(|error| (error.status.as_u16(),error.kind)), Some((409,"generation_conflict")));
    assert_eq!(deletes.load(Ordering::SeqCst),0);
    let result = crate::agent_view::delete_binding(&clients, &endpoint, &tenant, &target, &actor, &scope, "8").await;
    assert_eq!(result.unwrap_or_else(|error| panic!("{}",error.kind))["status"], "deleted");
    assert_eq!(deletes.load(Ordering::SeqCst),1);
    server.abort();
    std::fs::remove_file(token_path).unwrap();
}

#[tokio::test]
async fn delete_timeout_reports_unknown_and_never_retries() {
    use axum::{routing::get, Router, Json};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let deletes = Arc::new(AtomicUsize::new(0));
    let counter = deletes.clone();
    let upstream = Router::new().route("/templates/app/versions/1/bindings/user", get(|| async {
        Json(serde_json::json!({"binding":{"scope":{"tenant":"t","template":"app","template_ref":{"version":"1"},"binding_id":"user"},
            "generation":"8","phase":"active","current":{"version":"1","environment_id":"env-a"},"secondary":null,
            "parameters":{},"volumes":{},"creation_attempts":{}}}))
    }).delete(move || { let counter = counter.clone(); async move {
        counter.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        Json(serde_json::json!({"status":"deleted"}))
    } }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap(); });
    let token_path = std::env::temp_dir().join(format!("console-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"upstream-secret").unwrap();
    let tenant: crate::config::Tenant = serde_json::from_value(serde_json::json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let target = crate::authorization::AuthorizedTenant { tenant_id:"t".into(), tenant_type:crate::config::TenantType::Standard };
    let scope = Scope { tenant:"t".into(), template:"app".into(), template_ref:TemplateRef::Version("1".into()), binding_id:"user".into() };
    let actor = crate::session::SessionRecord { session_id:"s".into(),subject_id:"admin".into(),role:crate::session::Role::PlatformAdmin,tenant_id:None,created_at:0,expires_at:u64::MAX };
    let result = crate::agent_view::delete_binding(&crate::clients::Clients::new().unwrap(), &crate::config::Endpoint { base_url:format!("http://{address}"), timeout_ms:100 }, &tenant, &target, &actor, &scope, "8").await;
    assert_eq!(result.unwrap_or_else(|error| panic!("{}",error.kind))["status"], "unknown");
    assert_eq!(deletes.load(Ordering::SeqCst),1);
    server.abort();
    std::fs::remove_file(token_path).unwrap();
}

#[tokio::test]
async fn environment_query_failure_is_unknown_and_only_404_is_absent() {
    use axum::{routing::get, Router, http::StatusCode, Json, extract::Query};
    use std::collections::HashMap;
    let app = Router::new().route("/api/instances",get(|Query(params): Query<HashMap<String,String>>| async move {
        match params.get("instance_id").map(String::as_str) {
            Some("missing") => (StatusCode::NOT_FOUND,Json(serde_json::json!({}))),
            Some("outage") => (StatusCode::SERVICE_UNAVAILABLE,Json(serde_json::json!({"error":"internal secret"}))),
            Some("running") => (StatusCode::OK,Json(serde_json::json!([{"id":"running","tenant_id":"t","status":"running"}]))),
            _ => (StatusCode::OK,Json(serde_json::json!([]))),
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener,app).await.unwrap(); });
    let token_path = std::env::temp_dir().join(format!("console-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"secret").unwrap();
    let tenant: crate::config::Tenant = serde_json::from_value(serde_json::json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let clients = crate::clients::Clients::new().unwrap();
    let endpoint = crate::config::Endpoint { base_url:format!("http://{address}"),timeout_ms:1000 };
    for (id,status) in [("missing","absent"),("outage","unknown"),("running","running"),("empty","unknown")] {
        let result = crate::agent_view::environment_status(&clients,&endpoint,&tenant,id,"1").await;
        assert_eq!(result["environment_id"],id);
        assert_eq!(result["status"],status);
        assert!(!result.to_string().contains("internal secret"));
    }
    server.abort();
    std::fs::remove_file(token_path).unwrap();
}

#[test]
fn copy_publish_preserves_unedited_secret_without_publishing_redaction_marker() {
    let original = serde_json::json!({"env":{"API_KEY":"real-secret","NORMAL":"old"},"args":["a",null],"enabled":true});
    let edited = serde_json::json!({"env":{"API_KEY":"********","NORMAL":""},"args":["b",null],"enabled":false});
    let restored = crate::agent_view::restore_secrets(&original,edited).unwrap();
    assert_eq!(restored["env"]["API_KEY"],"real-secret");
    assert_eq!(restored["env"]["NORMAL"],"");
    assert_eq!(restored["args"],serde_json::json!(["b",null]));
    assert_eq!(restored["enabled"],false);
    assert!(crate::agent_view::restore_secrets(&serde_json::json!({}),serde_json::json!({"API_KEY":"********"})).is_err());
}

#[tokio::test]
async fn publish_uses_existing_management_api_and_returns_assigned_version() {
    use axum::{routing::post, Router, Json};
    let app = Router::new().route("/templates",post(|Json(value): Json<serde_json::Value>| async move {
        assert_eq!(value["name"],"app");
        assert!(value.get("version").is_none());
        assert_eq!(value["env"]["API_KEY"],"new-secret");
        Json(serde_json::json!({"template":{"name":"app","version":"3","env":{"API_KEY":"new-secret"}}}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener,app).await.unwrap(); });
    let token_path = std::env::temp_dir().join(format!("console-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"secret").unwrap();
    let tenant: crate::config::Tenant = serde_json::from_value(serde_json::json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let target = crate::authorization::AuthorizedTenant { tenant_id:"t".into(),tenant_type:crate::config::TenantType::Standard };
    let actor = crate::session::SessionRecord { session_id:"s".into(),subject_id:"admin".into(),role:crate::session::Role::PlatformAdmin,tenant_id:None,created_at:0,expires_at:u64::MAX };
    let draft = serde_json::json!({"name":"app","image":"app:3","isolation_runtime":"runc","resources":{"cpu_millis":100,"memory_mib":128},"env":{"API_KEY":"new-secret"}});
    let result = crate::agent_view::publish_version(&crate::clients::Clients::new().unwrap(),&crate::config::Endpoint { base_url:format!("http://{address}"),timeout_ms:1000 },&tenant,crate::agent_view::OperationContext { target:&target,actor:&actor },"app",draft,None).await.unwrap_or_else(|error| panic!("{}",error.kind));
    assert_eq!(result["template"]["version"],"3");
    assert_eq!(result["template"]["env"]["API_KEY"],"********");
    server.abort();
    std::fs::remove_file(token_path).unwrap();
}

#[tokio::test]
async fn alias_update_rejects_changed_seed_before_writing() {
    use axum::{routing::get, Router, Json};
    use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
    let writes = Arc::new(AtomicUsize::new(0));
    let counter = writes.clone();
    let app = Router::new().route("/templates/app/aliases/stable",get(|| async {
        Json(serde_json::json!({"alias":{"config":{"tenant":"t","template":"app","alias":"stable","seed":"original","versions":[{"version":"1","weight":100}]},"revision":"rev-1"}}))
    }).put(move || { let counter = counter.clone(); async move { counter.fetch_add(1,Ordering::SeqCst); Json(serde_json::json!({})) } }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener,app).await.unwrap(); });
    let token_path = std::env::temp_dir().join(format!("console-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"secret").unwrap();
    let tenant: crate::config::Tenant = serde_json::from_value(serde_json::json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let target = crate::authorization::AuthorizedTenant { tenant_id:"t".into(),tenant_type:crate::config::TenantType::Standard };
    let actor = crate::session::SessionRecord { session_id:"s".into(),subject_id:"admin".into(),role:crate::session::Role::PlatformAdmin,tenant_id:None,created_at:0,expires_at:u64::MAX };
    let input = crate::agent_view::AliasInput { seed:"changed".into(), versions:vec![adx_agent_core::alias::VersionWeight { version:"1".into(),weight:100 }], expected_revision:Some("rev-1".into()) };
    let result = crate::agent_view::update_alias(&crate::clients::Clients::new().unwrap(),&crate::config::Endpoint { base_url:format!("http://{address}"),timeout_ms:1000 },&tenant,crate::agent_view::OperationContext { target:&target,actor:&actor },"app","stable",&input).await;
    assert_eq!(result.err().map(|error| (error.status.as_u16(),error.kind)),Some((409,"seed_immutable")));
    assert_eq!(writes.load(Ordering::SeqCst),0);
    server.abort();
    std::fs::remove_file(token_path).unwrap();
}

#[test]
fn platform_resume_decodes_envelope_and_preserves_fixed_identity() {
    use base64::Engine;
    let wire = serde_json::json!({"code":200,"message":"","data":base64::engine::general_purpose::STANDARD.encode(br#"{"environmentId":"env-a","state":"running"}"#)});
    let result = crate::agent_view::resume_result(wire,"env-a").unwrap();
    assert_eq!(result["environment_id"],"env-a");
    assert_eq!(result["status"],"running");
    let wrong = serde_json::json!({"code":200,"data":base64::engine::general_purpose::STANDARD.encode(br#"{"environmentId":"env-b","state":"running"}"#)});
    assert!(crate::agent_view::resume_result(wrong,"env-a").is_err());
}

#[test]
fn wake_never_resumes_absent_unknown_or_changed_target() {
    let scope = Scope { tenant:"t".into(), template:"app".into(), template_ref:TemplateRef::Version("1".into()), binding_id:"user".into() };
    let binding = serde_json::from_value(serde_json::json!({"scope":scope,"generation":"7","phase":"active","current":{"version":"1","environment_id":"env-a"}})).unwrap();
    assert_eq!(crate::agent_view::wake_precondition(&binding,&scope,"8","current","env-a","paused"),Err("generation_conflict"));
    assert_eq!(crate::agent_view::wake_precondition(&binding,&scope,"7","current","env-b","paused"),Err("environment_context_conflict"));
    assert_eq!(crate::agent_view::wake_precondition(&binding,&scope,"7","current","env-a","absent"),Err("environment_absent"));
    assert_eq!(crate::agent_view::wake_precondition(&binding,&scope,"7","current","env-a","unknown"),Err("environment_status_unknown"));
    assert!(crate::agent_view::wake_precondition(&binding,&scope,"7","current","env-a","paused").is_ok());
    assert!(crate::agent_view::wake_precondition(&binding,&scope,"7","current","env-a","running").is_ok());
}

#[test]
fn resume_rejection_preserves_platform_code_and_unknown_outcome() {
    let value = crate::agent_view::resume_rejection(serde_json::json!({"code":409,"message":"secret internal details", "error":{"code":"CONFLICT","outcome":"not_started","retry":"never"}}),"env-a");
    assert_eq!(value["error"]["kind"],"conflict");
    assert_eq!(value["error"]["outcome"],"not_started");
    assert!(!value.to_string().contains("secret"));
    let value = crate::agent_view::resume_rejection(serde_json::json!({"error":{"code":"OUTCOME_UNKNOWN","outcome":"unknown","retry":"same_operation"}}),"env-a");
    assert_eq!(value["error"]["kind"],"outcome_unknown");
    assert_eq!(value["environment_id"],"env-a");
}
#[tokio::test]
async fn template_catalog_summary_is_pinned_to_latest_and_keeps_failed_rows() {
    use axum::{routing::get, Router, Json, http::StatusCode};
    use serde_json::json;
    let app=Router::new().route("/templates/{name}/versions/{version}",get(|axum::extract::Path((name,version)):axum::extract::Path<(String,String)>|async move {
        if name=="broken" {return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({})));}
        (StatusCode::OK,Json(json!({"template":{"name":name,"version":version,"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"port":80},{"port":81}],"env":{"SECRET":"hidden"}}})))
    }));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let server=tokio::spawn(async move {axum::serve(listener,app).await.unwrap();});
    let token_path=std::env::temp_dir().join(format!("console-summary-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token_path,"secret").unwrap();
    let tenant:crate::config::Tenant=serde_json::from_value(json!({"tenant_id":"t","display_name":"T","type":"standard","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token_path})).unwrap();
    let mut catalog=json!({"templates":[{"name":"jiuwen","latest_version":"12"},{"name":"broken","latest_version":"1"}],"total":2,"next_page_token":"cursor"});
    crate::agent_view::template_summaries(&crate::clients::Clients::new().unwrap(),&crate::config::Endpoint{base_url:format!("http://{address}"),timeout_ms:1000},&tenant,&mut catalog).await;
    assert_eq!(catalog["templates"][0]["configuration_summary"],json!({"cpu_millis":1000,"memory_mib":512,"service_count":2}));
    assert!(catalog["templates"][1]["configuration_summary"].is_null());
    assert_eq!(catalog["next_page_token"],"cursor");
    assert_eq!(catalog["total"],2);
    assert!(!catalog.to_string().contains("hidden"));
    server.abort();std::fs::remove_file(token_path).unwrap();
}

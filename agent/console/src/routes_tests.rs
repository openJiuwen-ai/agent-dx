use crate::{config::ConsoleConfig, routes::router};
use axum::{body::Body, http::{Request, StatusCode}};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn environment_http_rejects_stale_identity_before_platform_query() {
    use axum::{routing::get,Json,Router};
    use serde_json::json;
    use sha2::{Digest,Sha256};
    let upstream=Router::new().route("/templates/app/versions/1/bindings/user",get(||async {
        Json(json!({"binding":{"scope":{"tenant":"t","template":"app","template_ref":{"version":"1"},"binding_id":"user"},"generation":"7","phase":"active","current":{"version":"1","environment_id":"env-a"}}}))
    }));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
    let server=tokio::spawn(async move {axum::serve(listener,upstream).await.unwrap();});
    let token=std::env::temp_dir().join(format!("console-state-{}",uuid::Uuid::new_v4()));std::fs::write(&token,"secret").unwrap();
    let config=serde_json::from_value(json!({"listen":"127.0.0.1:8088","public_origin":"https://console.example.com",
        "agent_api":{"base_url":format!("http://{address}"),"timeout_ms":1000},"apiserver":{"base_url":"https://platform.invalid","timeout_ms":1000},
        "ingress":{"base_url":"https://gateway","execd_port":49983,"connect_timeout_ms":1000},"cookie_secret_file":"/secret","platform_admins":[],
        "tenants":[{"tenant_id":"t","display_name":"T","type":"standard","agent":{"enabled":true},"upstream_token_file":token,"admin_token_sha256":hex::encode(Sha256::digest(b"secret"))}]})).unwrap();
    let app=crate::routes::build_router(config,vec![1;32]).unwrap();
    let login=app.clone().oneshot(Request::post("/api/console/v1/session").header("content-type","application/json").header("origin","https://console.example.com").body(Body::from(r#"{"login_type":"tenant_admin","tenant_id":"t","token":"secret"}"#)).unwrap()).await.unwrap();
    let cookie=login.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap();
    for (query,status,kind) in [("expected_generation=8&environment_id=env-a",409,"generation_conflict"),("expected_generation=7&environment_id=env-b",409,"environment_context_conflict"),("expected_generation=7",400,"invalid_context")] {
        let result=app.clone().oneshot(Request::get(format!("/api/console/v1/tenants/t/bindings/app/versions/1/user/environment/current?{query}")).header("cookie",cookie).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(result.status().as_u16(),status);
        let value:serde_json::Value=serde_json::from_slice(&result.into_body().collect().await.unwrap().to_bytes()).unwrap();assert_eq!(value["error"]["kind"],kind);
    }
    server.abort();std::fs::remove_file(token).unwrap();
}

#[test]
fn environment_query_requires_paired_nonempty_context() {
    use crate::routes::EnvironmentQuery;
    for raw in [r#"{"expected_generation":"7"}"#,r#"{"environment_id":"env-a"}"#,r#"{"expected_generation":"","environment_id":"env-a"}"#] {
        let input:EnvironmentQuery=serde_json::from_str(raw).unwrap();
        assert!(input.context().is_err());
    }
    let input:EnvironmentQuery=serde_json::from_str(r#"{"expected_generation":"7","environment_id":"env-a"}"#).unwrap();
    assert_eq!(input.context().unwrap(),Some(("7","env-a")));
    assert_eq!(serde_json::from_str::<EnvironmentQuery>("{}").unwrap().context().unwrap(),None);
}

#[tokio::test]
async fn session_routes_enforce_authorized_tenant_and_business_gate() {
    use sha2::{Digest, Sha256};
    let config: ConsoleConfig = serde_json::from_value(serde_json::json!({
        "listen":"127.0.0.1:8088","public_origin":"https://console.example.com",
        "agent_api":{"base_url":"https://gateway/api/agent/v2","timeout_ms":1000},
        "apiserver":{"base_url":"https://platform","timeout_ms":1000},
        "ingress":{"base_url":"https://gateway","execd_port":49983,"connect_timeout_ms":1000},
        "cookie_secret_file":"/secret","platform_admins":[],
        "tenants":[{"tenant_id":"std","display_name":"Standard","type":"standard",
            "agent":{"enabled":true},"upstream_token_file":"/token",
            "admin_token_sha256":hex::encode(Sha256::digest(b"secret"))}]})).unwrap();
    let app = router(config, vec![1;32]).unwrap();
    for (path, content_type, expected) in [
        ("/console/", "text/html", "ADX 管理台"),
        ("/console/console.js", "text/javascript", "/api/console/v1"),
        ("/console/console.css", "text/css", ":root"),
    ] {
        let response = app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(response.headers()["content-type"].to_str().unwrap().starts_with(content_type));
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&body).unwrap().contains(expected));
    }
    let response = app.clone().oneshot(Request::get("/api/console/v1/tenants").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let login = app.clone().oneshot(Request::post("/api/console/v1/session")
        .header("content-type","application/json").header("origin","https://console.example.com")
        .body(Body::from(r#"{"login_type":"tenant_admin","tenant_id":"std","token":"secret"}"#)).unwrap()).await.unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap().to_owned();
    let filtered=app.clone().oneshot(Request::get("/api/console/v1/tenants?name=missing&page_size=1").header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();
    let filtered:serde_json::Value=serde_json::from_slice(&filtered.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(filtered["total"],0);
    assert!(login.headers()["set-cookie"].to_str().unwrap().contains("HttpOnly"));
    for (path, status) in [("/api/console/v1/tenants/std",200), ("/api/console/v1/tenants/other",403), ("/api/console/v1/tenants/std/users",403), ("/api/console/v1/tenants/other/templates",403),
        ("/api/console/v1/tenants/other/templates/app/versions",403),
        ("/api/console/v1/tenants/std/users/user-a",403),
        ("/api/console/v1/tenants/other/users/user-a",403),
        ("/api/console/v1/tenants/other/templates/app/versions/1",403),
        ("/api/console/v1/tenants/other/templates/app",403),
        ("/api/console/v1/tenants/other/templates/app/versions/1/bindings",403),
        ("/api/console/v1/tenants/other/templates/app/versions/1/alias-usage",403),
        ("/api/console/v1/tenants/other/templates/app/aliases",403),
        ("/api/console/v1/tenants/other/templates/app/aliases/stable",403),
        ("/api/console/v1/tenants/other/bindings",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user/files/mounts?slot=current&expected_generation=7&environment_id=env-a",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user/files?slot=current&expected_generation=7&environment_id=env-a&mount_id=fixed&path=private&mode=read",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user/files/download?slot=current&expected_generation=7&environment_id=env-a&mount_id=fixed&path=private",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user/terminal/current?expected_generation=7&environment_id=env-a",403),
        ("/api/console/v1/tenants/std/bindings/app/versions/1/user/terminal/current?expected_generation=7&environment_id=env-a",403),
        ("/api/console/v1/tenants/other/bindings/app/versions/1/user/environment/current",403)] {
        let response = app.clone().oneshot(Request::get(path).header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status().as_u16(),status);
        assert_eq!(response.headers()["cache-control"],"no-store");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(!String::from_utf8_lossy(&body).contains("secret"));
    }
    for (tenant,origin,kind) in [("other","https://console.example.com","tenant_forbidden"),("std","https://evil.example.com","origin_forbidden"),("std","https://console.example.com","feature_not_enabled")] {
        let response=app.clone().oneshot(Request::post(format!("/api/console/v1/tenants/{tenant}/users/alice/key/reveal"))
            .header("cookie",&cookie).header("origin",origin).header("content-type","application/json")
            .body(Body::from(r#"{"key_id":"fixed","purpose":"copy"}"#)).unwrap()).await.unwrap();
        assert_eq!(response.status(),StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["cache-control"],"no-store");
        let bytes=response.into_body().collect().await.unwrap().to_bytes();
        let value:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["kind"],kind);
    }
    let response = app.oneshot(Request::post("/api/console/v1/tenants/other/bindings/app/versions/1/user/environment/current/wake")
        .header("cookie",&cookie).header("origin","https://console.example.com").header("content-type","application/json")
        .body(Body::from(r#"{"expected_generation":"7","environment_id":"env-a"}"#)).unwrap()).await.unwrap();
    assert_eq!(response.status(),StatusCode::FORBIDDEN);
}

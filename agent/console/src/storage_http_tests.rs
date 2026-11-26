use serde_json::json;

#[tokio::test]
async fn volume_resolution_decodes_platform_envelope_and_checks_uid() {
    use axum::{Json, Router, routing::get};
    use base64::Engine;
    let volume = json!({"uid":"uid-a","revision":1,"spec":{"tenant_id":"wld","id":"workspace",
        "source":{"type":"directory","storage_domain":"sfs","path":"adx-wld/workspaces/test"}}});
    let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&volume).unwrap());
    let upstream = Router::new().route("/api/environment/v1/volumes/workspace", get(move || {
        let encoded = encoded.clone();
        async move { Json(json!({"code":200,"data":encoded,"message":""})) }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap(); });
    let directory = std::env::temp_dir().join(format!("console-volume-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("token"), "upstream").unwrap();
    let mut config:crate::config::ConsoleConfig = serde_json::from_str(include_str!("../config.example.json")).unwrap();
    config.apiserver.base_url = format!("http://{address}");
    let tenant = &mut config.tenants[0];
    tenant.tenant_id = "wld".into();
    tenant.upstream_token_file = directory.join("token").to_str().unwrap().into();
    let mut ctx = crate::storage_http::Context {
        binding: serde_json::from_value(json!({"scope":{"tenant":"wld","template":"app","template_ref":{"version":"1"},"binding_id":"test"},
            "generation":"g","phase":"active","current":{"version":"1","environment_id":"env"},"volumes":{"workspace":"uid-a"}})).unwrap(),
        mounts: serde_json::from_value(json!([{"type":"bind","volume_id":"workspace","target":"/data"}])).unwrap(),
        mapping: serde_json::from_value(json!({"revision":"map-1","domains":[{"storage_domain":"sfs","backend_id":"backend",
            "read_only_root":"/srv/adx-shared","mount_source":"nfs:/","tenant_paths":{"wld":"adx-wld/workspaces"}}]})).unwrap(),
    };
    let clients = crate::clients::Clients::new().unwrap();
    let resolved = crate::storage_http::resolve(&clients, &config, &config.tenants[0], &ctx, 0, "current").await.unwrap();
    assert_eq!(resolved.path, "adx-wld/workspaces/test");
    ctx.binding.volumes.insert("workspace".into(), "replaced-uid".into());
    assert_eq!(crate::storage_http::resolve(&clients, &config, &config.tenants[0], &ctx, 0, "current").await.err(), Some("volume_identity_conflict"));
    server.abort();
    std::fs::remove_dir_all(directory).unwrap();
}

/// Run with ADX_CONSOLE_STORAGE_FIXTURE pointing to a separate read-only mount,
/// as a non-root process. The deployment check deliberately remains enabled.
#[tokio::test]
#[ignore = "requires non-root execution and ADX_CONSOLE_STORAGE_FIXTURE read-only mount"]
async fn file_http_read_download_and_volume_race_on_real_readonly_mount() {
    use axum::{Json,Router,routing::get,http::Request,body::Body};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
    let root=std::env::var("ADX_CONSOLE_STORAGE_FIXTURE").expect("explicit storage fixture required");
    let info=std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    let source=info.lines().find_map(|line|{let (left,right)=line.split_once(" - ")?;(left.split_whitespace().nth(4)==Some(root.as_str())).then(||right.split_whitespace().nth(1).unwrap().to_owned())}).unwrap();
    let mode=Arc::new(AtomicUsize::new(0));let reads=Arc::new(AtomicUsize::new(0));
    let m=mode.clone();let r=reads.clone();
    let upstream=Router::new().route("/templates/app/versions/1/bindings/user",get(move ||{let m=m.clone();let r=r.clone();async move {
        let n=r.fetch_add(1,Ordering::SeqCst);let generation=if m.load(Ordering::SeqCst)==2&&n>0 {"8"}else{"7"};
        Json(json!({"binding":{"scope":{"tenant":"t","template":"app","template_ref":{"version":"1"},"binding_id":"user"},"generation":generation,"phase":"active","current":{"version":"1","environment_id":"env"},"volumes":{"workspace":"uid-a"},"parameters":{}}}))
    }})).route("/templates/app/versions/1",get(||async{Json(json!({"template":{"name":"app","version":"1","image":"image","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":128},"mounts":[{"type":"bind","volume_id":"workspace","target":"/data"}]}}))}));
    let volume_reads=Arc::new(AtomicUsize::new(0));let m=mode.clone();let v=volume_reads.clone();
    let upstream=upstream.route("/api/environment/v1/volumes/workspace",get(move ||{let m=m.clone();let v=v.clone();async move {
        let n=v.fetch_add(1,Ordering::SeqCst);let revision=if m.load(Ordering::SeqCst)==1&&n>0 {2}else{1};
        Json(json!({"uid":"uid-a","revision":revision,"spec":{"tenant_id":"t","id":"workspace","source":{"type":"directory","storage_domain":"sfs","path":"tenants/t/workspace"}}}))
    }}));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
    let server=tokio::spawn(async move{axum::serve(listener,upstream).await.unwrap();});
    let directory=std::env::temp_dir().join(format!("console-http-{}",uuid::Uuid::new_v4()));std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("token"),"upstream").unwrap();
    std::fs::write(directory.join("mappings"),serde_json::to_vec(&json!({"revision":"map-1","domains":[{"storage_domain":"sfs","backend_id":"backend","read_only_root":root,"mount_source":source,"tenant_paths":{"t":"tenants/t"}}]})).unwrap()).unwrap();
    use sha2::{Digest,Sha256};
    let config=serde_json::from_value(json!({"listen":"127.0.0.1:8088","public_origin":"https://console.example.com",
        "agent_api":{"base_url":format!("http://{address}"),"timeout_ms":1000},"apiserver":{"base_url":format!("http://{address}"),"timeout_ms":1000},
        "ingress":{"base_url":"https://gateway","execd_port":49983,"connect_timeout_ms":1000},"cookie_secret_file":"/secret","platform_admins":[],
        "shared_files":{"mappings_file":directory.join("mappings"),"directory_entries_limit":100},
        "tenants":[{"tenant_id":"t","display_name":"T","type":"standard","agent":{"enabled":true},"admin_token_sha256":hex::encode(Sha256::digest(b"login")),"upstream_token_file":directory.join("token")}]})).unwrap();
    let app=crate::routes::build_router(config,vec![1;32]).unwrap();
    let login=app.clone().oneshot(Request::post("/api/console/v1/session").header("origin","https://console.example.com").header("content-type","application/json").body(Body::from(r#"{"login_type":"tenant_admin","tenant_id":"t","token":"login"}"#)).unwrap()).await.unwrap();
    let cookie=login.headers()["set-cookie"].to_str().unwrap().split(';').next().unwrap().to_owned();
    let prefix="/api/console/v1/tenants/t/bindings/app/versions/1/user/files";let query="slot=current&expected_generation=7&environment_id=env";
    let response=app.clone().oneshot(Request::get(format!("{prefix}/mounts?{query}")).header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(),200);let bytes=response.into_body().collect().await.unwrap().to_bytes();let mounts:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
    let mount=mounts["mounts"][0]["mount_id"].as_str().unwrap();
    for (suffix,expected) in [("&mode=list&path=","readme.txt"),("&mode=read&path=readme.txt","fixture-content")] {
        let response=app.clone().oneshot(Request::get(format!("{prefix}?{query}&mount_id={mount}{suffix}")).header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();assert_eq!(response.status(),200);assert_eq!(response.headers()["cache-control"],"no-store");let body=response.into_body().collect().await.unwrap().to_bytes();assert!(String::from_utf8_lossy(&body).contains(expected));
    }
    let response=app.clone().oneshot(Request::get(format!("{prefix}/download?{query}&mount_id={mount}&path=readme.txt")).header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();assert_eq!(response.status(),200);assert_eq!(response.into_body().collect().await.unwrap().to_bytes(),"fixture-content");
    for race in [1,2] {for download in [false,true] {mode.store(race,Ordering::SeqCst);reads.store(0,Ordering::SeqCst);volume_reads.store(0,Ordering::SeqCst);
        let endpoint=if download {format!("{prefix}/download")}else{prefix.into()};
        let response=app.clone().oneshot(Request::get(format!("{endpoint}?{query}&mount_id={mount}&path=readme.txt&mode=read")).header("cookie",&cookie).body(Body::empty()).unwrap()).await.unwrap();assert_eq!(response.status(),409,"race {race} download {download}");let body=response.into_body().collect().await.unwrap().to_bytes();assert!(!String::from_utf8_lossy(&body).contains("fixture-content"));assert!(reads.load(Ordering::SeqCst)>=2);if race==1 {assert!(volume_reads.load(Ordering::SeqCst)>=2);}
    }}
    server.abort();std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn storage_context_uses_historical_secondary_and_rejects_stale_generation_before_template_lookup() {
    use axum::{Json,Router,routing::get};
    use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
    let queries=Arc::new(AtomicUsize::new(0));
    let counter=queries.clone();
    let upstream=Router::new()
        .route("/templates/app/aliases/stable/bindings/user",get(||async {Json(json!({"binding":{
            "scope":{"tenant":"t","template":"app","template_ref":{"alias":"stable"},"binding_id":"user"},
            "generation":"7","phase":"active","current":{"version":"2","environment_id":"env-new"},
            "secondary":{"environment":{"version":"1","environment_id":"env-old"},"intent":"retiring"},
            "parameters":{},"volumes":{},"creation_attempts":{}
        }}))}))
        .route("/templates/app/versions/1",get(move ||{let counter=counter.clone();async move {
            counter.fetch_add(1,Ordering::SeqCst);
            Json(json!({"template":{"name":"app","version":"1","image":"image","isolation_runtime":"runc",
                "resources":{"cpu_millis":1000,"memory_mib":128},"mounts":[]}}))
        }}));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let server=tokio::spawn(async move {axum::serve(listener,upstream).await.unwrap();});
    let directory=std::env::temp_dir().join(format!("console-storage-context-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("token"),"upstream-secret").unwrap();
    std::fs::write(directory.join("mappings"),r#"{"revision":"map-1","domains":[]}"#).unwrap();
    let config:crate::config::ConsoleConfig=serde_json::from_value(json!({
        "listen":"127.0.0.1:8088","public_origin":"https://console.example.com",
        "agent_api":{"base_url":format!("http://{address}"),"timeout_ms":1000},
        "apiserver":{"base_url":"https://platform","timeout_ms":1000},
        "ingress":{"base_url":"https://gateway","execd_port":49983,"connect_timeout_ms":1000},
        "shared_files":{"mappings_file":directory.join("mappings"),"directory_entries_limit":100},
        "cookie_secret_file":"/secret","platform_admins":[],
        "tenants":[{"tenant_id":"t","display_name":"T","type":"standard","agent":{"enabled":true},
            "admin_token_sha256":"00".repeat(32),"upstream_token_file":directory.join("token")}]
    })).unwrap();
    let clients=crate::clients::Clients::new().unwrap();
    let scope=adx_agent_core::Scope {tenant:"t".into(),template:"app".into(),template_ref:adx_agent_core::TemplateRef::Alias("stable".into()),binding_id:"user".into()};
    let stale=crate::storage_http::context(&clients,&config,&config.tenants[0],&scope,"8","secondary","env-old").await;
    assert_eq!(stale.err(),Some("generation_conflict"));
    assert_eq!(queries.load(Ordering::SeqCst),0);
    let context=crate::storage_http::context(&clients,&config,&config.tenants[0],&scope,"7","secondary","env-old").await.unwrap();
    assert!(context.mounts.is_empty());
    assert_eq!(queries.load(Ordering::SeqCst),1);
    server.abort();std::fs::remove_dir_all(directory).unwrap();
}

use serde_json::json;

#[tokio::test]
async fn accounts_reveal_uses_target_credential_and_preserves_conflict() {
    use axum::{Router,routing::post,Json,http::{HeaderMap,StatusCode}};
    let router=Router::new().route("/users/{user}/key/reveal",post(|headers:HeaderMap,Json(input):Json<serde_json::Value>|async move {
        assert_eq!(headers["authorization"],"Bearer accounts-token");
        assert_eq!(input["purpose"],"copy");
        (StatusCode::CONFLICT,Json(json!({"error":{"kind":"key_identity_conflict"}})))
    }));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let task=tokio::spawn(async move {axum::serve(listener,router).await.unwrap();});
    let path=std::env::temp_dir().join(format!("accounts-token-{}",uuid::Uuid::new_v4()));
    std::fs::write(&path,"accounts-token").unwrap();
    let endpoint=crate::config::AccountsApi {base_url:format!("http://{address}"),timeout_ms:1000,token_file:path.to_str().unwrap().into()};
    let result=crate::clients::Clients::new().unwrap().accounts(&endpoint,reqwest::Method::POST,&["users","a/b","key","reveal"],"",Some(&json!({"key_id":"old","purpose":"copy"})),"actor").await;
    let error=result.err().unwrap();
    assert_eq!(error.status,StatusCode::CONFLICT);
    assert_eq!(error.kind,"key_identity_conflict");
    task.abort();
    std::fs::remove_file(path).unwrap();
}

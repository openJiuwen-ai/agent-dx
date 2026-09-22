use adx_cli::{execute, Cli, Configuration};
use clap::Parser;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

type Requests = Arc<Mutex<Vec<(String, String, String)>>>;

#[tokio::test]
async fn public_management_routes_credentials_and_json_output() {
    use axum::{
        extract::{Request, State},
        response::IntoResponse,
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    async fn handler(State(seen): State<Requests>, request: Request) -> impl IntoResponse {
        let uri = request.uri().to_string();
        let auth = request
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let method = request.method().to_string();
        seen.lock().unwrap().push((method, uri.clone(), auth));
        if uri.contains("page_size") {
            axum::Json(json!({"environments":[],"next_page_token":"next"}))
        } else {
            axum::Json(json!({"status":"deleted"}))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .fallback(handler)
        .with_state(seen.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (args, expected_method, expected_path) in [
        (vec!["adx", "--output", "json", "env", "list", "--template", "app/name", "--version", "v 1", "--page-size", "2", "--page-token", "a+b"], "GET", "/api/agent/v2/templates/app%2Fname/versions/v%201/environments?page_size=2&page_token=a%2Bb"),
        (vec!["adx", "env", "delete", "demo", "--template", "app", "--version", "1"], "DELETE", "/api/agent/v2/templates/app/versions/1/environments/demo"),
    ] {
        let cli = Cli::try_parse_from(args).unwrap();
        let config = Configuration::new(&endpoint, "tenant-key".into(), None, std::time::Duration::from_secs(3), true).unwrap();
        let result = execute(&cli.command, &config).await.unwrap();
        let text = result.render(cli.output).unwrap();
        serde_json::from_str::<Value>(&text).unwrap();
        let call = seen.lock().unwrap().last().unwrap().clone();
        assert_eq!(call, (expected_method.into(), expected_path.into(), "Bearer tenant-key".into()));
    }
    server.abort();
}

#[test]
fn explicit_options_override_environment_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(
        &path,
        r#"{"endpoint":"https://file.example","timeout_seconds":20,"token_file":"unused"}"#,
    )
    .unwrap();
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, "file-token\n").unwrap();
    let cli = Cli::try_parse_from([
        "adx",
        "--config",
        path.to_str().unwrap(),
        "--endpoint",
        "https://argument.example",
        "--token-file",
        token_path.to_str().unwrap(),
        "template",
        "get",
        "app",
        "--version",
        "1",
    ])
    .unwrap();
    let config = Configuration::load_with_env(&cli, |name| match name {
        "ADX_SERVER_ADDRESS" => Some("https://env.example".into()),
        "ADX_TOKEN" => Some("env-token".into()),
        _ => None,
    })
    .unwrap();
    assert_eq!(config.endpoint.as_str(), "https://argument.example/");
    assert_eq!(config.token(), "file-token");
    assert_eq!(config.timeout.as_secs(), 20);
}

#[tokio::test]
async fn server_errors_do_not_replay_writes_or_expose_credentials() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let count = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let calls = count.clone();
    let app = axum::Router::new().fallback(move || {
        let calls = calls.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(json!({"code":"outcome_unknown","message":"inspect original identity"})),
            )
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cli = Cli::try_parse_from([
        "adx",
        "env",
        "delete",
        "demo",
        "--template",
        "app",
        "--version",
        "1",
    ])
    .unwrap();
    let error = execute(
        &cli.command,
        &Configuration::new(
            &endpoint,
            "secret-key".into(),
            None,
            std::time::Duration::from_secs(3),
            true,
        )
        .unwrap(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("503"));
    assert!(!error.to_string().contains("secret-key"));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_prints_machine_result_to_stdout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().fallback(|| async {
        axum::Json(json!({"environment":{"scope":{"environment_id":"demo"}}}))
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_adx"))
        .args([
            "--endpoint",
            &endpoint,
            "--allow-http",
            "--output",
            "json",
            "env",
            "get",
            "demo",
            "--template",
            "app",
            "--version",
            "1",
        ])
        .env("ADX_TOKEN", "opaque-tenant-key")
        .env_remove("ADX_CONFIG")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["environment"]["scope"]
            ["environment_id"],
        "demo"
    );
    assert!(output.stderr.is_empty());
    server.abort();
}

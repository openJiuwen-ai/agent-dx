use adx_cli::{http, Cli, Command, Configuration};
use clap::Parser;
use std::time::Duration;

#[tokio::test]
async fn http_routes_body_and_reports_environment_before_streaming_output() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().fallback(|request: axum::extract::Request| async {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.headers()["authorization"], "Bearer tenant-key");
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(request.uri().path(), "/agent/http/chat");
        let pairs: Vec<_> =
            url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes()).collect();
        assert!(pairs
            .iter()
            .any(|(k, v)| k == "target" && v == "urn:adx:template:demo:1"));
        assert_eq!(pairs.iter().filter(|(k, _)| k == "q").count(), 2);
        assert_eq!(
            axum::body::to_bytes(request.into_body(), 1024)
                .await
                .unwrap(),
            "{\"message\":\"hi\"}"
        );
        (
            [
                ("x-adx-environment-id", "generated"),
                (
                    "x-adx-environment-urn",
                    "urn:adx:environment:demo:1:generated",
                ),
            ],
            "data: hello\n\ndata: world\n\n",
        )
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cli = Cli::try_parse_from([
        "adx",
        "http",
        "--template",
        "demo",
        "--version",
        "1",
        "--method",
        "POST",
        "--path",
        "/chat?q=1&q=2",
        "--header",
        "Content-Type: application/json",
        "--data",
        "{\"message\":\"hi\"}",
    ])
    .unwrap();
    let Command::Http(args) = &cli.command else {
        panic!("http command")
    };
    let config = Configuration::new(
        &endpoint,
        "tenant-key".into(),
        None,
        Duration::from_secs(3),
        true,
    )
    .unwrap();
    let mut output = Vec::new();
    http::execute(args, &config, &mut output).await.unwrap();
    assert_eq!(String::from_utf8(output).unwrap(), "Environment: generated\nTarget: urn:adx:environment:demo:1:generated\ndata: hello\n\ndata: world\n\n");
    server.abort();
}

#[test]
fn http_validates_business_paths_and_preserves_explicit_environment() {
    let config = Configuration::new(
        "https://gateway.example",
        "key".into(),
        None,
        Duration::from_secs(3),
        false,
    )
    .unwrap();
    for path in [
        "https://other.example/",
        "//other.example/",
        "/../api/agent",
        "/%2e%2e/api",
        "/?target=other",
        "/?instance=other",
    ] {
        let cli = Cli::try_parse_from([
            "adx",
            "http",
            "--template",
            "demo",
            "--version",
            "1",
            "--path",
            path,
        ])
        .unwrap();
        let Command::Http(args) = cli.command else {
            panic!("http command")
        };
        assert!(args.url(&config.endpoint).is_err(), "{path}");
    }
    let cli = Cli::try_parse_from([
        "adx",
        "http",
        "--template",
        "demo",
        "--version",
        "1",
        "--env",
        "existing",
        "--port",
        "8080",
    ])
    .unwrap();
    let Command::Http(args) = cli.command else {
        panic!("http command")
    };
    let url = args.url(&config.endpoint).unwrap();
    assert!(url
        .query_pairs()
        .any(|(k, v)| k == "target" && v == "urn:adx:environment:demo:1:existing"));
    assert!(url.query_pairs().any(|(k, v)| k == "port" && v == "8080"));
}

#[tokio::test]
async fn response_stream_outlives_header_deadline_and_redirect_is_not_replayed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nfirst")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        socket.write_all(b"last!").await.unwrap();
    });
    let cli = Cli::try_parse_from(["adx", "http", "--template", "demo", "--version", "1"]).unwrap();
    let Command::Http(args) = &cli.command else {
        panic!("http command")
    };
    let config = Configuration::new(
        &endpoint,
        "key".into(),
        None,
        Duration::from_millis(100),
        true,
    )
    .unwrap();
    let mut output = Vec::new();
    http::execute(args, &config, &mut output).await.unwrap();
    assert_eq!(output, b"firstlast!");
    server.await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = calls.clone();
    let app = axum::Router::new().fallback(move || {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async {
            (
                axum::http::StatusCode::TEMPORARY_REDIRECT,
                [("location", "/again")],
                "redirect",
            )
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config =
        Configuration::new(&endpoint, "key".into(), None, Duration::from_secs(3), true).unwrap();
    let mut output = Vec::new();
    assert!(matches!(
        http::execute(args, &config, &mut output).await,
        Err(adx_cli::Error::Server { status: 307, .. })
    ));
    assert_eq!(output, b"redirect");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_stdin_upload_exits_while_input_pipe_remains_open() {
    use std::io::Write;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let notify = notify.clone();
        async move {
            let _body = request.into_body();
            notify.notify_one();
            std::future::pending::<String>().await
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_adx"))
        .args([
            "--endpoint",
            &endpoint,
            "--allow-http",
            "http",
            "--template",
            "demo",
            "--version",
            "1",
            "--method",
            "POST",
            "--data-file",
            "-",
        ])
        .env("ADX_TOKEN", "tenant-key")
        .env_remove("ADX_CONFIG")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(b"unfinished input\n").unwrap();
    tokio::time::timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    assert!(std::process::Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap()
        .success());
    let mut status = None;
    for _ in 0..30 {
        status = child.try_wait().unwrap();
        if status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if status.is_none() {
        child.kill().unwrap();
        child.wait().unwrap();
    }
    server.abort();
    assert_eq!(status.and_then(|s| s.code()), Some(130));
    drop(input);
}

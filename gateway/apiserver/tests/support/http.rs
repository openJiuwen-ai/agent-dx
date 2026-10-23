use adx_apiserver::http::Api;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn request(
    api: Arc<Api>,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &str,
) -> hyper::Response<Vec<u8>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |request| api.clone().serve(request)),
            )
            .await
            .unwrap();
    });
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    server.await.unwrap();
    let split = raw
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .unwrap();
    let head = std::str::from_utf8(&raw[..split]).unwrap();
    let status: u16 = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut response = hyper::Response::builder()
        .status(status)
        .body(raw[split + 4..].to_vec())
        .unwrap();
    for (name, value) in head.lines().skip(1).filter_map(|line| line.split_once(':')) {
        response.headers_mut().insert(
            name.parse::<hyper::header::HeaderName>().unwrap(),
            value.trim().parse().unwrap(),
        );
    }
    response
}

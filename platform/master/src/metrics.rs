//! Optional local Prometheus endpoint; deploy on a private monitoring interface.
use crate::rpc::MasterRpc;
use bytes::Bytes;
use http_body_util::Full;
use std::time::Duration;
use tokio::net::TcpListener;

pub async fn serve(listener: TcpListener, master: MasterRpc) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let master = master.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let master = master.clone();
                    async move {
                        let (status, body) = if request.method() != hyper::Method::GET
                            || request.uri().path() != "/metrics"
                        {
                            (404, String::new())
                        } else {
                            match master.metrics().await {
                                Ok(mut text) => {
                                    text.push_str(&adx_observability::trace::metrics());
                                    (200, text)
                                }
                                Err(_) => (503, "metrics temporarily unavailable\n".into()),
                            }
                        };
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .status(status)
                                .header("Content-Type", "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(body)))
                                .expect("static metrics response is valid"),
                        )
                    }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(10), connection).await;
        });
    }
}

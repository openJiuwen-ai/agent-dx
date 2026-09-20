use adx_api_server::{clients::Clients, config::Config, http::Api};
use adx_service_runtime::{read_config, shutdown};
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, task::JoinSet};
use tokio_rustls::TlsAcceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    adx_observability::init().map_err(|_| "logging initialization failed")?;
    let _tracing = adx_observability::trace::init("adx-api-server")
        .map_err(|_| "tracing initialization failed")?;
    let config: Config = read_config()?;
    config.validate()?;
    let clients = Clients::new(config.clone())?;
    let api = Api::new(clients).map_err(|_| "API initialization failed")?;
    let acceptor = if config.loopback_http {
        None
    } else {
        let cert = std::fs::read(&config.certificate)?;
        let key = std::fs::read(&config.private_key)?;
        let certificates =
            rustls_pemfile::certs(&mut cert.as_slice()).collect::<Result<Vec<_>, _>>()?;
        let private_key =
            rustls_pemfile::private_key(&mut key.as_slice())?.ok_or("missing private key")?;
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?;
        Some(TlsAcceptor::from(Arc::new(tls)))
    };
    let listener = TcpListener::bind(config.listen).await?;
    adx_observability::info!(address=%listener.local_addr()?,"adx-api-server listening");
    let (stop, receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let shutdown = shutdown();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    adx_observability::warn!(%error, "API connection task failed");
                }
            }
            accepted = listener.accept() => {
                let (socket, _peer_address) = accepted?;
                let api = api.clone();
                let acceptor = acceptor.clone();
                let stop = receiver.clone();
                tasks.spawn(async move {
                    if let Some(acceptor) = acceptor {
                        match tokio::time::timeout(
                            Duration::from_secs(10),
                            acceptor.accept(socket),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => serve(stream, api, stop).await,
                            Ok(Err(error)) => {
                                adx_observability::warn!(%error, "API TLS handshake failed");
                            }
                            Err(_) => {
                                adx_observability::warn!("API TLS handshake timed out");
                            }
                        }
                    } else {
                        serve(socket, api, stop).await;
                    }
                });
            }
        }
    }
    let _ = stop.send(true);
    if tokio::time::timeout(Duration::from_secs(15), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}
async fn serve<T>(stream: T, api: Arc<Api>, mut stop: watch::Receiver<bool>)
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10));
    let connection = builder.serve_connection(
        TokioIo::new(stream),
        service_fn(move |request| api.clone().serve(request)),
    );
    tokio::pin!(connection);
    tokio::select! {
        _ = connection.as_mut() => {}
        _ = stop.changed() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }
    }
}

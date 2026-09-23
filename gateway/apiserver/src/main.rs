use adx_agent_core::sandbox::Sandbox;
use adx_apiserver::{
    activator::ActivatorSandboxAdapter,
    clients::Clients,
    config::{Config, IngressMode},
    http::Api,
    ingress::EmbeddedIngress,
};
use adx_process::{read_config, shutdown};
use adx_transport::tls::http_server_acceptor;
use data_plane_gateway::ingress::sandbox_api::{EnvironmentRequestMapper, SandboxConfig};
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    adx_transport::install_crypto_provider();
    let _logging_guard = adx_observability::logging::init("adx-apiserver", true)
        .map_err(|_| "logging initialization failed")?;
    let config: Config = read_config()?;
    config.validate()?;
    let clients = Clients::new(config.clone())?;
    let api = Api::new(clients).map_err(|_| "API initialization failed")?;
    let acceptor = if config.loopback_http {
        None
    } else {
        Some(
            http_server_acceptor(
                &config.certificate,
                &config.private_key,
                None,
                vec![b"http/1.1".to_vec()],
            )
            .map_err(local_error)?,
        )
    };
    let listener = TcpListener::bind(config.listen).await?;
    let mut embedded_ingress = if config.ingress_mode == IngressMode::Embedded {
        let sandbox_service = embedded_sandbox_service(&api)?;
        Some(
            EmbeddedIngress::start(
                config
                    .ingress_control
                    .clone()
                    .ok_or("embedded Ingress control configuration missing")?,
                sandbox_service,
            )
            .await
            .map_err(local_error)?,
        )
    } else {
        None
    };
    adx_observability::info!(address=%listener.local_addr()?,"adx-apiserver listening");
    let (stop, receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let shutdown = shutdown();
    tokio::pin!(shutdown);
    let mut ingress_failure = None;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            result = async {
                match embedded_ingress.as_mut() {
                    Some(ingress) => ingress.failed().await,
                    None => std::future::pending().await,
                }
            } => {
                ingress_failure = Some(result.err().map_or_else(
                    || "embedded Ingress stopped".to_owned(),
                    |error| error.to_string(),
                ));
                break;
            }
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
    if let Some(ingress) = embedded_ingress {
        if ingress_failure.is_none() {
            ingress.shutdown().await.map_err(local_error)?;
        }
    }
    if let Some(error) = ingress_failure {
        return Err(error.into());
    }
    Ok(())
}

fn embedded_sandbox_service(
    api: &Arc<Api>,
) -> Result<Option<Arc<dyn Sandbox>>, Box<dyn std::error::Error>> {
    let Ok(path) = std::env::var("ADX_SANDBOX_CONFIG") else {
        return Ok(None);
    };
    let settings: SandboxConfig = serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|_| "invalid Sandbox configuration")?;
    let mapper = EnvironmentRequestMapper::new(
        settings.preinstalled_profiles,
        std::env::var("ADX_SANDBOX_EXECD_TOKEN")?,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(Some(ActivatorSandboxAdapter::new(
        api.sandbox_service.clone(),
        mapper,
    )))
}

fn local_error(error: Box<dyn std::error::Error + Send + Sync>) -> Box<dyn std::error::Error> {
    std::io::Error::other(error.to_string()).into()
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

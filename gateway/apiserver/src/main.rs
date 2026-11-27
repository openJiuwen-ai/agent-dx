use adx_activator::local_environment::LocalEnvironment;
use adx_agent_core::environment::Environment;
use adx_apiserver::{
    clients::Clients,
    config::{Config, IngressMode},
    http::Api,
    ingress::EmbeddedIngress,
};
use adx_process::{read_config, shutdown};
use adx_transport::tls::http_server_acceptor;
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
    // Bind before accepting traffic so a bad monitoring address fails startup.
    let metrics_listener = match config.metrics_listen {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
    // Bind the optional plain-HTTP readiness port the same way; it serves
    // kubelet httpGet probes in mtls deployments (see http::serve_readiness).
    let readiness_listener = match config.readiness_listen {
        Some(address) => Some(TcpListener::bind(address).await?),
        None => None,
    };
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
        let environment_service = embedded_environment_service(&api)?;
        // The embedded Ingress shares the host's shutdown budget and starts
        // draining at the same instant (SIGTERM), per the joint-exit
        // contract: one budget for management HTTP, background creates,
        // embedded Activator operations and the Ingress drain alike.
        let ingress = EmbeddedIngress::start(
            config
                .ingress_control
                .clone()
                .ok_or("embedded Ingress control configuration missing")?,
            environment_service,
            api.environment_service.clone(),
            api.draining_flag(),
            config.shutdown_timeout(),
        )
        .await
        .map_err(local_error)?;
        // Fold the embedded Ingress's admission readiness (route sync,
        // enabled entry paths — including the embedded Activator's — and
        // not draining) into this host's `/readyz`.
        let readiness = ingress.readiness().clone();
        api.set_embedded_readiness(Box::new(move || readiness.ready()));
        Some(ingress)
    } else {
        None
    };
    adx_observability::info!(address=%listener.local_addr()?,"adx-apiserver listening");
    let (stop, receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let shutdown = shutdown();
    tokio::pin!(shutdown);
    let mut ingress_failure = None;
    // The metrics and readiness listeners serve through the whole drain
    // window as their own tasks: `/readyz` answers 503 the moment draining
    // begins instead of a probe hanging on a listener nobody polls; they
    // end with the process.
    let metrics_service = api.environment_service.clone();
    let mut metrics_task = tokio::spawn(async move {
        match metrics_listener {
            Some(listener) => adx_apiserver::metrics::serve(listener, metrics_service).await,
            None => std::future::pending().await,
        }
    });
    let readiness_api = api.clone();
    let mut readiness_task = tokio::spawn(async move {
        match readiness_listener {
            Some(listener) => adx_apiserver::http::serve_readiness(listener, readiness_api).await,
            None => std::future::pending().await,
        }
    });
    loop {
        tokio::select! {
            result = &mut metrics_task => {
                ingress_failure = Some(match result {
                    Ok(Ok(())) => "metrics listener stopped".to_owned(),
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                });
                break;
            }
            result = &mut readiness_task => {
                ingress_failure = Some(match result {
                    Ok(Ok(())) => "readiness listener stopped".to_owned(),
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                });
                break;
            }
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
    // Draining first: new management operations are rejected before
    // acceptance with the structured not-started contract ("交接中，
    // 未接收") and `/readyz` turns not-ready; already-accepted requests
    // continue below.
    api.begin_draining();
    let _ = stop.send(true);
    // Trigger the embedded Ingress drain immediately so it runs in parallel
    // with management HTTP shutdown, instead of serially after it. Total exit
    // is ~max(management wait, ingress drain) rather than their sum; the
    // Ingress's drain timeout was aligned to the same budget at start.
    if let Some(ingress) = embedded_ingress.as_mut() {
        ingress.begin_drain();
    }
    // Joint exit, one budget started at the same instant (SIGTERM):
    // management HTTP tasks, background create-stream tasks, and in-process
    // embedded Activator operations — the ones inside Ingress connection
    // tasks are covered by the connection count, and the Activator's own
    // gauge covers reconciliation/preparation jobs running between
    // Environment calls. Abort stragglers when the budget expires.
    let shutdown_budget = config.shutdown_timeout();
    // The listeners keep serving through the drain window: kubelet's
    // /readyz gets an immediate 503, and new connections receive the
    // structured not-started rejection (with `Connection: close`) instead
    // of hanging in the accept backlog until process exit. Late
    // connections only ever see rejections, so they are aborted at the end
    // of the window rather than waited on.
    let mut late_connections = JoinSet::new();
    let mut accepting = true;
    let drained = tokio::time::timeout(shutdown_budget, async {
        let drain = async {
            while tasks.join_next().await.is_some() {}
            {
                let mut background = api.background.lock().await;
                while background.join_next().await.is_some() {}
            }
            while api.environment_service.embedded_operations() > 0
                || adx_activator::in_flight_operations() > 0
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::pin!(drain);
        loop {
            tokio::select! {
                _ = &mut drain => break,
                accepted = listener.accept(), if accepting => {
                    match accepted {
                        Ok((socket, _peer_address)) => {
                            let api = api.clone();
                            let acceptor = acceptor.clone();
                            let stop = receiver.clone();
                            late_connections.spawn(async move {
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
                        Err(error) => {
                            adx_observability::warn!(%error, "API accept failed during drain");
                            accepting = false;
                        }
                    }
                }
            }
        }
    })
    .await
    .is_ok();
    if !drained {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        let mut background = api.background.lock().await;
        background.abort_all();
        while background.join_next().await.is_some() {}
    }
    late_connections.abort_all();
    while late_connections.join_next().await.is_some() {}
    // Await the already-draining embedded Ingress even when another
    // listener failed: its own drain timeout bounds the wait, and dropping
    // it mid-drain would abort its in-flight sessions.
    if let Some(mut ingress) = embedded_ingress {
        if let Err(error) = ingress.await_completion().await {
            if ingress_failure.is_none() {
                ingress_failure = Some(error.to_string());
            }
        }
    }
    if let Some(error) = ingress_failure {
        return Err(error.into());
    }
    Ok(())
}

fn embedded_environment_service(
    api: &Arc<Api>,
) -> Result<Option<Arc<dyn Environment>>, Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("ADX_AGENT_CONFIG") else {
        return Ok(None);
    };
    let settings: data_plane_gateway::ingress::agent_api::AgentConfig =
        serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(embedded) = settings.embedded else {
        return Ok(None);
    };
    Ok(Some(Arc::new(LocalEnvironment::new(
        api.environment_service.clone(),
        embedded.request_timeout(),
        embedded.egress_isolation,
    )?)))
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

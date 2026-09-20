use adx_agent_store::{AgentState, DispatcherMember, RedisRepository};
use adx_dispatcher::{server, transport::HttpSandbox, Config, Dispatcher};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    listen: SocketAddr,
    node_id: String,
    advertised_url: String,
    redis_url: String,
    namespace: String,
    gateway_url: String,
    /// The HTTP listener is intended for a private TLS-terminating service proxy.
    allow_plaintext_transport: bool,
    gateway_ca_file: Option<String>,
    #[serde(default = "default_create_timeout_seconds")]
    create_timeout_seconds: u64,
}

fn default_create_timeout_seconds() -> u64 {
    adx_agent_core::limits::CREATE_TIMEOUT.as_secs()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("ADX_DISPATCHER_CONFIG")?;
    let settings: Settings = serde_json::from_slice(&std::fs::read(path)?)?;
    let service_token = std::env::var("ADX_DISPATCHER_SERVICE_TOKEN")?;
    let gateway_token = std::env::var("ADX_SANDBOX_SERVICE_TOKEN")?;
    if !settings.allow_plaintext_transport {
        return Err("this listener requires an explicit private plaintext hop behind a TLS service proxy; set allow_plaintext_transport for that deployment or local verification".into());
    }
    adx_agent_core::transport::service_origin(
        &settings.advertised_url,
        settings.allow_plaintext_transport,
    )?;
    let ca = settings.gateway_ca_file.map(std::fs::read).transpose()?;
    let sandbox = Arc::new(HttpSandbox::new(
        &settings.gateway_url,
        gateway_token,
        Duration::from_secs(20),
        ca.as_deref(),
        settings.allow_plaintext_transport,
    )?);
    let store = Arc::new(
        RedisRepository::connect_with_budget(
            &settings.redis_url,
            &settings.namespace,
            Duration::from_secs(3),
            64,
        )
        .await?,
    );
    let member = DispatcherMember {
        node_id: settings.node_id,
        boot_id: uuid::Uuid::new_v4().to_string(),
        address: settings.advertised_url,
    };
    let dispatcher = Arc::new(Dispatcher::new(
        AgentState::new(store.clone()),
        sandbox,
        member.boot_id.clone(),
        Config {
            create_timeout: Duration::from_secs(settings.create_timeout_seconds),
            ..Config::default()
        },
    )?);
    let ready = Arc::new(AtomicBool::new(false));
    let app = server::router(dispatcher.clone(), &service_token, ready.clone())?;
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    let ttl = Duration::from_secs(20);
    if !store.register_dispatcher(&member, ttl).await? {
        return Err("Dispatcher node_id already registered by another boot".into());
    }
    ready.store(true, Ordering::Release);
    let (stop_tx, stop_rx) = watch::channel(false);
    let membership = tokio::spawn({
        let store = store.clone();
        let member = member.clone();
        let ready = ready.clone();
        let mut stop = stop_rx.clone();
        let stop_tx = stop_tx.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    result=stop.changed()=>if result.is_err() || *stop.borrow() {break},
                    _=interval.tick()=> {
                        // Registration may have expired during an outage; CAS registration can safely reclaim only an empty/same-boot key.
                        match store.register_dispatcher(&member,ttl).await {
                            Ok(true)=>ready.store(true,Ordering::Release),
                            Ok(false)=> { ready.store(false,Ordering::Release); eprintln!("Dispatcher registration was replaced; shutting down this boot"); let _=stop_tx.send(true); break; },
                            Err(error)=> {ready.store(false,Ordering::Release); eprintln!("Dispatcher registration unavailable: {error}");},
                        }
                    }
                }
            }
        }
    });
    let recovery = tokio::spawn({
        let mut stop = stop_rx.clone();
        async move {
            let mut cursor = 0;
            let mut session_cursor = 0;
            loop {
                tokio::select! {
                    result=stop.changed()=>if result.is_err() || *stop.borrow() {break},
                    result=dispatcher.recover_page(cursor)=>match result {
                        Ok(next)=>{cursor=next; session_cursor=dispatcher.recover_sessions_page(session_cursor).await.unwrap_or_else(|error| {eprintln!("Session cleanup failed: {error}");0});},
                        Err(error)=>{eprintln!("Dispatcher state recovery failed: {error}");cursor=0;},
                    },
                }
                tokio::select! {
                    result=stop.changed()=>if result.is_err() || *stop.borrow() {break},
                    _=tokio::time::sleep(if cursor==0 {Duration::from_secs(2)} else {Duration::from_millis(50)})=>(),
                }
            }
        }
    });
    let signal = tokio::spawn({
        let stop_tx = stop_tx.clone();
        async move {
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {_=tokio::signal::ctrl_c()=>(), _=terminate.recv()=>()}
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await.expect("signal handler");
            let _ = stop_tx.send(true);
        }
    });
    let mut shutdown = stop_rx;
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
    ready.store(false, Ordering::Release);
    let _ = stop_tx.send(true);
    signal.abort();
    // Background tasks have bounded I/O; force cancellation if a peer stalls during shutdown.
    for mut task in [membership, recovery] {
        if tokio::time::timeout(Duration::from_secs(5), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
    if let Err(error) = store.unregister_dispatcher(&member).await {
        eprintln!("Dispatcher unregister failed; TTL will expire: {error}");
    }
    server_result?;
    Ok(())
}

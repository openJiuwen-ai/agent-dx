// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! runtime-mode: rrt acts as the adx sandbox runtime.
//! Connect to the rt server (the function-proxy POSIX port) and open the RuntimeRPC `MessageStream`.
//! Dispatch received `CallReq`: `is_create` returns a create ack; `function` routes to akernel method dispatch;
//! all other calls return unsupported. See docs/rust-sandbox-runtime/README.md §5.

use crate::posix::core_service::CallResult;
use crate::posix::runtime_rpc::runtime_rpc_client::RuntimeRpcClient;
use crate::posix::runtime_rpc::{streaming_message, StreamingMessage};
use crate::posix::runtime_service::CallResponse;
use std::collections::VecDeque;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

macro_rules! rrt_info {
    ($($arg:tt)*) => {
        $crate::runtime::log_info(format_args!($($arg)*))
    };
}

macro_rules! rrt_warn {
    ($($arg:tt)*) => {
        $crate::runtime::log_warn(format_args!($($arg)*))
    };
}

macro_rules! rrt_error {
    ($($arg:tt)*) => {
        $crate::runtime::log_error(format_args!($($arg)*))
    };
}

macro_rules! rrt_debug {
    ($($arg:tt)*) => {
        if $crate::runtime::debug_on() {
            $crate::runtime::log_debug(format_args!($($arg)*))
        }
    };
}

mod activity;
mod bash;
pub(crate) mod child_env;
mod cmd;
mod codec;
mod dispatch;
mod entrypoint;
mod fs;
mod httpserver;
mod tunnel;

/// Start only the RRT atomic-operation HTTP server without connecting to the function-proxy worker. Used for isolated verification.
pub async fn serve_http_only(
    port: u16,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    httpserver::serve(port, token).await
}

/// Start only the native Rust reverse-tunnel server (Port A ws / Port B http). Used for real Python
/// TunnelClient interoperability verification without RuntimeRPC dispatch.
pub async fn serve_tunnel_only(ws_port: u16, http_port: u16) {
    tunnel::run_standalone(ws_port, http_port).await;
}

#[derive(Default, Debug, Clone)]
pub struct Args {
    pub rt_server: String,
    pub runtime_id: String,
    pub instance_id: String,
    pub job_id: String,
    pub deploy_dir: String,
    pub log_level: String,
}

const RUNTIME_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
enum RuntimeReadyState {
    Starting,
    Ready,
    Failed(String),
}

#[derive(Clone, Default)]
struct RuntimeServiceControls {
    http: Option<httpserver::HttpServerControl>,
    tunnel: Option<tunnel::TunnelServerControl>,
    checkpoint: Option<httpserver::CheckpointServerControl>,
}

impl RuntimeServiceControls {
    fn rearm(&self) -> Result<Vec<(&'static str, u64)>, String> {
        let mut generations = Vec::with_capacity(2);
        if let Some(control) = &self.http {
            generations.push((
                "HTTP",
                control
                    .rearm()
                    .map_err(|error| format!("HTTP listener rearm failed: {error}"))?,
            ));
        }
        if let Some(control) = &self.tunnel {
            generations.push((
                "tunnel",
                control
                    .rearm()
                    .map_err(|error| format!("tunnel listener rearm failed: {error}"))?,
            ));
        }
        if let Some(control) = &self.checkpoint {
            generations.push((
                "checkpoint",
                control
                    .rearm()
                    .map_err(|error| format!("checkpoint listener rearm failed: {error}"))?,
            ));
        }
        Ok(generations)
    }

    fn rebind_instance_id(&self, instance_id: &str) {
        if let Some(control) = &self.checkpoint {
            control.rebind_instance_id(instance_id);
        }
    }
}

fn ready_runtime_receiver() -> watch::Receiver<RuntimeReadyState> {
    let (_tx, rx) = watch::channel(RuntimeReadyState::Ready);
    rx
}

fn failed_runtime_receiver(message: String) -> watch::Receiver<RuntimeReadyState> {
    let (_tx, rx) = watch::channel(RuntimeReadyState::Failed(message));
    rx
}

fn combine_runtime_readiness(
    mut services: Vec<watch::Receiver<RuntimeReadyState>>,
) -> watch::Receiver<RuntimeReadyState> {
    match services.len() {
        0 => ready_runtime_receiver(),
        1 => services.pop().expect("one readiness receiver"),
        _ => {
            let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
            let (update_tx, mut update_rx) = mpsc::unbounded_channel();
            let service_count = services.len();
            for (index, mut service) in services.into_iter().enumerate() {
                let update_tx = update_tx.clone();
                tokio::spawn(async move {
                    loop {
                        let state = service.borrow_and_update().clone();
                        if update_tx.send((index, state.clone())).is_err() {
                            return;
                        }
                        if service.changed().await.is_err() {
                            let _ = update_tx.send((
                                index,
                                RuntimeReadyState::Failed(format!(
                                    "RRT service readiness channel {index} closed"
                                )),
                            ));
                            return;
                        }
                    }
                });
            }
            drop(update_tx);
            tokio::spawn(async move {
                let mut states = vec![RuntimeReadyState::Starting; service_count];
                while let Some((index, state)) = update_rx.recv().await {
                    states[index] = state;
                    let combined = if let Some(message) =
                        states.iter().find_map(|state| match state {
                            RuntimeReadyState::Failed(message) => Some(message.clone()),
                            _ => None,
                        }) {
                        RuntimeReadyState::Failed(message)
                    } else if states
                        .iter()
                        .all(|state| *state == RuntimeReadyState::Ready)
                    {
                        RuntimeReadyState::Ready
                    } else {
                        RuntimeReadyState::Starting
                    };
                    if *ready_tx.borrow() != combined {
                        let _ = ready_tx.send(combined);
                    }
                }
                if !matches!(*ready_tx.borrow(), RuntimeReadyState::Failed(_)) {
                    let _ = ready_tx.send(RuntimeReadyState::Failed(
                        "RRT service readiness channels closed before startup completed"
                            .to_string(),
                    ));
                }
            });
            ready_rx
        }
    }
}

async fn start_http_server_with_control(
    port: u16,
    token: Option<String>,
) -> Result<
    (
        watch::Receiver<RuntimeReadyState>,
        httpserver::HttpServerControl,
    ),
    std::io::Error,
> {
    let listener = httpserver::bind(port).await.map_err(|err| {
        let message = format!("failed to bind RRT HTTP port {port}: {err}");
        rrt_error!("[rrt-http] readiness failed: {message}");
        std::io::Error::new(err.kind(), message)
    })?;
    let address = listener
        .local_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| format!("0.0.0.0:{port}"));
    let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
    let control = httpserver::HttpServerControl::start(listener, token, ready_tx)?;
    rrt_info!("[rrt-http] readiness ready address={address}");
    Ok((ready_rx, control))
}

async fn start_http_server(
    port: u16,
    token: Option<String>,
) -> Result<watch::Receiver<RuntimeReadyState>, std::io::Error> {
    start_http_server_with_control(port, token)
        .await
        .map(|(ready, _control)| ready)
}

async fn start_checkpoint_server_with_control(
    instance_id: String,
    tx: mpsc::Sender<StreamingMessage>,
) -> Result<
    Option<(
        watch::Receiver<RuntimeReadyState>,
        httpserver::CheckpointServerControl,
    )>,
    std::io::Error,
> {
    let Some(socket_path) = httpserver::checkpoint_socket_path_from_control_directory(
        std::env::var_os(httpserver::RRT_CONTROL_SOCKET_PATH_ENV).as_deref(),
    ) else {
        return Ok(None);
    };
    let listener = httpserver::bind_checkpoint_socket(&socket_path).await?;
    let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
    let control = httpserver::CheckpointServerControl::start(listener, instance_id, tx, ready_tx)?;
    rrt_info!(
        "[rrt-checkpoint] readiness ready socket={}",
        socket_path.display()
    );
    Ok(Some((ready_rx, control)))
}

async fn start_tunnel_runtime_server_with_control(
    ws_port: u16,
    http_port: u16,
) -> Result<
    (
        watch::Receiver<RuntimeReadyState>,
        tunnel::TunnelServerControl,
    ),
    String,
> {
    let bound = tunnel::BoundTunnelServers::bind(ws_port, http_port)
        .await
        .map_err(|message| {
            rrt_error!("[rrt-tunnel] readiness failed: {message}");
            message
        })?;
    let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
    let control = tunnel::TunnelServerControl::start(bound, ready_tx).map_err(|message| {
        rrt_error!("[rrt-tunnel] readiness failed: {message}");
        message
    })?;
    rrt_info!("[rrt-tunnel] readiness ready ws=0.0.0.0:{ws_port} http=127.0.0.1:{http_port}");
    Ok((ready_rx, control))
}

async fn start_tunnel_runtime_server(
    ws_port: u16,
    http_port: u16,
) -> Result<watch::Receiver<RuntimeReadyState>, String> {
    start_tunnel_runtime_server_with_control(ws_port, http_port)
        .await
        .map(|(ready, _control)| ready)
}

async fn wait_for_runtime_ready(
    mut ready: watch::Receiver<RuntimeReadyState>,
) -> Result<(), String> {
    loop {
        match ready.borrow_and_update().clone() {
            RuntimeReadyState::Ready => return Ok(()),
            RuntimeReadyState::Failed(message) => return Err(message),
            RuntimeReadyState::Starting => {}
        }
        if ready.changed().await.is_err() {
            return Err(
                "RRT service readiness channel closed before startup completed".to_string(),
            );
        }
    }
}

fn first_env<F>(keys: &[&str], get: &mut F) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    keys.iter()
        .find_map(|key| {
            get(key)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default()
}

fn load_args_from<F>(mut get: F) -> Args
where
    F: FnMut(&str) -> Option<String>,
{
    let runtime_id = first_env(&["ADX_RUNTIME_ID"], &mut get);
    let instance_id = first_env(&["INSTANCE_ID"], &mut get);
    Args {
        rt_server: first_env(&["POSIX_LISTEN_ADDR", "ADX_SERVER_ADDRESS"], &mut get),
        runtime_id: runtime_id.clone(),
        instance_id: if instance_id.is_empty() {
            instance_id_from_runtime(&runtime_id)
        } else {
            instance_id
        },
        job_id: first_env(&["ADX_JOB_ID"], &mut get),
        deploy_dir: first_env(
            &[
                "ADX_FUNCTION_LIB_PATH",
                "FUNCTION_LIB_PATH",
                "ADX_RT_WORKING_DIR",
            ],
            &mut get,
        ),
        log_level: {
            let level = first_env(&["ADX_LOG_LEVEL"], &mut get);
            if level.is_empty() {
                "INFO".to_string()
            } else {
                level
            }
        },
    }
}

/// Load RRT runtime config from environment variables injected by functionsystem/runtime-launcher.
pub fn load_args_from_env() -> Args {
    load_args_from(|key| std::env::var(key).ok())
}

fn invalid_target_identity(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn allows_logical_instance_id_rebind(
    environment: &std::collections::HashMap<String, String>,
) -> std::io::Result<bool> {
    let Some(raw_value) = environment.get("ADX_ALLOW_LOGICAL_INSTANCE_ID_REBIND") else {
        return Ok(false);
    };
    match raw_value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" | "" => Ok(false),
        value => Err(invalid_target_identity(format!(
            "target logical instance ID rebind authorization has invalid value {value:?}"
        ))),
    }
}

fn load_reconnect_control_args(
    source: &Args,
    environment_file: Option<&Path>,
) -> std::io::Result<Args> {
    let Some(environment_file) = environment_file else {
        return Ok(source.clone());
    };
    let environment = crate::startup::read_environment_file(environment_file)?;
    let rt_server = ["POSIX_LISTEN_ADDR", "ADX_SERVER_ADDRESS"]
        .iter()
        .find_map(|key| {
            environment
                .get(*key)
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .ok_or_else(|| invalid_target_identity("target RuntimeRPC address is missing"))?;
    let runtime_id = environment
        .get("ADX_RUNTIME_ID")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_target_identity("target runtime ID is missing"))?;
    let instance_id = environment
        .get("INSTANCE_ID")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_target_identity("target logical instance ID is missing"))?;
    if instance_id != source.instance_id && !allows_logical_instance_id_rebind(&environment)? {
        return Err(invalid_target_identity(format!(
            "target physical environment changed logical instance ID from {:?} to {:?}",
            source.instance_id, instance_id
        )));
    }

    Ok(Args {
        rt_server: rt_server.to_string(),
        runtime_id: runtime_id.to_string(),
        instance_id: instance_id.to_string(),
        job_id: source.job_id.clone(),
        deploy_dir: source.deploy_dir.clone(),
        log_level: source.log_level.clone(),
    })
}

/// Tracks the last validated logical identity across RuntimeRPC reconnects.
///
/// A reusable Snapshot restore is allowed to change the logical instance ID
/// exactly when the trusted target environment explicitly authorizes it. Once
/// validated, that target identity becomes the baseline for later ordinary
/// Pause/Resume reconnects of the cloned sandbox.
struct ReconnectControlState {
    baseline: Args,
}

impl ReconnectControlState {
    fn new(baseline: Args) -> Self {
        Self { baseline }
    }

    fn load(&mut self, environment_file: Option<&Path>) -> std::io::Result<Args> {
        let target = load_reconnect_control_args(&self.baseline, environment_file)?;
        if let Some(environment_file) = environment_file {
            let environment = crate::startup::read_environment_file(environment_file)?;
            child_env::refresh_from_map(&environment);
        }
        self.baseline.instance_id.clone_from(&target.instance_id);
        Ok(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    fn args_from(pairs: &[(&str, &str)]) -> Args {
        let env: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        load_args_from(|key| env.get(key).cloned())
    }

    #[test]
    fn load_args_from_env_names() {
        let args = args_from(&[
            ("POSIX_LISTEN_ADDR", "192.168.0.2:22773"),
            ("ADX_SERVER_ADDRESS", "ignored:1"),
            (
                "ADX_RUNTIME_ID",
                "runtime-akernel-my-test-sandbox-000033cfcc77",
            ),
            ("INSTANCE_ID", "akernel-my-test-sandbox"),
            ("ADX_JOB_ID", "job-f8ade5e6"),
            (
                "ADX_FUNCTION_LIB_PATH",
                "/usr/lib64/python3.11/site-packages/adx/inner/deploy/process/",
            ),
            ("ADX_LOG_LEVEL", "INFO"),
        ]);

        assert_eq!(args.rt_server, "192.168.0.2:22773");
        assert_eq!(
            args.runtime_id,
            "runtime-akernel-my-test-sandbox-000033cfcc77"
        );
        assert_eq!(args.instance_id, "akernel-my-test-sandbox");
        assert_eq!(args.job_id, "job-f8ade5e6");
        assert_eq!(
            args.deploy_dir,
            "/usr/lib64/python3.11/site-packages/adx/inner/deploy/process/"
        );
        assert_eq!(args.log_level, "INFO");
    }

    #[test]
    fn load_args_falls_back_to_runtime_id_and_defaults() {
        let args = args_from(&[("ADX_RUNTIME_ID", "runtime-demo-sandbox-abcdef")]);

        assert_eq!(args.instance_id, "demo-sandbox");
        assert_eq!(args.log_level, "INFO");
    }

    fn reconnect_args_from(source: &Args, environment: &str) -> std::io::Result<Args> {
        let mut file = tempfile::NamedTempFile::new().expect("create restore environment file");
        file.write_all(environment.as_bytes())
            .expect("write restore environment file");
        load_reconnect_control_args(source, Some(file.path()))
    }

    #[test]
    fn trusted_reusable_snapshot_restore_rebinds_logical_instance_id() {
        let source = Args {
            rt_server: "source-proxy:22773".to_string(),
            runtime_id: "runtime-source-attempt".to_string(),
            instance_id: "source-sandbox".to_string(),
            ..Default::default()
        };

        let restored = reconnect_args_from(
            &source,
            "POSIX_LISTEN_ADDR=target-proxy:22773\n\
             ADX_RUNTIME_ID=runtime-clone-attempt\n\
             INSTANCE_ID=clone-sandbox\n\
             ADX_ALLOW_LOGICAL_INSTANCE_ID_REBIND=true\n",
        )
        .expect("trusted reusable snapshot restore should permit the target logical identity");

        assert_eq!(restored.rt_server, "target-proxy:22773");
        assert_eq!(restored.runtime_id, "runtime-clone-attempt");
        assert_eq!(restored.instance_id, "clone-sandbox");
    }

    #[test]
    fn ordinary_resume_rejects_logical_instance_id_rebind() {
        let source = Args {
            rt_server: "source-proxy:22773".to_string(),
            runtime_id: "runtime-source-attempt".to_string(),
            instance_id: "source-sandbox".to_string(),
            ..Default::default()
        };

        let error = reconnect_args_from(
            &source,
            "POSIX_LISTEN_ADDR=target-proxy:22773\n\
             ADX_RUNTIME_ID=runtime-resume-attempt\n\
             INSTANCE_ID=other-sandbox\n",
        )
        .expect_err("ordinary pause/resume must keep the source logical identity");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("changed logical instance ID"));
    }

    #[test]
    fn invalid_logical_instance_id_rebind_authorization_fails_closed() {
        let source = Args {
            rt_server: "source-proxy:22773".to_string(),
            runtime_id: "runtime-source-attempt".to_string(),
            instance_id: "source-sandbox".to_string(),
            ..Default::default()
        };

        let error = reconnect_args_from(
            &source,
            "POSIX_LISTEN_ADDR=target-proxy:22773\n\
             ADX_RUNTIME_ID=runtime-clone-attempt\n\
             INSTANCE_ID=clone-sandbox\n\
             ADX_ALLOW_LOGICAL_INSTANCE_ID_REBIND=maybe\n",
        )
        .expect_err("invalid authorization must never permit logical identity rebind");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("invalid value"));
    }

    #[test]
    fn trusted_clone_identity_becomes_baseline_for_ordinary_resume() {
        let source = Args {
            rt_server: "source-proxy:22773".to_string(),
            runtime_id: "runtime-source-attempt".to_string(),
            instance_id: "source-sandbox".to_string(),
            ..Default::default()
        };
        let mut state = ReconnectControlState::new(source);
        let mut clone_environment =
            tempfile::NamedTempFile::new().expect("create clone restore environment file");
        clone_environment
            .write_all(
                b"POSIX_LISTEN_ADDR=clone-proxy:22773\n\
                  ADX_RUNTIME_ID=runtime-clone-attempt\n\
                  INSTANCE_ID=clone-sandbox\n\
                  ADX_ALLOW_LOGICAL_INSTANCE_ID_REBIND=true\n",
            )
            .expect("write clone restore environment file");

        let clone = state
            .load(Some(clone_environment.path()))
            .expect("trusted reusable restore should adopt the clone identity");
        assert_eq!(clone.instance_id, "clone-sandbox");

        let mut resume_environment =
            tempfile::NamedTempFile::new().expect("create ordinary resume environment file");
        resume_environment
            .write_all(
                b"POSIX_LISTEN_ADDR=resume-proxy:22773\n\
                  ADX_RUNTIME_ID=runtime-resume-attempt\n\
                  INSTANCE_ID=clone-sandbox\n",
            )
            .expect("write ordinary resume environment file");

        let resumed = state
            .load(Some(resume_environment.path()))
            .expect("ordinary resume of a cloned sandbox must retain the adopted identity");
        assert_eq!(resumed.instance_id, "clone-sandbox");
        assert_eq!(resumed.runtime_id, "runtime-resume-attempt");
    }

    #[test]
    fn reconnect_stream_metadata_uses_validated_target_logical_identity() {
        let target = Args {
            runtime_id: "runtime-clone-attempt".to_string(),
            instance_id: "clone-sandbox".to_string(),
            ..Default::default()
        };
        let (_tx, rx) = mpsc::channel(1);

        let request = build_stream_request(&target, rx).expect("build reconnect stream request");

        assert_eq!(
            request
                .metadata()
                .get("instance_id")
                .expect("instance_id metadata"),
            "clone-sandbox"
        );
        assert_eq!(
            request
                .metadata()
                .get("source_id")
                .expect("source_id metadata"),
            "clone-sandbox"
        );
    }

    #[tokio::test]
    async fn trusted_clone_dispatch_uses_validated_target_logical_identity() {
        let source = Args {
            instance_id: "source-sandbox".to_string(),
            ..Default::default()
        };
        let ctx = std::sync::Arc::new(dispatch::Ctx::new(source));
        let (tx, mut rx) = mpsc::channel(2);
        let inbound = StreamingMessage {
            message_id: "transport-message-id".to_string(),
            meta_data: Default::default(),
            body: Some(streaming_message::Body::CallReq(
                crate::posix::runtime_service::CallRequest {
                    request_id: "clone-call".to_string(),
                    ..Default::default()
                },
            )),
        };
        let (_ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Ready);

        assert!(
            handle_inbound_message(inbound, "clone-sandbox", ctx, tx, ready_rx).await,
            "the cloned runtime stream should remain usable"
        );
        rx.recv().await.expect("transport acknowledgement");
        let result = rx.recv().await.expect("call result");
        let Some(streaming_message::Body::CallResultReq(result)) = result.body else {
            panic!("expected CallResultReq");
        };
        assert_eq!(result.instance_id, "clone-sandbox");
    }

    #[test]
    fn call_result_keeps_inline_value_without_object_id() {
        let message = call_result_msg(
            "request-inline".to_string(),
            "sandbox-instance".to_string(),
            String::new(),
            0,
            "ok",
            vec![1, 2, 3],
        );

        let Some(streaming_message::Body::CallResultReq(result)) = message.body else {
            panic!("expected CallResultReq");
        };
        assert_eq!(result.small_objects.len(), 1);
        assert!(result.small_objects[0].id.is_empty());
        assert_eq!(result.small_objects[0].value, vec![1, 2, 3]);
    }

    #[test]
    fn call_result_preserves_supplied_object_id() {
        let message = call_result_msg(
            "request-object".to_string(),
            "sandbox-instance".to_string(),
            "object-result".to_string(),
            0,
            "ok",
            vec![4, 5, 6],
        );

        let Some(streaming_message::Body::CallResultReq(result)) = message.body else {
            panic!("expected CallResultReq");
        };
        assert_eq!(result.small_objects.len(), 1);
        assert_eq!(result.small_objects[0].id, "object-result");
        assert_eq!(result.small_objects[0].value, vec![4, 5, 6]);
    }

    #[test]
    fn call_result_omits_small_object_without_inline_value() {
        let message = call_result_msg(
            "request-empty".to_string(),
            "sandbox-instance".to_string(),
            "unused-object".to_string(),
            0,
            "created",
            Vec::new(),
        );

        let Some(streaming_message::Body::CallResultReq(result)) = message.body else {
            panic!("expected CallResultReq");
        };
        assert!(result.small_objects.is_empty());
    }

    #[tokio::test]
    async fn call_request_sends_transport_response_before_call_result() {
        let args = Args {
            instance_id: "sandbox-instance".to_string(),
            ..Default::default()
        };
        let ctx = std::sync::Arc::new(dispatch::Ctx::new(args));
        let (tx, mut rx) = mpsc::channel(2);
        let request_id = "create-request@initcall";
        let message_id = "transport-message-id";
        let call = crate::posix::runtime_service::CallRequest {
            is_create: true,
            sender_id: "caller-instance".to_string(),
            request_id: request_id.to_string(),
            ..Default::default()
        };
        let inbound = StreamingMessage {
            message_id: message_id.to_string(),
            meta_data: Default::default(),
            body: Some(streaming_message::Body::CallReq(call)),
        };
        let (_ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Ready);

        assert!(
            handle_inbound_message(inbound, "sandbox-instance", ctx, tx, ready_rx).await,
            "the inbound stream should remain usable"
        );

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for CallResponse")
            .expect("outbound channel closed before CallResponse");
        assert_eq!(first.message_id, message_id);
        match first.body {
            Some(streaming_message::Body::CallRsp(response)) => {
                assert_eq!(
                    response.code,
                    crate::posix::common::ErrorCode::ErrNone as i32
                );
                assert!(response.message.is_empty());
            }
            body => panic!(
                "first outbound message must be CallResponse, got {:?}",
                body.map(|body| body_kind(&body))
            ),
        }

        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for CallResult")
            .expect("outbound channel closed before CallResult");
        assert_eq!(second.message_id, request_id);
        match second.body {
            Some(streaming_message::Body::CallResultReq(result)) => {
                assert_eq!(result.request_id, request_id);
                assert_eq!(result.instance_id, "caller-instance");
                assert_eq!(result.code, 0);
            }
            body => panic!(
                "second outbound message must be CallResult, got {:?}",
                body.map(|body| body_kind(&body))
            ),
        }
    }

    #[tokio::test]
    async fn init_call_waits_for_all_configured_services_without_delaying_transport_ack() {
        let args = Args {
            instance_id: "sandbox-instance".to_string(),
            ..Default::default()
        };
        let ctx = std::sync::Arc::new(dispatch::Ctx::new(args));
        let (tx, mut rx) = mpsc::channel(2);
        let (http_ready_tx, http_ready_rx) = watch::channel(RuntimeReadyState::Starting);
        let (tunnel_ready_tx, tunnel_ready_rx) = watch::channel(RuntimeReadyState::Starting);
        let runtime_ready = combine_runtime_readiness(vec![http_ready_rx, tunnel_ready_rx]);
        let request_id = "create-waits-for-runtime-services@initcall";
        let message_id = "transport-ready-message-id";
        let call = crate::posix::runtime_service::CallRequest {
            is_create: true,
            sender_id: "caller-instance".to_string(),
            request_id: request_id.to_string(),
            ..Default::default()
        };
        let inbound = StreamingMessage {
            message_id: message_id.to_string(),
            meta_data: Default::default(),
            body: Some(streaming_message::Body::CallReq(call)),
        };

        assert!(
            handle_inbound_message(inbound, "sandbox-instance", ctx, tx, runtime_ready).await,
            "the inbound stream should remain usable"
        );

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for CallResponse")
            .expect("outbound channel closed before CallResponse");
        assert!(matches!(
            first.body,
            Some(streaming_message::Body::CallRsp(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "init CallResult must not be sent before runtime service readiness"
        );

        http_ready_tx
            .send(RuntimeReadyState::Ready)
            .expect("readiness receiver should still be alive");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "init CallResult must still wait for configured tunnel readiness"
        );

        tunnel_ready_tx
            .send(RuntimeReadyState::Ready)
            .expect("readiness receiver should still be alive");
        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for readiness-gated CallResult")
            .expect("outbound channel closed before CallResult");
        match second.body {
            Some(streaming_message::Body::CallResultReq(result)) => {
                assert_eq!(result.request_id, request_id);
                assert_eq!(result.code, 0);
            }
            body => panic!(
                "second outbound message must be CallResult, got {:?}",
                body.map(|body| body_kind(&body))
            ),
        }
    }

    #[tokio::test]
    async fn combined_readiness_skips_an_unconfigured_tunnel() {
        let (http_ready_tx, http_ready_rx) = watch::channel(RuntimeReadyState::Starting);
        let runtime_ready = combine_runtime_readiness(vec![http_ready_rx]);
        let mut waiter = Box::pin(wait_for_runtime_ready(runtime_ready));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err(),
            "the configured HTTP server must still gate readiness"
        );
        http_ready_tx
            .send(RuntimeReadyState::Ready)
            .expect("readiness receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("HTTP readiness should release the barrier when tunnel is absent")
            .expect("HTTP readiness should succeed");
    }

    #[tokio::test]
    async fn combined_readiness_propagates_failure_after_initial_ready() {
        let (http_ready_tx, http_ready_rx) = watch::channel(RuntimeReadyState::Starting);
        let (tunnel_ready_tx, tunnel_ready_rx) = watch::channel(RuntimeReadyState::Starting);
        let mut runtime_ready = combine_runtime_readiness(vec![http_ready_rx, tunnel_ready_rx]);

        http_ready_tx
            .send(RuntimeReadyState::Ready)
            .expect("HTTP readiness receiver should still be alive");
        tunnel_ready_tx
            .send(RuntimeReadyState::Ready)
            .expect("tunnel readiness receiver should still be alive");
        wait_for_runtime_ready(runtime_ready.clone())
            .await
            .expect("both configured services should become ready");
        assert_eq!(*runtime_ready.borrow_and_update(), RuntimeReadyState::Ready);

        http_ready_tx
            .send(RuntimeReadyState::Failed(
                "RRT HTTP server stopped".to_string(),
            ))
            .expect("HTTP readiness receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(1), runtime_ready.changed())
            .await
            .expect("combined readiness should observe the service failure")
            .expect("combined readiness channel should stay open for the failure");
        assert_eq!(
            *runtime_ready.borrow(),
            RuntimeReadyState::Failed("RRT HTTP server stopped".to_string())
        );
    }

    #[tokio::test]
    async fn init_call_reports_http_readiness_failure_after_transport_ack() {
        let args = Args {
            instance_id: "sandbox-instance".to_string(),
            ..Default::default()
        };
        let ctx = std::sync::Arc::new(dispatch::Ctx::new(args));
        let (tx, mut rx) = mpsc::channel(2);
        let (_ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Failed(
            "failed to bind RRT HTTP port 50090: address already in use".to_string(),
        ));
        let request_id = "create-http-bind-failed@initcall";
        let message_id = "transport-bind-failed-message-id";
        let call = crate::posix::runtime_service::CallRequest {
            is_create: true,
            sender_id: "caller-instance".to_string(),
            request_id: request_id.to_string(),
            ..Default::default()
        };
        let inbound = StreamingMessage {
            message_id: message_id.to_string(),
            meta_data: Default::default(),
            body: Some(streaming_message::Body::CallReq(call)),
        };

        assert!(
            handle_inbound_message(inbound, "sandbox-instance", ctx, tx, ready_rx).await,
            "the inbound stream should remain usable"
        );

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for CallResponse")
            .expect("outbound channel closed before CallResponse");
        assert!(matches!(
            first.body,
            Some(streaming_message::Body::CallRsp(_))
        ));

        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for readiness failure CallResult")
            .expect("outbound channel closed before CallResult");
        match second.body {
            Some(streaming_message::Body::CallResultReq(result)) => {
                assert_eq!(result.request_id, request_id);
                assert_eq!(
                    result.code,
                    crate::posix::common::ErrorCode::ErrInnerSystemError as i32
                );
                assert!(result.message.contains("runtime initialization failed"));
                assert!(result.message.contains("address already in use"));
            }
            body => panic!(
                "second outbound message must be CallResult, got {:?}",
                body.map(|body| body_kind(&body))
            ),
        }
    }

    #[tokio::test]
    async fn http_server_bind_failure_is_reported_before_runtime_stream_start() {
        let occupied = httpserver::bind(0)
            .await
            .expect("an ephemeral HTTP port should be available");
        let port = occupied
            .local_addr()
            .expect("occupied listener should have a local address")
            .port();
        let error = start_http_server(port, None)
            .await
            .expect_err("occupied HTTP port must fail before RuntimeRPC starts");

        assert!(error.to_string().contains(&format!("RRT HTTP port {port}")));
    }

    #[tokio::test]
    async fn http_server_port_is_reserved_before_start_returns() {
        let probe = httpserver::bind(0)
            .await
            .expect("an ephemeral HTTP port should be available");
        let port = probe
            .local_addr()
            .expect("probe listener should have a local address")
            .port();
        drop(probe);

        let ready = start_http_server(port, None)
            .await
            .expect("HTTP listener should bind before start returns");

        assert_eq!(*ready.borrow(), RuntimeReadyState::Ready);
        let error = httpserver::bind(port)
            .await
            .expect_err("the returned server must already reserve its port");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    }

    #[tokio::test]
    async fn tunnel_server_ports_are_reserved_before_start_returns() {
        let ws_probe = tokio::net::TcpListener::bind("0.0.0.0:0")
            .await
            .expect("an ephemeral tunnel WS port should be available");
        let ws_port = ws_probe.local_addr().expect("WS probe address").port();
        drop(ws_probe);
        let http_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral tunnel HTTP port should be available");
        let http_port = http_probe.local_addr().expect("HTTP probe address").port();
        drop(http_probe);

        let ready = start_tunnel_runtime_server(ws_port, http_port)
            .await
            .expect("both tunnel listeners should bind before start returns");

        assert_eq!(*ready.borrow(), RuntimeReadyState::Ready);
        let ws_error = tokio::net::TcpListener::bind(("0.0.0.0", ws_port))
            .await
            .expect_err("the tunnel server must already reserve its WS port");
        assert_eq!(ws_error.kind(), std::io::ErrorKind::AddrInUse);
        let http_error = tokio::net::TcpListener::bind(("127.0.0.1", http_port))
            .await
            .expect_err("the tunnel server must already reserve its HTTP port");
        assert_eq!(http_error.kind(), std::io::ErrorKind::AddrInUse);
    }

    #[tokio::test]
    async fn tunnel_bind_failure_is_exposed_as_readiness_failure() {
        let occupied = tokio::net::TcpListener::bind("0.0.0.0:0")
            .await
            .expect("an ephemeral tunnel WS port should be available");
        let ws_port = occupied.local_addr().expect("occupied address").port();
        let http_probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral tunnel HTTP port should be available");
        let http_port = http_probe.local_addr().expect("HTTP probe address").port();
        drop(http_probe);

        let error = start_tunnel_runtime_server(ws_port, http_port)
            .await
            .expect_err("occupied tunnel WS port must fail startup");

        assert!(error.contains(&format!("tunnel WS port {ws_port}")));
    }
}

/// Per-request logs are emitted only when `ADX_LOG_LEVEL=DEBUG`; lifecycle/error logs are always emitted.
static LOG_DEBUG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn debug_on() -> bool {
    *LOG_DEBUG.get().unwrap_or(&false)
}

pub(crate) fn log_info(args: std::fmt::Arguments<'_>) {
    log_stdout("INFO", args);
}

pub(crate) fn log_warn(args: std::fmt::Arguments<'_>) {
    log_stderr("WARN", args);
}

pub(crate) fn log_error(args: std::fmt::Arguments<'_>) {
    log_stderr("ERROR", args);
}

pub(crate) fn log_debug(args: std::fmt::Arguments<'_>) {
    log_stdout("DEBUG", args);
}

fn log_stdout(level: &str, args: std::fmt::Arguments<'_>) {
    let ts = format_local_timestamp();
    println!("[{ts} {level}] {args}");
}

fn log_stderr(level: &str, args: std::fmt::Arguments<'_>) {
    let ts = format_local_timestamp();
    eprintln!("[{ts} {level}] {args}");
}

fn format_local_timestamp() -> String {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(now) => now,
        Err(_) => Duration::from_secs(0),
    };
    let secs = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}")
}

// Howard Hinnant civil_from_days algorithm. Input is Unix days since
// 1970-01-01 UTC; output is Gregorian UTC date. It avoids adding a time crate
// just for log formatting.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

/// Derive instance_id from runtime_id: `runtime-<instance>-<hex>` -> `<instance>`.
pub(crate) fn instance_id_from_runtime(runtime_id: &str) -> String {
    let s = runtime_id.strip_prefix("runtime-").unwrap_or(runtime_id);
    match s.rsplit_once('-') {
        Some((inst, _hex)) => inst.to_string(),
        None => s.to_string(),
    }
}

/// runtime -> proxy result path: `callResultReq=14`, carrying CallResult with inline smallObjects.
pub(crate) fn call_result_msg(
    request_id: String,
    instance_id: String,
    object_id: String,
    code: i32,
    message: &str,
    value: Vec<u8>,
) -> StreamingMessage {
    let mut result = CallResult {
        request_id: request_id.clone(),
        instance_id, // proxy uses instanceID to look up the caller and forward CallResult; missing it yields "instance not found".
        code,
        message: message.to_string(),
        ..Default::default()
    };
    // Raw RRT invokes return their value inline even when the caller does not
    // allocate a DataSystem return object. Preserve a supplied object ID, but
    // do not use its absence to discard a valid inline result.
    if !value.is_empty() {
        result
            .small_objects
            .push(crate::posix::common::SmallObject {
                id: object_id,
                value,
                ..Default::default()
            });
    }
    StreamingMessage {
        message_id: request_id,
        meta_data: Default::default(),
        body: Some(streaming_message::Body::CallResultReq(result)),
    }
}

/// rrt activity signal value. It matches functionsystem `common/constants/signal.h`: core signals 1..22 are already used,
/// so use the next free value 23. It is not a POSIX signal; proxy feeds it into IdleMgr.
pub(crate) const IDLE_REPORT_SIGNAL: i32 = 23;
pub(crate) const CHECKPOINT_SIGNAL: i32 = 24;

pub(crate) fn checkpoint_request_msg(instance_id: &str) -> StreamingMessage {
    static CHECKPOINT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let sequence = CHECKPOINT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let request_id = format!("rrt-checkpoint-{nanos}-{sequence}");
    StreamingMessage {
        message_id: request_id.clone(),
        meta_data: Default::default(),
        body: Some(streaming_message::Body::KillReq(
            crate::posix::core_service::KillRequest {
                instance_id: instance_id.to_string(),
                signal: CHECKPOINT_SIGNAL,
                request_id,
                ..Default::default()
            },
        )),
    }
}

/// Build activity reports as `KillRequest{ instanceID, signal=23, payload=busy|idle }` sent upstream to function-proxy over MessageStream.
pub(crate) fn activity_report_msg(instance_id: &str, payload: Vec<u8>) -> StreamingMessage {
    StreamingMessage {
        message_id: String::new(),
        meta_data: Default::default(),
        body: Some(streaming_message::Body::KillReq(
            crate::posix::core_service::KillRequest {
                instance_id: instance_id.to_string(),
                signal: IDLE_REPORT_SIGNAL,
                payload,
                ..Default::default()
            },
        )),
    }
}

async fn send_snap_started_activity_and_response(
    response_tx: &mpsc::Sender<StreamingMessage>,
    message_id: String,
    instance_id: &str,
    code: crate::posix::common::ErrorCode,
    message: String,
) -> bool {
    let code_value = code as i32;
    if code_value == crate::posix::common::ErrorCode::ErrNone as i32 {
        // Publish the restored generation's activity before acknowledging
        // SnapStarted. FunctionSystem can then enqueue the state before its
        // completion callback commits the instance as RUNNING or closes the
        // candidate control stream.
        let state = activity::current_state();
        if response_tx
            .send(activity_report_msg(instance_id, state.as_bytes().to_vec()))
            .await
            .is_err()
        {
            return false;
        }
        rrt_info!(
            "[rrt-runtime] SnapStarted activity state published state={} active_count={}",
            state,
            activity::active_count()
        );
    }

    let response = StreamingMessage {
        message_id,
        meta_data: Default::default(),
        body: Some(streaming_message::Body::SnapStartedRsp(
            crate::posix::runtime_service::SnapStartedResponse {
                code: code_value,
                message,
            },
        )),
    };
    response_tx.send(response).await.is_ok()
}

fn shutdown_response_msg(
    request_id: String,
    code: crate::posix::common::ErrorCode,
    message: String,
) -> StreamingMessage {
    StreamingMessage {
        message_id: request_id,
        meta_data: Default::default(),
        body: Some(streaming_message::Body::ShutdownRsp(
            crate::posix::runtime_service::ShutdownResponse {
                code: code as i32,
                message,
            },
        )),
    }
}

fn parse_runtime_port(name: &str, raw_port: String) -> Result<u16, String> {
    match raw_port.parse::<u16>() {
        Ok(port) if port > 0 => Ok(port),
        _ => Err(format!(
            "invalid {name} '{raw_port}': expected an integer between 1 and 65535"
        )),
    }
}

async fn start_configured_http_server() -> Result<
    Option<(
        watch::Receiver<RuntimeReadyState>,
        httpserver::HttpServerControl,
    )>,
    String,
> {
    let raw_port = match std::env::var("RRT_HTTP_PORT") {
        Ok(raw_port) => raw_port,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(err) => return Err(format!("failed to read RRT_HTTP_PORT: {err}")),
    };
    let port = parse_runtime_port("RRT_HTTP_PORT", raw_port)?;
    let token = std::env::var("RRT_HTTP_TOKEN").ok();
    start_http_server_with_control(port, token)
        .await
        .map(Some)
        .map_err(|err| err.to_string())
}

async fn start_configured_tunnel_server() -> Result<
    Option<(
        watch::Receiver<RuntimeReadyState>,
        tunnel::TunnelServerControl,
    )>,
    String,
> {
    // RRT_TUNNEL_WS_PORT is the feature gate. Without it the tunnel is not part
    // of runtime readiness, even if the optional HTTP-port variable is present.
    let raw_ws_port = match std::env::var("RRT_TUNNEL_WS_PORT") {
        Ok(raw_port) => raw_port,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(err) => return Err(format!("failed to read RRT_TUNNEL_WS_PORT: {err}")),
    };
    let ws_port = parse_runtime_port("RRT_TUNNEL_WS_PORT", raw_ws_port)?;
    let http_port = match std::env::var("RRT_TUNNEL_HTTP_PORT") {
        Ok(raw_port) => parse_runtime_port("RRT_TUNNEL_HTTP_PORT", raw_port)?,
        Err(std::env::VarError::NotPresent) => ws_port.checked_add(1).ok_or_else(|| {
            "RRT_TUNNEL_HTTP_PORT is required when RRT_TUNNEL_WS_PORT is 65535".to_string()
        })?,
        Err(err) => return Err(format!("failed to read RRT_TUNNEL_HTTP_PORT: {err}")),
    };
    start_tunnel_runtime_server_with_control(ws_port, http_port)
        .await
        .map(Some)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    child_env::initialize();
    entrypoint::initialize();
    let args = load_args_from_env();
    let _ = LOG_DEBUG.set(args.log_level.eq_ignore_ascii_case("debug"));
    rrt_info!(
        "[rrt-runtime] start rt_server={} runtime_id={} instance_id={} job_id={} deploy_dir={}",
        args.rt_server,
        args.runtime_id,
        args.instance_id,
        args.job_id,
        args.deploy_dir
    );

    let (tx, rx) = mpsc::channel::<StreamingMessage>(64);
    let instance_id = args.instance_id.clone();
    activity::init();
    activity::init_reporter(instance_id.clone(), tx.clone());
    let command_activity_heartbeat = std::env::var("RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(command_activity_heartbeat));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The reconnect path sends the initial snapshot. Skip the immediate
        // first tick so normal startup does not emit a duplicate.
        interval.tick().await;
        loop {
            interval.tick().await;
            activity::report_command_snapshot();
        }
    });
    rrt_info!("[rrt-runtime] instance_id={}", instance_id);

    // Reserve the direct HTTP listener and, when configured, both tunnel
    // listeners concurrently. RuntimeRPC can open after the ports are reserved;
    // InitCall success is gated on the combined readiness result below.
    let (http_start, tunnel_start, checkpoint_start) = tokio::join!(
        start_configured_http_server(),
        start_configured_tunnel_server(),
        start_checkpoint_server_with_control(instance_id.clone(), tx.clone())
    );
    let mut configured_services = Vec::with_capacity(3);
    let mut service_controls = RuntimeServiceControls::default();
    match http_start {
        Ok(Some((ready, control))) => {
            configured_services.push(ready);
            service_controls.http = Some(control);
        }
        Ok(None) => {}
        Err(message) => configured_services.push(failed_runtime_receiver(message)),
    }
    match tunnel_start {
        Ok(Some((ready, control))) => {
            configured_services.push(ready);
            service_controls.tunnel = Some(control);
        }
        Ok(None) => {}
        Err(message) => configured_services.push(failed_runtime_receiver(message)),
    }
    match checkpoint_start {
        Ok(Some((ready, control))) => {
            configured_services.push(ready);
            service_controls.checkpoint = Some(control);
        }
        Ok(None) => {}
        Err(error) => configured_services.push(failed_runtime_receiver(format!(
            "checkpoint listener startup failed: {error}"
        ))),
    }
    let runtime_ready = combine_runtime_readiness(configured_services);

    let ctx = std::sync::Arc::new(dispatch::Ctx::new(args.clone()));

    // Busy is reasserted whenever activity restarts, and tunnel connections
    // reassert it even when they overlap another request. Idle remains
    // debounced until the last ActiveGuard drops. Function-proxy IdleMgr owns
    // the actual idle timeout.
    run_message_stream_loop(args, ctx, tx, rx, runtime_ready, service_controls).await
}

fn build_stream_request(
    args: &Args,
    stream_rx: mpsc::Receiver<StreamingMessage>,
) -> Result<tonic::Request<ReceiverStream<StreamingMessage>>, Box<dyn std::error::Error>> {
    let outbound = ReceiverStream::new(stream_rx);
    let mut req = tonic::Request::new(outbound);
    let md = req.metadata_mut();
    md.insert("runtime_id", args.runtime_id.parse()?);
    md.insert("instance_id", args.instance_id.parse()?);
    md.insert("source_id", args.instance_id.parse()?);
    md.insert("dst_id", "function-proxy".parse()?);
    Ok(req)
}

async fn run_message_stream_loop(
    args: Args,
    ctx: std::sync::Arc<dispatch::Ctx>,
    tx: mpsc::Sender<StreamingMessage>,
    mut rx: mpsc::Receiver<StreamingMessage>,
    runtime_ready: watch::Receiver<RuntimeReadyState>,
    service_controls: RuntimeServiceControls,
) -> Result<(), Box<dyn std::error::Error>> {
    const STREAM_CHANNEL_SIZE: usize = 256;
    const RECONNECT_MIN: Duration = Duration::from_millis(200);
    const RECONNECT_MAX: Duration = Duration::from_secs(5);

    let environment_file = crate::startup::restore_environment_file_path();
    let mut backoff = RECONNECT_MIN;
    let mut reconnect_seq: u64 = 0;
    let mut pending: Option<StreamingMessage> = None;
    let mut reconnect_control = ReconnectControlState::new(args);
    let mut checkpoint_handoff: Option<CheckpointHandoffFuture> = None;
    let mut buffered_inbound = VecDeque::new();

    loop {
        reconnect_seq += 1;
        let connection_args = match reconnect_control.load(environment_file.as_deref()) {
            Ok(connection_args) => connection_args,
            Err(error) => {
                rrt_error!(
                    "[rrt-runtime] target physical environment refresh failed seq={} file={:?} error={} retry_ms={}",
                    reconnect_seq,
                    environment_file,
                    error,
                    backoff.as_millis()
                );
                checkpoint_resilient_delay(backoff).await;
                backoff = next_backoff(backoff, RECONNECT_MAX);
                continue;
            }
        };
        let endpoint = format!("http://{}", connection_args.rt_server);
        activity::rebind_reporter_instance_id(&connection_args.instance_id);
        service_controls.rebind_instance_id(&connection_args.instance_id);
        let mut client = match RuntimeRpcClient::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(e) => {
                rrt_error!(
                    "[rrt-runtime] MessageStream connect failed seq={} endpoint={} error={} retry_ms={}",
                    reconnect_seq,
                    endpoint,
                    e,
                    backoff.as_millis()
                );
                checkpoint_resilient_delay(backoff).await;
                backoff = next_backoff(backoff, RECONNECT_MAX);
                continue;
            }
        };

        let (stream_tx, stream_rx) = mpsc::channel::<StreamingMessage>(STREAM_CHANNEL_SIZE);
        let req = build_stream_request(&connection_args, stream_rx)?;
        let mut inbound = match client.message_stream(req).await {
            Ok(stream) => stream.into_inner(),
            Err(e) => {
                rrt_error!(
                    "[rrt-runtime] MessageStream open failed seq={} endpoint={} error={} retry_ms={}",
                    reconnect_seq,
                    endpoint,
                    e,
                    backoff.as_millis()
                );
                checkpoint_resilient_delay(backoff).await;
                backoff = next_backoff(backoff, RECONNECT_MAX);
                continue;
            }
        };

        backoff = RECONNECT_MIN;
        let state = activity::current_state();
        if let Err(e) = stream_tx
            .send(activity_report_msg(
                &connection_args.instance_id,
                state.as_bytes().to_vec(),
            ))
            .await
        {
            drop(e);
            rrt_warn!(
                "[rrt-runtime] MessageStream opened but state sync failed seq={} state={} reconnecting",
                reconnect_seq,
                state
            );
            continue;
        }
        let command_state = format!("command:{}", activity::active_command_count());
        if let Err(e) = stream_tx
            .send(activity_report_msg(
                &connection_args.instance_id,
                command_state.as_bytes().to_vec(),
            ))
            .await
        {
            drop(e);
            rrt_warn!(
                "[rrt-runtime] MessageStream opened but command activity sync failed seq={} state={} reconnecting",
                reconnect_seq,
                command_state
            );
            continue;
        }
        rrt_info!(
            "[rrt-runtime] MessageStream opened seq={} endpoint={} activity_state={}",
            reconnect_seq,
            endpoint,
            state
        );

        let disconnect_reason = loop {
            if let Some(msg) = pending.take() {
                if let Err(e) = stream_tx.send(msg).await {
                    let msg = e.0;
                    if should_retry_outbound_msg(&msg) {
                        pending = Some(msg);
                    }
                    break "outbound_send_failed".to_string();
                }
                continue;
            }

            tokio::select! {
                maybe_msg = rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        return Ok(());
                    };
                    if let Err(e) = stream_tx.send(msg).await {
                        let msg = e.0;
                        if should_retry_outbound_msg(&msg) {
                            pending = Some(msg);
                        }
                        break "outbound_send_failed".to_string();
                    }
                }
                inbound_msg = async {
                    match buffered_inbound.pop_front() {
                        Some(message) => Ok(Some(message)),
                        None => inbound.message().await,
                    }
                } => {
                    match inbound_msg {
                        Ok(Some(msg)) => {
                            let keep_stream = if matches!(
                                msg.body.as_ref(), Some(streaming_message::Body::PrepareSnapReq(_))
                            ) {
                                handle_prepare_snap_on_stream(
                                    msg.message_id, &stream_tx, &mut inbound,
                                    &mut buffered_inbound, &mut checkpoint_handoff,
                                    service_controls.checkpoint.as_ref(),
                                ).await
                            } else {
                                handle_inbound_message_with_control_sender(
                                msg,
                                &connection_args.instance_id,
                                ctx.clone(),
                                tx.clone(),
                                runtime_ready.clone(),
                                Some(&stream_tx),
                                Some(&service_controls),
                                ).await
                            };
                            if !keep_stream {
                                break "handler_requested_reconnect".to_string();
                            }
                        }
                        Ok(None) => break "remote_closed".to_string(),
                        Err(e) => break format!("inbound_error={e}"),
                    }
                }
            }
        };

        // Buffered messages belong to this physical control stream. A restored
        // target must not process source messages after reloading its identity.
        buffered_inbound.clear();
        rrt_warn!(
            "[rrt-runtime] MessageStream disconnected seq={} reason={} pending={} retry_ms={}",
            reconnect_seq,
            disconnect_reason,
            pending.is_some(),
            backoff.as_millis()
        );
        checkpoint_resilient_delay(backoff).await;
        backoff = next_backoff(backoff, RECONNECT_MAX);
    }
}

async fn checkpoint_resilient_delay(delay: Duration) {
    // A restored control stream is detected as disconnected before this delay
    // is created, so this timer belongs to the current runtime generation.
    // Never block the executor: file, exec, HTTP and tunnel traffic share it.
    tokio::time::sleep(delay).await;
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    std::cmp::min(current.saturating_mul(2), max)
}

fn should_retry_outbound_msg(msg: &StreamingMessage) -> bool {
    match &msg.body {
        Some(streaming_message::Body::CallResultReq(_)) => true,
        Some(streaming_message::Body::KillReq(kill)) if kill.signal == IDLE_REPORT_SIGNAL => false,
        Some(streaming_message::Body::HeartbeatRsp(_)) => false,
        Some(_) => true,
        None => false,
    }
}

fn body_kind(body: &streaming_message::Body) -> &'static str {
    match body {
        streaming_message::Body::CreateReq(_) => "createReq",
        streaming_message::Body::CreateRsp(_) => "createRsp",
        streaming_message::Body::InvokeReq(_) => "invokeReq",
        streaming_message::Body::InvokeRsp(_) => "invokeRsp",
        streaming_message::Body::ExitReq(_) => "exitReq",
        streaming_message::Body::ExitRsp(_) => "exitRsp",
        streaming_message::Body::SaveReq(_) => "saveReq",
        streaming_message::Body::SaveRsp(_) => "saveRsp",
        streaming_message::Body::LoadReq(_) => "loadReq",
        streaming_message::Body::LoadRsp(_) => "loadRsp",
        streaming_message::Body::KillReq(_) => "killReq",
        streaming_message::Body::KillRsp(_) => "killRsp",
        streaming_message::Body::CallResultReq(_) => "callResultReq",
        streaming_message::Body::CallResultAck(_) => "callResultAck",
        streaming_message::Body::CallReq(_) => "callReq",
        streaming_message::Body::CallRsp(_) => "callRsp",
        streaming_message::Body::NotifyReq(_) => "notifyReq",
        streaming_message::Body::NotifyRsp(_) => "notifyRsp",
        streaming_message::Body::CheckpointReq(_) => "checkpointReq",
        streaming_message::Body::CheckpointRsp(_) => "checkpointRsp",
        streaming_message::Body::RecoverReq(_) => "recoverReq",
        streaming_message::Body::RecoverRsp(_) => "recoverRsp",
        streaming_message::Body::ShutdownReq(_) => "shutdownReq",
        streaming_message::Body::ShutdownRsp(_) => "shutdownRsp",
        streaming_message::Body::SignalReq(_) => "signalReq",
        streaming_message::Body::SignalRsp(_) => "signalRsp",
        streaming_message::Body::HeartbeatReq(_) => "heartbeatReq",
        streaming_message::Body::HeartbeatRsp(_) => "heartbeatRsp",
        streaming_message::Body::CreateReqs(_) => "createReqs",
        streaming_message::Body::CreateRsps(_) => "createRsps",
        streaming_message::Body::RGroupReq(_) => "rGroupReq",
        streaming_message::Body::RGroupRsp(_) => "rGroupRsp",
        streaming_message::Body::PrepareSnapReq(_) => "prepareSnapReq",
        streaming_message::Body::PrepareSnapRsp(_) => "prepareSnapRsp",
        streaming_message::Body::SnapStartedReq(_) => "snapStartedReq",
        streaming_message::Body::SnapStartedRsp(_) => "snapStartedRsp",
    }
}

/// Acknowledge receipt of a CallReq on the transport-level message ID.
///
/// CallResultReq is a separate business result keyed by CallRequest.requestID.
/// Function Proxy keeps the send future pending until this response arrives.
fn call_response_msg(message_id: String) -> StreamingMessage {
    StreamingMessage {
        message_id,
        meta_data: Default::default(),
        body: Some(streaming_message::Body::CallRsp(CallResponse {
            code: crate::posix::common::ErrorCode::ErrNone as i32,
            message: String::new(),
        })),
    }
}

#[cfg(test)]
mod checkpoint_prepare_tests {
    use super::*;
    use std::process::Command;

    #[tokio::test]
    async fn checkpoint_prepare_does_not_wait_for_activity() {
        let active = activity::enter(activity::ActivitySource::RuntimeRpc);
        let (tx, mut rx) = mpsc::channel(1);

        assert!(send_prepare_snap_response("prepare-checkpoint".to_string(), &tx, false).await);

        let response = rx.recv().await.expect("PrepareSnap response");
        let Some(streaming_message::Body::PrepareSnapRsp(response)) = response.body else {
            panic!("unexpected PrepareSnap response body")
        };
        assert_eq!(
            response.code,
            crate::posix::common::ErrorCode::ErrInnerSystemError as i32
        );
        drop(active);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snap_started_success_publishes_busy_before_response() {
        const ENV: &str = "ADX_RRT_SNAP_STARTED_ACTIVITY_ISOLATED";
        const TEST: &str =
            "runtime::checkpoint_prepare_tests::snap_started_success_publishes_busy_before_response";
        if std::env::var_os(ENV).is_none() {
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .arg(TEST)
                .arg("--exact")
                .arg("--test-threads=1")
                .env(ENV, "1")
                .status()
                .expect("run isolated SnapStarted activity test");
            assert!(
                status.success(),
                "isolated SnapStarted activity test failed"
            );
            return;
        }

        let active = activity::enter(activity::ActivitySource::RuntimeRpc);
        let (tx, mut rx) = mpsc::channel(2);

        assert!(
            send_snap_started_activity_and_response(
                &tx,
                "snap-started".to_string(),
                "restored-sandbox",
                crate::posix::common::ErrorCode::ErrNone,
                "ok".to_string(),
            )
            .await
        );

        let activity = rx.recv().await.expect("SnapStarted activity report");
        let Some(streaming_message::Body::KillReq(kill)) = activity.body else {
            panic!("expected activity KillReq before SnapStarted response")
        };
        assert_eq!(kill.instance_id, "restored-sandbox");
        assert_eq!(kill.signal, IDLE_REPORT_SIGNAL);
        assert_eq!(kill.payload, b"busy");
        let response = rx.recv().await.expect("SnapStarted response");
        assert!(matches!(
            response.body,
            Some(streaming_message::Body::SnapStartedRsp(_))
        ));

        drop(active);
    }
}

type CheckpointHandoffFuture = Pin<
    Box<
        dyn std::future::Future<Output = std::io::Result<crate::startup::CheckpointOutcome>> + Send,
    >,
>;

async fn handle_prepare_snap_on_stream<S>(
    message_id: String,
    response_tx: &mpsc::Sender<StreamingMessage>,
    inbound: &mut S,
    buffered: &mut VecDeque<StreamingMessage>,
    handoff: &mut Option<CheckpointHandoffFuture>,
    checkpoint_control: Option<&httpserver::CheckpointServerControl>,
) -> bool
where
    S: tokio_stream::Stream<Item = Result<StreamingMessage, tonic::Status>> + Unpin,
{
    rrt_info!("[rrt-runtime] PrepareSnapReq accepted");
    if handoff.is_none() {
        match crate::startup::open_checkpoint_handoff() {
            Ok(Some(opened)) => {
                *handoff = Some(Box::pin(crate::startup::wait_for_checkpoint_handoff(
                    opened,
                )));
            }
            Ok(None) => {}
            Err(error) => rrt_error!("[rrt-runtime] failed to open checkpoint handoff: {error}"),
        }
    }
    // Open before acknowledging PrepareSnap: the descriptor belongs to the next
    // backend checkpoint generation, including after an attempt rejected before RPC.
    if !send_prepare_snap_response(message_id, response_tx, handoff.is_some()).await {
        return false;
    }
    if handoff.is_none() {
        if let Some(control) = checkpoint_control {
            control.record_handoff_error("checkpoint handoff barrier is unavailable".to_string());
        }
        return true;
    }
    rrt_info!("[rrt-runtime] waiting for checkpoint handoff");
    let mut stream_open = true;
    loop {
        tokio::select! {
            outcome = handoff.as_mut().expect("checkpoint handoff exists") => {
                handoff.take();
                return finish_checkpoint_handoff(outcome, checkpoint_control);
            }
            message = inbound.next(), if stream_open && buffered.len() < 256 => {
                match message {
                    Some(Ok(message)) => {
                        if let Some(streaming_message::Body::KillRsp(response)) = message.body.as_ref() {
                            if response.code != 0 && response.checkpoint_not_started {
                                if let Some(control) = checkpoint_control {
                                    if control.reject_unstarted_checkpoint(
                                        &message.message_id, response.message.clone(),
                                    ) {
                                        // No backend checkpoint ran, so no handoff can arrive for
                                        // this attempt. Retain this single read for the next real
                                        // checkpoint instead of leaking a blocking reader per retry.
                                        return true;
                                    }
                                }
                            }
                        }
                        // Preserve normal ordering, particularly SnapStarted and target
                        // identity refresh on restore. Only a proven rejection interrupts.
                        buffered.push_back(message);
                    }
                    Some(Err(_)) | None => stream_open = false,
                }
            }
        }
    }
}

async fn send_prepare_snap_response(
    message_id: String,
    response_tx: &mpsc::Sender<StreamingMessage>,
    barrier_ready: bool,
) -> bool {
    let (code, message) = if barrier_ready {
        (
            crate::posix::common::ErrorCode::ErrNone,
            "PrepareSnap completed successfully",
        )
    } else {
        (
            crate::posix::common::ErrorCode::ErrInnerSystemError,
            "checkpoint handoff barrier is unavailable",
        )
    };
    let response = StreamingMessage {
        message_id,
        meta_data: Default::default(),
        body: Some(streaming_message::Body::PrepareSnapRsp(
            crate::posix::runtime_service::PrepareSnapResponse {
                code: code as i32,
                message: message.to_string(),
            },
        )),
    };
    if response_tx.send(response).await.is_err() {
        return false;
    }

    true
}

fn finish_checkpoint_handoff(
    outcome: std::io::Result<crate::startup::CheckpointOutcome>,
    checkpoint_control: Option<&httpserver::CheckpointServerControl>,
) -> bool {
    match outcome {
        Ok(crate::startup::CheckpointOutcome::Restore) => {
            if let Some(control) = checkpoint_control {
                control.record_handoff(crate::startup::CheckpointOutcome::Restore);
            }
            rrt_info!(
                "[rrt-runtime] checkpoint handoff outcome=restore; target physical identity will be loaded before reconnect"
            );
            return false;
        }
        Ok(crate::startup::CheckpointOutcome::Resume) => {
            if let Some(control) = checkpoint_control {
                control.record_handoff(crate::startup::CheckpointOutcome::Resume);
            }
            rrt_warn!(
                "[rrt-runtime] checkpoint handoff outcome=resume; source runtime remains active"
            );
        }
        Ok(crate::startup::CheckpointOutcome::Error) => {
            if let Some(control) = checkpoint_control {
                control.record_handoff(crate::startup::CheckpointOutcome::Error);
            }
            rrt_error!(
                "[rrt-runtime] checkpoint handoff outcome=error; source runtime remains authoritative"
            );
        }
        Err(error) => {
            if let Some(control) = checkpoint_control {
                control.record_handoff_error(format!("checkpoint handoff failed: {error}"));
            }
            rrt_error!("[rrt-runtime] checkpoint handoff failed: {error}");
        }
    }
    true
}

#[cfg(test)]
async fn handle_inbound_message(
    msg: StreamingMessage,
    instance_id: &str,
    ctx: std::sync::Arc<dispatch::Ctx>,
    tx: mpsc::Sender<StreamingMessage>,
    runtime_ready: watch::Receiver<RuntimeReadyState>,
) -> bool {
    handle_inbound_message_with_control_sender(msg, instance_id, ctx, tx, runtime_ready, None, None)
        .await
}

async fn handle_inbound_message_with_control_sender(
    msg: StreamingMessage,
    instance_id: &str,
    ctx: std::sync::Arc<dispatch::Ctx>,
    tx: mpsc::Sender<StreamingMessage>,
    runtime_ready: watch::Receiver<RuntimeReadyState>,
    control_tx: Option<&mpsc::Sender<StreamingMessage>>,
    service_controls: Option<&RuntimeServiceControls>,
) -> bool {
    let mid = msg.message_id.clone();
    match msg.body {
        Some(streaming_message::Body::CallReq(mut call)) => {
            if debug_on() {
                rrt_debug!(
                    "[rrt-runtime] CallReq is_create={} function={:?} request_id={} args={}",
                    call.is_create,
                    call.function,
                    call.request_id,
                    call.args.len()
                );
            }
            let response_tx = control_tx.unwrap_or(&tx);
            if response_tx.send(call_response_msg(mid)).await.is_err() {
                return false;
            }
            if call.sender_id.is_empty() {
                call.sender_id = instance_id.to_string();
            }
            // Each call uses its own spawn_blocking task: long commands must not block the receive loop because heartbeats must keep responding.
            let request_id = call.request_id.clone();
            let iid = call.sender_id.clone();
            let ctx2 = ctx.clone();
            let tx2 = tx.clone();
            tokio::spawn(async move {
                let _active = activity::enter(activity::ActivitySource::RuntimeRpc);
                let readiness = if call.is_create {
                    match tokio::time::timeout(
                        RUNTIME_READY_TIMEOUT,
                        wait_for_runtime_ready(runtime_ready),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(format!(
                            "RRT service readiness timed out after {} seconds",
                            RUNTIME_READY_TIMEOUT.as_secs()
                        )),
                    }
                } else {
                    Ok(())
                };
                let reply = match readiness {
                    Err(message) => call_result_msg(
                        request_id,
                        iid,
                        String::new(),
                        crate::posix::common::ErrorCode::ErrInnerSystemError as i32,
                        &format!("runtime initialization failed: {message}"),
                        Vec::new(),
                    ),
                    Ok(()) => {
                        match tokio::task::spawn_blocking(move || ctx2.handle_call(call)).await {
                            Ok(msg) => msg,
                            Err(e) => call_result_msg(
                                request_id,
                                iid,
                                String::new(),
                                crate::posix::common::ErrorCode::ErrInnerSystemError as i32,
                                &format!("dispatch panicked: {e}"),
                                Vec::new(),
                            ),
                        }
                    }
                };
                let _ = tx2.send(reply).await;
            });
        }
        Some(streaming_message::Body::CallResultAck(_)) => {
            if debug_on() {
                rrt_debug!("[rrt-runtime] CallResultAck");
            }
        }
        Some(streaming_message::Body::KillRsp(response)) => {
            if let Some(control) =
                service_controls.and_then(|controls| controls.checkpoint.as_ref())
            {
                control.record_proxy_ack(&mid, response.code, response.message.clone());
            }
            rrt_debug!(
                "[rrt-runtime] KillRsp received for activity/signal report message_id={}",
                mid
            );
        }
        Some(streaming_message::Body::ShutdownReq(req)) => {
            let tx2 = tx.clone();
            tokio::spawn(async move {
                let grace = Duration::from_secs(req.grace_period_second);
                rrt_info!(
                    "[rrt-runtime] ShutdownReq -> wait inflight grace_sec={} active_count={}",
                    req.grace_period_second,
                    activity::active_count()
                );
                let idle = activity::wait_until_idle(grace).await;
                let (code, message) = if idle {
                    (
                        crate::posix::common::ErrorCode::ErrNone,
                        "shutdown accepted after inflight drained".to_string(),
                    )
                } else {
                    (
                        crate::posix::common::ErrorCode::ErrInstanceBusy,
                        format!(
                            "shutdown rejected: {} in-flight request(s) still running after {}s",
                            activity::active_count(),
                            req.grace_period_second
                        ),
                    )
                };
                let _ = tx2.send(shutdown_response_msg(mid, code, message)).await;
            });
        }
        Some(streaming_message::Body::SnapStartedReq(_)) => {
            rrt_info!("[rrt-runtime] SnapStartedReq accepted");
            let rearm =
                service_controls.map_or_else(|| Ok(Vec::new()), |controls| controls.rearm());
            let (code, message) = match rearm {
                Ok(generations) => {
                    for (service, generation) in generations {
                        rrt_info!(
                            "[rrt-runtime] SnapStarted {service} listener rearmed generation={generation}"
                        );
                    }
                    (
                        crate::posix::common::ErrorCode::ErrNone,
                        "SnapStarted handled successfully".to_string(),
                    )
                }
                Err(error) => {
                    rrt_error!("[rrt-runtime] SnapStarted listener rearm failed: {error}");
                    (
                        crate::posix::common::ErrorCode::ErrInnerSystemError,
                        format!("SnapStarted listener rearm failed: {error}"),
                    )
                }
            };
            if let Some(control) =
                service_controls.and_then(|controls| controls.checkpoint.as_ref())
            {
                control.record_snap_started(code as i32, message.clone());
            }
            let response_tx = control_tx.unwrap_or(&tx);
            if !send_snap_started_activity_and_response(
                response_tx,
                mid,
                instance_id,
                code,
                message,
            )
            .await
            {
                return false;
            }
        }
        Some(streaming_message::Body::HeartbeatReq(_)) => {
            let rsp = StreamingMessage {
                message_id: mid,
                meta_data: Default::default(),
                body: Some(streaming_message::Body::HeartbeatRsp(
                    crate::posix::runtime_service::HeartbeatResponse::default(),
                )),
            };
            let response_tx = control_tx.unwrap_or(&tx);
            let _ = response_tx.send(rsp).await;
        }
        Some(other) => {
            rrt_debug!(
                "[rrt-runtime] ignored inbound body={} message_id={}",
                body_kind(&other),
                mid
            );
        }
        None => rrt_warn!("[rrt-runtime] empty body"),
    }
    true
}

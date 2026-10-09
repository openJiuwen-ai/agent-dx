//! Test-only Meta launcher plus RootCommand issuer for OwnerFs workspace bind validation.
//!
//! This binary intentionally has no production issuance RPC. It starts the real
//! local-file Meta services from a normal Meta config, then writes one bounded
//! RootCommand transaction through the same Store object after checking the
//! persisted active Home/session/generation facts. It never edits the WAL or
//! publishes a management API for issuing commands.

use std::{
    collections::HashMap,
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use afs::{
    config::{Cli, Config, MetaStoreBackend, Role, read_certificate_der},
    meta::{
        self, Meta,
        store::{
            MetaEntity, MetaKey, MetaRead, MetaStore, MetaTxn, OperationResult, RequestKey,
            RequestOutcome, RootAccessGrant, RootCommandRecord, RootCommandType, RootRecord, Store,
            StoreOperation, StoreRevision, TxnCondition, TxnMutation, TxnOutcome,
            local_file::LocalFileBackend,
        },
    },
    runtime::{self, BoxError, Observability, Services},
};
use clap::Parser;
use serde::{Deserialize, Serialize};

const MAX_COMMAND_ID_LEN: usize = 128;
const TRIGGER_TIMEOUT: Duration = Duration::from_secs(30);
const TRIGGER_POLL_INTERVAL: Duration = Duration::from_millis(50);
const ISSUER_ID: &str = "root-command-meta";

#[derive(Debug, Parser)]
#[command(about = "Test-only local-file Meta with one RootCommand issuer")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    trigger: Option<PathBuf>,
    #[arg(long)]
    receipt: Option<PathBuf>,
    #[arg(long)]
    workspace: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Trigger {
    command_id: String,
    #[serde(default)]
    mismatch_generation: bool,
    #[serde(default)]
    root_epoch: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Receipt {
    workspace: String,
    trigger: Trigger,
    meta_id: String,
    config: String,
    original_root: RootRecord,
    original_root_revision: StoreRevision,
    home_session: afs::meta::store::NodeSession,
    home_session_revision: StoreRevision,
    home_grant: RootAccessGrant,
    home_grant_revision: StoreRevision,
    issued: Vec<IssuedCommandReceipt>,
}

#[derive(Debug, Clone, Serialize)]
struct IssuedCommandReceipt {
    request: RequestKey,
    command: RootCommandRecord,
    outcome: IssuedOutcomeReceipt,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum IssuedOutcomeReceipt {
    Committed {
        revision: StoreRevision,
        request_outcome: RequestOutcome,
    },
    ConditionFailed {
        revision: StoreRevision,
        existing_outcome: Option<RequestOutcome>,
    },
}

struct HomeState {
    root: RootRecord,
    root_revision: StoreRevision,
    session: afs::meta::store::NodeSession,
    session_revision: StoreRevision,
    grant: RootAccessGrant,
    grant_revision: StoreRevision,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let trigger_path = required_path(args.trigger, "AFS_TEST_ROOT_COMMAND_TRIGGER", "--trigger")?;
    let receipt_path = required_path(args.receipt, "AFS_TEST_ROOT_COMMAND_RECEIPT", "--receipt")?;
    let workspace = required_text(
        args.workspace,
        "AFS_TEST_ROOT_COMMAND_WORKSPACE",
        "--workspace",
    )?;
    let cfg = Config::resolve(
        Role::Meta,
        Cli {
            config: Some(args.config.clone()),
            ..Cli::default()
        },
    )?;
    validate_config(&cfg)?;

    let obs = Observability::new()?;
    let _guards = runtime::initialize(&cfg, "afs-root-command-meta", &obs)?;
    let concrete_store = open_local_file_store(&cfg).await?;
    let meta_store: Arc<dyn MetaStore> = concrete_store.clone();
    let trusted = load_trusted_node_certs(&cfg.trusted_node_certs)?;
    let state = Arc::new(Meta::with_store_and_peer_identity(
        cfg.id.clone(),
        obs,
        meta_store,
        trusted,
        cfg.ownerfs,
    ));

    let grpc = tokio::net::TcpListener::bind(cfg.grpc_listen).await?;
    let rest = tokio::net::TcpListener::bind(cfg.rest_listen).await?;
    let mut services = Services::new();

    let stop = services.stop.subscribe();
    let grpc_config = afs_transport::grpc::GrpcConfig::default();
    let incoming =
        grpc_config.configure_tcp_incoming(tonic::transport::server::TcpIncoming::from(grpc));
    let server = afs_transport::grpc::SecurityManager::new(cfg.tls_config())?
        .configure_server(grpc_config.configure_server(tonic::transport::Server::builder()))?;
    services.spawn({
        let state = state.clone();
        async move {
            server
                .layer(afs_tracing::GrpcServerTraceLayer::default())
                .add_service(afs_protocol::meta::meta_server::MetaServer::new(
                    meta::rpc::MetaRpc(state.clone()),
                ))
                .add_service(
                    afs_protocol::meta::owner_roots_server::OwnerRootsServer::new(
                        meta::rpc::OwnerRootsRpc(state.clone()),
                    ),
                )
                .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                    meta::rpc::DfsMetaRpc(state),
                ))
                .serve_with_incoming_shutdown(incoming, runtime::cancelled(stop))
                .await
                .map_err(Into::into)
        }
    });

    let stop = services.stop.subscribe();
    services.spawn({
        let state = state.clone();
        async move {
            axum::serve(rest, meta::rest::router(state))
                .with_graceful_shutdown(runtime::cancelled(stop))
                .await
                .map_err(Into::into)
        }
    });

    let stop = services.stop.subscribe();
    services.spawn({
        let store = concrete_store.clone();
        let cfg_path = args.config.clone();
        let trigger_path = trigger_path.clone();
        let receipt_path = receipt_path.clone();
        let workspace = workspace.clone();
        let meta_id = cfg.id.clone();
        async move {
            let Some(trigger) = wait_for_trigger(&trigger_path, stop.clone()).await? else {
                return Ok(());
            };
            let receipt = issue_receipt(store, &workspace, &meta_id, &cfg_path, trigger).await?;
            write_receipt(&receipt_path, &receipt)?;
            runtime::cancelled(stop).await;
            Ok(())
        }
    });

    afs_logging::info!("root_command_meta.ready"; "grpc" => cfg.grpc_listen.to_string(), "rest" => cfg.rest_listen.to_string(), "workspace" => workspace);
    services.run().await
}

fn required_path(
    value: Option<PathBuf>,
    env_name: &str,
    flag_name: &str,
) -> Result<PathBuf, BoxError> {
    value
        .or_else(|| std::env::var_os(env_name).map(PathBuf::from))
        .ok_or_else(|| simple_error(format!("{flag_name} or {env_name} is required")))
}

fn required_text(
    value: Option<String>,
    env_name: &str,
    flag_name: &str,
) -> Result<String, BoxError> {
    let value = value
        .or_else(|| std::env::var(env_name).ok())
        .ok_or_else(|| simple_error(format!("{flag_name} or {env_name} is required")))?;
    if value.is_empty() {
        return Err(simple_error(format!(
            "{flag_name} or {env_name} must be nonempty"
        )));
    }
    Ok(value)
}

fn validate_config(cfg: &Config) -> Result<(), BoxError> {
    if cfg.meta_store != MetaStoreBackend::LocalFile {
        return Err(simple_error(
            "root_command_meta requires meta_store=local-file so the issuer and services share one Store",
        ));
    }
    if !cfg.ownerfs {
        return Err(simple_error(
            "root_command_meta requires ownerfs=true so production OwnerRoots RPC is active",
        ));
    }
    if cfg.trusted_node_certs.is_empty() {
        return Err(simple_error(
            "root_command_meta requires trusted_node_certs for production mTLS node identity binding",
        ));
    }
    Ok(())
}

async fn open_local_file_store(cfg: &Config) -> Result<Arc<Store>, BoxError> {
    let backend = LocalFileBackend::open(cfg.data_dir.join("meta-store"))?;
    Ok(Arc::new(Store::open(Arc::new(backend)).await?))
}

async fn wait_for_trigger(
    path: &Path,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Option<Trigger>, BoxError> {
    if *stop.borrow_and_update() {
        return Ok(None);
    }
    let bytes = tokio::time::timeout(TRIGGER_TIMEOUT, async {
        loop {
            match tokio::fs::read(path).await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let sleep = tokio::time::sleep(TRIGGER_POLL_INTERVAL);
                    tokio::pin!(sleep);
                    tokio::select! {
                        _ = &mut sleep => {}
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow_and_update() {
                                return Ok(None);
                            }
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| {
        simple_error(format!(
            "trigger {} was not created before timeout",
            path.display()
        ))
    })??;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let trigger: Trigger = serde_json::from_slice(&bytes)?;
    validate_trigger(&trigger)?;
    Ok(Some(trigger))
}

fn validate_trigger(trigger: &Trigger) -> Result<(), BoxError> {
    if trigger.command_id.is_empty() || trigger.command_id.len() > MAX_COMMAND_ID_LEN {
        return Err(simple_error(format!(
            "trigger command_id must be 1..={MAX_COMMAND_ID_LEN} bytes"
        )));
    }
    if trigger.root_epoch == Some(0) {
        return Err(simple_error(
            "trigger root_epoch must be nonzero when provided",
        ));
    }
    if (trigger.mismatch_generation || trigger.root_epoch.is_some())
        && wrong_tuple_command_id(&trigger.command_id).len() > MAX_COMMAND_ID_LEN
    {
        return Err(simple_error(format!(
            "trigger command_id plus wrong-tuple suffix must fit {MAX_COMMAND_ID_LEN} bytes"
        )));
    }
    Ok(())
}

async fn issue_receipt(
    store: Arc<Store>,
    workspace: &str,
    meta_id: &str,
    cfg_path: &Path,
    trigger: Trigger,
) -> Result<Receipt, BoxError> {
    store.health().await?;
    let state = read_home_state(&store, workspace).await?;
    let mut issued = Vec::new();

    let wrong_epoch_requested = trigger
        .root_epoch
        .is_some_and(|root_epoch| root_epoch != state.root.root_epoch);
    if trigger.mismatch_generation || wrong_epoch_requested {
        let wrong_epoch = trigger.root_epoch.unwrap_or(state.root.root_epoch);
        let wrong_generation = if trigger.mismatch_generation {
            checked_wrong_generation(state.grant.access_generation)?
        } else {
            state.grant.access_generation
        };
        let wrong = root_command(
            wrong_tuple_command_id(&trigger.command_id),
            &state,
            wrong_epoch,
            wrong_generation,
        );
        issued.push(commit_root_command(&store, &state, wrong).await?);
    }

    let matching = root_command(
        trigger.command_id.clone(),
        &state,
        state.root.root_epoch,
        state.grant.access_generation,
    );
    issued.push(commit_root_command(&store, &state, matching).await?);

    Ok(Receipt {
        workspace: workspace.to_owned(),
        trigger,
        meta_id: meta_id.to_owned(),
        config: cfg_path.display().to_string(),
        original_root: state.root,
        original_root_revision: state.root_revision,
        home_session: state.session,
        home_session_revision: state.session_revision,
        home_grant: state.grant,
        home_grant_revision: state.grant_revision,
        issued,
    })
}

async fn read_home_state(store: &Store, workspace: &str) -> Result<HomeState, BoxError> {
    let root_id = workspace_root_id(workspace)?;
    let root_snapshot = store
        .read(MetaRead::Root {
            root_id: root_id.clone(),
        })
        .await?;
    let root = match root_snapshot.entity {
        Some(MetaEntity::Root(root)) if root.root_id == root_id => root,
        Some(_) => {
            return Err(simple_error(
                "workspace root read returned wrong entity type",
            ));
        }
        None => {
            return Err(simple_error(format!(
                "workspace {workspace} root id {root_id} is not active"
            )));
        }
    };

    let current_session_snapshot = store
        .read(MetaRead::CurrentNodeSession {
            node_id: root.home_node_id.clone(),
        })
        .await?;
    let session = match current_session_snapshot.entity {
        Some(MetaEntity::NodeSession(session))
            if session.node_id == root.home_node_id
                && session.session_id == root.home_session_id =>
        {
            session
        }
        Some(MetaEntity::NodeSession(session)) => {
            return Err(simple_error(format!(
                "current Home session {} does not match active root session {}",
                session.session_id, root.home_session_id
            )));
        }
        Some(_) => return Err(simple_error("current Home read returned wrong entity type")),
        None => return Err(simple_error("active Home node has no current session")),
    };
    if !session.is_live_at_unix_ms(now_unix_ms()) {
        return Err(simple_error("active Home session lease is not current"));
    }

    let grant_snapshot = store
        .read(MetaRead::RootGrantByHolder {
            root_id: root.root_id.clone(),
            holder_node_id: root.home_node_id.clone(),
            holder_session_id: root.home_session_id.clone(),
        })
        .await?;
    let grant = match grant_snapshot.entity {
        Some(MetaEntity::RootGrant(grant))
            if grant.root_id == root.root_id
                && grant.root_epoch == root.root_epoch
                && grant.home_node_id == root.home_node_id
                && grant.home_session_id == root.home_session_id
                && grant.holder_node_id == root.home_node_id
                && grant.holder_session_id == root.home_session_id =>
        {
            grant
        }
        Some(MetaEntity::RootGrant(_)) => {
            return Err(simple_error("Home grant does not match active root tuple"));
        }
        Some(_) => return Err(simple_error("Home grant read returned wrong entity type")),
        None => {
            return Err(simple_error(
                "active Home session has no persisted root grant",
            ));
        }
    };

    Ok(HomeState {
        root,
        root_revision: root_snapshot.revision,
        session,
        session_revision: current_session_snapshot.revision,
        grant,
        grant_revision: grant_snapshot.revision,
    })
}

fn root_command(
    command_id: String,
    state: &HomeState,
    root_epoch: u64,
    old_access_generation: u64,
) -> RootCommandRecord {
    RootCommandRecord {
        command_id,
        home_node_id: state.root.home_node_id.clone(),
        home_session_id: state.root.home_session_id.clone(),
        root_id: state.root.root_id.clone(),
        root_epoch,
        old_access_generation,
        command_type: RootCommandType::RevokeAccess,
    }
}

async fn commit_root_command(
    store: &Store,
    state: &HomeState,
    command: RootCommandRecord,
) -> Result<IssuedCommandReceipt, BoxError> {
    let request = RequestKey::new(ISSUER_ID, command.command_id.clone());
    let existing_request = store
        .read(MetaRead::RequestOutcome(request.clone()))
        .await?
        .request_outcome;
    if existing_request.is_some() {
        return Err(simple_error(format!(
            "request id {} already has an outcome",
            command.command_id
        )));
    }

    let outcome = RequestOutcome {
        request: request.clone(),
        operation: StoreOperation::BeginRootRevocation,
        result: OperationResult::RootCommand(command.clone()),
    };
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::BeginRootRevocation);
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::Missing(MetaKey::RootCommand {
            command_id: command.command_id.clone(),
        }),
        TxnCondition::RevisionEquals {
            key: MetaKey::Root {
                root_id: state.root.root_id.clone(),
            },
            revision: state.root_revision,
        },
        TxnCondition::RootEpochEquals {
            root_id: state.root.root_id.clone(),
            root_epoch: state.root.root_epoch,
        },
        TxnCondition::RevisionEquals {
            key: MetaKey::RootGrant {
                root_id: state.grant.root_id.clone(),
                holder_node_id: state.grant.holder_node_id.clone(),
                holder_session_id: state.grant.holder_session_id.clone(),
            },
            revision: state.grant_revision,
        },
        TxnCondition::NodeSessionCurrent {
            node_id: state.session.node_id.clone(),
            session_id: state.session.session_id.clone(),
        },
    ]);
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::RootCommand(command.clone())),
        TxnMutation::RecordRequestOutcome(outcome),
    ]);

    let outcome = match store.compare_and_commit(txn).await? {
        TxnOutcome::Committed { revision, outcome } => IssuedOutcomeReceipt::Committed {
            revision,
            request_outcome: outcome,
        },
        TxnOutcome::ConditionFailed {
            revision,
            existing_outcome,
        } => IssuedOutcomeReceipt::ConditionFailed {
            revision,
            existing_outcome,
        },
    };
    Ok(IssuedCommandReceipt {
        request,
        command,
        outcome,
    })
}

fn write_receipt(path: &Path, receipt: &Receipt) -> Result<(), BoxError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut tmp = path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("json");
    tmp.set_extension(format!("{extension}.tmp"));
    std::fs::write(&tmp, serde_json::to_vec_pretty(receipt)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn workspace_root_id(workspace: &str) -> Result<String, BoxError> {
    Ok(afs::node::vfs::ownerfs::root::root_id_from_name(OsStr::new(workspace))?.0)
}

fn checked_wrong_generation(access_generation: u64) -> Result<u64, BoxError> {
    access_generation
        .checked_add(1)
        .ok_or_else(|| simple_error("cannot create wrong generation above u64::MAX"))
}

fn wrong_tuple_command_id(command_id: &str) -> String {
    format!("{command_id}.wrong-tuple")
}

fn now_unix_ms() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn load_trusted_node_certs(
    certs: &HashMap<String, PathBuf>,
) -> Result<HashMap<Vec<u8>, String>, BoxError> {
    let mut out = HashMap::new();
    for (node_id, path) in certs {
        if node_id.is_empty() {
            return Err(simple_error("trusted_node_certs contains an empty node id"));
        }
        let cert = read_certificate_der(path)?;
        if let Some(previous) = out.insert(cert, node_id.clone()) {
            return Err(simple_error(format!(
                "trusted_node_certs maps the same certificate to both {previous} and {node_id}"
            )));
        }
    }
    Ok(out)
}

fn simple_error(message: impl Into<String>) -> BoxError {
    std::io::Error::other(message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use afs::meta::store::{NodeSession, RootRight};

    fn home_state(access_generation: u64) -> HomeState {
        HomeState {
            root: RootRecord {
                root_id: workspace_root_id("workspace").unwrap(),
                root_epoch: 7,
                home_node_id: "node-a".into(),
                home_session_id: "session-a".into(),
                local_prepare_id: "prepare-a".into(),
                created_at_revision: StoreRevision(1),
                updated_at_revision: StoreRevision(2),
            },
            root_revision: StoreRevision(2),
            session: NodeSession {
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                storage_devices: Vec::new(),
                lease_epoch: 3,
                expires_at_unix_ms: u64::MAX,
            },
            session_revision: StoreRevision(3),
            grant: RootAccessGrant {
                root_id: workspace_root_id("workspace").unwrap(),
                root_epoch: 7,
                home_node_id: "node-a".into(),
                home_session_id: "session-a".into(),
                holder_node_id: "node-a".into(),
                holder_session_id: "session-a".into(),
                access_generation,
                rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
                fencing_token: "token-a".into(),
                issued_at_revision: StoreRevision(3),
            },
            grant_revision: StoreRevision(3),
        }
    }

    #[test]
    fn workspace_name_maps_to_ownerfs_root_id_and_revoke_command() {
        assert_eq!(
            workspace_root_id("workspace").unwrap(),
            "root-776f726b7370616365"
        );
        let state = home_state(11);
        let command = root_command("cmd-a".into(), &state, state.root.root_epoch, 11);
        assert_eq!(command.root_id, "root-776f726b7370616365");
        assert_eq!(command.command_type, RootCommandType::RevokeAccess);
    }

    #[test]
    fn trigger_validation_rejects_empty_long_and_zero_epoch() {
        assert!(
            validate_trigger(&Trigger {
                command_id: String::new(),
                mismatch_generation: false,
                root_epoch: None,
            })
            .is_err()
        );
        assert!(
            validate_trigger(&Trigger {
                command_id: "x".repeat(MAX_COMMAND_ID_LEN + 1),
                mismatch_generation: false,
                root_epoch: None,
            })
            .is_err()
        );
        assert!(
            validate_trigger(&Trigger {
                command_id: "cmd".into(),
                mismatch_generation: false,
                root_epoch: Some(0),
            })
            .is_err()
        );
    }

    #[test]
    fn wrong_generation_uses_checked_add() {
        assert_eq!(checked_wrong_generation(41).unwrap(), 42);
        assert!(checked_wrong_generation(u64::MAX).is_err());
    }

    #[tokio::test]
    async fn missing_trigger_returns_none_when_service_is_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trigger.json");
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        stop_tx.send(true).unwrap();
        let result = wait_for_trigger(&path, stop_rx).await.unwrap();
        assert!(result.is_none());
    }
}

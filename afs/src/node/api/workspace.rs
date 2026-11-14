use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::OpenOptions,
    io::Write,
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt},
        },
    },
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use afs_error::Error;
use afs_protocol::{
    meta::LookupNodeReply,
    node_control::{
        PrepareOwnerRootRequest, WorkspaceDefaultsRequest, owner_control_client::OwnerControlClient,
    },
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::Channel;

use crate::node::{
    Node, bridge,
    outlet::{imports, mirror, topology},
    rpc::{meta::GrpcRootMeta, ownerfs::reservation_wire, peer::PeerConnectionPool},
    vfs::{
        ownerfs::{
            OwnerFs,
            root::{
                OutletRoot, RootCreateParams, RootLocation, RootMeta, RootReservation,
                root_id_from_name, root_name_from_id,
            },
        },
        types::FileAttributes,
    },
};

/// Concurrent outlet probes allowed per Home on one API node.
const PROBES_PER_HOME: usize = 2;

type ApiResult = Result<(StatusCode, Value), ApiError>;

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self::home(error)
    }
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, message).to_string(),
        }
    }
    fn unavailable(code: &'static str, error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code,
            message: Error::coded(afs_error::IO_UNAVAILABLE, error.to_string()).to_string(),
        }
    }
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "owner_conflict",
            message: Error::coded(afs_error::IO_ALREADY_EXISTS, message).to_string(),
        }
    }
    fn missing() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: Error::coded(afs_error::NODE_VFS_NOT_FOUND, "workspace is not registered")
                .to_string(),
        }
    }
    fn meta(error: Error) -> Self {
        Self::unavailable("meta_unavailable", error)
    }
    fn operation(error: Error) -> Self {
        use afs_error::ErrorKind;
        let (status, code) = match error.kind() {
            ErrorKind::PermissionDenied | ErrorKind::Unauthenticated => {
                (StatusCode::FORBIDDEN, "permission_denied")
            }
            ErrorKind::FailedPrecondition | ErrorKind::AlreadyExists | ErrorKind::NotFound => {
                (StatusCode::CONFLICT, "workspace_conflict")
            }
            ErrorKind::InvalidArgument | ErrorKind::OutOfRange => {
                (StatusCode::BAD_REQUEST, "invalid_request")
            }
            ErrorKind::Internal
            | ErrorKind::DataLoss
            | ErrorKind::Unimplemented
            | ErrorKind::Unknown => (StatusCode::INTERNAL_SERVER_ERROR, "operation_failed"),
            _ => return Self::home(error),
        };
        Self {
            status,
            code,
            message: error.to_string(),
        }
    }
    fn home(error: impl std::fmt::Display) -> Self {
        Self::unavailable("owner_node_unavailable", error)
    }
    /// Home 报告同名旧代次已隔离、尚未注销时，与同名创建进行中一样可重试。
    fn attribute(error: Error) -> Self {
        if error.code() == afs_error::META_OWNER_ROOT_BUSY {
            Self::unavailable("claim_conflict", error)
        } else {
            Self::home(error)
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({"code": self.code, "message": self.message})),
        )
            .into_response();
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(RETRY_AFTER, axum::http::HeaderValue::from_static("1"));
        }
        response
    }
}

pub(crate) struct WorkspaceApi {
    node: Arc<Node>,
    owner: Arc<OwnerFs>,
    meta: Arc<GrpcRootMeta>,
    peers: Arc<PeerConnectionPool>,
    runtime: tokio::runtime::Handle,
    /// Outlet probes touch hard-mounted NFS and can block indefinitely; each
    /// Home gets a few, so one stuck Home cannot occupy the blocking pool.
    probes: Mutex<BTreeMap<String, Weak<Semaphore>>>,
    #[cfg(test)]
    after_prepare: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    requests: afs_metrics::IntCounterVec,
    duration: afs_metrics::HistogramVec,
    outlet_wait: afs_metrics::Histogram,
}

impl WorkspaceApi {
    pub(crate) fn new(
        node: Arc<Node>,
        meta: Arc<GrpcRootMeta>,
        peers: Arc<PeerConnectionPool>,
    ) -> Result<Arc<Self>, crate::runtime::BoxError> {
        let owner = node.ownerfs.clone().ok_or_else(|| {
            Error::coded(afs_error::CONFIG_INVALID, "Workspace API requires OwnerFs")
        })?;
        let requests = afs_metrics::IntCounterVec::new(
            afs_metrics::Opts::new(
                "afs_workspace_api_requests_total",
                "Workspace API responses",
            ),
            &["operation", "status"],
        )?;
        let duration = afs_metrics::HistogramVec::new(
            afs_metrics::HistogramOpts::new(
                "afs_workspace_api_duration_seconds",
                "Workspace API response latency",
            ),
            &["operation", "status"],
        )?;
        let outlet_wait = afs_metrics::Histogram::with_opts(afs_metrics::HistogramOpts::new(
            "afs_workspace_outlet_wait_seconds",
            "Workspace outlet readiness wait",
        ))?;
        node.observability
            .registry
            .register(Box::new(requests.clone()))?;
        node.observability
            .registry
            .register(Box::new(duration.clone()))?;
        node.observability
            .registry
            .register(Box::new(outlet_wait.clone()))?;
        Ok(Arc::new(Self {
            node,
            owner,
            meta,
            peers,
            runtime: tokio::runtime::Handle::current(),
            probes: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            after_prepare: Mutex::new(None),
            requests,
            duration,
            outlet_wait,
        }))
    }

    pub(crate) fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/v1/workspaces", get(list).post(create))
            .route("/v1/workspaces/{owner}", get(lookup).delete(delete))
            .layer(DefaultBodyLimit::max(1024 * 1024))
            .with_state(self)
    }

    async fn execute(
        self: Arc<Self>,
        operation: &'static str,
        call: impl FnOnce(Arc<Self>) -> ApiResult + Send + 'static,
    ) -> Response {
        let started = Instant::now();
        let deadline = started + self.timeout();
        let this = self.clone();
        let work = bridge::spawn_blocking(move || {
            // A request queued behind blocked work must not start after its caller gave up.
            if Instant::now() >= deadline {
                return Err(ApiError::home(
                    "workspace operation expired before it started",
                ));
            }
            this.require_session()?;
            call(this)
        });
        let result = match tokio::time::timeout(self.timeout(), work).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ApiError::home(error)),
            Err(_) => Err(ApiError::home("workspace operation timed out")),
        };
        let response = match result {
            Ok((status, value)) => (status, Json(value)).into_response(),
            Err(error) => error.into_response(),
        };
        let status = response.status().as_u16().to_string();
        self.requests.with_label_values(&[operation, &status]).inc();
        self.duration
            .with_label_values(&[operation, &status])
            .observe(started.elapsed().as_secs_f64());
        response
    }

    fn probe_slot(&self, home: &str) -> Result<OwnedSemaphorePermit, ApiError> {
        let mut probes = self
            .probes
            .lock()
            .map_err(|_| ApiError::home("probe capacity lock poisoned"))?;
        probes.retain(|_, semaphore| semaphore.strong_count() != 0);
        let capacity = probes.get(home).and_then(Weak::upgrade).unwrap_or_else(|| {
            let capacity = Arc::new(Semaphore::new(PROBES_PER_HOME));
            probes.insert(home.to_owned(), Arc::downgrade(&capacity));
            capacity
        });
        capacity
            .try_acquire_owned()
            .map_err(|_| ApiError::home("Home outlet probe capacity exhausted"))
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.node.config.timeout_ms)
    }

    fn require_session(&self) -> Result<(), ApiError> {
        let reply = self
            .meta
            .workspace_node(&self.node.config.id)
            .map_err(ApiError::meta)?;
        if !reply.found
            || reply
                .node
                .as_ref()
                .is_none_or(|node| node.session_id != self.node.session_id)
        {
            return Err(ApiError::unavailable(
                "meta_unavailable",
                "Node has no current Meta session",
            ));
        }
        Ok(())
    }

    fn node_info(&self, home: &str) -> Result<LookupNodeReply, ApiError> {
        self.meta.workspace_node(home).map_err(ApiError::meta)
    }

    fn channel(&self, home: &str, info: &LookupNodeReply) -> Result<Channel, ApiError> {
        self.channel_until(home, info, Instant::now() + self.timeout())
    }

    fn channel_until(
        &self,
        home: &str,
        info: &LookupNodeReply,
        deadline: Instant,
    ) -> Result<Channel, ApiError> {
        let endpoint = info
            .node
            .as_ref()
            .filter(|_| info.found)
            .and_then(|node| node.endpoint.as_ref())
            .ok_or_else(|| ApiError::home("Home has no current Meta session"))?;
        bridge::block_on(&self.runtime, async {
            tokio::time::timeout_at(
                deadline.into(),
                self.peers
                    .channel(home, info.lease_epoch, &endpoint.grpc_addr),
            )
            .await
            .map_err(ApiError::home)?
            .map_err(ApiError::home)
        })
    }

    fn home_defaults(&self, home: &str, info: &LookupNodeReply) -> Result<(u32, u32), ApiError> {
        if home == self.node.config.id {
            return Ok(self.owner.workspace_defaults());
        }
        let mut client = OwnerControlClient::new(self.channel(home, info)?);
        let reply = bridge::block_on(&self.runtime, async {
            tokio::time::timeout(
                self.timeout(),
                client.workspace_defaults(WorkspaceDefaultsRequest {}),
            )
            .await
            .map_err(ApiError::home)?
            .map_err(ApiError::home)
        })?
        .into_inner();
        Ok((reply.uid, reply.gid))
    }

    fn prepare(
        &self,
        reservation: &RootReservation,
        request: &CreateWorkspace,
    ) -> Result<bool, ApiError> {
        let result = if reservation.home_node_id == self.node.config.id {
            let (uid, gid) = self.owner.workspace_defaults();
            let params = RootCreateParams {
                name: request.owner.as_bytes().to_vec(),
                uid: request.uid.unwrap_or(uid),
                gid: request.gid.unwrap_or(gid),
                mode: request.permissions()?,
            };
            self.owner
                .peer_executor()
                .map_err(ApiError::home)?
                .prepare_owner_root(&self.node.config.id, reservation, &params)
        } else {
            let info = self.node_info(&reservation.home_node_id)?;
            let mut client =
                OwnerControlClient::new(self.channel(&reservation.home_node_id, &info)?);
            let request = PrepareOwnerRootRequest {
                reservation: Some(reservation_wire(reservation)),
                name: request.owner.as_bytes().to_vec(),
                uid: request.uid,
                gid: request.gid,
                mode: request.permissions()?,
            };
            bridge::block_on(&self.runtime, async {
                Ok::<_, ApiError>(
                    tokio::time::timeout(self.timeout(), client.prepare_owner_root(request))
                        .await
                        .map_err(ApiError::home)?
                        .map(|_| ())
                        .map_err(afs_transport::grpc::error_status::status_to_error),
                )
            })?
        };
        match result {
            Ok(()) => Ok(true),
            Err(error) if error.code() == afs_error::IO_ALREADY_EXISTS => Ok(false),
            Err(error) => Err(prepare_error(error)),
        }
    }

    fn attributes(
        &self,
        location: &RootLocation,
        info: &LookupNodeReply,
        deadline: Instant,
    ) -> Result<FileAttributes, ApiError> {
        if !info.found {
            return Err(ApiError::home("Home has no current Meta session"));
        }
        if location.home_node_id == self.node.config.id {
            if Instant::now() >= deadline {
                return Err(ApiError::home("workspace attribute budget exhausted"));
            }
            let owner = self.owner.clone();
            let location = location.clone();
            let work = bridge::spawn_blocking_on(&self.runtime, move || {
                owner.workspace_attributes(&location)
            });
            return bridge::block_on(&self.runtime, async {
                tokio::time::timeout_at(deadline.into(), work)
                    .await
                    .map_err(ApiError::home)?
                    .map_err(ApiError::home)?
                    .map_err(ApiError::attribute)
            });
        }
        for attempt in 0..2 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ApiError::home("workspace attribute budget exhausted"));
            }
            let grant = self
                .meta
                .workspace_root_grant(
                    &location.id,
                    crate::node::vfs::ownerfs::root::RootRight::Lookup,
                    remaining,
                )
                .map_err(ApiError::home)?;
            if grant.epoch != location.epoch || grant.home_node_id != location.home_node_id {
                return Err(ApiError::home("workspace epoch changed"));
            }
            let mut client = afs_protocol::node_data::owner_files_client::OwnerFilesClient::new(
                self.channel_until(&location.home_node_id, info, deadline)?,
            );
            let request = afs_protocol::node_data::OwnerLookupRequest {
                access: Some(crate::node::rpc::peer::root_access(&grant)),
                path: Vec::new(),
                expected_parent_identity: None,
            };
            let rpc_deadline = deadline.min(
                Instant::now()
                    + Duration::from_millis((self.node.config.timeout_ms / 4).clamp(1, 1000)),
            );
            let result = bridge::block_on(&self.runtime, async {
                tokio::time::timeout_at(rpc_deadline.into(), client.lookup(request))
                    .await
                    .map_err(|_| {
                        Error::coded(
                            afs_error::IO_TIMEOUT,
                            "workspace attribute lookup timed out",
                        )
                    })?
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            });
            match result {
                Ok(reply) => {
                    return Ok(crate::node::rpc::peer::owner_entry(
                        &grant,
                        reply.into_inner().attr,
                    )
                    .map_err(ApiError::home)?
                    .attributes);
                }
                Err(error)
                    if attempt == 0
                        && matches!(
                            error.code(),
                            afs_error::NODE_OWNER_INVALID_GRANT
                                | afs_error::NODE_OWNER_GRANT_UNAVAILABLE
                                | afs_error::NODE_OWNER_STALE_ACCESS
                        ) =>
                {
                    continue;
                }
                Err(error) => return Err(ApiError::attribute(error)),
            }
        }
        Err(ApiError::home("workspace attribute lookup did not recover"))
    }

    fn create(&self, request: CreateWorkspace) -> ApiResult {
        validate_owner(&request.owner)?;
        let mode = request.permissions()?;
        if request.uid == Some(u32::MAX) || request.gid == Some(u32::MAX) {
            return Err(ApiError::invalid("uid/gid must be below 4294967295"));
        }
        let id = root_id_from_name(OsStr::new(&request.owner)).map_err(ApiError::invalid_error)?;
        let mut location = self.meta.lookup_root(&id).map_err(ApiError::meta)?;
        let mut created = false;
        if location.is_none() {
            match self.owner.reserve_workspace(OsStr::new(&request.owner)) {
                Ok(reservation) => match self.prepare(&reservation, &request) {
                    Ok(was_created) => {
                        created = was_created;
                        #[cfg(test)]
                        if let Some(call) = self.after_prepare.lock().unwrap().take() {
                            call();
                        }
                        location = self.meta.lookup_root(&id).map_err(ApiError::meta)?;
                        if !location.as_ref().is_some_and(|location| {
                            location.epoch == reservation.epoch
                                && location.home_node_id == reservation.home_node_id
                        }) {
                            return Err(ApiError::conflict("workspace changed after preparation"));
                        }
                    }
                    Err(error) => {
                        if error.status == StatusCode::CONFLICT {
                            return Err(error);
                        }
                        let _ = self.meta.abort_root(&reservation);
                        location = self.meta.lookup_root(&id).map_err(ApiError::meta)?;
                        if !location.as_ref().is_some_and(|location| {
                            location.epoch == reservation.epoch
                                && location.home_node_id == reservation.home_node_id
                        }) {
                            return Err(error);
                        }
                        created = true;
                    }
                },
                Err(error) if error.code() == afs_error::META_OWNER_ROOT_EXISTS => {
                    location = self.meta.lookup_root(&id).map_err(ApiError::meta)?;
                }
                Err(error) => {
                    return Err(ApiError::unavailable(
                        if error.code() == afs_error::META_OWNER_ROOT_BUSY {
                            "claim_conflict"
                        } else if error.code() == afs_error::META_OWNER_HOME_UNAVAILABLE {
                            "no_candidate"
                        } else {
                            "meta_unavailable"
                        },
                        error,
                    ));
                }
            }
        }
        let location = location.ok_or_else(|| ApiError::home("workspace was not activated"))?;
        let info = self.node_info(&location.home_node_id)?;
        let defaults = if request.uid.is_none() || request.gid.is_none() {
            self.home_defaults(&location.home_node_id, &info)?
        } else {
            (0, 0)
        };
        let attributes = self.attributes(&location, &info, Instant::now() + self.timeout())?;
        if attributes.uid != request.uid.unwrap_or(defaults.0)
            || attributes.gid != request.gid.unwrap_or(defaults.1)
            || attributes.mode & 0o7777 != mode
        {
            return Err(ApiError::conflict(
                "workspace uid/gid/mode differ from request",
            ));
        }
        let _timer = self.outlet_wait.start_timer();
        self.wait_outlet(&request.owner, &location, &info, &attributes)?;
        let mut value = self.record(&request.owner, &location, &info, Some(&attributes));
        value["created"] = json!(created);
        Ok((
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            value,
        ))
    }

    fn record(
        &self,
        owner: &str,
        location: &RootLocation,
        info: &LookupNodeReply,
        attributes: Option<&FileAttributes>,
    ) -> Value {
        let node_ip = info
            .node
            .as_ref()
            .filter(|_| info.found)
            .and_then(|node| node.endpoint.as_ref())
            .and_then(|endpoint| endpoint_host(&endpoint.grpc_addr));
        json!({"owner": owner, "epoch": location.epoch, "node_id": location.home_node_id, "node_ip": node_ip,
            "uid": attributes.map(|a| a.uid), "gid": attributes.map(|a| a.gid), "mode": attributes.map(|a| format!("{:04o}", a.mode & 0o7777)),
            // #85 keeps `state` only for client compatibility; the ADX resolver accepts only "ready".
            "state": "ready", "access_path": self.public_path(owner)})
    }

    fn public_path(&self, owner: &str) -> PathBuf {
        self.node
            .config
            .outlet
            .as_ref()
            .map(|outlet| outlet.public_root.join(owner))
            .unwrap_or_default()
    }

    fn lookup(&self, owner: String) -> ApiResult {
        validate_owner(&owner)?;
        let id = root_id_from_name(OsStr::new(&owner)).map_err(ApiError::invalid_error)?;
        let location = self
            .meta
            .lookup_root(&id)
            .map_err(ApiError::meta)?
            .ok_or_else(ApiError::missing)?;
        let info = self.node_info(&location.home_node_id)?;
        let attributes = match self.attributes(&location, &info, Instant::now() + self.timeout()) {
            Ok(attributes) => Some(attributes),
            Err(error) if error.code == "claim_conflict" => return Err(ApiError::missing()),
            Err(_) => None,
        };
        Ok((
            StatusCode::OK,
            self.record(&owner, &location, &info, attributes.as_ref()),
        ))
    }

    fn list(&self, filter: Option<String>, deadline: Instant) -> ApiResult {
        let filter = filter
            .map(|ip| {
                ip.parse::<std::net::IpAddr>()
                    .map_err(|_| ApiError::invalid("node_ip must be an IP address"))
            })
            .transpose()?;
        let snapshot = self
            .meta
            .list_active_outlet_roots(None)
            .map_err(ApiError::meta)?;
        let mut groups: BTreeMap<String, Vec<(String, RootLocation)>> = BTreeMap::new();
        for root in snapshot.roots {
            let Ok(name) = root_name_from_id(&root.root_id) else {
                continue;
            };
            let Some(owner) = name.to_str().filter(|owner| validate_owner(owner).is_ok()) else {
                continue;
            };
            let location = RootLocation {
                id: root.root_id,
                epoch: root.root_epoch,
                home_node_id: root.home_node_id.clone(),
                home_session_id: String::new(),
            };
            groups
                .entry(root.home_node_id)
                .or_default()
                .push((owner.to_owned(), location));
        }
        let mut items = Vec::new();
        let mut complete = true;
        for (home, roots) in groups {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let details = if remaining.is_zero() {
                None
            } else {
                self.meta.workspace_node_with_timeout(&home, remaining).ok()
            };
            if details.is_none() && filter.is_some() {
                complete = false;
            }
            let info = details.unwrap_or_default();
            let ip = info
                .node
                .as_ref()
                .filter(|_| info.found)
                .and_then(|node| node.endpoint.as_ref())
                .and_then(|endpoint| endpoint_host(&endpoint.grpc_addr))
                .and_then(|ip| ip.parse::<std::net::IpAddr>().ok());
            if filter.is_some_and(|filter| ip != Some(filter)) {
                if ip.is_none() {
                    complete = false;
                }
                continue;
            }
            let mut available = info.found;
            for (owner, location) in roots {
                let attributes = if available && Instant::now() < deadline {
                    match self.attributes(&location, &info, deadline) {
                        Ok(attributes) => Some(attributes),
                        Err(error) if error.code == "claim_conflict" => None,
                        Err(_) => {
                            available = false;
                            None
                        }
                    }
                } else {
                    None
                };
                items.push(self.record(&owner, &location, &info, attributes.as_ref()));
            }
        }
        Ok((
            StatusCode::OK,
            json!({"total": items.len(), "items": items, "complete": complete}),
        ))
    }

    /// Deletes the generation that Meta reports now; Home rejects any other epoch.
    /// An optional `epoch` stops a late retry from deleting a recreated workspace.
    fn delete(&self, owner: String, force: bool, epoch: Option<u64>) -> ApiResult {
        validate_owner(&owner)?;
        let id = root_id_from_name(OsStr::new(&owner)).map_err(ApiError::invalid_error)?;
        let Some(location) = self.meta.lookup_root(&id).map_err(ApiError::meta)? else {
            return if force {
                Err(ApiError::missing())
            } else {
                Ok((StatusCode::OK, json!({"owner": owner})))
            };
        };
        if epoch.is_some_and(|epoch| epoch != location.epoch) {
            return Err(ApiError::operation(Error::coded(
                afs_error::NODE_OWNER_STALE_HANDLE,
                "workspace epoch differs from the delete request",
            )));
        }
        let mut info = self.node_info(&location.home_node_id)?;
        let data_lost = if force && !info.found {
            match self.meta.force_deactivate_workspace(&location) {
                Ok(()) => true,
                Err(error) => {
                    if self.meta.lookup_root(&id).map_err(ApiError::meta)?.as_ref()
                        != Some(&location)
                    {
                        return Err(ApiError::home(error));
                    }
                    // The Home came back: delete through it with its current address.
                    info = self.node_info(&location.home_node_id)?;
                    if !info.found {
                        return Err(ApiError::meta(error));
                    }
                    false
                }
            }
        } else {
            if !info.found {
                return Err(ApiError::home("Home has no current Meta session"));
            }
            false
        };
        if !data_lost {
            if location.home_node_id == self.node.config.id {
                self.owner
                    .delete_workspace(&location)
                    .map_err(ApiError::operation)?;
            } else {
                let mut client =
                    OwnerControlClient::new(self.channel(&location.home_node_id, &info)?);
                bridge::block_on(&self.runtime, async {
                    tokio::time::timeout(
                        self.timeout(),
                        client.delete_workspace(
                            afs_protocol::node_control::DeleteWorkspaceRequest {
                                root_id: location.id.0.clone(),
                                expected_epoch: location.epoch,
                                requester_session_id: self.node.session_id.clone(),
                            },
                        ),
                    )
                    .await
                    .map_err(ApiError::home)?
                    .map_err(afs_transport::grpc::error_status::status_to_error)
                    .map_err(ApiError::operation)
                })?;
            }
        }
        let mut result = json!({"owner": owner});
        if force {
            result["data_lost"] = json!(data_lost);
        }
        Ok((StatusCode::OK, result))
    }

    fn wait_outlet(
        &self,
        owner: &str,
        location: &RootLocation,
        info: &LookupNodeReply,
        attributes: &FileAttributes,
    ) -> Result<(), ApiError> {
        let _probe = self.probe_slot(&location.home_node_id)?;
        let deadline = Instant::now() + self.timeout();
        loop {
            if self.outlet_ready(owner, location, info, attributes).is_ok() {
                if self
                    .meta
                    .lookup_root(&location.id)
                    .map_err(ApiError::meta)?
                    .as_ref()
                    == Some(location)
                {
                    return Ok(());
                }
                return Err(ApiError::home(
                    "workspace changed during outlet readiness check",
                ));
            }
            if Instant::now() >= deadline {
                return Err(ApiError::home(
                    "outlet did not become accessible before deadline",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn outlet_ready(
        &self,
        owner: &str,
        location: &RootLocation,
        info: &LookupNodeReply,
        attributes: &FileAttributes,
    ) -> std::io::Result<()> {
        let outlet = self
            .node
            .config
            .outlet
            .as_ref()
            .ok_or_else(|| std::io::Error::other("outlet is disabled"))?;
        let expected = PathBuf::from(mirror::target_path(
            &outlet.import_root.to_string_lossy(),
            &OutletRoot {
                root_id: location.id.clone(),
                root_epoch: location.epoch,
                home_node_id: location.home_node_id.clone(),
                home_nfs_addr: String::new(),
            },
        ));
        if std::fs::read_link(self.public_path(owner))? != expected {
            return Err(std::io::Error::other(
                "outlet points at a different root generation",
            ));
        }
        let mounts = topology::parse(&std::fs::read("/proc/self/mountinfo")?);
        let point = imports::mount_point(&outlet.import_root, &location.home_node_id);
        let mount = mounts
            .iter()
            .rev()
            .find(|mount| mount.mount_point == point.as_os_str().as_bytes())
            .ok_or_else(|| std::io::Error::other("outlet import is not mounted"))?;
        if location.home_node_id == self.node.config.id {
            let a = std::fs::metadata(&point)?;
            let b = std::fs::metadata(self.node.config.data_dir.join("ownerfs"))?;
            if (a.dev(), a.ino()) != (b.dev(), b.ino()) {
                return Err(std::io::Error::other(
                    "local import is not the OwnerFs storage root",
                ));
            }
        } else {
            let address = info
                .node
                .as_ref()
                .and_then(|node| node.endpoint.as_ref())
                .map(|endpoint| endpoint.nfs_addr.as_str())
                .unwrap_or_default();
            if !matches!(mount.fs_type.as_str(), "nfs" | "nfs4")
                || !imports::mount_source_host(&mount.source)
                    .is_some_and(|host| source_host_matches(address, host))
                || imports::mount_source_path(&mount.source).map(|p| p.trim_end_matches('/'))
                    != Some(
                        outlet
                            .users_root
                            .to_str()
                            .unwrap_or_default()
                            .trim_end_matches('/'),
                    )
            {
                return Err(std::io::Error::other(
                    "outlet import source differs from Home export",
                ));
            }
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(expected)?;
        let metadata = directory.metadata()?;
        if (metadata.uid(), metadata.gid(), metadata.mode() & 0o7777)
            != (attributes.uid, attributes.gid, attributes.mode & 0o7777)
        {
            return Err(std::io::Error::other("outlet attributes differ from Home"));
        }
        let held = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        std::fs::read_dir(&held)?;
        let probe = held.join(format!(".afs-ready-{}", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&probe)?;
        let written = file.write_all(b"ready");
        let removed = std::fs::remove_file(probe);
        written?;
        removed
    }
}

impl ApiError {
    fn invalid_error(error: Error) -> Self {
        Self::invalid(error.to_string())
    }
}

fn endpoint_host(endpoint: &str) -> Option<String> {
    let uri = endpoint
        .parse::<axum::http::Uri>()
        .ok()
        .or_else(|| format!("http://{endpoint}").parse().ok())?;
    uri.host()
        .map(|host| host.trim_matches(['[', ']']).to_owned())
}

fn source_host_matches(expected: &str, actual: &str) -> bool {
    use std::net::{IpAddr, ToSocketAddrs};
    let expected = imports::normalize_host(expected);
    let actual = imports::normalize_host(actual);
    if expected.eq_ignore_ascii_case(actual) {
        return true;
    }
    let Ok(actual) = actual.parse::<IpAddr>() else {
        return false;
    };
    if let Ok(expected) = expected.parse::<IpAddr>() {
        return expected == actual;
    }
    (expected, 2049)
        .to_socket_addrs()
        .is_ok_and(|mut addresses| addresses.any(|address| address.ip() == actual))
}

fn prepare_error(error: Error) -> ApiError {
    if error.code() == afs_error::IO_INVALID {
        ApiError::conflict(error.to_string())
    } else {
        ApiError::operation(error)
    }
}

fn validate_owner(owner: &str) -> Result<(), ApiError> {
    let bytes = owner.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_alphanumeric()
        || bytes
            .iter()
            .any(|b| !b.is_ascii_alphanumeric() && !b"._-".contains(b))
    {
        return Err(ApiError::invalid("invalid workspace owner"));
    }
    Ok(())
}

#[derive(Deserialize)]
struct CreateWorkspace {
    owner: String,
    uid: Option<u32>,
    gid: Option<u32>,
    mode: Option<String>,
}
impl CreateWorkspace {
    fn permissions(&self) -> Result<u32, ApiError> {
        let mode = self.mode.as_deref().unwrap_or("0700");
        if mode.is_empty() || !mode.bytes().all(|b| (b'0'..=b'7').contains(&b)) {
            return Err(ApiError::invalid("mode must contain octal digits"));
        }
        let value = u32::from_str_radix(mode, 8).map_err(|_| ApiError::invalid("invalid mode"))?;
        if value > 0o7777 {
            return Err(ApiError::invalid("mode exceeds 07777"));
        }
        Ok(value)
    }
}

#[derive(Deserialize, Default)]
struct WorkspaceQuery {
    node_ip: Option<String>,
    force: Option<String>,
    epoch: Option<u64>,
}

async fn health(State(api): State<Arc<WorkspaceApi>>) -> Response {
    let node_id = api.node.config.id.clone();
    let response = api
        .execute("health", |api| {
            let info = api.node_info(&api.node.config.id)?;
            let ip = info
                .node
                .as_ref()
                .and_then(|node| node.endpoint.as_ref())
                .and_then(|endpoint| endpoint_host(&endpoint.grpc_addr));
            Ok((
                StatusCode::OK,
                json!({"status": "ok", "node_id": api.node.config.id, "node_ip": ip, "meta": "ok"}),
            ))
        })
        .await;
    if response.status() != StatusCode::SERVICE_UNAVAILABLE {
        return response;
    }
    (StatusCode::SERVICE_UNAVAILABLE, [(RETRY_AFTER, "1")], Json(json!({"status": "unavailable", "node_id": node_id, "node_ip": null, "meta": "unavailable"}))).into_response()
}
async fn create(
    State(api): State<Arc<WorkspaceApi>>,
    body: Result<Json<CreateWorkspace>, JsonRejection>,
) -> Response {
    api.execute("create", move |api| {
        api.create(body.map_err(|e| ApiError::invalid(e.body_text()))?.0)
    })
    .await
}
async fn lookup(
    State(api): State<Arc<WorkspaceApi>>,
    owner: Result<Path<String>, PathRejection>,
) -> Response {
    api.execute("lookup", move |api| {
        api.lookup(owner.map_err(|e| ApiError::invalid(e.body_text()))?.0)
    })
    .await
}
async fn list(
    State(api): State<Arc<WorkspaceApi>>,
    query: Result<Query<WorkspaceQuery>, QueryRejection>,
) -> Response {
    let deadline = Instant::now() + api.timeout() / 2;
    api.execute("list", move |api| {
        api.list(
            query
                .map_err(|e| ApiError::invalid(e.body_text()))?
                .node_ip
                .clone(),
            deadline,
        )
    })
    .await
}
async fn delete(
    State(api): State<Arc<WorkspaceApi>>,
    owner: Result<Path<String>, PathRejection>,
    query: Result<Query<WorkspaceQuery>, QueryRejection>,
) -> Response {
    let operation = if query
        .as_ref()
        .is_ok_and(|query| matches!(query.force.as_deref(), Some("true" | "1")))
    {
        "force_delete"
    } else {
        "delete"
    };
    api.execute(operation, move |api| {
        let query = query.map_err(|e| ApiError::invalid(e.body_text()))?;
        let force = match query.force.as_deref() {
            None | Some("false" | "0") => false,
            Some("true" | "1") => true,
            _ => return Err(ApiError::invalid("force must be true, false, 1 or 0")),
        };
        if query.epoch == Some(0) {
            return Err(ApiError::invalid("epoch must be positive"));
        }
        api.delete(
            owner.map_err(|e| ApiError::invalid(e.body_text()))?.0,
            force,
            query.epoch,
        )
    })
    .await
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    #[test]
    fn owner_mode_and_endpoint_boundaries() {
        for name in ["a", "Alice_2.test-3", &"a".repeat(64)] {
            assert!(validate_owner(name).is_ok());
        }
        for name in [
            "",
            ".",
            "..",
            ".hidden",
            "a/b",
            "a\0b",
            "中文",
            &"a".repeat(65),
        ] {
            assert!(validate_owner(name).is_err());
        }
        for (mode, valid) in [
            ("0700", true),
            ("7777", true),
            ("0000", true),
            ("", false),
            ("0788", false),
            ("-1", false),
            ("10000", false),
        ] {
            assert_eq!(
                CreateWorkspace {
                    owner: "a".into(),
                    uid: None,
                    gid: None,
                    mode: Some(mode.into())
                }
                .permissions()
                .is_ok(),
                valid
            );
        }
        assert_eq!(endpoint_host("http://[::1]:18888"), Some("::1".into()));
        assert_eq!(
            endpoint_host("http://10.0.0.1:18888"),
            Some("10.0.0.1".into())
        );
        assert!(source_host_matches("[::1]", "0:0:0:0:0:0:0:1"));
        assert!(!source_host_matches("127.0.0.1", "127.0.0.2"));
    }
}

#[cfg(test)]
mod tests;

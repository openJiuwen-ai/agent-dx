use super::NodeProxy;
use std::collections::HashMap;
use std::net::IpAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("adx.node.v1");
}
use proto::{
    node_proxy_service_server::{NodeProxyService, NodeProxyServiceServer},
    update_binding_request::Binding,
    UpdateBindingRequest, UpdateBindingResponse,
};

/// One shared handler per local proxy, also usable by an embedded Node Manager.
/// Tombstones survive for this process lifetime; startup must reconcile before
/// opening data-plane admission. A fresh proxy UUID fences delayed RPCs after
/// restart; Node Manager supplies the complete catalog before admission opens.
pub struct BindingService {
    proxy: Arc<NodeProxy>,
    session: String,
    state: Mutex<Bindings>,
}
#[derive(Default)]
struct Bindings {
    epoch: u64,
    owner: String,
    ready: bool,
    versions: HashMap<String, UpdateBindingRequest>,
    snapshot: Option<proto::BindingSnapshot>,
}
impl BindingService {
    pub fn new(proxy: Arc<NodeProxy>) -> Self {
        proxy.set_bindings_ready(false);
        Self {
            proxy,
            session: uuid::Uuid::new_v4().to_string(),
            state: Mutex::default(),
        }
    }
    fn state_message(&self, s: &Bindings) -> proto::BindingState {
        proto::BindingState {
            proxy_session_id: self.session.clone(),
            sync_epoch: s.epoch,
            ready: s.ready,
            controller_session_id: s.owner.clone(),
        }
    }
    pub async fn inspect(&self) -> proto::BindingState {
        self.state_message(&*self.state.lock().await)
    }
    pub async fn begin(
        &self,
        expected: proto::BindingState,
    ) -> Result<proto::BindingState, Status> {
        let mut s = self.state.lock().await;
        if expected.proxy_session_id != self.session || expected.sync_epoch != s.epoch {
            return Err(Status::failed_precondition(
                "proxy synchronization session changed",
            ));
        }
        s.epoch = s
            .epoch
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("sync epoch exhausted"))?;
        s.owner = expected.controller_session_id;
        s.ready = false;
        s.snapshot = None;
        self.proxy.clear_bindings();
        Ok(self.state_message(&s))
    }
    pub async fn replace(
        &self,
        request: proto::BindingSnapshot,
    ) -> Result<proto::BindingState, Status> {
        let mut s = self.state.lock().await;
        if request.proxy_session_id != self.session || request.sync_epoch != s.epoch || s.epoch == 0
        {
            return Err(Status::failed_precondition(
                "proxy synchronization session changed",
            ));
        }
        if s.ready {
            return if s.snapshot.as_ref() == Some(&request) {
                Ok(self.state_message(&s))
            } else {
                Err(Status::failed_precondition(
                    "full snapshot already completed",
                ))
            };
        }
        let mut next = HashMap::new();
        for binding in &request.bindings {
            validate(binding)?;
            if binding.proxy_session_id != self.session
                || binding.sync_epoch != s.epoch
                || next
                    .insert(binding.instance_id.clone(), binding.clone())
                    .is_some()
            {
                return Err(Status::invalid_argument(
                    "duplicate binding or session mismatch",
                ));
            }
            check_version(s.versions.get(&binding.instance_id), binding)?;
        }
        // Preserve negative knowledge for identities omitted from this authoritative snapshot.
        for (id, old) in &s.versions {
            next.entry(id.clone())
                .or_insert_with(|| UpdateBindingRequest {
                    instance_id: id.clone(),
                    ownership_generation: old.ownership_generation,
                    binding_revision: u64::MAX,
                    binding: Some(Binding::Retired(proto::Retired {})),
                    proxy_session_id: self.session.clone(),
                    sync_epoch: s.epoch,
                });
        }
        for binding in next.values() {
            if let Some(Binding::Active(target)) = &binding.binding {
                self.proxy
                    .activate_route(
                        binding.instance_id.clone(),
                        target.runtime_id.clone(),
                        target.ip.parse().unwrap(),
                    )
                    .await;
            }
        }
        s.versions = next;
        s.snapshot = Some(request);
        s.ready = true;
        self.proxy.set_bindings_ready(true);
        Ok(self.state_message(&s))
    }
    pub async fn apply(
        &self,
        request: UpdateBindingRequest,
    ) -> Result<UpdateBindingResponse, Status> {
        validate(&request)?;
        let mut s = self.state.lock().await;
        if !s.ready || request.proxy_session_id != self.session || request.sync_epoch != s.epoch {
            return Err(Status::failed_precondition(
                "complete binding synchronization required",
            ));
        }
        check_version(s.versions.get(&request.instance_id), &request)?;
        if let Some(previous) = s.versions.get(&request.instance_id) {
            if previous.ownership_generation == request.ownership_generation
                && previous.binding_revision == request.binding_revision
            {
                return Ok(ack(&request));
            }
            if let Some(Binding::Active(old)) = &previous.binding {
                self.proxy
                    .retire_route(request.instance_id.clone(), old.runtime_id.clone())
                    .await;
            }
        }
        if let Some(Binding::Active(target)) = &request.binding {
            self.proxy
                .activate_route(
                    request.instance_id.clone(),
                    target.runtime_id.clone(),
                    target.ip.parse().unwrap(),
                )
                .await;
        }
        let response = ack(&request);
        s.versions.insert(request.instance_id.clone(), request);
        Ok(response)
    }
}
#[allow(clippy::result_large_err)]
fn validate(r: &UpdateBindingRequest) -> Result<(), Status> {
    if r.instance_id.trim().is_empty() || r.ownership_generation == 0 || r.binding_revision == 0 {
        return Err(Status::invalid_argument("versioned binding required"));
    }
    match &r.binding {
        Some(Binding::Active(t))
            if !t.runtime_id.trim().is_empty() && t.ip.parse::<IpAddr>().is_ok() =>
        {
            Ok(())
        }
        Some(Binding::Retired(_)) => Ok(()),
        _ => Err(Status::invalid_argument("valid binding target required")),
    }
}
#[allow(clippy::result_large_err)]
fn check_version(
    old: Option<&UpdateBindingRequest>,
    new: &UpdateBindingRequest,
) -> Result<(), Status> {
    if let Some(old) = old {
        let a = (old.ownership_generation, old.binding_revision);
        let b = (new.ownership_generation, new.binding_revision);
        if b < a || (b == a && old.binding != new.binding) {
            return Err(Status::failed_precondition(
                "stale or conflicting binding version",
            ));
        }
    }
    Ok(())
}
fn ack(request: &UpdateBindingRequest) -> UpdateBindingResponse {
    UpdateBindingResponse {
        ownership_generation: request.ownership_generation,
        binding_revision: request.binding_revision,
    }
}
#[tonic::async_trait]
impl NodeProxyService for BindingService {
    async fn get_binding_state(
        &self,
        _: Request<proto::GetBindingStateRequest>,
    ) -> Result<Response<proto::BindingState>, Status> {
        Ok(Response::new(self.inspect().await))
    }
    async fn begin_bindings(
        &self,
        r: Request<proto::BindingState>,
    ) -> Result<Response<proto::BindingState>, Status> {
        Ok(Response::new(self.begin(r.into_inner()).await?))
    }
    async fn replace_bindings(
        &self,
        r: Request<proto::BindingSnapshot>,
    ) -> Result<Response<proto::BindingState>, Status> {
        Ok(Response::new(self.replace(r.into_inner()).await?))
    }

    async fn update_binding(
        &self,
        request: Request<UpdateBindingRequest>,
    ) -> Result<Response<UpdateBindingResponse>, Status> {
        Ok(Response::new(self.apply(request.into_inner()).await?))
    }
}

pub async fn bind_route_control(uds_path: &str) -> std::io::Result<UnixListener> {
    let path = Path::new(uds_path);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // Do not unlink an active socket owned by another process.
    if tokio::net::UnixStream::connect(path).await.is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "route control socket is active",
        ));
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660)).await?;
    Ok(listener)
}

pub async fn serve_route_control(
    proxy: Arc<NodeProxy>,
    listener: UnixListener,
) -> std::io::Result<()> {
    tonic::transport::Server::builder()
        .add_service(
            NodeProxyServiceServer::new(BindingService::new(proxy))
                .max_decoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_incoming(UnixListenerStream::new(listener))
        .await
        .map_err(std::io::Error::other)
}

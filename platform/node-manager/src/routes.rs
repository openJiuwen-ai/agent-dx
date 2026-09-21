//! Versioned bindings and complete startup synchronization over a protected UDS.
use crate::Routes;
use adx_core::{Error, InstanceRecord, Result};
use adx_protocol::node_proxy::{
    self as pb, node_proxy_service_client::NodeProxyServiceClient, update_binding_request::Binding,
    Retired, RuntimeTarget, UpdateBindingRequest,
};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::UnixStream, sync::Mutex};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
#[derive(Default)]
struct Local {
    session: Option<pb::BindingState>,
    buffering: bool,
    initialized: bool,
    bindings: BTreeMap<String, UpdateBindingRequest>,
}
#[derive(Clone)]
pub struct UdsRoutes {
    path: PathBuf,
    timeout: Duration,
    local: Arc<Mutex<Local>>,
    owner: String,
}
impl UdsRoutes {
    pub fn new(path: PathBuf, timeout: Duration) -> Result<Self> {
        if path.as_os_str().is_empty() || timeout.is_zero() {
            return Err(Error::Invalid(
                "route socket and positive timeout required".into(),
            ));
        }
        Ok(Self {
            path,
            timeout,
            local: Arc::default(),
            owner: uuid::Uuid::new_v4().to_string(),
        })
    }
    async fn connect(&self) -> Result<NodeProxyServiceClient<Channel>> {
        let path = self.path.clone();
        let channel = tokio::time::timeout(
            self.timeout,
            Endpoint::from_static("http://node-proxy").connect_with_connector(service_fn(
                move |_| {
                    let path = path.clone();
                    async move { UnixStream::connect(path).await.map(TokioIo::new) }
                },
            )),
        )
        .await
        .map_err(|_| unavailable("connection deadline"))?
        .map_err(unavailable)?;
        Ok(NodeProxyServiceClient::new(channel).max_encoding_message_size(64 * 1024 * 1024))
    }
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut r = tonic::Request::new(message);
        r.set_timeout(self.timeout);
        r
    }
    async fn remote(
        &self,
        client: &mut NodeProxyServiceClient<Channel>,
    ) -> Result<pb::BindingState> {
        Ok(client
            .get_binding_state(self.request(pb::GetBindingStateRequest {}))
            .await
            .map_err(rpc_error)?
            .into_inner())
    }
    async fn full(
        &self,
        s: &mut Local,
        client: &mut NodeProxyServiceClient<Channel>,
    ) -> Result<()> {
        let session = s.session.clone().ok_or(Error::Conflict)?;
        let bindings = s
            .bindings
            .values()
            .cloned()
            .map(|mut r| {
                r.proxy_session_id = session.proxy_session_id.clone();
                r.sync_epoch = session.sync_epoch;
                r
            })
            .collect();
        let accepted = client
            .replace_bindings(self.request(pb::BindingSnapshot {
                proxy_session_id: session.proxy_session_id.clone(),
                sync_epoch: session.sync_epoch,
                bindings,
            }))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if accepted.proxy_session_id != session.proxy_session_id
            || accepted.sync_epoch != session.sync_epoch
            || !accepted.ready
        {
            return Err(Error::Conflict);
        }
        s.session = Some(accepted);
        Ok(())
    }
    async fn ensure(
        &self,
        s: &mut Local,
        client: &mut NodeProxyServiceClient<Channel>,
    ) -> Result<()> {
        if !s.initialized || s.buffering {
            return Err(unavailable("complete local catalog required"));
        }
        let mut remote = self.remote(client).await?;
        if let Some(old) = &s.session {
            if remote.proxy_session_id == old.proxy_session_id {
                if remote.sync_epoch != old.sync_epoch && remote.controller_session_id != self.owner
                {
                    return Err(Error::Conflict);
                }
                s.session = Some(remote.clone());
                if remote.ready {
                    return Ok(());
                }
                return self.full(s, client).await;
            }
        }
        remote.controller_session_id = self.owner.clone();
        s.session = Some(
            client
                .begin_bindings(self.request(remote))
                .await
                .map_err(rpc_error)?
                .into_inner(),
        );
        self.full(s, client).await
    }
    async fn send(&self, mut r: UpdateBindingRequest) -> Result<()> {
        let mut s = self.local.lock().await;
        if let Some(old) = s.bindings.get(&r.instance_id) {
            let a = (old.ownership_generation, old.binding_revision);
            let b = (r.ownership_generation, r.binding_revision);
            if b < a || (b == a && old.binding != r.binding) {
                return Err(Error::Conflict);
            }
        }
        if !s.initialized && !s.buffering {
            return Err(unavailable("binding catalog not initialized"));
        }
        s.bindings.insert(r.instance_id.clone(), r.clone());
        if s.buffering {
            return Ok(());
        }
        let mut client = self.connect().await?;
        self.ensure(&mut s, &mut client).await?;
        let session = s.session.as_ref().ok_or(Error::Conflict)?;
        r.proxy_session_id = session.proxy_session_id.clone();
        r.sync_epoch = session.sync_epoch;
        let accepted = client
            .update_binding(self.request(r.clone()))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if accepted.ownership_generation != r.ownership_generation
            || accepted.binding_revision != r.binding_revision
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    async fn apply(&self, r: &InstanceRecord, active: bool) -> Result<()> {
        let revision = r
            .revision
            .checked_mul(2)
            .and_then(|v| v.checked_add(u64::from(!active)))
            .ok_or(Error::Conflict)?;
        if revision == 0 || r.assignment.generation == 0 || r.spec.id.is_empty() {
            return Err(Error::Invalid("versioned binding required".into()));
        }
        let binding = if active {
            Binding::Active(RuntimeTarget {
                runtime_id: r.runtime_id.clone(),
                ip: r
                    .runtime_ip
                    .ok_or_else(|| Error::Invalid("runtime IP required".into()))?
                    .to_string(),
            })
        } else {
            Binding::Retired(Retired {})
        };
        self.send(UpdateBindingRequest {
            instance_id: r.spec.id.clone(),
            ownership_generation: r.assignment.generation,
            binding_revision: revision,
            binding: Some(binding),
            ..Default::default()
        })
        .await
    }
}
fn unavailable(e: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("Node Proxy: {e}"))
}
fn rpc_error(s: tonic::Status) -> Error {
    if s.code() == tonic::Code::FailedPrecondition {
        Error::Conflict
    } else {
        unavailable(s)
    }
}
#[async_trait]
impl Routes for UdsRoutes {
    async fn activity(&self, record: &InstanceRecord) -> Result<(String, u64, u64)> {
        let mut client = self.connect().await?;
        let result = client
            .get_instance_activity(self.request(pb::GetInstanceActivityRequest {
                instance_id: record.spec.id.clone(),
                runtime_id: record.runtime_id.clone(),
            }))
            .await
            .map_err(rpc_error)?
            .into_inner();
        Ok((
            result.proxy_session_id,
            result.activity_revision,
            result.active_streams,
        ))
    }

    async fn begin_reconcile(&self) -> Result<()> {
        let mut s = self.local.lock().await;
        let mut client = self.connect().await?;
        let mut remote = self.remote(&mut client).await?;
        if let Some(old) = &s.session {
            if old.proxy_session_id == remote.proxy_session_id
                && old.sync_epoch != remote.sync_epoch
                && remote.controller_session_id != self.owner
            {
                return Err(Error::Conflict);
            }
        }
        remote.controller_session_id = self.owner.clone();
        let session = client
            .begin_bindings(self.request(remote))
            .await
            .map_err(rpc_error)?
            .into_inner();
        s.session = Some(session);
        s.buffering = true;
        s.bindings.clear();
        Ok(())
    }
    async fn finish_reconcile(&self) -> Result<()> {
        let mut s = self.local.lock().await;
        let mut client = self.connect().await?;
        self.full(&mut s, &mut client).await?;
        s.buffering = false;
        s.initialized = true;
        Ok(())
    }
    async fn ensure_synced(&self) -> Result<()> {
        let mut s = self.local.lock().await;
        let mut client = self.connect().await?;
        self.ensure(&mut s, &mut client).await
    }
    async fn retire_orphan(&self, r: &crate::RuntimeObservation) -> Result<()> {
        self.send(UpdateBindingRequest {
            instance_id: r.instance_id.clone(),
            ownership_generation: r.generation,
            binding_revision: u64::MAX,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        })
        .await
    }
    async fn activate(&self, r: &InstanceRecord) -> Result<()> {
        self.apply(r, true).await
    }
    async fn retire(&self, r: &InstanceRecord) -> Result<()> {
        self.apply(r, false).await
    }
}

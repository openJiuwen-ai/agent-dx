use crate::common::protocol::ConnectTarget;
use crate::common::route::{DataPlaneAuthMode, DataPlaneSecurityMode, InstanceStatus, RouteInfo};
use crate::edge::route_store::{RouteChange, RouteStore};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccessKind {
    Direct,
    Tunnel,
    PortForwarding,
    Ssh,
}

impl FromStr for AccessKind {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "tunnel" => Ok(Self::Tunnel),
            "port-forwarding" | "port_forwarding" | "portforwarding" => Ok(Self::PortForwarding),
            "ssh" => Ok(Self::Ssh),
            _ => Err(()),
        }
    }
}

impl AccessKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Tunnel => "tunnel",
            Self::PortForwarding => "port-forwarding",
            Self::Ssh => "ssh",
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResolveError {
    #[error("route cache is not ready")]
    NotReady,
    #[error("sandbox route not found")]
    NotFound,
    #[error("sandbox instance is not connectable: code={code}, msg={msg}, err_code={err_code}, exit_code={exit_code}, type={kind}")]
    InstanceStatus {
        code: i32,
        msg: String,
        err_code: i32,
        exit_code: i32,
        kind: i32,
    },
    #[error("route does not contain a complete gateway endpoint")]
    MissingEndpoint,
    #[error("route point-get is temporarily unavailable: {0}")]
    Unavailable(String),
}

#[derive(Clone)]
pub struct EdgeRouteResolver {
    store: Arc<RouteStore>,
    point_getter: Option<Arc<dyn RoutePointGetter>>,
}

#[derive(Debug, Clone)]
pub struct RouteHandle {
    pub access_kind: AccessKind,
    pub node_proxy_address: String,
    pub tenant_id: String,
    pub tunnel_security_mode: DataPlaneSecurityMode,
    pub port_forward_auth_mode: DataPlaneAuthMode,
    pub target: ConnectTarget,
}

#[derive(Debug, Error)]
pub enum PointGetError {
    #[error("route backend point-get failed: {0}")]
    Backend(String),
    #[error("coalesced etcd point-get failed")]
    CoalescedFailure,
}

#[async_trait]
pub trait RoutePointGetter: Send + Sync {
    async fn point_get(&self, instance_id: &str) -> Result<Option<RouteInfo>, PointGetError>;
}

impl EdgeRouteResolver {
    pub fn new(store: Arc<RouteStore>) -> Self {
        Self {
            store,
            point_getter: None,
        }
    }

    pub fn with_point_getter(mut self, point_getter: Arc<dyn RoutePointGetter>) -> Self {
        self.point_getter = Some(point_getter);
        self
    }

    pub fn ready(&self) -> bool {
        self.store.ready()
    }

    pub fn cache_len(&self) -> usize {
        self.store.len()
    }

    pub fn watch_revision(&self) -> i64 {
        self.store.watch_revision()
    }

    pub fn watch_lag_seconds(&self) -> u64 {
        self.store.watch_lag_seconds()
    }

    pub fn point_get_total(&self) -> u64 {
        self.store.point_get_total()
    }

    pub fn subscribe_changes(&self) -> tokio::sync::broadcast::Receiver<RouteChange> {
        self.store.subscribe()
    }

    pub fn route_is_current(&self, handle: &RouteHandle) -> bool {
        self.store
            .get(&handle.target.instance_id)
            .is_some_and(|route| {
                is_connectable(&route.instance_status)
                    && route.node_proxy_address == handle.node_proxy_address
                    && route.sandbox_id == handle.target.workload_id
                    && route.sandbox_ip.parse().ok() == Some(handle.target.target_ip)
            })
    }

    pub async fn resolve(
        &self,
        instance_id: &str,
        target_port: u16,
        access_kind: AccessKind,
        request_id: impl Into<String>,
    ) -> Result<RouteHandle, ResolveError> {
        if !self.store.ready() {
            return Err(ResolveError::NotReady);
        }
        let route = match self.store.get(instance_id) {
            Some(route) => route,
            None => match &self.point_getter {
                Some(point_getter) => point_getter
                    .point_get(instance_id)
                    .await
                    .map_err(|error| ResolveError::Unavailable(error.to_string()))?
                    .ok_or(ResolveError::NotFound)?,
                None => return Err(ResolveError::NotFound),
            },
        };
        if !is_connectable(&route.instance_status) {
            return Err(ResolveError::InstanceStatus {
                code: route.instance_status.code,
                msg: route.instance_status.msg.clone(),
                err_code: route.instance_status.err_code,
                exit_code: route.instance_status.exit_code,
                kind: route.instance_status.kind,
            });
        }
        if route.node_proxy_address.is_empty() {
            return Err(ResolveError::MissingEndpoint);
        }
        let target = route
            .connect_target(target_port, request_id)
            .ok_or(ResolveError::MissingEndpoint)?;
        let port_forward_auth_mode = route.port_forward_auth_mode(target_port);
        Ok(RouteHandle {
            access_kind,
            node_proxy_address: route.node_proxy_address,
            tenant_id: route.tenant_id,
            tunnel_security_mode: route.tunnel_security_mode,
            port_forward_auth_mode,
            target,
        })
    }
}

// FunctionSystem's existing RUNNING status code is retained; this module does
// not create another state machine or rewrite status messages.
fn is_connectable(status: &InstanceStatus) -> bool {
    status.code == 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::route::RouteInfo;

    fn route(status: i32) -> RouteInfo {
        RouteInfo {
            instance_id: "i".into(),
            instance_status: InstanceStatus {
                code: status,
                msg: "failed".into(),
                ..Default::default()
            },
            tenant_id: "tenant-a".into(),
            sandbox_id: "s".into(),
            node_proxy_address: "node-proxy:8443".into(),
            sandbox_ip: "10.88.0.2".into(),
            tunnel_security_mode: DataPlaneSecurityMode::Inherit,
            port_forward_security_mode: DataPlaneSecurityMode::Inherit,
            port_forward_routes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn all_access_kinds_share_one_l4_target() {
        let store = Arc::new(RouteStore::new());
        store.put(route(3));
        store.set_ready(true);
        let resolver = EdgeRouteResolver::new(store);
        for kind in [
            AccessKind::Direct,
            AccessKind::Tunnel,
            AccessKind::PortForwarding,
            AccessKind::Ssh,
        ] {
            let handle = resolver.resolve("i", 22, kind, "req").await.unwrap();
            assert_eq!(handle.target.socket_addr().port(), 22);
            assert_eq!(handle.access_kind, kind);
        }
    }

    #[tokio::test]
    async fn preserves_instance_status_error() {
        let store = Arc::new(RouteStore::new());
        store.put(route(5));
        store.set_ready(true);
        let resolver = EdgeRouteResolver::new(store);
        assert!(matches!(
            resolver.resolve("i", 22, AccessKind::Ssh, "req").await,
            Err(ResolveError::InstanceStatus { code: 5, .. })
        ));
    }
}

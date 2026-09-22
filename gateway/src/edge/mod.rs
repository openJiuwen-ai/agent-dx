pub mod auth;
pub mod connector;
pub mod http_pool;
#[cfg(feature = "activity-client")]
pub mod master_routes;
pub mod path;
pub mod pool;
pub mod resolver;
pub mod reverse_proxy;
pub mod route_store;
#[cfg(feature = "etcd-watch")]
pub mod route_watch;
pub mod server;
#[cfg(feature = "activity-client")]
pub mod service;

pub use auth::{AuthError, EdgeAuthenticator};
pub use connector::DataPlaneL4Connector;
pub use http_pool::{BackendHttpPool, BackendHttpPoolConfig, BackendHttpPoolKey};
pub use pool::{H2ConnectionPool, H2PoolConfig};
pub use resolver::{AccessKind, EdgeRouteResolver, ResolveError, RouteHandle};
pub use reverse_proxy::{parse_proxy_routes, ProxyRoute, ReverseProxyConfig};
pub use route_store::{RouteChange, RouteStore};
#[cfg(feature = "etcd-watch")]
pub use route_watch::RouteWatcher;
pub use server::{
    parse_static_routes, CommandWatchConfig, EdgeFrontend, EdgeOpenError, IngressSecurity,
    StaticRoute,
};
#[cfg(feature = "activity-client")]
pub use service::EdgeFrontendService;

#[cfg(feature = "agent-api")]
pub mod sandbox_api;

#[cfg(feature = "agent-api")]
pub mod agent_api;

#[cfg(feature = "agent-api")]
pub mod inline_api;
#[cfg(feature = "agent-api")]
mod inline_auth;

#[cfg(feature = "agent-api")]
mod agent_access;

#[cfg(feature = "agent-api")]
pub mod ssh;

#[cfg(feature = "agent-api")]
mod agent_response;

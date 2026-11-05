//! T10 `RestServer`.

use crate::{
    node::{Node, api},
    runtime::{Services, cancelled},
};
use std::sync::Arc;

/// T10 `RestServer`: serves `api::rest::router` on the REST listener bound
/// at startup.
///
/// Fatal class: `axum::serve` returning `Err` shuts the Node down. No log
/// events of its own. Stop: `services.stop` drives graceful shutdown.
pub(crate) struct RestServer {
    listener: tokio::net::TcpListener,
    state: Arc<Node>,
}

impl RestServer {
    pub(crate) fn new(listener: tokio::net::TcpListener, state: Arc<Node>) -> Self {
        Self { listener, state }
    }

    /// Start the server as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let rest = self.listener;
        let stop = services.stop.subscribe();
        let rest_state = self.state;
        services.spawn(async move {
            axum::serve(rest, api::rest::router(rest_state))
                .with_graceful_shutdown(cancelled(stop))
                .await
                .map_err(Into::into)
        });
    }
}

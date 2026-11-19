//! T10 `RestServer`.

use crate::runtime::{Services, cancelled};

/// T10 `RestServer`: serves the Node's routes on the REST listener bound at startup.
///
/// Fatal class: `axum::serve` returning `Err` shuts the Node down. No log
/// events of its own. Stop: `services.stop` drives graceful shutdown.
pub(crate) struct RestServer {
    listener: tokio::net::TcpListener,
    routes: axum::Router,
}

impl RestServer {
    pub(crate) fn new(listener: tokio::net::TcpListener, routes: axum::Router) -> Self {
        Self { listener, routes }
    }

    /// Start the server as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let rest = self.listener;
        let stop = services.stop.subscribe();
        services.spawn(async move {
            axum::serve(
                rest,
                self.routes
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(cancelled(stop))
            .await
            .map_err(Into::into)
        });
    }
}

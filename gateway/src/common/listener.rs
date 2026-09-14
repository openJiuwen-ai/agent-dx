use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// Accept a connection without permanently losing a listener on transient
/// resource pressure such as EMFILE/ENFILE. The future remains cancellable by
/// its caller's `select!`, while repeated failures use a bounded backoff.
pub async fn accept_with_backoff(
    listener: &TcpListener,
    listener_name: &'static str,
) -> (TcpStream, SocketAddr) {
    let mut backoff = Duration::from_millis(10);
    loop {
        match listener.accept().await {
            Ok(accepted) => return accepted,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                tracing::warn!(
                    %error,
                    listener = listener_name,
                    backoff_ms = backoff.as_millis(),
                    "TCP accept failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
        }
    }
}

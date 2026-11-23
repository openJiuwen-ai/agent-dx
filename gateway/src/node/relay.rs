use bytes::Bytes;
use futures_util::future::poll_fn;
use h2::RecvStream;
use h2::SendStream;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone, Default)]
pub struct RelayStats {
    bytes_up: Arc<AtomicU64>,
    bytes_down: Arc<AtomicU64>,
}

impl RelayStats {
    pub(crate) fn track(
        &self,
        meter: &Arc<crate::common::traffic::TrafficMeter>,
    ) -> crate::common::traffic::Registration {
        meter.register(crate::common::traffic::ByteCounters {
            up: self.bytes_up.clone(),
            down: self.bytes_down.clone(),
        })
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes_up.load(Ordering::Relaxed),
            self.bytes_down.load(Ordering::Relaxed),
        )
    }
}

/// Relay one CONNECT stream to a TCP socket. No protocol bytes are inspected.
pub async fn relay_h2_tcp<S>(
    recv: RecvStream,
    send: SendStream<Bytes>,
    tcp: S,
) -> io::Result<(u64, u64)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let stats = RelayStats::default();
    relay_h2_tcp_with_stats(recv, send, tcp, stats.clone()).await?;
    Ok(stats.snapshot())
}

/// Relay with counters that remain observable even when the future is
/// cancelled by an explicit route change or finishes with a stream error.
pub async fn relay_h2_tcp_with_stats<S>(
    mut recv: RecvStream,
    mut send: SendStream<Bytes>,
    mut tcp: S,
    stats: RelayStats,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Match the CONNECT transport frame budget so a full TCP read normally
    // becomes one DATA frame instead of four copies and four scheduler turns.
    let mut buf = [0u8; 64 * 1024];
    let mut h2_input_open = true;
    let mut tcp_input_open = true;
    while h2_input_open || tcp_input_open {
        tokio::select! {
            frame = recv.data(), if h2_input_open => {
                match frame {
                    Some(Ok(data)) => {
                        let n = data.len() as u64;
                        recv.flow_control().release_capacity(data.len()).map_err(|e| io::Error::other(e.to_string()))?;
                        tcp.write_all(&data).await?;
                        stats.bytes_up.fetch_add(n, Ordering::Relaxed);
                    }
                    Some(Err(e)) => return Err(io::Error::other(e.to_string())),
                    None => {
                        tcp.shutdown().await?;
                        h2_input_open = false;
                    }
                }
            }
            // The closed receive half no longer observes a client reset.
            reset = poll_fn(|cx| send.poll_reset(cx)), if !h2_input_open && tcp_input_open => {
                let error = match reset {
                    Ok(reason) => h2::Error::from(reason),
                    Err(error) => error,
                };
                return Err(io::Error::other(error));
            }
            result = tcp.read(&mut buf), if tcp_input_open => {
                let n = result?;
                if n == 0 {
                    send.send_data(Bytes::new(), true).map_err(|e| io::Error::other(e.to_string()))?;
                    tcp_input_open = false;
                    continue;
                }
                stats.bytes_down.fetch_add(n as u64, Ordering::Relaxed);
                let mut data = Bytes::copy_from_slice(&buf[..n]);
                while !data.is_empty() {
                    send.reserve_capacity(data.len());
                    let capacity = poll_fn(|cx| send.poll_capacity(cx)).await
                        .ok_or_else(|| io::Error::other("h2 stream closed"))?
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    if capacity == 0 { continue; }
                    let amount = capacity.min(data.len());
                    let chunk = data.split_to(amount);
                    send.send_data(chunk, false).map_err(|e| io::Error::other(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    #[test]
    fn relay_stats_survive_cloned_owner_and_accumulate() {
        let stats = RelayStats::default();
        let observer = stats.clone();
        stats.bytes_up.fetch_add(17, Ordering::Relaxed);
        stats.bytes_down.fetch_add(29, Ordering::Relaxed);
        drop(stats);
        assert_eq!(observer.snapshot(), (17, 29));
    }

    async fn check_client_half_close(cancel: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut guest, _) = listener.accept().await.unwrap();
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (relay_result, finished) = oneshot::channel();
        let server_driver = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server_io).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            let send = response
                .send_response(http::Response::new(()), false)
                .unwrap();
            tokio::spawn(async move {
                let result = relay_h2_tcp(request.into_body(), send, tcp).await;
                let _ = relay_result.send(result);
            });
            let _ = poll_fn(|cx| connection.poll_closed(cx)).await;
        });

        let (mut client, connection) = h2::client::handshake(client_io).await.unwrap();
        let client_driver = tokio::spawn(connection);
        let (response, mut send) = client
            .send_request(
                http::Request::connect("localhost:50090").body(()).unwrap(),
                false,
            )
            .unwrap();
        let mut recv = response.await.unwrap().into_body();
        send.send_data(Bytes::new(), true).unwrap();
        assert_eq!(guest.read(&mut [0]).await.unwrap(), 0);

        if cancel {
            send.send_reset(h2::Reason::CANCEL);
            let error = finished.await.unwrap().unwrap_err();
            let reset = error
                .get_ref()
                .unwrap()
                .downcast_ref::<h2::Error>()
                .unwrap();
            assert_eq!(reset.reason(), Some(h2::Reason::CANCEL));
        } else {
            guest.write_all(b"tail").await.unwrap();
            guest.shutdown().await.unwrap();
            let mut tail = Vec::new();
            while let Some(frame) = recv.data().await {
                tail.extend(frame.unwrap());
            }
            assert_eq!(tail, b"tail");
            assert_eq!(finished.await.unwrap().unwrap(), (0, 4));
        }
        client_driver.abort();
        server_driver.abort();
    }

    #[tokio::test]
    async fn client_cancel_after_half_close_releases_relay() {
        timeout(Duration::from_secs(3), check_client_half_close(true))
            .await
            .expect("Relay did not finish after client cancellation");
    }

    #[tokio::test]
    async fn client_half_close_allows_tcp_tail_and_eof() {
        timeout(Duration::from_secs(3), check_client_half_close(false))
            .await
            .expect("Relay did not deliver the TCP tail and EOF");
    }
}

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

    #[test]
    fn relay_stats_survive_cloned_owner_and_accumulate() {
        let stats = RelayStats::default();
        let observer = stats.clone();
        stats.bytes_up.fetch_add(17, Ordering::Relaxed);
        stats.bytes_down.fetch_add(29, Ordering::Relaxed);
        drop(stats);
        assert_eq!(observer.snapshot(), (17, 29));
    }
}

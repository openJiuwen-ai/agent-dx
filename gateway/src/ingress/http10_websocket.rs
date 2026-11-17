//! Normalize bodyless HTTP/1.0 WebSocket handshakes before Hyper parses them.
//!
//! The reader tracks HTTP/1 request framing so a handshake can follow ordinary
//! requests on the same connection. Bodies and upgraded protocols are opaque.
use bytes::{Buf, Bytes, BytesMut};
use futures_util::task::AtomicWaker;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// Match Hyper's default maximum HTTP/1 read buffer. On unrecognized framing,
// pass the original bytes to Hyper, which remains responsible for validation.
const MAX_HEAD_BYTES: usize = 8192 + 4096 * 100;
const PENDING: u8 = 0;
const HTTP: u8 = 1;
const UPGRADED: u8 = 2;

#[derive(Default)]
struct Decision {
    state: AtomicU8,
    waker: AtomicWaker,
}

#[derive(Clone)]
pub(super) struct UpgradeDecision(Arc<Decision>);

impl UpgradeDecision {
    pub(super) fn response(&self, upgraded: bool) {
        self.0
            .state
            .store(if upgraded { UPGRADED } else { HTTP }, Ordering::Release);
        self.0.waker.wake();
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Head,
    Fixed(u64),
    ChunkSize,
    Chunk(u64),
    ChunkEnd,
    Trailers,
    AwaitResponse,
    Opaque,
}

pub(super) struct Http10WebSocketIo<T> {
    inner: T,
    input: BytesMut,
    output: Bytes,
    phase: Phase,
    scan: usize,
    await_response: bool,
    decision: UpgradeDecision,
}

impl<T> Http10WebSocketIo<T> {
    pub(super) fn new(inner: T) -> (Self, UpgradeDecision) {
        let decision = UpgradeDecision(Arc::new(Decision::default()));
        (
            Self {
                inner,
                input: BytesMut::new(),
                output: Bytes::new(),
                phase: Phase::Head,
                scan: 0,
                await_response: false,
                decision: decision.clone(),
            },
            decision,
        )
    }

    fn emit(&mut self, length: usize, next: Phase) {
        self.output = self.input.split_to(length).freeze();
        self.phase = next;
        self.scan = 0;
    }

    fn delimiter(&mut self, delimiter: &[u8]) -> Option<usize> {
        let end = self.input[self.scan..]
            .windows(delimiter.len())
            .position(|part| part == delimiter)
            .map(|offset| self.scan + offset + delimiter.len());
        if end.is_none() {
            self.scan = self.input.len().saturating_sub(delimiter.len() - 1);
        }
        end
    }

    fn after_body(&self) -> Phase {
        if self.await_response {
            Phase::AwaitResponse
        } else {
            Phase::Head
        }
    }

    fn head_end(&mut self) -> Option<usize> {
        // httparse/Hyper also accept LF line endings. Recognize the same head
        // boundary so ordinary requests keep their existing parsing behavior.
        for index in self.scan..self.input.len() {
            if self.input[index] == b'\n' {
                let rest = &self.input[index + 1..];
                if rest.starts_with(b"\r\n") {
                    return Some(index + 3);
                }
                if rest.starts_with(b"\n") {
                    return Some(index + 2);
                }
            }
        }
        self.scan = self.input.len().saturating_sub(2);
        None
    }

    fn head(&mut self, length: usize) -> Option<Phase> {
        let mut headers = [httparse::EMPTY_HEADER; 100];
        let mut request = httparse::Request::new(&mut headers);
        if !matches!(request.parse(&self.input[..length]).ok()?, httparse::Status::Complete(parsed) if parsed == length)
        {
            return None;
        }
        let mut content_length = None;
        let mut transfer_encoding = false;
        let mut chunked = false;
        let mut connection_upgrade = false;
        let mut websocket = false;
        for header in request.headers.iter() {
            if ![
                "content-length",
                "transfer-encoding",
                "connection",
                "upgrade",
            ]
            .iter()
            .any(|name| header.name.eq_ignore_ascii_case(name))
            {
                continue;
            }
            let value = std::str::from_utf8(header.value).ok()?.trim();
            if header.name.eq_ignore_ascii_case("content-length") {
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return None;
                }
                let length = value.parse::<u64>().ok()?;
                if content_length.is_some_and(|previous| previous != length) {
                    return None;
                }
                content_length = Some(length);
            } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
                transfer_encoding = true;
                chunked = value
                    .split(',')
                    .next_back()?
                    .trim()
                    .eq_ignore_ascii_case("chunked");
            } else if header.name.eq_ignore_ascii_case("connection") {
                connection_upgrade |= value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
            } else if header.name.eq_ignore_ascii_case("upgrade") {
                websocket = value.eq_ignore_ascii_case("websocket");
            }
        }
        if transfer_encoding && (content_length.is_some() || !chunked) {
            return None;
        }
        let bodyless = !transfer_encoding && content_length.unwrap_or(0) == 0;
        let normalize = request.version == Some(0)
            && request.method == Some("GET")
            && connection_upgrade
            && websocket
            && bodyless;
        self.await_response = connection_upgrade || request.method == Some("CONNECT");
        if self.await_response {
            self.decision.0.state.store(PENDING, Ordering::Release);
        }
        if normalize {
            let start = self.input[..length]
                .iter()
                .position(|byte| !matches!(byte, b'\r' | b'\n'))?;
            let line_end = start
                + self.input[start..length]
                    .iter()
                    .position(|byte| *byte == b'\n')?;
            let line = &self.input[start..line_end];
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if !line.ends_with(b"HTTP/1.0") {
                return None;
            }
            let line_end = start + line.len();
            self.input[line_end - 1] = b'1';
        }
        Some(if chunked {
            Phase::ChunkSize
        } else if let Some(length @ 1..) = content_length {
            Phase::Fixed(length)
        } else {
            self.after_body()
        })
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Http10WebSocketIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if !self.output.is_empty() {
                let length = self.output.len().min(buf.remaining());
                buf.put_slice(&self.output[..length]);
                self.output.advance(length);
                return Poll::Ready(Ok(()));
            }
            let phase = self.phase;
            match phase {
                Phase::Head => {
                    if let Some(length) = self.head_end() {
                        let next = self.head(length).unwrap_or(Phase::Opaque);
                        self.emit(length, next);
                        continue;
                    }
                }
                Phase::Fixed(remaining) | Phase::Chunk(remaining) if !self.input.is_empty() => {
                    let length = usize::try_from(remaining)
                        .unwrap_or(usize::MAX)
                        .min(self.input.len());
                    let remaining = remaining - length as u64;
                    let next = match self.phase {
                        Phase::Fixed(_) if remaining == 0 => self.after_body(),
                        Phase::Fixed(_) => Phase::Fixed(remaining),
                        _ if remaining == 0 => Phase::ChunkEnd,
                        _ => Phase::Chunk(remaining),
                    };
                    self.emit(length, next);
                    continue;
                }
                Phase::ChunkSize if self.delimiter(b"\r\n").is_some() => {
                    match httparse::parse_chunk_size(&self.input) {
                        Ok(httparse::Status::Complete((length, size))) => {
                            self.emit(
                                length,
                                if size == 0 {
                                    Phase::Trailers
                                } else {
                                    Phase::Chunk(size)
                                },
                            );
                        }
                        _ => self.phase = Phase::Opaque,
                    }
                    continue;
                }
                Phase::ChunkEnd if self.input.len() >= 2 => {
                    if self.input.starts_with(b"\r\n") {
                        self.emit(2, Phase::ChunkSize);
                    } else {
                        self.phase = Phase::Opaque;
                    }
                    continue;
                }
                Phase::Trailers => {
                    if let Some(length) = self.delimiter(b"\r\n") {
                        let next = if length == 2 {
                            self.after_body()
                        } else {
                            Phase::Trailers
                        };
                        self.emit(length, next);
                        continue;
                    }
                }
                Phase::AwaitResponse => {
                    self.decision.0.waker.register(cx.waker());
                    match self.decision.0.state.load(Ordering::Acquire) {
                        HTTP => self.phase = Phase::Head,
                        UPGRADED => self.phase = Phase::Opaque,
                        _ => return Poll::Pending,
                    }
                    continue;
                }
                Phase::Opaque => {
                    if self.input.is_empty() {
                        return Pin::new(&mut self.inner).poll_read(cx, buf);
                    }
                    let length = self.input.len();
                    self.emit(length, Phase::Opaque);
                    continue;
                }
                _ => {}
            }
            if self.input.len() >= MAX_HEAD_BYTES {
                self.phase = Phase::Opaque;
                continue;
            }
            let mut bytes = [0; 8192];
            let mut read = ReadBuf::new(&mut bytes);
            ready!(Pin::new(&mut self.inner).poll_read(cx, &mut read))?;
            if read.filled().is_empty() {
                let length = self.input.len();
                if length == 0 {
                    return Poll::Ready(Ok(()));
                }
                self.emit(length, Phase::Opaque);
            } else {
                self.input.extend_from_slice(read.filled());
            }
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Http10WebSocketIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn transformed(wire: &[u8]) -> Vec<u8> {
        let (mut writer, reader) = tokio::io::duplex(16384);
        writer.write_all(wire).await.unwrap();
        let (mut reader, _) = Http10WebSocketIo::new(reader);
        let mut result = vec![0; wire.len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reader.read_exact(&mut result),
        )
        .await
        .unwrap()
        .unwrap();
        result
    }

    #[tokio::test]
    async fn http10_websocket_only_bodyless_get_is_normalized() {
        let base =
            b"GET /ws HTTP/1.0\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n";
        for suffix in [b"\r\n".as_slice(), b"Content-Length: 0\r\n\r\n"] {
            let wire = [base.as_slice(), suffix].concat();
            let expected = String::from_utf8(wire.clone())
                .unwrap()
                .replacen("HTTP/1.0", "HTTP/1.1", 1);
            assert_eq!(transformed(&wire).await, expected.as_bytes());
        }
        let lf_head = String::from_utf8([base.as_slice(), b"\r\n"].concat())
            .unwrap()
            .replace("\r\n", "\n");
        assert_eq!(
            transformed(lf_head.as_bytes()).await,
            lf_head.replacen("HTTP/1.0", "HTTP/1.1", 1).as_bytes()
        );
        for suffix in [
            b"Content-Length: 1\r\n\r\nx".as_slice(),
            b"Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            b"Content-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            b"Content-Length: 0\r\nContent-Length: 1\r\n\r\nx",
        ] {
            let wire = [base.as_slice(), suffix].concat();
            assert_eq!(transformed(&wire).await, wire);
        }
        for wire in [
            "GET /normal HTTP/1.0\r\nHost: test\r\n\r\n",
            "POST /ws HTTP/1.0\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            "GET /ws HTTP/1.0\r\nUpgrade: websocket\r\n\r\n",
            "GET /ws HTTP/1.0\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n",
            "GET /ws HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
        ] {
            assert_eq!(transformed(wire.as_bytes()).await, wire.as_bytes());
        }
    }

    #[tokio::test]
    async fn http10_websocket_preserves_pre_read_upgrade_data() {
        let head = b"GET /ws HTTP/1.0\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
        let (mut writer, reader) = tokio::io::duplex(4096);
        writer
            .write_all(&[head.as_slice(), head.as_slice()].concat())
            .await
            .unwrap();
        let (mut reader, decision) = Http10WebSocketIo::new(reader);
        let mut received = vec![0; head.len()];
        reader.read_exact(&mut received).await.unwrap();
        assert!(received.starts_with(b"GET /ws HTTP/1.1"));
        decision.response(true);
        reader.read_exact(&mut received).await.unwrap();
        assert_eq!(received, head);
    }
}

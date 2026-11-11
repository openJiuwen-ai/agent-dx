//! Scripted RESP peer for deterministic connection faults; never contacts Redis.
// Shared by discovery and API test targets, which exercise different fault modes.
#![allow(dead_code)]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{broadcast, Notify},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Default)]
pub enum Reply {
    #[default]
    Healthy,
    Raw(Vec<u8>),
    Close,
    CloseAfter(Duration),
    Delay(Duration),
}

#[derive(Default)]
pub struct State {
    pub connections: AtomicUsize,
    pub queries: AtomicUsize,
    pub closed: AtomicUsize,
    pub reply: Mutex<Reply>,
    pub handshake_delay: Mutex<Duration>,
    pub observed: Notify,
    pub address: Mutex<String>,
}

pub struct RedisPeer {
    pub url: String,
    pub state: Arc<State>,
    close: broadcast::Sender<()>,
    task: JoinHandle<()>,
}

impl RedisPeer {
    pub async fn new(address: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}/", listener.local_addr().unwrap());
        let state = Arc::new(State::default());
        *state.address.lock().unwrap() = address.into();
        let (close, _) = broadcast::channel(16);
        let task = {
            let state = state.clone();
            let close = close.clone();
            tokio::spawn(async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            state.connections.fetch_add(1, Ordering::SeqCst);
                            let state = state.clone();
                            let mut close = close.subscribe();
                            tasks.spawn(async move {
                                tokio::select! {
                                    _ = serve(stream, state.clone()) => {}
                                    _ = close.recv() => {}
                                }
                                state.closed.fetch_add(1, Ordering::SeqCst);
                                state.observed.notify_one();
                            });
                        }
                        Some(result) = tasks.join_next() => { result.unwrap(); }
                    }
                }
            })
        };
        Self {
            url,
            state,
            close,
            task,
        }
    }

    pub async fn disconnect(&self) {
        let expected = self.state.closed.load(Ordering::SeqCst) + self.close.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.state.closed.load(Ordering::SeqCst) < expected {
                self.state.observed.notified().await;
            }
        })
        .await
        .unwrap();
    }

    pub async fn wait_for_queries(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.state.queries.load(Ordering::SeqCst) < count {
                self.state.observed.notified().await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for RedisPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn command(reader: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let count: usize = line.strip_prefix('*')?.trim().parse().ok()?;
    let mut args = Vec::new();
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.ok()?;
        let length: usize = line.strip_prefix('$')?.trim().parse().ok()?;
        assert!(length < 64 * 1024);
        let mut arg = vec![0; length + 2];
        reader.read_exact(&mut arg).await.ok()?;
        arg.truncate(length);
        args.push(arg);
    }
    Some(args)
}

async fn serve(stream: TcpStream, state: Arc<State>) {
    let mut reader = BufReader::new(stream);
    while let Some(args) = command(&mut reader).await {
        let reply = if args.first().is_some_and(|arg| arg == b"EVAL") {
            let reply = state.reply.lock().unwrap().clone();
            state.queries.fetch_add(1, Ordering::SeqCst);
            state.observed.notify_one();
            match reply {
                Reply::Close => return,
                Reply::CloseAfter(delay) => {
                    tokio::time::sleep(delay).await;
                    return;
                }
                Reply::Raw(value) => value,
                Reply::Healthy | Reply::Delay(_) => {
                    if let Reply::Delay(delay) = reply {
                        tokio::time::sleep(delay).await;
                    }
                    let endpoint = serde_json::json!({
                        "schema":1,"epoch":7,"address":*state.address.lock().unwrap()
                    })
                    .to_string();
                    let header = r#"{"epoch":7}"#;
                    format!(
                        "*2\r\n${}\r\n{endpoint}\r\n${}\r\n{header}\r\n",
                        endpoint.len(),
                        header.len()
                    )
                    .into_bytes()
                }
            }
        } else {
            let delay = *state.handshake_delay.lock().unwrap();
            tokio::time::sleep(delay).await;
            b"+OK\r\n".to_vec()
        };
        if reader.get_mut().write_all(&reply).await.is_err() {
            return;
        }
    }
}

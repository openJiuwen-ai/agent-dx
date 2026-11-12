//! Reusable peer gRPC channels.

use super::*;

/// Node-owned connection resources shared by file reads, owner forwarding and
/// replica commands. Epoch changes invalidate cached channels; grant/handle
/// validity remains with the business operation, never with this pool.
pub(super) type PeerChannelKey = (String, u64, String, bool);

pub struct PeerConnectionPool {
    #[cfg(feature = "dfs")]
    pub(super) read_crc_mode: crate::config::DfsReadCrcMode,
    #[cfg(feature = "dfs")]
    pub(super) read_work_metrics: Option<crate::node::metrics::work::ReadWorkMetrics>,
    pub(super) config: afs_transport::GrpcConfig,
    security: afs_transport::SecurityManager,
    capacity: usize,
    /// Runtime that drives every pooled channel's connection task (R82). A
    /// channel is shared by sync owner clients, which block on private
    /// current-thread runtimes, and by the async read path on the Node runtime;
    /// dialing under a private runtime would park the connection there.
    channel_runtime: Option<tokio::runtime::Handle>,
    pub(super) state: tokio::sync::Mutex<PeerPoolState>,
    #[cfg(feature = "dfs")]
    pub(super) stream_work: Arc<PeerStreamWorkPool>,
}

#[derive(Default)]
pub(super) struct PeerPoolState {
    pub(super) channels: std::collections::HashMap<PeerChannelKey, CachedPeerChannel>,
    // A channel eviction does not erase the highest authority generation seen.
    pub(super) high_water_epochs: std::collections::HashMap<String, u64>,
}

pub(super) struct CachedPeerChannel {
    pub(super) channel: Channel,
    touched: std::time::Instant,
}

impl PeerConnectionPool {
    pub fn new(
        config: afs_transport::GrpcConfig,
        tls: afs_transport::TlsConfig,
        capacity: usize,
    ) -> afs_error::Result<Self> {
        if capacity == 0 {
            return Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "peer pool capacity must be positive",
            ));
        }
        let security = afs_transport::SecurityManager::new(tls).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        Ok(Self {
            config,
            security,
            capacity,
            channel_runtime: None,
            state: tokio::sync::Mutex::new(PeerPoolState::default()),
            #[cfg(feature = "dfs")]
            read_crc_mode: crate::config::DfsReadCrcMode::Both,
            #[cfg(feature = "dfs")]
            stream_work: Arc::new(PeerStreamWorkPool::default()),
            #[cfg(feature = "dfs")]
            read_work_metrics: None,
        })
    }

    /// Dial every pooled channel on `runtime`, normally the Node runtime that
    /// outlives every client of this pool. Only a Node with a backend builds a pool.
    #[cfg(any(feature = "ownerfs", feature = "dfs", test))]
    #[must_use]
    pub(crate) fn with_channel_runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.channel_runtime = Some(runtime);
        self
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: crate::node::metrics::work::ReadWorkMetrics,
    ) -> Self {
        self.read_work_metrics = Some(metrics);
        self
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn with_read_crc_mode(mut self, mode: crate::config::DfsReadCrcMode) -> Self {
        self.read_crc_mode = mode;
        self
    }

    pub async fn channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, true, false)
            .await
    }

    /// OwnerFiles receive frames may carry large replies. Keep this profile
    /// separate from DFS channels while sharing epoch fencing and pool bounds.
    #[cfg(feature = "ownerfs")]
    pub async fn owner_files_channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, true, true)
            .await
    }

    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    pub async fn long_wait_channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, false, false)
            .await
    }

    async fn channel_with_timeout(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
        request_timeout: bool,
        owner_files: bool,
    ) -> afs_error::Result<Channel> {
        if node_id.is_empty() || node_epoch == 0 || endpoint.is_empty() {
            return Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "peer identity or endpoint is incomplete",
            ));
        }
        let key = (
            node_id.to_owned(),
            node_epoch,
            endpoint.to_owned(),
            owner_files,
        );
        let mut state = self.state.lock().await;
        self.channel_locked(&mut state, &key, request_timeout)
    }

    // Both callers already hold the connection metadata lock. Stream admission
    // also holds its own short lock, so channel creation cannot race active work.
    fn channel_locked(
        &self,
        state: &mut PeerPoolState,
        key: &PeerChannelKey,
        request_timeout: bool,
    ) -> afs_error::Result<Channel> {
        let (node_id, node_epoch, endpoint, owner_files) = key;
        let node_epoch = *node_epoch;
        let owner_files = *owner_files;
        if state
            .high_water_epochs
            .get(node_id)
            .is_some_and(|epoch| node_epoch < *epoch)
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "peer candidate has an older Node epoch than the connection pool",
            ));
        }
        if request_timeout && let Some(entry) = state.channels.get_mut(key) {
            entry.touched = std::time::Instant::now();
            return Ok(entry.channel.clone());
        }
        let endpoint =
            Endpoint::from_shared(self.security.endpoint_uri(endpoint)).map_err(|error| {
                afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
            })?;
        let endpoint = if request_timeout {
            self.config.configure_client(endpoint)
        } else {
            self.config.configure_long_wait_client(endpoint)
        };
        // This bounds each incoming frame, not the message/flow-control window.
        // Smaller frames remain valid; TLS, deadlines and message limits remain.
        // This only changes the client's receive cap. Frames still consume the
        // existing flow-control windows; message limits, budgets and deadlines
        // remain unchanged. Peers may continue to send smaller frames.
        let endpoint = endpoint.max_frame_size(if owner_files {
            256 * 1024
        } else {
            DFS_PEER_RECEIVE_FRAME_BYTES
        });
        // tonic spawns the connection task where the channel is built.
        let _channel_runtime = self.channel_runtime.as_ref().map(|runtime| runtime.enter());
        let channel = self.security.connect_lazy(endpoint).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        if request_timeout {
            self.publish_channel(state, key.clone(), channel.clone());
        } else {
            self.publish_epoch(state, node_id, node_epoch);
        }
        Ok(channel)
    }

    #[cfg(feature = "dfs")]
    pub(super) async fn acquire_stream_work(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Arc<PeerStreamActive>> {
        self.acquire_stream_work_mode(node_id, node_epoch, endpoint, false)
            .await?
            .ok_or_else(|| dfs_protocol_error("Peer foreground stream admission stopped"))
    }

    #[cfg(feature = "dfs")]
    pub(super) async fn acquire_stream_work_mode(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
        background: bool,
    ) -> afs_error::Result<Option<Arc<PeerStreamActive>>> {
        let _waiting = if background {
            None
        } else {
            Some(PeerStreamWaiter::new(self.stream_work.clone())?)
        };
        if node_id.is_empty()
            || node_epoch == 0
            || endpoint.is_empty()
            || node_id.len().saturating_add(endpoint.len()) > STREAM_KEY_BYTES
        {
            return Err(dfs_protocol_error(
                "Peer stream connection identity exceeds its bound",
            ));
        }
        let key = (node_id.to_owned(), node_epoch, endpoint.to_owned(), false);
        if key.0.capacity().saturating_add(key.2.capacity()) > STREAM_KEY_BYTES {
            return Err(dfs_protocol_error(
                "Peer stream connection key allocation exceeds its bound",
            ));
        }
        loop {
            // Register before checking admission, so the last drop cannot be lost.
            let changed = self.stream_work.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Fence and select under the connection metadata lock. Never ask
            // the LRU for a replacement while another Channel is active.
            let mut channels = if background {
                match self.state.try_lock() {
                    Ok(channels) => channels,
                    Err(_) => return Ok(None),
                }
            } else {
                self.state.lock().await
            };
            if channels
                .high_water_epochs
                .get(node_id)
                .is_some_and(|epoch| node_epoch < *epoch)
            {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "peer candidate has an older Node epoch than the connection pool",
                ));
            }
            {
                let mut state = if background {
                    match self.stream_work.state.try_lock() {
                        Ok(state) => state,
                        Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(dfs_protocol_error("Peer stream admission lock poisoned"));
                        }
                    }
                } else {
                    self.stream_work
                        .state
                        .lock()
                        .map_err(|_| dfs_protocol_error("Peer stream admission lock poisoned"))?
                };
                if background && state.foreground_waiters != 0 {
                    return Ok(None);
                }
                if state.active == 0 {
                    let candidate = self.channel_locked(&mut channels, &key, true)?;
                    let mut blocks = Vec::with_capacity(2);
                    for _ in 0..2 {
                        blocks.push(PeerStreamBlock::new(self.read_work_metrics.as_ref())?);
                    }
                    state.session = Some(Arc::new(PeerStreamSession {
                        key: key.clone(),
                        channel: candidate,
                        blocks: std::sync::Mutex::new(blocks),
                        available: tokio::sync::Notify::new(),
                        controls: Arc::new(tokio::sync::Semaphore::new(STREAM_FRAME_BACKING_BYTES)),
                        _work: std::sync::Mutex::new(
                            self.read_work_metrics
                                .as_ref()
                                .map(|metrics| metrics.peer_stream_pipeline(0)),
                        ),
                    }));
                }
                if let Some(session) = &state.session
                    && state.active < 2
                    && session.key == key
                {
                    let session = session.clone();
                    state.active += 1;
                    return Ok(Some(Arc::new(PeerStreamActive {
                        pool: self.stream_work.clone(),
                        session,
                    })));
                }
            }
            drop(channels);
            if background {
                return Ok(None);
            }
            changed.await;
        }
    }

    fn publish_epoch(&self, state: &mut PeerPoolState, node_id: &str, node_epoch: u64) {
        state
            .high_water_epochs
            .insert(node_id.to_owned(), node_epoch);
        state
            .channels
            .retain(|(id, epoch, _, _), _| id != node_id || *epoch == node_epoch);
    }

    fn publish_channel(&self, state: &mut PeerPoolState, key: PeerChannelKey, channel: Channel) {
        let (node_id, node_epoch, _, _) = &key;
        // No await occurs from the epoch check through publication. Lazy
        // connection completion cannot reinsert a stale channel after an epoch
        // advance; both the channel and its high-water mark publish under lock.
        self.publish_epoch(state, node_id, *node_epoch);
        let channels = &mut state.channels;
        if channels.len() >= self.capacity
            && let Some(oldest) = channels
                .iter()
                .min_by_key(|(_, value)| value.touched)
                .map(|(key, _)| key.clone())
        {
            channels.remove(&oldest);
        }
        channels.insert(
            key,
            CachedPeerChannel {
                channel,
                touched: std::time::Instant::now(),
            },
        );
    }
}

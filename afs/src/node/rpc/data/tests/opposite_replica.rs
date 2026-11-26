use super::*;
use crate::dfs::{OperationId, ReplicaAck, ReplicaGroupId, ReplicaTarget};
use crate::node::{
    chunk::StagedChunk,
    replication::{ReplicaDataPlane, ReplicaPeerOp, ReplicaTransferMode, validate_peer_acks},
    rpc::peer::{GrpcReplicaDataPlane, PeerConnectionPool},
};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

const WAVE: usize = 8;

#[derive(Default)]
struct ForwardGate {
    arrived: AtomicUsize,
    released: AtomicBool,
    wake: tokio::sync::Notify,
}

struct GatedPlane {
    inner: Arc<GrpcReplicaDataPlane>,
    gate: Arc<ForwardGate>,
    runtime: tokio::runtime::Handle,
}

impl ReplicaDataPlane for GatedPlane {
    fn mode(&self) -> ReplicaTransferMode {
        self.inner.mode()
    }

    fn prepare_peer(&self, op: &ReplicaPeerOp) -> afs_error::Result<()> {
        self.inner.prepare_peer(op)
    }

    fn put_peer_replica(
        &self,
        op: &ReplicaPeerOp,
        staged: &StagedChunk,
    ) -> afs_error::Result<Vec<ReplicaAck>> {
        self.gate.arrived.fetch_add(1, Ordering::SeqCst);
        let released = self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                let notified = self.gate.wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.gate.released.load(Ordering::SeqCst) {
                    notified.await;
                }
            })
            .await
        });
        released
            .map_err(|_| afs_error::Error::coded(afs_error::IO_TIMEOUT, "test gate timed out"))?;
        self.inner.put_peer_replica(op, staged)
    }
}

struct TestGrants {
    ops: Vec<ReplicaPeerOp>,
    first_hops: Arc<AtomicUsize>,
}

impl DfsReplicaAuthorizer for TestGrants {
    fn authorize(
        &self,
        _: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        let op = self
            .ops
            .iter()
            .find(|op| {
                op.chunk_id.0 == header.chunk_id && op.target_index == header.target_index as usize
            })
            .ok_or_else(|| replica_denied("test grant does not match replica header"))?;
        if op.target_index == 1 {
            self.first_hops.fetch_add(1, Ordering::SeqCst);
        }
        Ok(AuthorizedReplicaWrite {
            op: op.clone(),
            expires_at_unix_ms: u64::MAX,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dfs_replica_opposite_chains_complete_on_real_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let mut listeners = Vec::new();
    let mut stores = Vec::new();
    let mut targets = Vec::new();
    for node in ["b", "c"] {
        let store = Arc::new(LocalChunkStore::open(dir.path().join(node), node).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        targets.push(ReplicaTarget {
            node_id: node.into(),
            node_epoch: 1,
            data_endpoint: format!("http://{}", listener.local_addr().unwrap()),
            device: store.device_descriptor().unwrap(),
        });
        stores.push(store);
        listeners.push(listener);
    }
    let a = ReplicaTarget {
        node_id: "writer".into(),
        data_endpoint: "http://127.0.0.1:1".into(),
        ..targets[0].clone()
    };
    let pool = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            WAVE,
        )
        .unwrap(),
    );
    let inner = Arc::new(GrpcReplicaDataPlane::new(pool, Duration::from_secs(5)).unwrap());

    let mut writes = Vec::new();
    let mut per_node = [Vec::new(), Vec::new()];
    for i in 0..2 * WAVE {
        let side = i % 2;
        let staged = StagedChunk::new(
            OperationId::new(format!("opposite-chain-{i}")),
            vec![u8::try_from(i).unwrap(); 32 * 1024],
        );
        let first = ReplicaPeerOp {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: ReplicaGroupId::new(format!("opposite-{i}")),
            initiator_node_id: "writer".into(),
            initiator_node_epoch: 1,
            target: targets[side].clone(),
            chain_tail: vec![targets[1 - side].clone()],
            ordered_targets: vec![a.clone(), targets[side].clone(), targets[1 - side].clone()],
            sync_target_count: 3,
            target_index: 1,
            repair_claim: None,
        };
        per_node[side].push(first.clone());
        per_node[1 - side].push(first.next_hop().unwrap().unwrap());
        writes.push((first, staged));
    }

    let mut peers = Vec::new();
    let mut servers = Vec::new();
    for ((listener, store), ops) in listeners.into_iter().zip(&stores).zip(per_node) {
        let gate = Arc::new(ForwardGate::default());
        let first_hops = Arc::new(AtomicUsize::new(0));
        let permits = Arc::new(tokio::sync::Semaphore::new(WAVE));
        let forward_permits = Arc::new(tokio::sync::Semaphore::new(WAVE - 1));
        let auth_permits = Arc::new(tokio::sync::Semaphore::new(WAVE));
        let service = DfsChunksService {
            local_chunks: Some(store.clone()),
            authenticator: Arc::new(AllowDfsTestPeer),
            authorizer: Arc::new(DenyDfsReadAuthorizer),
            replica_authorizer: Arc::new(TestGrants {
                ops,
                first_hops: first_hops.clone(),
            }),
            replica_data_plane: Some(Arc::new(GatedPlane {
                inner: inner.clone(),
                gate: gate.clone(),
                runtime: tokio::runtime::Handle::current(),
            })),
            replica_permits: permits.clone(),
            replica_forward_permits: forward_permits.clone(),
            replica_authorization_permits: auth_permits.clone(),
            read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            replica_timeout: Duration::from_secs(3),
            rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
            suspect_reviews: Default::default(),
            holders: None,
        };
        peers.push((gate, first_hops, permits, forward_permits, auth_permits));
        servers.push(tokio::spawn(async move {
            Server::builder()
                .add_service(DfsChunksServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
    }

    let requests: Vec<_> = writes
        .iter()
        .map(|(op, staged)| {
            let inner = inner.clone();
            let op = op.clone();
            let staged = staged.clone();
            tokio::task::spawn_blocking(move || {
                let acks = inner.put_peer_replica(&op, &staged)?;
                validate_peer_acks(&op, &staged, &acks)?;
                Ok::<_, afs_error::Error>(acks.len())
            })
        })
        .collect();

    // On the old code auth for all eight first hops proves every receive
    // permit is taken. The fixed path can authenticate the queued eighth
    // request without occupying the slot that its peer's tail needs.
    let reached = tokio::time::timeout(Duration::from_secs(2), async {
        while peers.iter().any(|(gate, first_hops, ..)| {
            first_hops.load(Ordering::SeqCst) != WAVE
                || gate.arrived.load(Ordering::SeqCst) < WAVE - 1
        }) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    for (gate, _, permits, ..) in &peers {
        eprintln!(
            "opposite replica wave: staged={}, free receive slots={}",
            gate.arrived.load(Ordering::SeqCst),
            permits.available_permits()
        );
        gate.released.store(true, Ordering::SeqCst);
        gate.wake.notify_waiters();
    }
    reached.expect("both waves must reach the forwarding barrier before release");

    let mut counts = [0; 2];
    let mut failures = Vec::new();
    for (i, request) in requests.into_iter().enumerate() {
        match request.await.unwrap() {
            Ok(2) => counts[i % 2] += 1,
            result => failures.push(format!("{i}: {result:?}")),
        }
    }
    eprintln!("completed opposite writes: {counts:?}; errors: {failures:?}");
    assert_eq!(counts, [WAVE, WAVE], "{failures:?}");

    for (_, _, permits, forward, auth) in &peers {
        assert_eq!(permits.available_permits(), WAVE);
        assert_eq!(forward.available_permits(), WAVE - 1);
        assert_eq!(auth.available_permits(), WAVE);
    }
    eprintln!(
        "both peers returned all {WAVE} receive / {} forward / {WAVE} authorization slots",
        WAVE - 1
    );
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    drop(stores);
    for node in ["b", "c"] {
        let store = LocalChunkStore::open(dir.path().join(node), node).unwrap();
        for (_, staged) in &writes {
            let mut body = vec![0; staged.bytes().len()];
            assert_eq!(
                store.read_at(&staged.chunk.id, 0, &mut body).unwrap(),
                body.len()
            );
            assert_eq!(body, staged.bytes());
        }
    }
    eprintln!(
        "reopened both replica stores and checked {} chunk bodies each",
        writes.len()
    );
}

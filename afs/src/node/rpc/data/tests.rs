use super::*;
#[cfg(feature = "ownerfs")]
use super::{make_owner_files_server, make_owner_files_server_with_handler};
#[cfg(feature = "dfs")]
use crate::node::chunk::ChunkSink;
use crate::node::rpc::{
    control::make_control_server,
    peer::{DataClientOptions, DataMode, connect_data_client},
};
#[cfg(feature = "ownerfs")]
use afs_protocol::node_data::{
    FileIdentity as PbFileIdentity, OwnerCaller, OwnerDirectoryHandle, OwnerFsyncRequest,
    OwnerGetAttrRequest, OwnerHandle, OwnerLinkRequest, OwnerOpenRequest, OwnerReadReply,
    OwnerReadRequest, OwnerReaddirRequest, OwnerReadlinkRequest, OwnerSymlinkRequest, RootAccess,
    owner_files_client::OwnerFilesClient,
};
#[cfg(feature = "ownerfs")]
use afs_transport::grpc::error_status::status_to_error;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[cfg(feature = "dfs")]
#[test]
fn wi6_outboard_header_shares_owner_and_preserves_legacy_wire_fields() {
    use crate::node::chunk::outboard::{OutboardBudget, build_borrowed};
    use prost::Message;
    let body = vec![39; 96 * 1024 + 17];
    let budget = OutboardBudget::new(1024 * 1024);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let encoded = proof.encoded().to_vec();
    let address = proof.encoded().as_ptr();
    let bytes = prost::bytes::Bytes::from_owner(PeerOutboardOwner(proof.clone()));
    assert_eq!(
        bytes.as_ptr(),
        address,
        "A must not copy proof metadata into the header"
    );
    let header = DfsReadRangesHeader {
        crc32c_table: Default::default(),
        read_id: "read".into(),
        attempt_id: "attempt".into(),
        operation_index: 3,
        chunk_id: "chunk".into(),
        chunk_offset: 0,
        length: body.len() as u64,
        source_copy_id: "copy".into(),
        blake3_outboard: bytes,
    };
    let queued = header.clone();
    drop(proof);
    drop(header);
    assert!(
        budget.used() > 0,
        "queued header retains the original proof lease"
    );
    // Older readers ignore the new optional tag. Existing identity, range
    // and Completion fields retain their numbers and semantics.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyHeader {
        #[prost(string, tag = "1")]
        read_id: String,
        #[prost(string, tag = "2")]
        attempt_id: String,
        #[prost(uint32, tag = "3")]
        operation_index: u32,
        #[prost(string, tag = "4")]
        chunk_id: String,
        #[prost(uint64, tag = "5")]
        chunk_offset: u64,
        #[prost(uint64, tag = "6")]
        length: u64,
        #[prost(string, tag = "7")]
        source_copy_id: String,
    }
    let wire = queued.encode_to_vec();
    let old = LegacyHeader::decode(wire.as_slice()).unwrap();
    assert_eq!(old.read_id, queued.read_id);
    assert_eq!(old.length, queued.length);
    let decoded = DfsReadRangesHeader::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded.blake3_outboard.as_ref(), encoded);
    let from_old = DfsReadRangesHeader::decode(old.encode_to_vec().as_slice()).unwrap();
    assert!(from_old.blake3_outboard.is_empty());
    let capability = DfsReadRangesRequest {
        include_blake3_outboard: true,
        ..Default::default()
    };
    assert_eq!(
        capability.encode_to_vec(),
        [0x38, 0x01],
        "new capability is only tag 7"
    );
    assert!(
        !DfsReadRangesRequest::decode(&[][..])
            .unwrap()
            .include_blake3_outboard
    );
    drop(queued);
    assert_eq!(
        budget.used(),
        0,
        "codec output is separate transport ownership"
    );
}

#[tokio::test]
async fn grpc_inline_write_then_read_uses_storage() {
    let temp = tempfile::tempdir().unwrap();
    let storage = DiagnosticStorage::new(temp.path()).unwrap();
    let service = NodeDataService::new(Arc::new(storage), RdmaSessionRegistry::new(None));

    let written = service
        .write(Request::new(DataWriteRequest {
            session_id: 0,
            transfer: DataTransfer::GrpcInline.into(),
            name: "eight.bin".into(),
            offset: 0,
            data: b"12345678".to_vec(),
            length: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.written, 8);

    let read = service
        .read(Request::new(DataReadRequest {
            session_id: 0,
            transfer: DataTransfer::GrpcInline.into(),
            name: "eight.bin".into(),
            offset: 0,
            length: 8,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.length, 8);
    assert_eq!(read.data, b"12345678");
}

#[tokio::test]
async fn rdma_read_validates_session_before_touching_storage() {
    let temp = tempfile::tempdir().unwrap();
    let storage = DiagnosticStorage::new(temp.path()).unwrap();
    let service = NodeDataService::new(Arc::new(storage), RdmaSessionRegistry::new(None));

    let error = service
        .read(Request::new(DataReadRequest {
            session_id: 99,
            transfer: DataTransfer::RdmaOneSided.into(),
            name: "missing.bin".into(),
            offset: 0,
            length: 8,
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn public_grpc_client_and_servers_move_eight_bytes() {
    let (_temp, endpoint, server) = spawn_grpc_server().await;

    let mut client = connect_data_client(DataClientOptions {
        endpoint: endpoint.clone(),
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: std::time::Duration::from_secs(5),
        tls: Default::default(),
    })
    .await
    .unwrap();
    assert_eq!(client.mode(), "grpc");
    assert_eq!(
        client
            .write("eight.bin", 0, b"abcdefgh".to_vec())
            .await
            .unwrap(),
        8
    );
    assert_eq!(client.read("eight.bin", 0, 8).await.unwrap(), b"abcdefgh");
    client.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn auto_without_rdma_device_falls_back_to_grpc() {
    let (_temp, endpoint, server) = spawn_grpc_server().await;

    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Auto,
        rdma_device: None,
        timeout: std::time::Duration::from_secs(5),
        tls: Default::default(),
    })
    .await
    .unwrap();

    assert_eq!(client.mode(), "grpc");
    assert_eq!(
        client
            .write("auto.bin", 0, b"abcdefgh".to_vec())
            .await
            .unwrap(),
        8
    );
    assert_eq!(client.read("auto.bin", 0, 8).await.unwrap(), b"abcdefgh");
    server.abort();
}

#[tokio::test]
async fn forced_rdma_without_device_does_not_fallback_to_grpc() {
    let (_temp, endpoint, server) = spawn_grpc_server().await;

    let result = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Rdma,
        rdma_device: None,
        timeout: std::time::Duration::from_secs(5),
        tls: Default::default(),
    })
    .await;
    let error = match result {
        Ok(_) => panic!("forced RDMA unexpectedly fell back to gRPC"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("RDMA"));
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_files_service_is_registered_but_not_faked() {
    let (_temp, endpoint, server) = spawn_grpc_server().await;
    let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

    let error = client
        .open(OwnerOpenRequest {
            access: Some(root_access()),
            path: b"notes.txt".to_vec(),
            flags: 0,
            mode: 0,
            expected_file_identity: None,
            kill_suidgid: false,
            disable_prefetch: false,
        })
        .await
        .unwrap_err();

    assert_owner_unimplemented(error);
    server.abort();
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_files_attr_dir_and_fsync_rpcs_are_registered_but_not_faked() {
    let (_temp, endpoint, server) = spawn_grpc_server().await;
    let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

    assert_owner_unimplemented(
        client
            .get_attr(OwnerGetAttrRequest {
                access: Some(root_access()),
                path: b"notes.txt".to_vec(),
                expected_file_identity: None,
                handle: None,
            })
            .await
            .unwrap_err(),
    );
    assert_owner_unimplemented(
        client
            .readdir(OwnerReaddirRequest {
                access: Some(root_access()),
                handle: Some(OwnerDirectoryHandle {
                    opaque: b"dir-handle".to_vec(),
                }),
                offset: 0,
                max_entries: 16,
            })
            .await
            .unwrap_err(),
    );
    assert_owner_unimplemented(
        client
            .fsync(OwnerFsyncRequest {
                access: Some(root_access()),
                handle: Some(OwnerHandle {
                    opaque: b"file-handle".to_vec(),
                }),
                datasync: true,
            })
            .await
            .unwrap_err(),
    );
    assert_owner_unimplemented(
        client
            .readlink(OwnerReadlinkRequest {
                access: Some(root_access()),
                path: b"link.txt".to_vec(),
                expected_file_identity: None,
            })
            .await
            .unwrap_err(),
    );

    server.abort();
}

#[cfg(feature = "ownerfs")]
struct AllowOwnerTestPeer;

#[cfg(feature = "ownerfs")]
impl PeerAuthenticator for AllowOwnerTestPeer {
    fn authenticate(
        &self,
        _: &MetadataMap,
        _: Option<SocketAddr>,
        _: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Ok("node-b".into())
    }
}

#[cfg(feature = "ownerfs")]
#[derive(Default)]
struct CaptureOwnerNamespaceHandler {
    seen_symlink: std::sync::Mutex<Option<OwnerSymlinkRequest>>,
    seen_link: std::sync::Mutex<Option<OwnerLinkRequest>>,
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesHandler for CaptureOwnerNamespaceHandler {
    fn symlink(
        &self,
        peer: &str,
        request: OwnerSymlinkRequest,
    ) -> afs_error::Result<OwnerSymlinkReply> {
        assert_eq!(peer, "node-b");
        *self.seen_symlink.lock().unwrap() = Some(request);
        Ok(OwnerSymlinkReply {
            attr: Some(test_owner_attr(
                b"symlink-id",
                afs_protocol::node_data::OwnerFileKind::Symlink,
            )),
            owner_session_id: "node-a-session-9".into(),
        })
    }

    fn link(&self, peer: &str, request: OwnerLinkRequest) -> afs_error::Result<OwnerLinkReply> {
        assert_eq!(peer, "node-b");
        *self.seen_link.lock().unwrap() = Some(request);
        Ok(OwnerLinkReply {
            attr: Some(test_owner_attr(
                b"link-id",
                afs_protocol::node_data::OwnerFileKind::Regular,
            )),
            owner_session_id: "node-a-session-9".into(),
        })
    }
}

#[cfg(feature = "ownerfs")]
enum OwnerReadFixture {
    Reply(OwnerReadReply),
    Error(afs_error::Error),
    Panic,
}

#[cfg(feature = "ownerfs")]
struct OwnerReadFixtureHandler {
    result: std::sync::Mutex<Option<OwnerReadFixture>>,
    seen_thread: std::sync::Mutex<Option<std::thread::ThreadId>>,
}

#[cfg(feature = "ownerfs")]
impl OwnerReadFixtureHandler {
    fn reply(data: &[u8], read: u32, eof: bool) -> Arc<Self> {
        Arc::new(Self {
            result: std::sync::Mutex::new(Some(OwnerReadFixture::Reply(OwnerReadReply {
                data: data.to_vec().into(),
                read,
                eof,
                data_checksum: b"must-be-cleared".to_vec(),
            }))),
            seen_thread: std::sync::Mutex::new(None),
        })
    }

    fn error(code: afs_error::ErrorCode) -> Arc<Self> {
        Arc::new(Self {
            result: std::sync::Mutex::new(Some(OwnerReadFixture::Error(afs_error::Error::coded(
                code,
                "fixture application error",
            )))),
            seen_thread: std::sync::Mutex::new(None),
        })
    }

    fn panic() -> Arc<Self> {
        Arc::new(Self {
            result: std::sync::Mutex::new(Some(OwnerReadFixture::Panic)),
            seen_thread: std::sync::Mutex::new(None),
        })
    }

    fn seen_thread(&self) -> std::thread::ThreadId {
        self.seen_thread.lock().unwrap().unwrap()
    }
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesHandler for OwnerReadFixtureHandler {
    fn read(&self, peer: &str, request: OwnerReadRequest) -> afs_error::Result<OwnerReadReply> {
        assert_eq!(peer, "node-b");
        assert_eq!(request.access.unwrap().root_id, "workspace-1");
        assert_eq!(request.handle.unwrap().opaque, b"file-handle".to_vec());
        *self.seen_thread.lock().unwrap() = Some(std::thread::current().id());
        match self.result.lock().unwrap().take().unwrap() {
            OwnerReadFixture::Reply(reply) => Ok(reply),
            OwnerReadFixture::Error(error) => Err(error),
            OwnerReadFixture::Panic => panic!("fixture owner read panic"),
        }
    }
}

#[cfg(feature = "ownerfs")]
fn owner_read_request(length: u32) -> OwnerReadRequest {
    OwnerReadRequest {
        access: Some(root_access()),
        handle: Some(OwnerHandle {
            opaque: b"file-handle".to_vec(),
        }),
        offset: 0,
        length,
        plane: Some(DataPlane {
            transfer: DataTransfer::GrpcInline.into(),
            rdma_session_id: 0,
            buffer_offset: 0,
        }),
    }
}

#[cfg(feature = "ownerfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_read_grpc_inline_keeps_short_read_and_eof_contract() {
    let handler = OwnerReadFixtureHandler::reply(b"abc", 3, true);
    let service = OwnerFilesService::new(handler.clone(), Arc::new(AllowOwnerTestPeer));

    let reply = service
        .read(Request::new(owner_read_request(8)))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(&reply.data[..], b"abc");
    assert_eq!(reply.read, 3);
    assert!(reply.eof);
    assert!(reply.data_checksum.is_empty());
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_read_grpc_inline_current_thread_keeps_spawn_blocking_fallback() {
    let handler = OwnerReadFixtureHandler::reply(b"abc", 3, true);
    let caller_thread = std::thread::current().id();
    let service = OwnerFilesService::new(handler.clone(), Arc::new(AllowOwnerTestPeer));

    let reply = service
        .read(Request::new(owner_read_request(8)))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(&reply.data[..], b"abc");
    assert_ne!(handler.seen_thread(), caller_thread);
}

#[cfg(feature = "ownerfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_read_grpc_inline_rejects_malformed_reply_length() {
    let handler = OwnerReadFixtureHandler::reply(b"abc", 4, false);
    let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

    let error = service
        .read(Request::new(owner_read_request(8)))
        .await
        .unwrap_err();

    assert_eq!(
        status_to_error(error).code(),
        afs_error::NODE_TRANSFER_INVALID
    );
}

#[cfg(feature = "ownerfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_read_grpc_inline_preserves_application_errors() {
    let handler = OwnerReadFixtureHandler::error(afs_error::IO_PERMISSION_DENIED);
    let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

    let error = service
        .read(Request::new(owner_read_request(8)))
        .await
        .unwrap_err();

    assert_eq!(
        status_to_error(error).code(),
        afs_error::IO_PERMISSION_DENIED
    );
}

#[cfg(feature = "ownerfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_read_grpc_inline_maps_handler_panic_to_worker_failure() {
    let handler = OwnerReadFixtureHandler::panic();
    let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

    let error = service
        .read(Request::new(owner_read_request(8)))
        .await
        .unwrap_err();

    assert_eq!(
        status_to_error(error).code(),
        afs_error::CLIENT_WORKER_FAILED
    );
}

#[cfg(feature = "ownerfs")]
fn test_owner_attr(
    identity: &[u8],
    kind: afs_protocol::node_data::OwnerFileKind,
) -> afs_protocol::node_data::OwnerFileAttr {
    afs_protocol::node_data::OwnerFileAttr {
        identity: Some(PbFileIdentity {
            opaque: identity.to_vec(),
        }),
        kind: kind as i32,
        mode: 0o644,
        uid: 60001,
        gid: 60001,
        size: 0,
        blocks: 0,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        nlink: 1,
        blksize: 4096,
        special_node: None,
    }
}

#[cfg(feature = "ownerfs")]
#[tokio::test]
async fn owner_files_namespace_rpcs_forward_caller_and_expected_identity() {
    let handler = Arc::new(CaptureOwnerNamespaceHandler::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn({
        let handler = handler.clone();
        async move {
            Server::builder()
                .add_service(make_owner_files_server_with_handler(
                    handler,
                    Arc::new(AllowOwnerTestPeer),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }
    });
    let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

    client
        .symlink(OwnerSymlinkRequest {
            access: Some(root_access()),
            link_path: b"dir/link".to_vec(),
            target: b"target".to_vec(),
            expected_parent: Some(PbFileIdentity {
                opaque: b"parent-id".to_vec(),
            }),
            caller: Some(OwnerCaller {
                uid: 60001,
                gid: 60002,
                pid: 77,
                umask: 0o027,
                supplementary_gids: vec![60003, 60004],
            }),
        })
        .await
        .unwrap();
    client
        .link(OwnerLinkRequest {
            access: Some(root_access()),
            old_path: b"dir/source".to_vec(),
            new_path: b"dir/hard".to_vec(),
            expected_old_identity: Some(PbFileIdentity {
                opaque: b"source-id".to_vec(),
            }),
            expected_new_parent: Some(PbFileIdentity {
                opaque: b"parent-id".to_vec(),
            }),
            caller: Some(OwnerCaller {
                uid: 60001,
                gid: 60002,
                pid: 78,
                umask: 0o077,
                supplementary_gids: vec![60003],
            }),
        })
        .await
        .unwrap();

    let symlink = handler.seen_symlink.lock().unwrap().take().unwrap();
    assert_eq!(symlink.link_path, b"dir/link");
    assert_eq!(symlink.target, b"target");
    assert_eq!(
        symlink.expected_parent.unwrap().opaque,
        b"parent-id".to_vec()
    );
    let symlink_caller = symlink.caller.unwrap();
    assert_eq!(symlink_caller.uid, 60001);
    assert_eq!(symlink_caller.gid, 60002);
    assert_eq!(symlink_caller.supplementary_gids, vec![60003, 60004]);

    let link = handler.seen_link.lock().unwrap().take().unwrap();
    assert_eq!(link.old_path, b"dir/source");
    assert_eq!(link.new_path, b"dir/hard");
    assert_eq!(
        link.expected_old_identity.unwrap().opaque,
        b"source-id".to_vec()
    );
    assert_eq!(
        link.expected_new_parent.unwrap().opaque,
        b"parent-id".to_vec()
    );
    assert_eq!(link.caller.unwrap().supplementary_gids, vec![60003]);
    server.abort();
}

#[cfg(feature = "ownerfs")]
fn root_access() -> RootAccess {
    RootAccess {
        root_id: "workspace-1".into(),
        root_epoch: 7,
        access_generation: 3,
        holder_node_id: "node-b".into(),
        home_node_id: "node-a".into(),
        session_id: "node-b-session-11".into(),
        fencing_token: "grant-token-7-3".into(),
        home_session_id: "node-a-session-9".into(),
    }
}

#[cfg(feature = "ownerfs")]
fn assert_owner_unimplemented(error: tonic::Status) {
    assert_eq!(error.code(), tonic::Code::Unimplemented);
    assert_eq!(
        status_to_error(error).code(),
        afs_error::NODE_VFS_UNIMPLEMENTED
    );
}

async fn spawn_grpc_server() -> (tempfile::TempDir, String, tokio::task::JoinHandle<()>) {
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap().join("target"))
        .join("afs-test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    let temp = tempfile::Builder::new().tempdir_in(base).unwrap();
    let storage = Arc::new(DiagnosticStorage::new(temp.path()).unwrap());
    let registry = RdmaSessionRegistry::new(None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn({
        let registry = registry.clone();
        async move {
            let server = Server::builder()
                .add_service(make_control_server(registry.clone()))
                .add_service(make_data_server(storage, registry));
            #[cfg(feature = "ownerfs")]
            let server = server.add_service(make_owner_files_server());
            server
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }
    });
    (temp, endpoint, server)
}
#[cfg(feature = "dfs")]
struct AllowDfsTestPeer;
#[cfg(feature = "dfs")]
impl PeerAuthenticator for AllowDfsTestPeer {
    fn authenticate(
        &self,
        _: &MetadataMap,
        _: Option<SocketAddr>,
        _: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Ok("reader".into())
    }
}
#[cfg(feature = "dfs")]
struct FixtureReplicaGrant(crate::node::replication::ReplicaPeerOp);
#[cfg(feature = "dfs")]
impl DfsReplicaAuthorizer for FixtureReplicaGrant {
    fn authorize(
        &self,
        peer: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        self.0.validate_sender(peer)?;
        if header.chunk_id != self.0.chunk_id.0 {
            return Err(replica_denied("fixture immutable identity differs"));
        }
        Ok(AuthorizedReplicaWrite {
            op: self.0.clone(),
            expires_at_unix_ms: u64::MAX,
        })
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_replica_grpc_stream_durable_retry_and_malformed_body() {
    use crate::node::chunk::StagedChunk;
    use afs_protocol::node_data::{
        dfs_chunks_client::DfsChunksClient, dfs_put_replica_frame::Body,
    };
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "receiver").unwrap());
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("replica-wire"),
        vec![37; 150 * 1024],
    );
    let target = crate::dfs::ReplicaTarget {
        node_id: "receiver".into(),
        node_epoch: 1,
        data_endpoint: "http://127.0.0.1:1".into(),
        device: local.device_descriptor().unwrap(),
    };
    let op = crate::node::replication::ReplicaPeerOp {
        chunk_id: staged.chunk.id.clone(),
        placement_revision: 1,
        placement_epoch: 1,
        replica_group_id: crate::dfs::ReplicaGroupId::new("fixture"),
        initiator_node_id: "reader".into(),
        initiator_node_epoch: 1,
        ordered_targets: vec![target.clone()],
        sync_target_count: 1,
        target_index: 0,
        target,
        chain_tail: vec![],
        repair_claim: None,
    };
    let service = make_dfs_chunks_server_with_replication(
        Some(local.clone()),
        Arc::new(AllowDfsTestPeer),
        Arc::new(DenyDfsReadAuthorizer),
        Arc::new(FixtureReplicaGrant(op.clone())),
        None,
        std::time::Duration::from_secs(5),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = DfsChunksClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let header = super::super::peer::replica_header(&op, &staged).unwrap();
    let frames = || {
        let mut result = vec![DfsPutReplicaFrame {
            body: Some(Body::Header(header.clone())),
        }];
        result.extend(staged.bytes().chunks(DFS_REPLICA_FRAME_BYTES).map(|data| {
            DfsPutReplicaFrame {
                body: Some(Body::Data(data.to_vec().into())),
            }
        }));
        result
    };
    let first = client
        .put_replica_stream(tokio_stream::iter(frames()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.durable_acks.len(), 1);
    assert_eq!(first.durable_acks[0].persisted_bytes, 150 * 1024);
    let retry = client
        .put_replica_stream(tokio_stream::iter(frames()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retry.durable_acks[0].verified_digest,
        first.durable_acks[0].verified_digest
    );
    let mut bad = frames();
    if let Some(Body::Data(bytes)) = &mut bad[1].body {
        let mut corrupt = bytes.to_vec();
        corrupt[0] ^= 1;
        *bytes = corrupt.into();
    }
    // A well-framed body that fails its immutable digest is corrupt data,
    // distinct from an invalid frame sequence or declared length.
    let corruption = client
        .put_replica_stream(tokio_stream::iter(bad))
        .await
        .unwrap_err();
    assert_eq!(corruption.code(), tonic::Code::DataLoss);
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(corruption).code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    let mut truncated = frames();
    truncated.pop();
    let mut oversized = frames();
    oversized[1].body = Some(Body::Data(vec![0; DFS_REPLICA_FRAME_BYTES + 1].into()));
    for malformed in [truncated, oversized] {
        assert_eq!(
            client
                .put_replica_stream(tokio_stream::iter(malformed))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    let duplicate = vec![
        DfsPutReplicaFrame {
            body: Some(Body::Header(header.clone())),
        },
        DfsPutReplicaFrame {
            body: Some(Body::Header(header)),
        },
    ];
    assert_eq!(
        client
            .put_replica_stream(tokio_stream::iter(duplicate))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut recovered = vec![0; staged.bytes().len()];
    assert_eq!(
        local.read_at(&staged.chunk.id, 0, &mut recovered).unwrap(),
        recovered.len()
    );
    assert_eq!(recovered, staged.bytes());
    assert!(local.quarantined_chunks().unwrap().is_empty());
    server.abort();
    drop(local);
    let reopened = LocalChunkStore::open(temp.path(), "receiver").unwrap();
    let mut recovered = vec![0; staged.bytes().len()];
    assert_eq!(
        reopened
            .read_at(&staged.chunk.id, 0, &mut recovered)
            .unwrap(),
        recovered.len()
    );
    assert_eq!(recovered, staged.bytes());
}

#[cfg(feature = "dfs")]
struct FixtureReplicaPeer(String);
#[cfg(feature = "dfs")]
impl PeerAuthenticator for FixtureReplicaPeer {
    fn authenticate(
        &self,
        _: &MetadataMap,
        _: Option<SocketAddr>,
        _: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Ok(self.0.clone())
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dfs_replica_grpc_three_hops_retry_missing_tail() {
    use crate::node::chunk::StagedChunk;
    use crate::node::replication::ReplicaDataPlane;
    let temp = tempfile::tempdir().unwrap();
    let mut stores = Vec::new();
    let mut listeners = Vec::new();
    let mut targets = Vec::new();
    for id in ["a", "b", "c"] {
        let store = Arc::new(LocalChunkStore::open(temp.path().join(id), id).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        targets.push(crate::dfs::ReplicaTarget {
            node_id: id.into(),
            node_epoch: 1,
            data_endpoint: format!("http://{}", listener.local_addr().unwrap()),
            device: store.device_descriptor().unwrap(),
        });
        stores.push(store);
        listeners.push(Some(listener));
    }
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("chain-wire"),
        vec![71; 180 * 1024],
    );
    let plane = Arc::new(
        super::super::peer::GrpcReplicaDataPlane::new(
            Arc::new(
                super::super::peer::PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    8,
                )
                .unwrap(),
            ),
            std::time::Duration::from_secs(2),
        )
        .unwrap(),
    );
    let op = crate::node::replication::ReplicaPeerOp {
        chunk_id: staged.chunk.id.clone(),
        placement_revision: 1,
        placement_epoch: 1,
        replica_group_id: crate::dfs::ReplicaGroupId::new("wire-chain"),
        initiator_node_id: "writer".into(),
        initiator_node_epoch: 1,
        ordered_targets: targets.clone(),
        sync_target_count: 3,
        target_index: 0,
        target: targets[0].clone(),
        chain_tail: targets[1..].to_vec(),
        repair_claim: None,
    };
    let mut ops = vec![op.clone()];
    ops.push(ops[0].next_hop().unwrap().unwrap());
    ops.push(ops[1].next_hop().unwrap().unwrap());
    let mut servers = Vec::new();
    for index in 0..2 {
        let service = make_dfs_chunks_server_with_replication(
            Some(stores[index].clone()),
            Arc::new(FixtureReplicaPeer(
                if index == 0 { "writer" } else { "a" }.into(),
            )),
            Arc::new(DenyDfsReadAuthorizer),
            Arc::new(FixtureReplicaGrant(ops[index].clone())),
            Some(plane.clone()),
            std::time::Duration::from_secs(5),
        );
        let listener = listeners[index].take().unwrap();
        servers.push(tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
    }
    let transfer = plane.clone();
    let first_op = op.clone();
    let first_staged = staged.clone();
    assert!(
        tokio::task::spawn_blocking(move || transfer.put_peer_replica(&first_op, &first_staged))
            .await
            .unwrap()
            .is_err()
    );
    for store in &stores[..2] {
        let mut bytes = vec![0; staged.bytes().len()];
        assert_eq!(
            store.read_at(&staged.chunk.id, 0, &mut bytes).unwrap(),
            bytes.len()
        );
        assert_eq!(bytes, staged.bytes());
    }
    let service = make_dfs_chunks_server_with_replication(
        Some(stores[2].clone()),
        Arc::new(FixtureReplicaPeer("b".into())),
        Arc::new(DenyDfsReadAuthorizer),
        Arc::new(FixtureReplicaGrant(ops[2].clone())),
        Some(plane.clone()),
        std::time::Duration::from_secs(5),
    );
    let listener = listeners[2].take().unwrap();
    servers.push(tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let retry_staged = staged.clone();
    let acks = tokio::task::spawn_blocking(move || plane.put_peer_replica(&op, &retry_staged))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        acks.iter()
            .map(|ack| ack.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    for server in servers {
        server.abort();
    }
    drop(stores);
    for id in ["a", "b", "c"] {
        let store = LocalChunkStore::open(temp.path().join(id), id).unwrap();
        let mut bytes = vec![0; staged.bytes().len()];
        assert_eq!(
            store.read_at(&staged.chunk.id, 0, &mut bytes).unwrap(),
            bytes.len()
        );
        assert_eq!(bytes, staged.bytes());
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone)]
struct DelayFirstMetaReplyLayer {
    claim: Arc<std::sync::atomic::AtomicBool>,
    report: Arc<std::sync::atomic::AtomicBool>,
    claim_hits: Arc<std::sync::atomic::AtomicUsize>,
    report_hits: Arc<std::sync::atomic::AtomicUsize>,
    delay: std::time::Duration,
}

#[cfg(feature = "dfs")]
impl DelayFirstMetaReplyLayer {
    fn new(delay: std::time::Duration) -> Self {
        Self {
            claim: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            report: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            claim_hits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            report_hits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            delay,
        }
    }

    fn claim_hits(&self) -> usize {
        self.claim_hits.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn report_hits(&self) -> usize {
        self.report_hits.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(feature = "dfs")]
impl<S> tower::Layer<S> for DelayFirstMetaReplyLayer {
    type Service = DelayFirstMetaReplyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DelayFirstMetaReplyService {
            inner,
            claim: self.claim.clone(),
            report: self.report.clone(),
            claim_hits: self.claim_hits.clone(),
            report_hits: self.report_hits.clone(),
            delay: self.delay,
        }
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone)]
struct DelayFirstMetaReplyService<S> {
    inner: S,
    claim: Arc<std::sync::atomic::AtomicBool>,
    report: Arc<std::sync::atomic::AtomicBool>,
    claim_hits: Arc<std::sync::atomic::AtomicUsize>,
    report_hits: Arc<std::sync::atomic::AtomicUsize>,
    delay: std::time::Duration,
}

#[cfg(feature = "dfs")]
impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for DelayFirstMetaReplyService<S>
where
    S: tower::Service<tonic::codegen::http::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let future = self.inner.call(request);
        let claim = self.claim.clone();
        let report = self.report.clone();
        let claim_hits = self.claim_hits.clone();
        let report_hits = self.report_hits.clone();
        let delay = self.delay;
        Box::pin(async move {
            let response = future.await?;
            let should_delay = match path.as_str() {
                "/afs.meta.v1.DfsMeta/ClaimReplicationTask"
                    if claim.swap(false, std::sync::atomic::Ordering::SeqCst) =>
                {
                    claim_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    true
                }
                "/afs.meta.v1.DfsMeta/ReportReplicationTask"
                    if report.swap(false, std::sync::atomic::Ordering::SeqCst) =>
                {
                    report_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    true
                }
                _ => false,
            };
            if should_delay {
                tokio::time::sleep(delay).await;
            }
            Ok(response)
        })
    }
}

#[cfg(feature = "dfs")]
async fn grpc_unknown_repair_fixture() -> (
    tempfile::TempDir,
    Arc<LocalChunkStore>,
    Arc<LocalChunkStore>,
    afs_metrics::Registry,
    crate::dfs::ChunkObject,
    Arc<dyn crate::meta::store::MetaStore>,
    String,
    DelayFirstMetaReplyLayer,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    use crate::dfs::*;
    use crate::meta::store::{
        MetaEntity, MetaStore, MetaTxn, NodeSessionLease, OperationResult, RequestKey,
        RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation, journal::MemoryLog,
    };
    use crate::node::chunk::StagedChunk;

    let temp = tempfile::tempdir().unwrap();
    let source_store =
        Arc::new(LocalChunkStore::open(temp.path().join("source"), "source").unwrap());
    let target_store =
        Arc::new(LocalChunkStore::open(temp.path().join("target"), "target").unwrap());
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_endpoint = format!("http://{}", target_listener.local_addr().unwrap());

    let staged = StagedChunk::new(OperationId::new("grpc-unknown-source"), vec![23; 64 * 1024]);
    let source_target = ReplicaTarget {
        node_id: "source".into(),
        node_epoch: 1,
        data_endpoint: "http://127.0.0.1:1".into(),
        device: source_store.device_descriptor().unwrap(),
    };
    let source_acks = source_store
        .persist_batch(std::slice::from_ref(&staged), &source_target, 1, 1)
        .unwrap();
    let source_ack = source_acks.into_iter().next().unwrap();
    let source_copy = CopyRecord {
        id: CopyId::new(format!(
            "source:1:{}:{}",
            source_ack.device_id, staged.chunk.id.0
        )),
        chunk_id: staged.chunk.id.clone(),
        role: CopyRole::DurableReplica,
        location: CopyLocation::Node {
            node_id: "source".into(),
            node_epoch: 1,
            device_id: source_ack.device_id.clone(),
            device_epoch: source_ack.device_epoch,
            catalog_revision: source_ack.catalog_revision,
        },
        state: CopyState::Ready,
        persisted_bytes: source_ack.persisted_bytes,
        verified_digest: source_ack.verified_digest,
    };

    let store: Arc<dyn MetaStore> = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    for (node_id, data_addr, device) in [
        (
            "source",
            "http://127.0.0.1:1".to_owned(),
            source_store.device_descriptor().unwrap(),
        ),
        (
            "target",
            target_endpoint.clone(),
            target_store.device_descriptor().unwrap(),
        ),
    ] {
        store
            .register_node_session(
                RequestKey::new(node_id, "register"),
                NodeSessionLease {
                    node_id: node_id.into(),
                    session_id: format!("{node_id}-session"),
                    grpc_addr: data_addr.clone(),
                    data_addr,
                    rest_addr: "http://127.0.0.1:1".into(),
                    ownerfs_enabled: true,
                    storage_devices: vec![device],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
    }
    let dfs = crate::meta::dfs::DfsService::with_replication_config(
        store.clone(),
        ReplicationConfig {
            desired_copies: 2,
            sync_required_copies: 2,
            min_distinct_nodes: 1,
            min_distinct_failure_domains: 1,
            local_copy: LocalCopyPolicy::Required,
        },
    );
    dfs.initialize_replication_config().await.unwrap();
    let key = RequestKey::new("fixture", "seed-underreplicated");
    let task_id = ReplicationTaskId::new(format!("repair:{}", staged.chunk.id.0));
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
        TxnMutation::Put(MetaEntity::DfsCopy(source_copy.clone())),
        TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
            chunk_id: staged.chunk.id.clone(),
            replica_group_id: ReplicaGroupId::new("repair-grpc"),
            placement_epoch: 1,
            desired_copies: 2,
            copies: vec![source_copy.id.clone()],
            health: PlacementHealth::UnderReplicated,
        })),
        TxnMutation::Put(MetaEntity::DfsReplicationTask(ReplicationTask {
            id: task_id,
            chunk_id: staged.chunk.id.clone(),
            placement_epoch: 1,
            desired_copies: 2,
            existing_copies: vec![source_copy.id],
            state: ReplicationTaskState::Pending,
            attempt: 0,
            next_retry_unix_ms: 0,
            last_error: None,
            claim: None,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();

    let mut meta = crate::meta::Meta::with_store(
        "meta-grpc-unknown".into(),
        crate::runtime::Observability::new().unwrap(),
        store.clone(),
    );
    // Preserve the failure-injecting DFS authority rather than replacing
    // it with the constructor's ordinary service.
    meta.dfs = Some(dfs);
    let meta = Arc::new(meta);
    let meta_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_endpoint = format!("http://{}", meta_listener.local_addr().unwrap());
    let delay_layer = DelayFirstMetaReplyLayer::new(std::time::Duration::from_secs(2));
    let meta_delay_probe = delay_layer.clone();
    let meta_server = tokio::spawn(async move {
        Server::builder()
            .layer(delay_layer)
            .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                crate::meta::rpc::DfsMetaRpc(meta),
            ))
            .serve_with_incoming(TcpListenerStream::new(meta_listener))
            .await
            .unwrap();
    });

    let meta_for_target = Arc::new(
        super::super::meta::GrpcDfsMeta::new(
            &meta_endpoint,
            "target".into(),
            "target-session".into(),
            NamespaceId::new("default"),
            std::time::Duration::from_secs(2),
            afs_transport::TlsConfig::Disabled,
        )
        .unwrap(),
    );
    let registry = afs_metrics::Registry::new();
    let metrics = DfsPayloadMetrics::register(&registry).unwrap();
    let target_service = make_dfs_chunks_server_with_transport(
        Some(target_store.clone()),
        Arc::new(FixtureReplicaPeer("source".into())),
        Arc::new(DenyDfsReadAuthorizer),
        Arc::new(MetaReplicaAuthorizer {
            meta: meta_for_target,
            node_id: "target".into(),
            node_epoch: 1,
        }),
        None,
        std::time::Duration::from_secs(2),
        DfsChunkTransportResources {
            rdma_sessions: super::super::control::RdmaSessionRegistry::new(None),
            payload_metrics: Some(metrics),
        },
    );
    let target_server = tokio::spawn(async move {
        Server::builder()
            .add_service(target_service)
            .serve_with_incoming(TcpListenerStream::new(target_listener))
            .await
            .unwrap();
    });

    (
        temp,
        source_store,
        target_store.clone(),
        registry,
        staged.chunk,
        store,
        meta_endpoint,
        meta_delay_probe,
        meta_server,
        target_server,
    )
}

#[cfg(feature = "dfs")]
async fn stored_replication_task(
    store: &dyn crate::meta::store::MetaStore,
    chunk: &crate::dfs::ChunkObject,
) -> crate::dfs::ReplicationTask {
    read_repair_meta_snapshot(store, chunk).await.task
}

#[cfg(feature = "dfs")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairMetaSnapshot {
    task_revision: crate::meta::store::StoreRevision,
    placement_revision: crate::meta::store::StoreRevision,
    claim_outcome_revision: crate::meta::store::StoreRevision,
    report_outcome_revision: Option<crate::meta::store::StoreRevision>,
    task: crate::dfs::ReplicationTask,
    placement: crate::dfs::PlacementRecord,
    copies: Vec<(
        crate::dfs::CopyId,
        crate::meta::store::StoreRevision,
        crate::dfs::CopyRecord,
    )>,
    claim_outcome: crate::meta::store::RequestOutcome,
    report_outcome: Option<crate::meta::store::RequestOutcome>,
}

#[cfg(feature = "dfs")]
async fn read_repair_meta_snapshot(
    store: &dyn crate::meta::store::MetaStore,
    chunk: &crate::dfs::ChunkObject,
) -> RepairMetaSnapshot {
    use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
    let task_snapshot = store
        .read(MetaRead::DfsReplicationTask(
            crate::dfs::ReplicationTaskId::new(format!("repair:{}", chunk.id.0)),
        ))
        .await
        .unwrap();
    let task = match task_snapshot.entity {
        Some(MetaEntity::DfsReplicationTask(task)) => task,
        _ => panic!("replication task missing"),
    };
    let placement_snapshot = store
        .read(MetaRead::DfsPlacement(chunk.id.clone()))
        .await
        .unwrap();
    let placement = match placement_snapshot.entity {
        Some(MetaEntity::DfsPlacement(placement)) => placement,
        _ => panic!("placement missing"),
    };
    let mut copies = Vec::new();
    let mut copy_ids = placement.copies.clone();
    copy_ids.sort();
    for copy_id in copy_ids {
        let copy_snapshot = store
            .read(MetaRead::DfsCopy(copy_id.clone()))
            .await
            .unwrap();
        let copy = match copy_snapshot.entity {
            Some(MetaEntity::DfsCopy(copy)) => copy,
            _ => panic!("copy missing"),
        };
        copies.push((copy_id, copy_snapshot.revision, copy));
    }
    let claim_outcome_snapshot = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "source",
            "repair-claim:source-session:1",
        )))
        .await
        .unwrap();
    let claim_outcome = claim_outcome_snapshot
        .request_outcome
        .expect("claim request outcome missing");
    let report_outcome_snapshot = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "source",
            "repair-report:source-session:2",
        )))
        .await
        .unwrap();
    RepairMetaSnapshot {
        task_revision: task_snapshot.revision,
        placement_revision: placement_snapshot.revision,
        claim_outcome_revision: claim_outcome_snapshot.revision,
        report_outcome_revision: report_outcome_snapshot
            .request_outcome
            .as_ref()
            .map(|_| report_outcome_snapshot.revision),
        task,
        placement,
        copies,
        claim_outcome,
        report_outcome: report_outcome_snapshot.request_outcome,
    }
}

#[cfg(feature = "dfs")]
async fn run_repair_tick(
    worker: Arc<crate::node::replication::ReplicationWorker>,
) -> afs_error::Result<Option<crate::dfs::ReplicationTask>> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || worker.run_once()),
    )
    .await
    .expect("repair worker tick timed out")
    .unwrap()
}

#[cfg(feature = "dfs")]
fn read_whole_chunk(store: &LocalChunkStore, chunk: &crate::dfs::ChunkObject) -> Vec<u8> {
    let mut bytes = vec![0; chunk.length as usize];
    assert_eq!(
        store.read_at(&chunk.id, 0, &mut bytes).unwrap(),
        bytes.len()
    );
    bytes
}

#[cfg(feature = "dfs")]
fn grpc_replica_payload_bytes(registry: &afs_metrics::Registry) -> u64 {
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .find_map(|line| {
            if line.starts_with("afs_dfs_payload_bytes_total{")
                && line.contains("direction=\"recv\"")
                && line.contains("operation=\"replica\"")
                && line.contains("transport=\"grpc\"")
            {
                line.rsplit_once(' ')
                    .and_then(|(_, value)| value.parse::<u64>().ok())
            } else {
                None
            }
        })
        .unwrap_or(0)
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_worker_replays_real_grpc_claim_and_report_after_lost_ack() {
    use crate::meta::store::{OperationResult, RequestKey, StoreOperation};
    use crate::node::replication::ReplicationWorker;
    let (
        _temp,
        source_store,
        target_store,
        registry,
        chunk,
        store,
        meta_endpoint,
        meta_delay_probe,
        meta_server,
        target_server,
    ) = grpc_unknown_repair_fixture().await;
    let authority = Arc::new(
        super::super::meta::GrpcDfsMeta::new(
            &meta_endpoint,
            "source".into(),
            "source-session".into(),
            crate::dfs::NamespaceId::new("default"),
            std::time::Duration::from_millis(200),
            afs_transport::TlsConfig::Disabled,
        )
        .unwrap(),
    );
    let data_plane = Arc::new(
        super::super::peer::GrpcReplicaDataPlane::new(
            Arc::new(
                super::super::peer::PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    4,
                )
                .unwrap(),
            ),
            std::time::Duration::from_secs(2),
        )
        .unwrap(),
    );
    let worker = Arc::new(ReplicationWorker::new(
        "source".into(),
        1,
        "source-session".into(),
        source_store,
        authority,
        data_plane,
    ));

    let first = run_repair_tick(worker.clone()).await;
    assert!(first.is_err());
    assert_eq!(meta_delay_probe.claim_hits(), 1);
    assert_eq!(meta_delay_probe.report_hits(), 0);
    assert_eq!(grpc_replica_payload_bytes(&registry), 0);
    let running = stored_replication_task(store.as_ref(), &chunk).await;
    assert_eq!(running.state, crate::dfs::ReplicationTaskState::Running);
    let running_claim = running
        .claim
        .as_deref()
        .expect("claim committed before timeout");
    assert_eq!(
        running_claim.operation_id.0,
        "repair-claim:source-session:1"
    );
    assert_eq!(running_claim.worker_node_id, "source");
    assert_eq!(running_claim.worker_session_id, "source-session");
    let after_claim_unknown = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
    assert_eq!(
        after_claim_unknown.claim_outcome.request,
        RequestKey::new("source", "repair-claim:source-session:1")
    );
    assert_eq!(
        after_claim_unknown.claim_outcome.operation,
        StoreOperation::DfsClaimReplicationTask
    );
    assert_eq!(after_claim_unknown.report_outcome, None);
    assert_eq!(after_claim_unknown.report_outcome_revision, None);
    match &after_claim_unknown.claim_outcome.result {
        OperationResult::DfsReplicationClaim {
            claim: Some(claim), ..
        } => {
            assert_eq!(claim.operation_id.0, "repair-claim:source-session:1");
        }
        other => panic!("unexpected claim outcome: {other:?}"),
    }

    let second = run_repair_tick(worker.clone()).await;
    assert!(second.is_err());
    assert_eq!(meta_delay_probe.claim_hits(), 1);
    assert_eq!(meta_delay_probe.report_hits(), 1);
    assert_eq!(grpc_replica_payload_bytes(&registry), chunk.length);
    assert_eq!(
        read_whole_chunk(target_store.as_ref(), &chunk),
        vec![23; chunk.length as usize]
    );
    let after_report_unknown = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
    assert_eq!(
        after_report_unknown.claim_outcome_revision,
        after_claim_unknown.claim_outcome_revision.next()
    );
    assert_eq!(
        after_report_unknown.task.state,
        crate::dfs::ReplicationTaskState::Completed
    );
    assert_eq!(after_report_unknown.task.claim, None);
    assert_eq!(after_report_unknown.task.attempt, 1);
    assert_eq!(after_report_unknown.task.existing_copies.len(), 2);
    assert_eq!(
        after_report_unknown.placement.health,
        crate::dfs::PlacementHealth::Satisfied
    );
    assert_eq!(after_report_unknown.placement.copies.len(), 2);
    for (_, _, copy) in &after_report_unknown.copies {
        assert_eq!(copy.state, crate::dfs::CopyState::Ready);
        assert_eq!(copy.persisted_bytes, chunk.length);
        assert_eq!(copy.verified_digest, chunk.content_digest);
    }
    let report_outcome = after_report_unknown.report_outcome.as_ref().unwrap();
    assert_eq!(
        report_outcome.request,
        RequestKey::new("source", "repair-report:source-session:2")
    );
    assert_eq!(
        report_outcome.operation,
        StoreOperation::DfsReportReplicationTask
    );
    assert!(after_report_unknown.report_outcome_revision.is_some());
    match &report_outcome.result {
        OperationResult::DfsNamespace { result, .. } => match result.as_ref() {
            OperationResult::DfsReplicationTask(task) => {
                assert_eq!(task.state, crate::dfs::ReplicationTaskState::Completed);
                assert_eq!(
                    task.existing_copies,
                    after_report_unknown.task.existing_copies
                );
            }
            other => panic!("unexpected report outcome: {other:?}"),
        },
        other => panic!("unexpected report outcome wrapper: {other:?}"),
    }

    let third = run_repair_tick(worker.clone()).await.unwrap();
    assert_eq!(third.unwrap(), after_report_unknown.task);
    assert_eq!(meta_delay_probe.claim_hits(), 1);
    assert_eq!(meta_delay_probe.report_hits(), 1);
    assert_eq!(grpc_replica_payload_bytes(&registry), chunk.length);
    assert_eq!(
        read_whole_chunk(target_store.as_ref(), &chunk),
        vec![23; chunk.length as usize]
    );
    let after_report_replay = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
    assert_eq!(after_report_replay, after_report_unknown);

    target_server.abort();
    meta_server.abort();
}

#[cfg(feature = "dfs")]
struct MemoryReadGrantValidator {
    service: crate::meta::dfs::DfsService,
    runtime: tokio::runtime::Handle,
    calls: std::sync::atomic::AtomicUsize,
    unavailable: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "dfs")]
struct SkewedReadGrantValidator {
    change_identity: bool,
    exceed_grant_expiry: bool,
}

#[cfg(feature = "dfs")]
impl DfsReadGrantValidator for SkewedReadGrantValidator {
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Ok(request
            .validations
            .into_iter()
            .map(|mut validation| {
                let expires_at_unix_ms = if self.exceed_grant_expiry {
                    validation.grant.expires_at_unix_ms + 1
                } else {
                    assert!(validation.grant.expires_at_unix_ms > now + 5_600);
                    now + 5_500
                };
                let allowed_ranges = vec![(validation.chunk_offset, validation.length)];
                if self.change_identity {
                    validation.copy_id = crate::dfs::CopyId::new("another-copy");
                }
                crate::dfs::DfsAuthorizedRead {
                    validation,
                    allowed_ranges,
                    expires_at_unix_ms,
                }
            })
            .collect())
    }
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_read_authority_tolerates_meta_clock_skew_without_extending_cache() {
    let (_meta, _local, _temp, request) = memory_read_authority_fixture().await;
    let authorizer = CachedDfsReadAuthorizer::new(
        Arc::new(SkewedReadGrantValidator {
            change_identity: false,
            exceed_grant_expiry: false,
        }),
        "receiver".into(),
        1,
    );
    authorizer.authorize("reader", &request).unwrap();
    let cache = authorizer.cache.lock().unwrap();
    assert!(!cache.is_empty());
    for entry in cache.values() {
        let remaining = entry
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        assert!(remaining > std::time::Duration::ZERO);
        assert!(remaining <= std::time::Duration::from_secs(5));
    }
    drop(cache);

    for (change_identity, exceed_grant_expiry) in [(true, false), (false, true)] {
        let authorizer = CachedDfsReadAuthorizer::new(
            Arc::new(SkewedReadGrantValidator {
                change_identity,
                exceed_grant_expiry,
            }),
            "receiver".into(),
            1,
        );
        assert!(authorizer.authorize("reader", &request).is_err());
        assert!(authorizer.cache.lock().unwrap().is_empty());
    }
}
#[cfg(feature = "dfs")]
impl DfsReadGrantValidator for MemoryReadGrantValidator {
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "test Meta unavailable",
            ));
        }
        self.runtime
            .block_on(self.service.validate_read_grants(request))
    }
}

#[cfg(feature = "dfs")]
async fn memory_read_authority_fixture() -> (
    Arc<crate::meta::Meta>,
    Arc<LocalChunkStore>,
    tempfile::TempDir,
    DfsReadRangesRequest,
) {
    use crate::dfs::*;
    use crate::meta::store::{
        MetaEntity, MetaStore, MetaTxn, NodeSessionLease, OperationResult, RequestKey,
        RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation, journal::MemoryLog,
    };
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "receiver").unwrap());
    let staged = StagedChunk::new(OperationId::new("read-fixture"), vec![9; 150 * 1024]);
    local.put(staged.clone()).unwrap();
    let device = local.device_descriptor().unwrap();
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    for id in ["reader", "receiver"] {
        store
            .register_node_session(
                RequestKey::new(id, "register"),
                NodeSessionLease {
                    node_id: id.into(),
                    session_id: format!("session-{id}"),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:1".into(),
                    rest_addr: "http://127.0.0.1:1".into(),
                    ownerfs_enabled: true,
                    storage_devices: vec![device.clone()],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
    }
    let meta = Arc::new(crate::meta::Meta::with_store(
        "meta-test".into(),
        crate::runtime::Observability::new().unwrap(),
        store.clone(),
    ));
    let mut inode = crate::meta::dfs::DfsService::new(store.clone())
        .get_inode(InodeId::new("1"))
        .await
        .unwrap();
    inode.inode_id = InodeId::new("read-inode");
    inode.kind = InodeKind::Regular;
    inode.head_version = Some(FileVersionId::new("version"));
    let layout = LayoutRoot {
        id: LayoutRootId::new("layout"),
        file_length: 100_000,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: 100_000,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 1024,
        }],
    };
    let copy = CopyRecord {
        id: CopyId::new("copy"),
        chunk_id: staged.chunk.id.clone(),
        role: CopyRole::DurableReplica,
        state: CopyState::Ready,
        location: CopyLocation::Node {
            node_id: "receiver".into(),
            node_epoch: 1,
            device_id: device.device_id,
            device_epoch: device.device_epoch,
            catalog_revision: device.catalog_revision,
        },
        persisted_bytes: staged.chunk.length,
        verified_digest: staged.chunk.content_digest.clone(),
    };
    let key = RequestKey::new("fixture", "seed");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations = vec![
        TxnMutation::Put(MetaEntity::DfsInode(inode)),
        TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
            id: FileVersionId::new("version"),
            inode_id: InodeId::new("read-inode"),
            parent_version: None,
            length: layout.file_length,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        })),
        TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
        TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
        TxnMutation::Put(MetaEntity::DfsCopy(copy)),
        TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
            chunk_id: staged.chunk.id.clone(),
            replica_group_id: ReplicaGroupId::new("group"),
            placement_epoch: 1,
            desired_copies: 1,
            copies: vec![CopyId::new("copy")],
            health: PlacementHealth::Satisfied,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ];
    store.compare_and_commit(txn).await.unwrap();
    let sources = meta
        .dfs
        .as_ref()
        .unwrap()
        .chunk_sources(DfsChunkSourcesRequest {
            caller_id: "reader".into(),
            namespace_id: NamespaceId::new("default"),
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            chunk_ids: vec![staged.chunk.id.clone()],
        })
        .await
        .unwrap();
    let source = &sources.chunks[0].sources[0];
    let grant = &source.read_grant;
    let request = DfsReadRangesRequest {
        include_blake3_outboard: false,
        read_id: "read".into(),
        attempt_id: "attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        operations: vec![afs_protocol::node_data::DfsChunkReadOp {
            operation_index: 0,
            chunk_id: staged.chunk.id.0,
            chunk_offset: 1024,
            length: 75_000,
            destination_offset: 0,
            source_copy_id: "copy".into(),
            grant: Some(afs_protocol::node_data::DfsReadGrant {
                namespace_id: grant.namespace_id.0.clone(),
                file_version_id: grant.file_version_id.0.clone(),
                layout_root_id: grant.layout_root_id.0.clone(),
                caller_node_id: grant.caller_node_id.clone(),
                caller_node_epoch: grant.caller_node_epoch,
                expires_at_unix_ms: grant.expires_at_unix_ms,
                fence: grant.fence,
                token: grant.token.clone(),
            }),
        }],
    };
    (meta, local, temp, request)
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_read_authority_batch_cache_and_adversarial_identity() {
    let (meta, _local, _temp, request) = memory_read_authority_fixture().await;
    let validator = Arc::new(MemoryReadGrantValidator {
        service: meta.dfs.as_ref().unwrap().clone(),
        runtime: tokio::runtime::Handle::current(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    });
    let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
        validator.clone(),
        "receiver".into(),
        1,
    ));
    let good = request.clone();
    let actor = authorizer.clone();
    tokio::task::spawn_blocking(move || {
        let mut batched = good.clone();
        let mut second = batched.operations[0].clone();
        second.operation_index = 1;
        second.chunk_offset = 80_000;
        second.length = 4_000;
        second.destination_offset = 75_000;
        batched.operations.push(second);
        actor.authorize("reader", &batched).unwrap();
        actor.authorize("reader", &good).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(authorizer.cache.lock().unwrap().len(), 1);
    let original_deadline = authorizer
        .cache
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .deadline;
    validator
        .unavailable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let actor = authorizer.clone();
    let good = request.clone();
    tokio::task::spawn_blocking(move || actor.authorize("reader", &good))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        authorizer
            .cache
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .deadline,
        original_deadline
    );
    for entry in authorizer.cache.lock().unwrap().values_mut() {
        entry.deadline = std::time::Instant::now();
    }
    let actor = authorizer.clone();
    let good = request.clone();
    assert!(
        tokio::task::spawn_blocking(move || actor.authorize("reader", &good))
            .await
            .unwrap()
            .is_err()
    );
    validator
        .unavailable
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let mutations: Vec<fn(&mut DfsReadRangesRequest)> = vec![
        |request| {
            request.operations[0].grant.as_mut().unwrap().token =
                format!("dfs-read-v1:{}", "0".repeat(64))
        },
        |request| {
            request.operations[0]
                .grant
                .as_mut()
                .unwrap()
                .caller_node_epoch += 1
        },
        |request| {
            request.operations[0]
                .grant
                .as_mut()
                .unwrap()
                .expires_at_unix_ms = 1
        },
        |request| request.operations[0].grant.as_mut().unwrap().namespace_id = "another".into(),
        |request| request.operations[0].source_copy_id = "other-copy".into(),
        |request| request.operations[0].chunk_id = "other-chunk".into(),
        |request| request.operations[0].chunk_offset = 0,
        |request| request.operations[0].length = 150 * 1024,
    ];
    for mutate in mutations {
        let mut bad = request.clone();
        mutate(&mut bad);
        let actor = authorizer.clone();
        assert!(
            tokio::task::spawn_blocking(move || actor.authorize("reader", &bad))
                .await
                .unwrap()
                .is_err()
        );
    }
    let actor = authorizer.clone();
    let good = request.clone();
    assert!(
        tokio::task::spawn_blocking(move || actor.authorize("intruder", &good))
            .await
            .unwrap()
            .is_err()
    );
    let wrong_receiver = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 2);
    let good = request.clone();
    assert!(
        tokio::task::spawn_blocking(move || wrong_receiver.authorize("reader", &good))
            .await
            .unwrap()
            .is_err()
    );
    // Simulate capability-cache expiry; a Meta restart changes its MAC key,
    // so an old grant cannot recover through a permissive miss fallback.
    for entry in authorizer.cache.lock().unwrap().values_mut() {
        entry.deadline = std::time::Instant::now();
    }
    let restarted = crate::meta::dfs::DfsService::new(meta.store.as_ref().unwrap().clone());
    let replacement = MemoryReadGrantValidator {
        service: restarted,
        runtime: tokio::runtime::Handle::current(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    };
    let restarted_authorizer =
        CachedDfsReadAuthorizer::new(Arc::new(replacement), "receiver".into(), 1);
    assert!(
        tokio::task::spawn_blocking(move || restarted_authorizer.authorize("reader", &request))
            .await
            .unwrap()
            .is_err()
    );
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_read_authority_session_rotation_is_bounded_by_original_cache_lease() {
    use crate::meta::store::{NodeSessionLease, RequestKey};
    let (meta, local, _temp, request) = memory_read_authority_fixture().await;
    let validator = Arc::new(MemoryReadGrantValidator {
        service: meta.dfs.as_ref().unwrap().clone(),
        runtime: tokio::runtime::Handle::current(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    });
    let actor = Arc::new(CachedDfsReadAuthorizer::new(
        validator.clone(),
        "receiver".into(),
        1,
    ));
    let authorized = actor.clone();
    let good = request.clone();
    tokio::task::spawn_blocking(move || authorized.authorize("reader", &good))
        .await
        .unwrap()
        .unwrap();
    meta.store
        .as_ref()
        .unwrap()
        .register_node_session(
            RequestKey::new("reader", "replacement-registration"),
            NodeSessionLease {
                node_id: "reader".into(),
                session_id: "new-reader-session".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:1".into(),
                rest_addr: "http://127.0.0.1:1".into(),
                ownerfs_enabled: true,
                storage_devices: vec![local.device_descriptor().unwrap()],
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    // Already validated capabilities retain only their original lease.
    let authorized = actor.clone();
    let good = request.clone();
    tokio::task::spawn_blocking(move || authorized.authorize("reader", &good))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    for entry in actor.cache.lock().unwrap().values_mut() {
        entry.deadline = std::time::Instant::now();
    }
    assert!(
        tokio::task::spawn_blocking(move || actor.authorize("reader", &request))
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dfs_read_grants_meta_and_peer_grpc_stream_real_authority() {
    use afs_protocol::node_data::dfs_chunks_client::DfsChunksClient;
    use tokio_stream::StreamExt;
    let (meta, local, _temp, request) = memory_read_authority_fixture().await;
    let meta_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_endpoint = format!("http://{}", meta_listener.local_addr().unwrap());
    let meta_server = tokio::spawn(async move {
        Server::builder()
            .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                crate::meta::rpc::DfsMetaRpc(meta),
            ))
            .serve_with_incoming(TcpListenerStream::new(meta_listener))
            .await
            .unwrap();
    });
    let adapter = Arc::new(
        super::super::meta::GrpcDfsMeta::new(
            &meta_endpoint,
            "receiver".into(),
            "session-receiver".into(),
            crate::dfs::NamespaceId::new("default"),
            std::time::Duration::from_secs(5),
            afs_transport::TlsConfig::Disabled,
        )
        .unwrap(),
    );
    let service = make_dfs_chunks_server(
        Some(local),
        Arc::new(AllowDfsTestPeer),
        Arc::new(CachedDfsReadAuthorizer::new(adapter, "receiver".into(), 1)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let peer_server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = DfsChunksClient::connect(peer_endpoint).await.unwrap();
    let mut stream = client
        .read_ranges(request.clone())
        .await
        .unwrap()
        .into_inner();
    let mut received = Vec::new();
    let mut completions = 0;
    while let Some(frame) = stream.next().await {
        match frame.unwrap().body.unwrap() {
            dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
            dfs_read_ranges_frame::Body::Completion(_) => completions += 1,
            _ => {}
        }
    }
    assert_eq!(received, vec![9; 75_000]);
    assert_eq!(completions, 1);
    let mut forged = request;
    forged.operations[0].grant.as_mut().unwrap().token = format!("dfs-read-v1:{}", "0".repeat(64));
    assert_eq!(
        client.read_ranges(forged).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    peer_server.abort();
    meta_server.abort();
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_rdma_packed_ranges_reject_gaps_capacity_and_wrong_caller() {
    let mut request = dfs_read_request("chunk".into(), 75_000);
    assert_eq!(validate_packed_dfs_read(&request).unwrap(), 75_000);
    assert_eq!(dfs_read_caller_epoch("reader", &request).unwrap(), 1);
    assert!(dfs_read_caller_epoch("intruder", &request).is_err());
    request.operations[0].destination_offset = 1;
    assert!(validate_packed_dfs_read(&request).is_err());
    request.operations[0].destination_offset = 0;
    request.operations[0].length = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64 + 1;
    assert!(validate_packed_dfs_read(&request).is_err());
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit Linux RXE device; product payload, not diagnostic NodeData"]
async fn dfs_product_rdma_replica_and_read_real_verbs() {
    dfs_product_rdma_replica_and_read_real_verbs_with_mode(
        super::super::peer::DataMode::Rdma,
        true,
    )
    .await;
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit Linux RXE device; product payload, not diagnostic NodeData"]
async fn dfs_auto_prefers_rdma_replica_and_read_real_verbs() {
    dfs_product_rdma_replica_and_read_real_verbs_with_mode(
        super::super::peer::DataMode::Auto,
        true,
    )
    .await;
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux RXE client; receiver deliberately has no RDMA device"]
async fn dfs_auto_unsupported_peer_uses_grpc_before_dispatch() {
    dfs_product_rdma_replica_and_read_real_verbs_with_mode(
        super::super::peer::DataMode::Auto,
        false,
    )
    .await;
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux RXE client; receiver deliberately has no RDMA device"]
async fn dfs_required_unsupported_peer_never_uses_grpc() {
    dfs_product_rdma_replica_and_read_real_verbs_with_mode(
        super::super::peer::DataMode::Rdma,
        false,
    )
    .await;
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux RXE, GDB posted checkpoint, and explicit resume marker"]
async fn dfs_replica_posted_rdma_deadline_has_unknown_outcome() {
    use crate::node::chunk::StagedChunk;
    use crate::node::rpc::peer::{DfsRdmaPool, PeerConnectionPool, make_replica_data_plane};
    let device = std::env::var("AFS_TEST_RDMA_DEVICE")
        .expect("set AFS_TEST_RDMA_DEVICE to the Linux RXE device");
    let checkpoint = std::path::PathBuf::from(
        std::env::var("AFS_TEST_RDMA_CHECKPOINT")
            .expect("set AFS_TEST_RDMA_CHECKPOINT to the GDB evidence directory"),
    );
    save_dfs_rdma_resources(&checkpoint, "baseline");

    let (meta, local, _temp, _read) = memory_read_authority_fixture().await;
    let validator = Arc::new(MemoryReadGrantValidator {
        service: meta.dfs.as_ref().unwrap().clone(),
        runtime: tokio::runtime::Handle::current(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("rdma-deadline"),
        dfs_deadline_payload(4096),
    );
    let target = crate::dfs::ReplicaTarget {
        node_id: "receiver".into(),
        node_epoch: 1,
        data_endpoint: endpoint.clone(),
        device: local.device_descriptor().unwrap(),
    };
    let op = crate::node::replication::ReplicaPeerOp {
        chunk_id: staged.chunk.id.clone(),
        placement_revision: 1,
        placement_epoch: 1,
        replica_group_id: crate::dfs::ReplicaGroupId::new("deadline-fixture"),
        initiator_node_id: "reader".into(),
        initiator_node_epoch: 1,
        ordered_targets: vec![target.clone()],
        sync_target_count: 1,
        target_index: 0,
        target,
        chain_tail: vec![],
        repair_claim: None,
    };
    let registry = afs_metrics::Registry::new();
    let metrics = DfsPayloadMetrics::register(&registry).unwrap();
    let rdma_registry = super::super::control::RdmaSessionRegistry::new(Some(device.clone()));
    let service = make_dfs_chunks_server_with_transport(
        Some(local.clone()),
        Arc::new(AllowDfsTestPeer),
        Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        )),
        Arc::new(FixtureReplicaGrant(op.clone())),
        None,
        std::time::Duration::from_secs(10),
        DfsChunkTransportResources {
            rdma_sessions: rdma_registry.clone(),
            payload_metrics: Some(metrics.clone()),
        },
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let peers = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            16,
        )
        .unwrap(),
    );
    let pool = Arc::new(
        DfsRdmaPool::new(peers.clone(), device, std::time::Duration::from_secs(4)).unwrap(),
    );
    let writer = make_replica_data_plane(
        super::super::peer::DataMode::Rdma,
        peers,
        std::time::Duration::from_secs(4),
        Some(pool.clone()),
    )
    .unwrap();
    let saved = staged.clone();
    let saved_op = op.clone();
    let mut write_task = tokio::task::spawn_blocking(move || {
        writer
            .put_peer_replica(&saved_op, &saved)
            .map(|acks| acks.len())
    });
    wait_for_dfs_rdma_checkpoint(&checkpoint.join("posted")).await;
    save_dfs_rdma_resources(&checkpoint, "connected");

    let identity = super::super::control::PeerSessionIdentity::new("reader".into(), 1).unwrap();
    let session = rdma_registry.session_for(1, &identity).await.unwrap();
    let retained_endpoint = Arc::downgrade(&session.endpoint);
    drop(session);
    let error = tokio::time::timeout(std::time::Duration::from_secs(10), &mut write_task)
        .await
        .expect("posted DFS replica write must return the caller deadline")
        .unwrap()
        .expect_err("posted DFS replica write must not report success before resume");
    std::fs::write(
        checkpoint.join("caller-error.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "code": error.code().raw(),
            "kind": format!("{:?}", error.kind()),
            "message": error.message(),
        }))
        .unwrap(),
    )
    .expect("save the actual DFS deadline classification");
    assert_dfs_replica_deadline(&error);
    assert!(
        dfs_chunk_content(&local, &staged).is_none(),
        "replica must not be persisted while the native worker is paused"
    );
    assert_eq!(dfs_payload_bytes(&metrics, "grpc", "recv", "replica"), 0);
    assert_eq!(dfs_payload_bytes(&metrics, "rdma", "recv", "replica"), 0);
    close_dfs_rdma_session(&endpoint, 1, 1).await;
    wait_for_dfs_rdma_lookup_removed(&rdma_registry, 1, &identity).await;
    {
        let endpoint = retained_endpoint
            .upgrade()
            .expect("blocked worker retains server endpoint after lookup removal");
        assert!(
            endpoint.try_lock().is_err(),
            "native worker still owns the server endpoint while paused"
        );
    }
    save_dfs_rdma_resources(&checkpoint, "closed-paused");
    eprintln!(
        "AFS_DFS_REPLICA_DEADLINE caller=TIMEOUT lookup=STALE server=RETAINED content=PENDING replay=ABSENT client=RETAINED close=EXPLICIT"
    );
    std::fs::write(checkpoint.join("resume"), b"resume native worker\n")
        .expect("write GDB resume marker");

    wait_for_dfs_endpoint_release(retained_endpoint).await;
    let bytes = wait_for_dfs_exact_content(&local, &staged).await;
    assert_eq!(bytes, staged.bytes());
    assert_eq!(dfs_payload_bytes(&metrics, "rdma", "recv", "replica"), 4096);
    assert_eq!(dfs_payload_bytes(&metrics, "grpc", "recv", "replica"), 0);
    drop(pool);
    save_dfs_rdma_resources(&checkpoint, "drained");
    eprintln!(
        "AFS_DFS_REPLICA_DEADLINE drain=COMPLETE content=EXACT endpoint=RELEASED replay=ABSENT client=EXPLICITLY_RETIRED"
    );
    server.abort();
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn dfs_deadline_payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| ((index.wrapping_mul(41).wrapping_add(index / 127)) & 0xff) as u8)
        .collect()
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn wait_for_dfs_rdma_checkpoint(path: &std::path::Path) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("GDB must prove DFS replica data WQE posted before the caller deadline");
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn save_dfs_rdma_resources(directory: &std::path::Path, phase: &str) {
    let tids: Vec<u32> = std::fs::read_dir("/proc/self/task")
        .expect("Linux thread inventory")
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .parse()
                .unwrap()
        })
        .collect();
    std::fs::write(
        directory.join(format!("{phase}-process.json")),
        serde_json::to_vec(&serde_json::json!({"pid": std::process::id(), "tids": tids})).unwrap(),
    )
    .expect("save process identity");
    for kind in ["qp", "mr", "cq", "pd", "ctx"] {
        let output = std::process::Command::new("rdma")
            .args(["-j", "resource", "show", kind])
            .output()
            .expect("rdma resource inventory");
        assert!(output.status.success(), "RDMA {kind} inventory failed");
        std::fs::write(
            directory.join(format!("{phase}-{kind}.json")),
            output.stdout,
        )
        .expect("save exact RDMA resources");
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn assert_dfs_replica_deadline(error: &afs_error::Error) {
    assert!(
        error.kind() == afs_error::ErrorKind::DeadlineExceeded
            || (error.code() == afs_error::CLIENT_REMOTE_STATUS
                && error.kind() == afs_error::ErrorKind::Cancelled
                && error.message() == "Timeout expired"),
        "posted DFS replica write must fail with the request deadline, got {error:?}"
    );
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn close_dfs_rdma_session(endpoint: &str, session_id: u64, peer_node_epoch: u64) {
    let mut client =
        afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::connect(endpoint.to_owned())
            .await
            .expect("connect DFS close client");
    let mut request = Request::new(DfsCloseRdmaRequest {
        session_id,
        peer_node_epoch,
    });
    request.set_timeout(std::time::Duration::from_secs(4));
    client
        .close_rdma(request)
        .await
        .expect("explicit fixture close removes registry lookup while native worker is paused");
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn wait_for_dfs_rdma_lookup_removed(
    registry: &super::super::control::RdmaSessionRegistry,
    session_id: u64,
    identity: &super::super::control::PeerSessionIdentity,
) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match registry.session_for(session_id, identity).await {
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                Err(status) => {
                    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                    if status.message() == "RDMA session poisoned" {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    assert!(
                        status.message().contains("unknown RDMA session"),
                        "stale lookup must prove removal, not a live poisoned session: {status:?}"
                    );
                    break;
                }
            }
        }
    })
    .await
    .expect("explicit DFS client teardown must remove the server registry lookup");
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn wait_for_dfs_endpoint_release(
    endpoint: std::sync::Weak<super::super::control::RdmaServerEndpoint>,
) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while endpoint.upgrade().is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server endpoint must release after the paused native transfer resolves");
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn wait_for_dfs_exact_content(
    local: &LocalChunkStore,
    staged: &crate::node::chunk::StagedChunk,
) -> Vec<u8> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(bytes) = dfs_chunk_content(local, staged) {
                return bytes;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("resumed DFS RDMA READ should persist the exact staged Chunk")
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn dfs_chunk_content(
    local: &LocalChunkStore,
    staged: &crate::node::chunk::StagedChunk,
) -> Option<Vec<u8>> {
    let mut bytes = vec![0; staged.bytes().len()];
    match local.read_at(&staged.chunk.id, 0, &mut bytes) {
        Ok(read) => {
            assert_eq!(read, bytes.len());
            Some(bytes)
        }
        Err(error) => {
            assert!(
                error.message().contains("not cataloged"),
                "unexpected replica read error: {error:?}"
            );
            None
        }
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn dfs_payload_bytes(
    metrics: &DfsPayloadMetrics,
    transport: &str,
    direction: &str,
    purpose: &str,
) -> u64 {
    metrics
        .bytes
        .with_label_values(&[transport, direction, purpose])
        .get()
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
async fn dfs_product_rdma_replica_and_read_real_verbs_with_mode(
    mode: super::super::peer::DataMode,
    server_rdma: bool,
) {
    use crate::node::chunk::StagedChunk;
    use crate::node::read::engine::{ChunkReadOp, PeerReadBatch, ResolvedReadOp};
    use crate::node::rpc::peer::{
        DfsRdmaPool, PeerConnectionPool, make_chunk_transfer, make_replica_data_plane,
    };
    let device = std::env::var("AFS_TEST_RDMA_DEVICE")
        .expect("set AFS_TEST_RDMA_DEVICE to the Linux RXE device");
    let (meta, local, _temp, read) = memory_read_authority_fixture().await;
    let validator = Arc::new(MemoryReadGrantValidator {
        service: meta.dfs.as_ref().unwrap().clone(),
        runtime: tokio::runtime::Handle::current(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("rdma-product"),
        vec![37; crate::node::chunk::MAX_STAGED_CHUNK_BYTES],
    );
    let target = crate::dfs::ReplicaTarget {
        node_id: "receiver".into(),
        node_epoch: 1,
        data_endpoint: endpoint.clone(),
        device: local.device_descriptor().unwrap(),
    };
    let op = crate::node::replication::ReplicaPeerOp {
        chunk_id: staged.chunk.id.clone(),
        placement_revision: 1,
        placement_epoch: 1,
        replica_group_id: crate::dfs::ReplicaGroupId::new("fixture"),
        initiator_node_id: "reader".into(),
        initiator_node_epoch: 1,
        ordered_targets: vec![target.clone()],
        sync_target_count: 1,
        target_index: 0,
        target,
        chain_tail: vec![],
        repair_claim: None,
    };
    let registry = afs_metrics::Registry::new();
    let metrics = DfsPayloadMetrics::register(&registry).unwrap();
    let service = make_dfs_chunks_server_with_transport(
        Some(local.clone()),
        Arc::new(AllowDfsTestPeer),
        Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        )),
        Arc::new(FixtureReplicaGrant(op.clone())),
        None,
        std::time::Duration::from_secs(10),
        DfsChunkTransportResources {
            rdma_sessions: super::super::control::RdmaSessionRegistry::new(
                server_rdma.then(|| device.clone()),
            ),
            payload_metrics: Some(metrics.clone()),
        },
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let peers = Arc::new(
        PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            16,
        )
        .unwrap(),
    );
    let pool = Arc::new(
        DfsRdmaPool::new(peers.clone(), device, std::time::Duration::from_secs(10)).unwrap(),
    );
    let writer = make_replica_data_plane(
        mode,
        peers.clone(),
        std::time::Duration::from_secs(10),
        Some(pool.clone()),
    )
    .unwrap();
    let saved = staged.clone();
    let saved_op = op.clone();
    let acks = tokio::task::spawn_blocking(move || {
        if !server_rdma && mode == super::super::peer::DataMode::Rdma {
            let error = writer.put_peer_replica(&saved_op, &saved).unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            return None;
        }
        let acks = writer.put_peer_replica(&saved_op, &saved).unwrap();
        writer.put_peer_replica(&saved_op, &saved).unwrap();
        Some(acks)
    })
    .await
    .unwrap();
    if let Some(acks) = &acks {
        assert_eq!(
            acks[0].persisted_bytes,
            crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
        );
    }
    let plane = if server_rdma { "rdma" } else { "grpc" };
    let other_plane = if server_rdma { "grpc" } else { "rdma" };
    let wire = read.operations[0].grant.as_ref().unwrap();
    let item = &read.operations[0];
    let batch = PeerReadBatch {
        file_version_id: crate::dfs::FileVersionId::new(read.file_version_id.clone()),
        layout_root_id: crate::dfs::LayoutRootId::new(read.layout_root_id.clone()),
        operations: vec![ResolvedReadOp {
            op: ChunkReadOp {
                chunk_id: crate::dfs::ChunkId::new(item.chunk_id.clone()),
                chunk_offset: item.chunk_offset,
                length: item.length,
                output_offset: 0,
            },
            source: crate::dfs::SourceCandidate {
                copy_id: crate::dfs::CopyId::new(item.source_copy_id.clone()),
                chunk_id: crate::dfs::ChunkId::new(item.chunk_id.clone()),
                role: crate::dfs::CopyRole::DurableReplica,
                state: crate::dfs::CopyState::Ready,
                location: crate::dfs::CopyLocation::Node {
                    node_id: "receiver".into(),
                    node_epoch: 1,
                    device_id: op.target.device.device_id.clone(),
                    device_epoch: op.target.device.device_epoch,
                    catalog_revision: op.target.device.catalog_revision,
                },
                data_endpoint: Some(endpoint.clone()),
                load_hint: 0,
                read_grant: crate::dfs::DfsReadGrant {
                    namespace_id: crate::dfs::NamespaceId::new(wire.namespace_id.clone()),
                    file_version_id: crate::dfs::FileVersionId::new(wire.file_version_id.clone()),
                    layout_root_id: crate::dfs::LayoutRootId::new(wire.layout_root_id.clone()),
                    caller_node_id: wire.caller_node_id.clone(),
                    caller_node_epoch: wire.caller_node_epoch,
                    expires_at_unix_ms: wire.expires_at_unix_ms,
                    fence: wire.fence,
                    token: wire.token.clone(),
                },
            },
        }],
    };
    if acks.is_none() {
        let transfer =
            make_chunk_transfer(mode, peers, std::time::Duration::from_secs(10), Some(pool))
                .unwrap();
        let bytes = tokio::task::spawn_blocking(move || {
            let mut bytes = vec![0; 75_000];
            let error = transfer.read_ranges(&batch, &mut bytes).unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            bytes
        })
        .await
        .unwrap();
        assert_eq!(bytes, vec![0; 75_000]);
        for transport in ["rdma", "grpc"] {
            assert_eq!(
                metrics
                    .bytes
                    .with_label_values(&[transport, "recv", "replica"])
                    .get(),
                0
            );
            assert_eq!(
                metrics
                    .bytes
                    .with_label_values(&[transport, "send", "read"])
                    .get(),
                0
            );
        }
        println!(
            "DFS_REQUIRED_UNSUPPORTED write=REJECTED read=REJECTED error=NODE_TRANSFER_UNSUPPORTED grpc_payload_bytes=0 rdma_payload_bytes=0"
        );
        server.abort();
        return;
    }
    let transfer =
        make_chunk_transfer(mode, peers, std::time::Duration::from_secs(10), Some(pool)).unwrap();
    let bytes = tokio::task::spawn_blocking(move || {
        let mut bytes = vec![0; 75_000];
        transfer.read_ranges(&batch, &mut bytes).unwrap();
        bytes
    })
    .await
    .unwrap();
    assert_eq!(bytes, vec![9; 75_000]);
    assert_eq!(
        metrics
            .bytes
            .with_label_values(&[plane, "recv", "replica"])
            .get(),
        2 * crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
    );
    assert_eq!(
        metrics
            .bytes
            .with_label_values(&[plane, "send", "read"])
            .get(),
        75_000
    );
    assert_eq!(
        metrics
            .bytes
            .with_label_values(&[other_plane, "recv", "replica"])
            .get(),
        0
    );
    assert_eq!(
        metrics
            .bytes
            .with_label_values(&[other_plane, "send", "read"])
            .get(),
        0
    );
    let mut client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::connect(endpoint)
        .await
        .unwrap();
    let mut forged = read;
    forged.operations[0].grant.as_mut().unwrap().token = "forged".into();
    assert_eq!(
        client
            .negotiate_rdma(DfsNegotiateRdmaRequest {
                client_info: vec![],
                capacity: crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u32,
                handshake_version: super::super::control::RDMA_HANDSHAKE_VERSION,
                authority: Some(
                    afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReadRequest(
                        forged
                    )
                )
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        metrics
            .bytes
            .with_label_values(&[plane, "send", "read"])
            .get(),
        75_000
    );
    println!(
        "DFS_PRODUCT_TRANSPORT mode={mode:?} plane={plane} replica_bytes={} read_bytes=75000 other_payload_bytes=0 exact_retry=PASS forged_grant=DENIED",
        2 * crate::node::chunk::MAX_STAGED_CHUNK_BYTES
    );
    server.abort();
}

#[cfg(feature = "dfs")]
struct AllowDfsTestGrant;
#[cfg(feature = "dfs")]
impl DfsReadAuthorizer for AllowDfsTestGrant {
    fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        if peer == "reader"
            && request.operations.iter().all(|op| {
                op.grant
                    .as_ref()
                    .is_some_and(|grant| grant.token == "fixture-grant")
            })
        {
            Ok(())
        } else {
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_INVALID,
                "fixture grant rejected",
            ))
        }
    }
}

#[cfg(feature = "dfs")]
fn dfs_read_request(chunk_id: String, length: u64) -> DfsReadRangesRequest {
    DfsReadRangesRequest {
        include_blake3_outboard: false,
        read_id: "read".into(),
        attempt_id: "attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        operations: vec![afs_protocol::node_data::DfsChunkReadOp {
            operation_index: 0,
            chunk_id,
            chunk_offset: 0,
            length,
            destination_offset: 0,
            source_copy_id: "copy".into(),
            grant: Some(afs_protocol::node_data::DfsReadGrant {
                namespace_id: "namespace".into(),
                file_version_id: "version".into(),
                layout_root_id: "layout".into(),
                caller_node_id: "reader".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "fixture-grant".into(),
            }),
        }],
    }
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_shared_data_frame_preserves_protobuf_wire_encoding() {
    use prost::Message;
    let frame = DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Data(
            prost::bytes::Bytes::from_static(b"abc"),
        )),
    };
    // The original bytes field is still field 2, length-delimited.
    let wire = [0x12, 0x03, b'a', b'b', b'c'];
    assert_eq!(frame.encode_to_vec(), wire);
    let decoded = DfsReadRangesFrame::decode(&wire[..]).unwrap();
    assert_eq!(decoded, frame);
}

#[cfg(feature = "dfs")]
#[test]
fn crc_read_modes_serving_node_follows_caller_mode_not_its_own() {
    use crate::config::DfsReadCrcMode;
    use afs_protocol::node_data::DfsDigestAlgorithm;
    // (configured on the serving Node, requested by the caller)
    for (configured, requested) in [
        (DfsReadCrcMode::Off, DfsReadCrcMode::Receiver),
        (DfsReadCrcMode::Receiver, DfsReadCrcMode::Off),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_read_crc_mode(configured);
        let staged = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("mixed"),
            vec![61; 65_536],
        )
        .unwrap();
        local.put(staged.clone()).unwrap();
        let mut damaged = staged.bytes().to_vec();
        damaged[3] ^= 1;
        std::fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            &damaged,
        )
        .unwrap();
        let serve = |offset: u64, length: u64| {
            let mut request = dfs_read_request(staged.chunk.id.0.clone(), length);
            request.operations[0].chunk_offset = offset;
            let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
            let result = stream_dfs_ranges_until(
                &local,
                &request,
                &sender,
                None,
                local.peer_serve_deadline(),
                requested,
            );
            drop(sender);
            let mut body = Vec::new();
            let mut algorithm = None;
            while let Ok(frame) = receiver.try_recv() {
                match frame.unwrap().body.unwrap() {
                    dfs_read_ranges_frame::Body::Data(data) => body.extend_from_slice(&data),
                    dfs_read_ranges_frame::Body::Completion(done) => {
                        algorithm = Some(done.range_checksum_algorithm)
                    }
                    _ => {}
                }
            }
            result.map(|()| (body, algorithm.unwrap()))
        };
        // Aligned segments: the receiver alone verifies, so the sender
        // streams bytes and signals the caller's policy, not its own.
        let (body, algorithm) = serve(0, 32_768).unwrap();
        assert_eq!(body, damaged[..32_768]);
        assert_eq!(
            algorithm,
            match requested {
                DfsReadCrcMode::Receiver => DfsDigestAlgorithm::Crc32cReceiverOnly as i32,
                _ => DfsDigestAlgorithm::ReadUnchecked as i32,
            },
            "configured={configured:?} requested={requested:?}"
        );
        // Unaligned fragment: receiver mode still verifies on the sender;
        // off delivers the stored bytes unchecked.
        let fragment = serve(1, 7);
        match requested {
            DfsReadCrcMode::Receiver => {
                assert_eq!(fragment.unwrap_err().code(), tonic::Code::DataLoss)
            }
            _ => {
                let (body, algorithm) = fragment.unwrap();
                assert_eq!(body, damaged[1..8]);
                assert_eq!(algorithm, DfsDigestAlgorithm::ReadUnchecked as i32);
            }
        }
    }
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_corrupt_range_publishes_no_grpc_frames_or_rdma_completions() {
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let local = LocalChunkStore::open(temp.path(), "node")
        .unwrap()
        .with_read_work_metrics(metrics.clone());
    let staged = StagedChunk::new(crate::dfs::OperationId::new("corrupt"), vec![42; 150_000]);
    local.put(staged.clone()).unwrap();
    let path = temp.path().join("chunks").join(&staged.chunk.id.0);
    let mut damaged = staged.bytes().to_vec();
    damaged[149_999] ^= 1;
    std::fs::write(path, damaged).unwrap();
    let mut request = dfs_read_request(staged.chunk.id.0.clone(), 75_000);
    request.operations[0].chunk_offset = 1;
    validate_dfs_read_request(&request).unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    assert_eq!(
        stream_dfs_ranges(&local, &request, &sender, None)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    assert!(receiver.try_recv().is_err());
    assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    let served = snapshot
        .records
        .iter()
        .find(|record| {
            record.stage == crate::node::metrics::read_timing::ReadDiagnosticStage::PeerServe
        })
        .unwrap();
    assert!(served.completed && !served.success);
    assert_eq!(
        served.first_data_ns, 0,
        "corrupt data never enters the channel"
    );
    #[cfg(feature = "rdma")]
    assert_eq!(
        read_packed_dfs_ranges(&local, &request, 75_000)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_verified_prefix_precedes_later_corruption_without_completion() {
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "node").unwrap();
    let body = vec![42; 3 * 32 * 1024];
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("r4-late-corrupt"),
        body.clone(),
    );
    local.put(staged.clone()).unwrap();
    // Persist a valid standard-root sidecar before corrupting only the last
    // group. Healthy earlier groups must not wait for a whole-object scan.
    drop(local.open_verified(&staged.chunk.id).unwrap());
    let mut damaged = body.clone();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), damaged).unwrap();
    let mut request = dfs_read_request(staged.chunk.id.0.clone(), body.len() as u64);
    request.include_blake3_outboard = true;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let result = std::thread::scope(|scope| {
        let producer = scope.spawn(|| stream_dfs_ranges(&local, &request, &sender, None));
        let mut received = Vec::new();
        let mut completed = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !producer.is_finished() || !receiver.is_empty() {
            if std::time::Instant::now() >= deadline {
                receiver.close();
                panic!("bounded verified-prefix producer did not finish");
            }
            match receiver.try_recv() {
                Ok(Ok(frame)) => match frame.body.unwrap() {
                    dfs_read_ranges_frame::Body::Data(data) => received.extend_from_slice(&data),
                    dfs_read_ranges_frame::Body::Completion(_) => completed = true,
                    dfs_read_ranges_frame::Body::Header(header) => {
                        assert!(!header.blake3_outboard.is_empty())
                    }
                },
                Ok(Err(error)) => panic!("direct producer must return its error: {error}"),
                Err(_) => std::thread::yield_now(),
            }
        }
        (producer.join().unwrap(), received, completed)
    });
    assert_eq!(result.0.unwrap_err().code(), tonic::Code::DataLoss);
    assert_eq!(
        result.1.len(),
        2 * 32 * 1024,
        "only completely verified prefix groups may leave A"
    );
    assert!(result.1 == body[..result.1.len()]);
    assert!(!result.2, "a partial stream must never publish Completion");
    assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
}

#[cfg(feature = "dfs")]
#[test]
fn p2_r4_corrupt_first_group_sends_no_unverified_data() {
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "node").unwrap();
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("r4-first-corrupt"),
        vec![42; 3 * 32 * 1024],
    );
    local.put(staged.clone()).unwrap();
    drop(local.open_verified(&staged.chunk.id).unwrap());
    let mut damaged = staged.bytes().to_vec();
    damaged[0] ^= 1;
    std::fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), damaged).unwrap();
    let mut request = dfs_read_request(staged.chunk.id.0.clone(), staged.chunk.length);
    request.include_blake3_outboard = true;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    assert_eq!(
        stream_dfs_ranges(&local, &request, &sender, None)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    while let Ok(frame) = receiver.try_recv() {
        assert!(matches!(
            frame.unwrap().body.unwrap(),
            dfs_read_ranges_frame::Body::Header(_)
        ));
    }
    assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_stream_whole_digest_reuse_preserves_wire_and_partial_hash_work() {
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let local = LocalChunkStore::open(temp.path(), "node")
        .unwrap()
        .with_read_work_metrics(metrics.clone());
    let bytes: Vec<_> = (0..150_001).map(|n| (n % 251) as u8).collect();
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("stream-whole-proof"),
        bytes.clone(),
    );
    local.put(staged.clone()).unwrap();
    let hash_bytes = || {
        afs_metrics::encode_text(&registry)
            .unwrap()
            .lines()
            .find_map(|line| {
                line.strip_prefix("afs_dfs_read_hash_bytes_total{stage=\"stream_range_hash\"} ")
            })
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    for (offset, length, want_proof) in [
        (0, bytes.len(), false),
        (0, bytes.len(), true),
        (1, 75_000, false),
        (1, 75_000, true),
    ] {
        let before = hash_bytes();
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), length as u64);
        request.include_blake3_outboard = want_proof;
        request.operations[0].chunk_offset = offset as u64;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        std::thread::scope(|scope| {
            let producer = scope.spawn(|| {
                let result = stream_dfs_ranges(&local, &request, &sender, None);
                drop(sender);
                result
            });
            let mut received = Vec::new();
            let mut header_count = 0;
            let mut completions = 0;
            while let Some(frame) = receiver.blocking_recv() {
                match frame.unwrap().body.unwrap() {
                    dfs_read_ranges_frame::Body::Header(header) => {
                        header_count += 1;
                        assert_eq!(header.chunk_offset, offset as u64);
                        assert_eq!(header.length, length as u64);
                        if want_proof && offset == 0 {
                            assert!(!header.blake3_outboard.is_empty());
                            crate::node::chunk::outboard::ChunkOutboard::validate_encoded_shape(
                                &staged.chunk,
                                &header.blake3_outboard,
                            )
                            .unwrap();
                        } else {
                            assert!(
                                header.blake3_outboard.is_empty(),
                                "ordinary/partial reads keep their original wire contract"
                            );
                        }
                    }
                    dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
                    dfs_read_ranges_frame::Body::Completion(completion) => {
                        completions += 1;
                        assert_eq!(completion.transferred_bytes, length as u64);
                        assert_eq!(
                            completion.range_checksum_algorithm,
                            afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
                        );
                        assert_eq!(
                            completion.range_checksum,
                            blake3::hash(&received).as_bytes().to_vec()
                        );
                    }
                }
            }
            assert_eq!(header_count, 1);
            assert_eq!(completions, 1);
            assert_eq!(received, bytes[offset..offset + length]);
            assert_eq!(
                hash_bytes() - before,
                if offset == 0 { 0 } else { length as u64 }
            );
            producer.join().unwrap().unwrap();
        });
    }
    assert_eq!(dfs_reuse_scan_bytes(&registry), bytes.len() as u64);
    let diagnostic = metrics.diagnostics_snapshot().unwrap();
    assert_eq!(diagnostic.overflow, 0);
    let served = diagnostic
        .records
        .iter()
        .filter(|record| {
            record.stage == crate::node::metrics::read_timing::ReadDiagnosticStage::PeerServe
        })
        .collect::<Vec<_>>();
    assert_eq!(served.len(), 4);
    for record in served {
        assert!(record.completed && record.success);
        assert!(record.first_data_ns >= record.start_ns);
        assert!(record.end_ns >= record.first_data_ns);
    }
    let enqueue = diagnostic
        .coarse_phases
        .iter()
        .find(|phase| phase.phase == "peer_first_frame_enqueue")
        .unwrap();
    assert_eq!(enqueue.calls, 4);
}

#[cfg(feature = "dfs")]
fn dfs_reuse_test_service(
    local: Arc<LocalChunkStore>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
) -> DfsChunksService {
    DfsChunksService {
        local_chunks: Some(local),
        authenticator: Arc::new(AllowDfsTestPeer),
        authorizer,
        replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
        replica_data_plane: None,
        replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout: std::time::Duration::from_secs(30),
        rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
        payload_metrics: None,
        suspect_reviews: Default::default(),
    }
}

#[cfg(feature = "dfs")]
fn dfs_reuse_scan_bytes(registry: &afs_metrics::Registry) -> u64 {
    afs_metrics::encode_text(registry)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("afs_dfs_read_scan_bytes_total "))
        .unwrap()
        .parse()
        .unwrap()
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn p2_t1_native_read_waits_for_an_existing_rpc_slot() {
    use crate::node::chunk::StagedChunk;
    use std::{future::Future, task::Poll};
    use tokio_stream::StreamExt;

    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("t1-rpc-admission"),
        vec![37; 65536],
    );
    local.put(staged.clone()).unwrap();
    let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
    let held = service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let request = dfs_read_request(staged.chunk.id.0.clone(), staged.chunk.length);
    let mut pending = Box::pin(service.read_ranges(Request::new(request)));
    // Poll exactly once while all eight real slots are held. This tests
    // admission behavior without inferring it from scheduler/timing noise.
    let waiting =
        std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await;
    assert!(
        waiting,
        "temporary slot pressure must wait within the fixed admission deadline"
    );
    drop(held);
    let mut stream = pending.await.unwrap().into_inner();
    let mut received = Vec::new();
    let mut completions = 0;
    while let Some(frame) = stream.next().await {
        match frame.unwrap().body.unwrap() {
            dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
            dfs_read_ranges_frame::Body::Completion(_) => completions += 1,
            dfs_read_ranges_frame::Body::Header(_) => {}
        }
    }
    assert_eq!(received, staged.bytes());
    assert_eq!(completions, 1);
    assert_eq!(service.read_permits.available_permits(), 8);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn p2_t1_zero_wait_rpc_slots_fail_without_queueing() {
    use std::{future::Future, task::Poll};
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_wait(std::time::Duration::ZERO)
            .unwrap(),
    );
    let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
    let _held = service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let mut pending =
        Box::pin(service.read_ranges(Request::new(dfs_read_request("chunk".into(), 1))));
    let result = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
    match result {
        Poll::Ready(Err(error)) => assert_eq!(error.code(), tonic::Code::ResourceExhausted),
        _ => panic!("zero admission allowance must not queue behind occupied RPC slots"),
    }
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn p2_t1_rpc_slot_timeout_and_node_stop_return_no_stream() {
    use std::{future::Future, task::Poll};
    for stop in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
        let _held = service
            .read_permits
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        let mut pending =
            Box::pin(service.read_ranges(Request::new(dfs_read_request("chunk".into(), 1))));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await
        );
        if stop {
            local.close_peer_serve_admission();
        }
        let error = pending.await.err().unwrap();
        assert_eq!(
            error.code(),
            if stop {
                tonic::Code::Unavailable
            } else {
                tonic::Code::ResourceExhausted
            }
        );
    }
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn p2_t1_grant_expiry_is_rechecked_after_rpc_queue() {
    use std::{future::Future, task::Poll};
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_wait(std::time::Duration::from_millis(50))
            .unwrap(),
    );
    let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
    let held = service
        .read_permits
        .clone()
        .acquire_many_owned(8)
        .await
        .unwrap();
    let mut request = dfs_read_request("chunk".into(), 1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    request.operations[0]
        .grant
        .as_mut()
        .unwrap()
        .expires_at_unix_ms = now + 10;
    let mut pending = Box::pin(service.read_ranges(Request::new(request)));
    assert!(std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await);
    std::thread::sleep(std::time::Duration::from_millis(20));
    drop(held);
    assert_eq!(
        pending.await.err().unwrap().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(service.read_permits.available_permits(), 8);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_peer_object_reuse_scans_once_and_still_checks_authority() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_read_work_metrics(
                crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap(),
            ),
    );
    let bytes: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let staged = StagedChunk::new(crate::dfs::OperationId::new("reuse"), bytes.clone());
    local.put(staged.clone()).unwrap();
    let scans_before = dfs_reuse_scan_bytes(&registry);
    let allowed = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
    for part in 0..4 {
        let start = part * 1024 * 1024;
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), 1024 * 1024);
        request.operations[0].chunk_offset = start as u64;
        let mut stream = allowed
            .read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        let mut received = Vec::new();
        let mut completions = 0;
        while let Some(frame) = stream.next().await {
            match frame.unwrap().body.unwrap() {
                dfs_read_ranges_frame::Body::Data(data) => {
                    assert!(data.len() <= 64 * 1024);
                    received.extend(data);
                }
                dfs_read_ranges_frame::Body::Completion(completion) => {
                    assert_eq!(
                        completion.range_checksum,
                        blake3::hash(&received).as_bytes().to_vec()
                    );
                    completions += 1;
                }
                dfs_read_ranges_frame::Body::Header(_) => {}
            }
        }
        assert_eq!(received, bytes[start..start + 1024 * 1024]);
        assert_eq!(completions, 1);
    }
    assert_eq!(
        dfs_reuse_scan_bytes(&registry) - scans_before,
        bytes.len() as u64
    );
    let encoded = afs_metrics::encode_text(&registry).unwrap();
    let count = |prefix: &str| {
        encoded
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(
        count("afs_dfs_peer_read_events_total{event=\"blocking_combined\"} "),
        4
    );
    assert_eq!(
        count("afs_dfs_peer_read_events_total{event=\"data_frame\"} "),
        64
    );
    assert_eq!(
        count("afs_dfs_peer_read_events_total{event=\"shared_frame\"} "),
        64
    );
    for event in [
        "blocking_authorize",
        "blocking_produce",
        "body_vec_allocation",
    ] {
        assert_eq!(
            count(&format!(
                "afs_dfs_peer_read_events_total{{event=\"{event}\"}} "
            )),
            0
        );
    }
    assert_eq!(
        count("afs_dfs_read_copy_bytes_total{stage=\"range_to_frame\"} "),
        0
    );
    // A warmed immutable object is data reuse, never permission reuse.
    let denied = dfs_reuse_test_service(local.clone(), Arc::new(DenyDfsReadAuthorizer));
    assert!(
        denied
            .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
            .await
            .is_err()
    );
    assert_eq!(
        dfs_reuse_scan_bytes(&registry) - scans_before,
        bytes.len() as u64
    );
}

#[cfg(feature = "dfs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dfs_peer_r6_followers_keep_independent_expiry_and_stop_checks() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    struct CountingAuthorizer(std::sync::atomic::AtomicUsize);
    impl DfsReadAuthorizer for CountingAuthorizer {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            AllowDfsTestGrant.authorize(peer, request)
        }
    }
    for stop in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let staged = StagedChunk::new(crate::dfs::OperationId::new("r6-delivery"), vec![73; 4096]);
        local.put(staged.clone()).unwrap();
        let (entered, release) = local.pause_next_peer_fill_for_test(staged.chunk.id.clone());
        let authorizer = Arc::new(CountingAuthorizer(std::sync::atomic::AtomicUsize::new(0)));
        let service = dfs_reuse_test_service(local.clone(), authorizer.clone());
        let mut leader = service
            .read_ranges(Request::new(dfs_read_request(
                staged.chunk.id.0.clone(),
                4096,
            )))
            .await
            .unwrap()
            .into_inner();
        entered
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap();
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 100;
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), 4096);
        if !stop {
            request.operations[0]
                .grant
                .as_mut()
                .unwrap()
                .expires_at_unix_ms = expires;
        }
        let mut follower = service
            .read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while local.waiting_peer_fill_readers_for_test() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "RPC follower did not join flight"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(authorizer.0.load(std::sync::atomic::Ordering::SeqCst), 2);
        if stop {
            local.close_peer_serve_admission();
        } else {
            while std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                <= u128::from(expires)
            {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        release.send(()).unwrap();
        let failure = follower.next().await.unwrap().unwrap_err();
        if !stop {
            assert_eq!(failure.code(), tonic::Code::PermissionDenied);
        } else {
            assert_eq!(failure.code(), tonic::Code::Unavailable);
        }
        assert!(
            follower.next().await.is_none(),
            "rejected follower must receive no Header or body"
        );
        let mut bytes = Vec::new();
        while let Some(frame) = leader.next().await {
            if stop {
                assert!(frame.is_err());
            } else if let Some(dfs_read_ranges_frame::Body::Data(data)) = frame.unwrap().body {
                bytes.extend_from_slice(&data);
            }
        }
        if !stop {
            assert_eq!(bytes, staged.bytes());
        }
        drop(leader);
        drop(follower);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while service.read_permits.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        local.shutdown_peer_serve().unwrap();
    }
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_peer_object_reuse_cancel_releases_slot_and_shared_budget() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_config(1, 8 * 1024 * 1024)
            .unwrap(),
    );
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("cancel"),
        vec![7; 4 * 1024 * 1024],
    );
    local.put(staged.clone()).unwrap();
    let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
    let mut stream = service
        .read_ranges(Request::new(dfs_read_request(
            staged.chunk.id.0,
            4 * 1024 * 1024,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.next().await.unwrap().is_ok());
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while service.read_permits.available_permits() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The retained slot can be evicted; no cancelled producer owns a hidden lease.
    let reservation = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
    drop(reservation);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_peer_cancel_during_authorization_keeps_permit_until_exit_and_skips_io() {
    use crate::node::chunk::StagedChunk;
    struct BlockedAuthorizer {
        started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl DfsReadAuthorizer for BlockedAuthorizer {
        fn authorize(&self, _peer: &str, _request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            self.started
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_read_work_metrics(
                crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap(),
            ),
    );
    let staged = StagedChunk::new(
        crate::dfs::OperationId::new("cancel-authorization"),
        vec![31; 4 * 1024 * 1024],
    );
    local.put(staged.clone()).unwrap();
    let before = dfs_reuse_scan_bytes(&registry);
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let service = Arc::new(dfs_reuse_test_service(
        local,
        Arc::new(BlockedAuthorizer {
            started: std::sync::Mutex::new(Some(started)),
            release: std::sync::Mutex::new(wait),
        }),
    ));
    let call = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
                .await
                .map(|_| ())
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(service.read_permits.available_permits(), 7);
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while service.read_permits.available_permits() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dfs_reuse_scan_bytes(&registry), before);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_peer_frame_keeps_shared_lease_after_cancel_and_object_replacement() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_config(1, 8 * 1024 * 1024)
            .unwrap(),
    );
    let first = StagedChunk::new(
        crate::dfs::OperationId::new("held-frame"),
        vec![7; 4 * 1024 * 1024],
    );
    let replacement = StagedChunk::new(
        crate::dfs::OperationId::new("replace-held-frame"),
        vec![9; 4 * 1024 * 1024],
    );
    local.put(first.clone()).unwrap();
    local.put(replacement.clone()).unwrap();
    let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
    let mut stream = service
        .read_ranges(Request::new(dfs_read_request(
            first.chunk.id.0,
            4 * 1024 * 1024,
        )))
        .await
        .unwrap()
        .into_inner();
    let held_frame = loop {
        let frame = stream.next().await.unwrap().unwrap();
        if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Data(_))) {
            break frame;
        }
    };
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while service.read_permits.available_permits() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // Replace the resident slot; the consumer still owns the old frame.
    let next = local
        .read_peer_range(&replacement.chunk.id, 0, 64 * 1024)
        .unwrap();
    assert_eq!(next.as_slice(), &[9; 64 * 1024]);
    drop(next);
    if let Some(dfs_read_ranges_frame::Body::Data(data)) = &held_frame.body {
        assert_eq!(&data[..], &[7; 64 * 1024]);
    } else {
        unreachable!();
    }
    // Even evicting the replacement cannot reclaim the held object's lease.
    assert!(local.reserve_peer_read_payload(5 * 1024 * 1024).is_err());
    drop(held_frame);
    let all = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
    drop(all);
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_peer_object_reuse_full_budget_returns_error_before_any_frame() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_config(1, 8 * 1024 * 1024)
            .unwrap(),
    );
    let staged = StagedChunk::new(crate::dfs::OperationId::new("full-budget"), vec![3; 1024]);
    local.put(staged.clone()).unwrap();
    let reservation = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
    let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
    let mut stream = service
        .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert!(stream.next().await.is_none());
    drop(reservation);
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[test]
fn dfs_peer_object_reuse_rdma_packed_shares_budget_until_payload_drop() {
    use crate::node::chunk::StagedChunk;
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "node")
        .unwrap()
        .with_peer_serve_config(1, 8 * 1024 * 1024)
        .unwrap();
    let staged = StagedChunk::new(crate::dfs::OperationId::new("packed"), vec![9; 1024]);
    local.put(staged.clone()).unwrap();
    let request = dfs_read_request(staged.chunk.id.0, 1024);
    let all = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
    assert_eq!(
        read_packed_dfs_ranges(&local, &request, 1024)
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    drop(all);
    let packed = read_packed_dfs_ranges(&local, &request, 1024).unwrap();
    assert_eq!(packed.bytes, vec![9; 1024]);
    assert_eq!(
        packed.completions[0].range_checksum,
        blake3::hash(&packed.bytes).as_bytes().to_vec()
    );
    assert!(local.reserve_peer_read_payload(8 * 1024 * 1024).is_err());
    drop(packed);
    assert!(local.reserve_peer_read_payload(8 * 1024 * 1024).is_ok());
}

#[cfg(feature = "dfs")]
#[tokio::test]
async fn dfs_range_service_requires_authority_and_streams_bounded_frames() {
    use crate::node::chunk::StagedChunk;
    use tokio_stream::StreamExt;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
    let bytes = vec![42; 150 * 1024];
    let staged = StagedChunk::new(crate::dfs::OperationId::new("fixture-write"), bytes.clone());
    let request = dfs_read_request(staged.chunk.id.0.clone(), bytes.len() as u64);
    local.put(staged).unwrap();
    let denied = DfsChunksService {
        local_chunks: Some(local.clone()),
        authenticator: Arc::new(AllowDfsTestPeer),
        authorizer: Arc::new(DenyDfsReadAuthorizer),
        replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
        replica_data_plane: None,
        replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout: std::time::Duration::from_secs(30),
        rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
        payload_metrics: None,
        suspect_reviews: Default::default(),
    };
    assert!(
        denied
            .read_ranges(Request::new(request.clone()))
            .await
            .is_err()
    );
    let allowed = DfsChunksService {
        local_chunks: Some(local),
        authenticator: Arc::new(AllowDfsTestPeer),
        authorizer: Arc::new(AllowDfsTestGrant),
        replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
        replica_data_plane: None,
        replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout: std::time::Duration::from_secs(30),
        rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
        payload_metrics: None,
        suspect_reviews: Default::default(),
    };
    let mut stream = allowed
        .read_ranges(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut received = Vec::new();
    let mut completions = 0;
    while let Some(frame) = stream.next().await {
        match frame.unwrap().body.unwrap() {
            dfs_read_ranges_frame::Body::Data(data) => {
                assert!(data.len() <= 64 * 1024);
                received.extend(data);
            }
            dfs_read_ranges_frame::Body::Completion(completion) => {
                assert_eq!(
                    completion.range_checksum,
                    blake3::hash(&bytes).as_bytes().to_vec()
                );
                completions += 1;
            }
            dfs_read_ranges_frame::Body::Header(_) => {}
        }
    }
    assert_eq!(received, bytes);
    assert_eq!(completions, 1);
}

#[cfg(feature = "dfs")]
#[test]
fn dfs_range_request_rejects_expired_grants_and_unbounded_batches() {
    let mut request = dfs_read_request("chunk".into(), 1);
    request.operations[0]
        .grant
        .as_mut()
        .unwrap()
        .expires_at_unix_ms = 1;
    assert!(validate_dfs_read_request(&request).is_err());
    request.operations[0]
        .grant
        .as_mut()
        .unwrap()
        .expires_at_unix_ms = u64::MAX;
    request.operations[0].length = MAX_DFS_READ_BYTES + 1;
    assert!(validate_dfs_read_request(&request).is_err());
}
#[cfg(feature = "dfs")]
#[tokio::test]
async fn retired_open_read_token_gets_no_special_authority() {
    use crate::dfs::*;
    let (meta, _local, _temp, request) = memory_read_authority_fixture().await;
    let service = meta.dfs.as_ref().unwrap();
    let grant = request.operations[0].grant.as_ref().unwrap();
    let mut check = ValidateDfsReadGrants {
        receiver_node_id: "receiver".into(),
        receiver_node_epoch: 1,
        peer_node_id: "reader".into(),
        validations: vec![DfsReadValidation {
            grant: DfsReadGrant {
                namespace_id: NamespaceId::new(grant.namespace_id.clone()),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                caller_node_id: "reader".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: grant.expires_at_unix_ms,
                fence: grant.fence,
                token: grant.token.clone(),
            },
            chunk_id: ChunkId::new(request.operations[0].chunk_id.clone()),
            copy_id: CopyId::new("copy"),
            chunk_offset: 1024,
            length: 1024,
        }],
    };
    service.validate_read_grants(check.clone()).await.unwrap();
    check.validations[0].grant.token = "dfs-open-v1:read-open:old-process".into();
    let error = service.validate_read_grants(check).await.unwrap_err();
    assert_eq!(error.code(), afs_error::IO_PERMISSION_DENIED);
}

/// R59：持有方只信自己读盘：坏副本被隔离（之后由修复 worker 自报 Meta），好副本不变；
/// 同一 chunk 冷却期内不重复全量校验。
#[cfg(feature = "dfs")]
#[tokio::test]
async fn a_suspect_review_quarantines_only_a_copy_that_is_bad_on_this_disk() {
    use crate::{dfs::OperationId, node::chunk::StagedChunk};
    fn find_body(dir: &std::path::Path, body: &[u8]) -> Option<std::path::PathBuf> {
        std::fs::read_dir(dir).unwrap().find_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                find_body(&path, body)
            } else {
                (std::fs::read(&path).unwrap() == body).then_some(path)
            }
        })
    }
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-b").unwrap());
    let good = StagedChunk::new(OperationId::new("good"), vec![1; 8192]);
    let bad = StagedChunk::new(OperationId::new("bad"), vec![2; 8192]);
    for staged in [good.clone(), bad.clone()] {
        local.put(staged).unwrap();
    }
    let path = find_body(temp.path(), &vec![2; 8192]).unwrap();
    let mut body = std::fs::read(&path).unwrap();
    body[100] ^= 0xff;
    std::fs::write(&path, body).unwrap();
    let service = dfs_reuse_test_service(local.clone(), Arc::new(DenyDfsReadAuthorizer));
    let review = |chunk: &StagedChunk| {
        Request::new(afs_protocol::node_data::DfsReviewSuspectChunkRequest {
            chunk_id: chunk.chunk.id.0.clone(),
        })
    };
    let reply = service
        .review_suspect_chunk(review(&good))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.reviewed && !reply.copy_invalid);
    let reply = service
        .review_suspect_chunk(review(&bad))
        .await
        .unwrap()
        .into_inner();
    assert!(reply.reviewed && reply.copy_invalid);
    let quarantined = local.quarantined_chunks().unwrap();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].chunk.id, bad.chunk.id);
    let again = service
        .review_suspect_chunk(review(&good))
        .await
        .unwrap()
        .into_inner();
    assert!(!again.reviewed, "cooling down");
}

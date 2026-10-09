use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc, time::Duration};

use afs::node::{
    api::local::{LocalApiServer, serve_local_api},
    storage::{MAX_TRANSFER_BYTES, Storage},
};
use afs_client::{
    DfsAccess, DfsFileVersion, DfsLocalClient, DfsLocalClientConfig, DfsReadBuffer, DfsWriteBuffer,
    DiagnosticLocalClient, DiagnosticLocalClientConfig, LocalClientError,
    max_parallel_shm_operations,
};
use afs_protocol::local_api::{LocalWriteRequest, local_data_client::LocalDataClient};
use hyper_util::rt::TokioIo;
use tonic::{Code, transport::Endpoint};
use tower::service_fn;

async fn start_local_api(temp: &tempfile::TempDir) -> (LocalApiServer, DiagnosticLocalClient) {
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("data")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");
    assert!(!server.is_finished());
    assert!(server.abort_handle().is_some());
    let client = DiagnosticLocalClient::connect(DiagnosticLocalClientConfig::new(socket_path))
        .await
        .expect("connect local client");
    (server, client)
}

async fn raw_local_client(path: &Path) -> LocalDataClient<tonic::transport::Channel> {
    let path = Arc::new(path.to_path_buf());
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("static endpoint URI")
        .connect_with_connector(service_fn(move |_| {
            let path = Arc::clone(&path);
            async move {
                let stream = tokio::net::UnixStream::connect(path.as_ref()).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("raw local client");
    LocalDataClient::new(channel)
}

#[tokio::test]
async fn diagnostic_local_sdk_writes_and_reads_eight_bytes_through_shm() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (server, client) = start_local_api(&temp).await;

    let written = client
        .write("alpha.bin", 0, b"12345678".to_vec())
        .await
        .expect("write through local sdk");
    assert_eq!(written, 8);

    let read = client
        .read("alpha.bin", 0, 8)
        .await
        .expect("read through local sdk");
    assert_eq!(read, b"12345678");

    server.shutdown().await.expect("shutdown local api");
}

#[tokio::test]
async fn local_api_rejects_missing_shm_grant_without_data_fallback() {
    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("data")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");
    let mut client = raw_local_client(&socket_path).await;

    let error = client
        .write(LocalWriteRequest {
            name: "missing.bin".to_owned(),
            file_offset: 0,
            length: 8,
            source: None,
        })
        .await
        .expect_err("missing SHM grant must fail");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("missing SHM source grant"));

    server.shutdown().await.expect("shutdown local api");
}

#[tokio::test]
async fn dfs_local_sdk_is_typed_and_never_falls_through_to_diagnostics_storage() {
    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");
    let diagnostic =
        DiagnosticLocalClient::connect(DiagnosticLocalClientConfig::new(socket_path.clone()))
            .await
            .expect("connect diagnostic client");
    diagnostic
        .write("inode-test", 0, b"diagnostic-only".to_vec())
        .await
        .expect("seed diagnostics object");

    let dfs = DfsLocalClient::connect(DfsLocalClientConfig::new(socket_path))
        .await
        .expect("connect DFS local client");
    let error = dfs
        .open("default", "inode-test", DfsAccess::ReadWrite, None)
        .await
        .expect_err("unwired DFS service must fail explicitly");
    assert!(
        matches!(error, LocalClientError::Status(status) if status.code() == afs_error::NODE_VFS_UNIMPLEMENTED)
    );
    assert_eq!(
        diagnostic
            .read("inode-test", 0, 15)
            .await
            .expect("diagnostics object remains independent"),
        b"diagnostic-only"
    );

    server.shutdown().await.expect("shutdown local api");
}

#[test]
fn dfs_sdk_buffers_and_fixed_version_validate_ownership_inputs() {
    let read = DfsReadBuffer::new(8).expect("allocate DFS read buffer");
    assert_eq!(read.capacity(), 8);
    assert_eq!(read.read_prefix(0).unwrap(), Vec::<u8>::new());

    let write = DfsWriteBuffer::from_bytes(b"payload".to_vec()).expect("DFS write buffer");
    assert_eq!(write.length(), 7);

    let version = DfsFileVersion::new("default", "inode-test", "version:1")
        .expect("complete fixed version identity");
    assert_eq!(
        version,
        DfsFileVersion::new("default", "inode-test", "version:1").unwrap()
    );
    assert!(matches!(
        DfsFileVersion::new("", "inode-test", "version:1"),
        Err(LocalClientError::InvalidArgument {
            field: "namespace_id",
            ..
        })
    ));
}

#[tokio::test]
async fn local_api_socket_is_owner_only() {
    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("data")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");

    let socket_mode = std::fs::metadata(&socket_path)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    let parent_mode = std::fs::metadata(temp.path())
        .expect("parent metadata")
        .permissions()
        .mode()
        & 0o077;
    assert!(
        socket_mode == 0o600 || parent_mode == 0,
        "socket mode {socket_mode:o} must be 0600 unless parent dir blocks group/other access"
    );

    server.shutdown().await.expect("shutdown local api");
}

#[tokio::test]
async fn server_shutdown_removes_owned_socket() {
    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = Storage::new(temp.path().join("data")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");
    assert!(socket_path.exists());

    server.shutdown().await.expect("shutdown local api");
    assert!(!socket_path.exists());
}

#[tokio::test]
async fn local_sdk_rejects_oversized_write_without_grpc_data_fallback() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (server, client) = start_local_api(&temp).await;
    let data = vec![b'x'; MAX_TRANSFER_BYTES + 1];

    let error = client
        .write("too-large.bin", 0, data)
        .await
        .expect_err("oversized storage write must fail");
    match error {
        LocalClientError::Status(status) => {
            assert_eq!(status.kind(), afs_error::ErrorKind::InvalidArgument)
        }
        LocalClientError::Shm(afs_transport::shm::ShmError::InvalidArgument { field, .. }) => {
            assert_eq!(field, "len");
        }
        other => panic!("unexpected error: {other}"),
    }

    server.shutdown().await.expect("shutdown local api");
}

#[tokio::test]
async fn cancelled_sdk_calls_do_not_exhaust_static_shm_slots() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (server, client) = start_local_api(&temp).await;

    for idx in 0..(max_parallel_shm_operations() + 8) {
        let client = client.clone();
        let handle = tokio::spawn(async move {
            let _ = client
                .write(&format!("cancel-{idx}.bin"), 0, b"x".to_vec())
                .await;
        });
        tokio::task::yield_now().await;
        handle.abort();
    }

    let written = tokio::time::timeout(
        Duration::from_secs(5),
        client.write("after-cancel.bin", 0, b"z".to_vec()),
    )
    .await
    .expect("post-cancel write must not hang")
    .expect("post-cancel write must succeed");
    assert_eq!(written, 1);

    server.shutdown().await.expect("shutdown local api");
}

#[tokio::test]
async fn missing_file_preserves_rpc_error_and_releases_broker_before_return() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (server, client) = start_local_api(&temp).await;
    let error = tokio::time::timeout(Duration::from_secs(3), client.read("absent.bin", 0, 8))
        .await
        .expect("failure must be bounded")
        .expect_err("file absent");
    assert!(
        matches!(error, LocalClientError::Status(status) if status.code() == afs_error::NODE_STORAGE_NOT_FOUND)
    );
    assert_eq!(
        client
            .write("after-failure", 0, b"ok".to_vec())
            .await
            .unwrap(),
        2
    );
    server.shutdown().await.unwrap();
}

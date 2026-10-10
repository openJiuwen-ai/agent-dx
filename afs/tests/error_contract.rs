//! Verify observable failures through real TCP/UDS adapters, not only mapping helpers.
use afs::{
    node::{
        api::local::serve_local_api,
        rpc::{
            control::RdmaSessionRegistry,
            data::make_data_server,
            peer::{DataClientOptions, DataMode, connect_data_client},
        },
        storage::Storage,
    },
    runtime::Observability,
};
use afs_client::{DiagnosticLocalClient, DiagnosticLocalClientConfig};
use afs_error::*;
use axum::response::IntoResponse;
use std::{sync::Arc, time::Duration};
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn tcp_peer_and_local_sdk_preserve_identical_storage_identity() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new(temp.path().join("data")).unwrap();
    let local = serve_local_api(storage.clone(), temp.path().join("node.sock"))
        .await
        .unwrap();
    let sdk = DiagnosticLocalClient::connect(DiagnosticLocalClientConfig::new(
        temp.path().join("node.sock"),
    ))
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(make_data_server(
                Arc::new(storage),
                RdmaSessionRegistry::default(),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let mut peer = connect_data_client(DataClientOptions {
        endpoint: format!("http://{address}"),
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(2),
    })
    .await
    .unwrap();
    for (name, expected) in [
        ("missing", NODE_STORAGE_NOT_FOUND),
        ("../escape", NODE_STORAGE_INVALID),
    ] {
        let a = peer.read(name, 0, 1).await.unwrap_err();
        let b = sdk.read(name, 0, 1).await.unwrap_err();
        assert_eq!(a.code(), expected);
        assert_eq!(a.code(), b.code());
        assert_eq!(a.kind(), b.kind());
    }
    // Same transport remains usable; an ordinary file error is not a connection failure.
    peer.write("ok", 0, b"ok".to_vec()).await.unwrap();
    assert_eq!(sdk.read("ok", 0, 2).await.unwrap(), b"ok");
    peer.close().await.unwrap();
    local.shutdown().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn rest_body_and_posix_errno_preserve_meaning() {
    for (code, http, errno) in [
        (IO_PERMISSION_DENIED, 403, libc::EACCES),
        (IO_NOT_DIRECTORY, 412, libc::ENOTDIR),
        (NODE_VFS_UNIMPLEMENTED, 501, libc::ENOSYS),
        (NODE_MOUNT_CONFLICT, 409, libc::EEXIST),
        (IO_TIMEOUT, 504, libc::ETIMEDOUT),
        (IO_OUT_OF_MEMORY, 429, libc::ENOMEM),
        (IO_BAD_FILE_DESCRIPTOR, 412, libc::EBADF),
        (IO_NAME_TOO_LONG, 400, libc::ENAMETOOLONG),
        (IO_TOO_MANY_SYMLINKS, 412, libc::ELOOP),
        (IO_FILE_TOO_LARGE, 400, libc::EFBIG),
        (NODE_OWNER_STALE_ACCESS, 412, libc::ESTALE),
        (NODE_OWNER_STALE_HANDLE, 412, libc::ESTALE),
    ] {
        let error = Error::coded(code, "diagnostic only");
        assert_eq!(afs::error::errno(&error), errno);
        let response = afs::error::RestError(error.clone()).into_response();
        assert_eq!(response.status().as_u16(), http);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"].as_u64(), Some(code.raw() as u64));
        assert_eq!(json["error"]["message"], error.message());
    }
}

#[tokio::test]
async fn meta_grpc_keeps_native_validation_code() {
    use afs_protocol::meta::{PingRequest, meta_server::Meta as _};
    let service = afs::meta::rpc::MetaRpc(Arc::new(afs::meta::Meta::new(
        "test".into(),
        Observability::new().unwrap(),
    )));
    let status = service
        .ping(tonic::Request::new(PingRequest {
            node_id: "x".repeat(129),
        }))
        .await
        .unwrap_err();
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(status).code(),
        META_CATALOG_INVALID_REQUEST
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_raw_errno_preserves_posix_file_errors() {
    for (raw, code, errno) in [
        (9, IO_BAD_FILE_DESCRIPTOR, libc::EBADF),
        (4, IO_INTERRUPTED, libc::EINTR),
        (11, IO_WOULD_BLOCK, libc::EAGAIN),
        (35, IO_DEADLOCK, libc::EDEADLK),
        (37, IO_NO_LOCKS, libc::ENOLCK),
        (27, IO_FILE_TOO_LARGE, libc::EFBIG),
        (36, IO_NAME_TOO_LONG, libc::ENAMETOOLONG),
        (40, IO_TOO_MANY_SYMLINKS, libc::ELOOP),
    ] {
        let error = Error::from(std::io::Error::from_raw_os_error(raw));
        assert_eq!(error.code(), code);
        assert_eq!(afs::error::errno(&error), errno);
    }
}

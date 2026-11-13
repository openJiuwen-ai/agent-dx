#![cfg(feature = "dfs")]

//! Meta read RPCs accept only certificates that map to a trusted node
//! (R41, model M0: one trust domain; a trusted node may read all DFS metadata).

use std::{collections::HashMap, path::Path, process::Command, sync::Arc, time::Duration};

use afs::{
    dfs::{InodeAttributes, InodeId, NamespaceId, OperationId, WriteLease},
    meta::{
        Meta,
        dfs::{CreateFileRequest, DfsService},
        rpc::{DfsMetaRpc, MetaRpc},
        store::{MetaStore, Store, journal::MemoryLog},
    },
    runtime::Observability,
};
use afs_protocol::meta::{
    DfsCallerContext, DfsGetXattrRequest, DfsListXattrRequest, DfsLookupRequest, DfsReadDirRequest,
    DfsReadLinkRequest, GetDfsInodeRequest, GetFileVersionRequest, LookupNodeRequest, PingRequest,
    dfs_meta_client::DfsMetaClient,
    dfs_meta_server::{DfsMeta, DfsMetaServer},
    meta_client::MetaClient,
    meta_server::MetaServer,
};
use afs_transport::{SecurityManager, TlsConfig};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code, Request,
    transport::{Channel, Endpoint, Server},
};

const FILE: &[u8] = b"authz.bin";

fn openssl(dir: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .current_dir(dir)
        .args(args)
        .output()
        .expect("mTLS contracts require openssl");
    assert!(
        output.status.success(),
        "openssl {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// One CA signs every leaf, so TLS accepts `outsider`; only the trusted-node
/// map tells it apart from `node-a`.
fn generate_certificates(dir: &Path) -> HashMap<Vec<u8>, String> {
    std::fs::write(
        dir.join("ca.cnf"),
        concat!(
            "[req]\ndistinguished_name=dn\nx509_extensions=ca\nprompt=no\n",
            "[dn]\nCN=AFS read authz test CA\n[ca]\n",
            "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n",
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("leaf.ext"),
        "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n",
    )
    .unwrap();
    openssl(
        dir,
        &[
            "req", "-config", "ca.cnf", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-keyout", "ca.key", "-out", "ca.pem",
        ],
    );
    for (serial, name) in ["meta", "node-a", "outsider"].into_iter().enumerate() {
        let (key, csr, pem, der) = (
            format!("{name}.key"),
            format!("{name}.csr"),
            format!("{name}.pem"),
            format!("{name}.der"),
        );
        let subject = format!("/CN={name}");
        openssl(
            dir,
            &[
                "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", &subject, "-keyout", &key,
                "-out", &csr,
            ],
        );
        let serial = (serial + 1).to_string();
        openssl(
            dir,
            &[
                "x509",
                "-req",
                "-in",
                &csr,
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-set_serial",
                &serial,
                "-days",
                "2",
                "-extfile",
                "leaf.ext",
                "-out",
                &pem,
            ],
        );
        openssl(dir, &["x509", "-in", &pem, "-outform", "DER", "-out", &der]);
    }
    HashMap::from([(
        std::fs::read(dir.join("node-a.der")).unwrap(),
        "node-a".to_string(),
    )])
}

fn tls(dir: &Path, name: &str) -> TlsConfig {
    TlsConfig::MutualTls {
        encryption_enabled: true,
        ca_certificate: dir.join("ca.pem"),
        identity_certificate: dir.join(format!("{name}.pem")),
        identity_private_key: dir.join(format!("{name}.key")),
        server_name: "localhost".into(),
    }
}

async fn store_with_file() -> (Arc<dyn MetaStore>, WriteLease) {
    let store: Arc<dyn MetaStore> = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let (_, lease) = DfsService::new(store.clone())
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-authz"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: FILE.to_vec(),
            attributes: InodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds: 30,
            first_generation: None,
        })
        .await
        .unwrap();
    (store, lease)
}

fn lookup() -> DfsLookupRequest {
    DfsLookupRequest {
        namespace_id: "default".into(),
        parent_inode_id: "1".into(),
        name: FILE.to_vec(),
    }
}

fn read_dir() -> DfsReadDirRequest {
    DfsReadDirRequest {
        namespace_id: "default".into(),
        parent_inode_id: "1".into(),
        ..Default::default()
    }
}

fn read_link(inode: &str) -> DfsReadLinkRequest {
    DfsReadLinkRequest {
        namespace_id: "default".into(),
        inode_id: inode.into(),
    }
}

fn get_inode(inode: &str) -> GetDfsInodeRequest {
    GetDfsInodeRequest {
        inode_id: inode.into(),
    }
}

fn caller() -> Option<DfsCallerContext> {
    Some(DfsCallerContext {
        uid: 0,
        gid: 0,
        supplementary_gids: Vec::new(),
    })
}

fn get_xattr(inode: &str) -> DfsGetXattrRequest {
    DfsGetXattrRequest {
        inode_id: inode.into(),
        name: b"user.none".to_vec(),
        caller: caller(),
    }
}

fn list_xattr(inode: &str) -> DfsListXattrRequest {
    DfsListXattrRequest {
        inode_id: inode.into(),
        caller: caller(),
    }
}

fn get_file_version() -> GetFileVersionRequest {
    GetFileVersionRequest {
        version_id: "no-such-version".into(),
    }
}

struct MtlsMeta {
    dir: tempfile::TempDir,
    endpoint: String,
    inode: String,
    server: tokio::task::JoinHandle<()>,
}

impl MtlsMeta {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let trusted = tokio::task::spawn_blocking(move || generate_certificates(&path))
            .await
            .unwrap();
        let (store, lease) = store_with_file().await;
        let meta = Arc::new(Meta::with_store_and_peer_identity(
            "read-authz".into(),
            Observability::new().unwrap(),
            store,
            trusted,
            true,
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let security = SecurityManager::new(tls(dir.path(), "meta")).unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MetaServer::new(MetaRpc(Arc::clone(&meta))))
                .add_service(DfsMetaServer::new(DfsMetaRpc(meta)))
                .serve_with_incoming(security.incoming(TcpListenerStream::new(listener)).unwrap())
                .await
                .unwrap();
        });
        Self {
            dir,
            endpoint,
            inode: lease.inode_id.0,
            server,
        }
    }

    async fn channel(&self, name: &str) -> Channel {
        let security = SecurityManager::new(tls(self.dir.path(), name)).unwrap();
        let endpoint = Endpoint::from_shared(security.endpoint_uri(&self.endpoint))
            .unwrap()
            .timeout(Duration::from_secs(5));
        security.connect(endpoint).await.unwrap()
    }

    async fn client(&self, name: &str) -> DfsMetaClient<Channel> {
        DfsMetaClient::new(self.channel(name).await)
    }

    async fn meta_client(&self, name: &str) -> MetaClient<Channel> {
        MetaClient::new(self.channel(name).await)
    }
}

impl Drop for MtlsMeta {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn read_rpcs_reject_a_same_ca_certificate_outside_the_trusted_node_list() {
    let meta = MtlsMeta::start().await;
    let mut client = meta.client("outsider").await;
    let inode = meta.inode.as_str();
    let errors = [
        ("lookup", client.lookup(lookup()).await.unwrap_err()),
        ("read_dir", client.read_dir(read_dir()).await.unwrap_err()),
        (
            "read_link",
            client.read_link(read_link(inode)).await.unwrap_err(),
        ),
        (
            "get_inode",
            client.get_inode(get_inode(inode)).await.unwrap_err(),
        ),
        (
            "get_file_version",
            client
                .get_file_version(get_file_version())
                .await
                .unwrap_err(),
        ),
        (
            "get_xattr",
            client.get_xattr(get_xattr(inode)).await.unwrap_err(),
        ),
        (
            "list_xattr",
            client.list_xattr(list_xattr(inode)).await.unwrap_err(),
        ),
    ];
    let mut meta_client = meta.meta_client("outsider").await;
    let errors = errors.into_iter().chain([
        (
            "ping",
            meta_client
                .ping(PingRequest {
                    node_id: "node-a".into(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "lookup_node",
            meta_client
                .lookup_node(LookupNodeRequest {
                    request_id: "lookup".into(),
                    node_id: "node-a".into(),
                })
                .await
                .unwrap_err(),
        ),
    ]);
    for (rpc, error) in errors {
        assert_eq!(error.code(), Code::PermissionDenied, "{rpc}: {error:?}");
        assert!(
            error.message().contains("not trusted for any node"),
            "{rpc}: {error:?}"
        );
    }
}

#[tokio::test]
async fn read_rpcs_serve_a_trusted_node_certificate() {
    let meta = MtlsMeta::start().await;
    let mut client = meta.client("node-a").await;
    let inode = meta.inode.as_str();
    let found = client.lookup(lookup()).await.unwrap().into_inner();
    assert_eq!(found.inode.unwrap().inode_id, inode);
    let entries = client.read_dir(read_dir()).await.unwrap().into_inner();
    assert!(entries.entries.iter().any(|entry| entry.name == FILE));
    assert_eq!(
        client
            .get_inode(get_inode(inode))
            .await
            .unwrap()
            .into_inner()
            .inode
            .unwrap()
            .inode_id,
        inode
    );
    // The fixture has no symlink or committed version: these two pass the
    // certificate check and then fail on the request itself.
    for error in [
        client.read_link(read_link(inode)).await.unwrap_err(),
        client
            .get_file_version(get_file_version())
            .await
            .unwrap_err(),
    ] {
        assert_ne!(error.code(), Code::PermissionDenied, "{error:?}");
    }
    let mut meta_client = meta.meta_client("node-a").await;
    meta_client
        .ping(PingRequest {
            node_id: "node-a".into(),
        })
        .await
        .unwrap();
    let node = meta_client
        .lookup_node(LookupNodeRequest {
            request_id: "lookup".into(),
            node_id: "node-a".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!node.found, "the fixture registers no node session");
}

#[tokio::test]
async fn read_rpcs_without_enforced_peer_identity_need_no_certificate() {
    let (store, lease) = store_with_file().await;
    let plain = DfsMetaRpc(Arc::new(Meta::with_store(
        "read-authz".into(),
        Observability::new().unwrap(),
        store,
    )));
    let inode = lease.inode_id.0.as_str();
    plain.lookup(Request::new(lookup())).await.unwrap();
    plain.read_dir(Request::new(read_dir())).await.unwrap();
    plain
        .get_inode(Request::new(get_inode(inode)))
        .await
        .unwrap();
    for error in [
        plain
            .read_link(Request::new(read_link(inode)))
            .await
            .unwrap_err(),
        plain
            .get_file_version(Request::new(get_file_version()))
            .await
            .unwrap_err(),
    ] {
        assert_ne!(error.code(), Code::PermissionDenied, "{error:?}");
    }
}

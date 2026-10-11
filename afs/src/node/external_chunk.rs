//! Bounded synchronous access to immutable S3 chunks. External receipts never
//! manufacture a Node replica acknowledgement or a placement request.

use std::{
    fs::OpenOptions,
    future::poll_fn,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use afs_error::{Error, Result};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig, aws::AmazonS3Builder,
    path::Path as ObjectPath,
};
use serde::Deserialize;

use crate::{
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, CopyLocation, CopyRole,
        CopyState, DigestAlgorithm, ExternalCommitReceipt, OperationId, ReplicaGroupId,
        SourceCandidate, external_object_key,
    },
    node::{
        chunk::{ChunkStore, MAX_STAGED_CHUNK_BYTES, StagedChunk},
        dfs_read::ExternalChunkReader,
    },
};

const MAX_BATCH: usize = 128;
const MAX_CREDENTIAL_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug)]
pub enum S3CredentialsSource {
    Environment,
    /// JSON containing access_key_id, secret_access_key and optional session_token.
    /// The file must be owned by this process's effective UID, with mode 0600 or stricter.
    File(PathBuf),
}

#[derive(Clone, Debug)]
pub struct S3ChunkStoreOptions {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub store_id: String,
    pub generation: String,
    pub allow_http: bool,
    pub credentials: S3CredentialsSource,
    /// Deadline for a whole batch, including retry and response-body collection.
    pub request_timeout: Duration,
    pub max_retries: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

enum Request {
    Put(Vec<StagedChunk>),
    Read(ChunkObject),
}

enum Reply {
    Put(Vec<ChunkReceipt>),
    Read(Vec<u8>),
}

struct Command {
    request: Request,
    reply: mpsc::SyncSender<Result<Reply>>,
}

/// Cache-OFF S3 storage: at most one active request and one queued request.
///
/// A single owned native thread runs all asynchronous operations, so synchronous
/// VFS calls never invoke Tokio block_on on a Tokio business worker. Each batch
/// is bounded to 128 staged 4MiB chunks, matching the producer operation budget.
/// Uploads and downloads execute sequentially; only one extra chunk payload is
/// allocated for an upload and one 4MiB buffer for a download.
/// Dropping the adapter closes and joins its
/// worker; no operation task is detached. Failed PUTs produce no receipt, though
/// a timeout may leave an unreferenced immutable object in S3.
pub struct S3ChunkStore {
    options: S3ChunkStoreOptions,
    sender: Option<mpsc::SyncSender<Command>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl S3ChunkStore {
    /// Rejects invalid identity, credentials, endpoint or resource budgets.
    pub fn new(options: S3ChunkStoreOptions) -> Result<Self> {
        validate_options(&options)?;
        let credentials = load_credentials(&options.credentials)?;
        adx_transport::install_crypto_provider();
        let retry = RetryConfig {
            max_retries: options.max_retries,
            retry_timeout: options.request_timeout,
            ..Default::default()
        };
        let client = ClientOptions::new()
            .with_timeout(options.request_timeout)
            .with_connect_timeout(options.request_timeout.min(Duration::from_secs(10)))
            .with_pool_max_idle_per_host(1);
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&options.endpoint)
            .with_bucket_name(&options.bucket)
            .with_region(&options.region)
            .with_virtual_hosted_style_request(false)
            .with_access_key_id(credentials.access_key_id)
            .with_secret_access_key(credentials.secret_access_key)
            .with_retry(retry)
            .with_client_options(client)
            .with_allow_http(options.allow_http);
        if let Some(token) = credentials.session_token {
            builder = builder.with_token(token);
        }
        let backend = builder
            .build()
            .map_err(|_| invalid("cannot construct S3 client from configured settings"))?;
        Self::with_backend(options, Arc::new(backend))
    }

    fn with_backend(options: S3ChunkStoreOptions, backend: Arc<dyn ObjectStore>) -> Result<Self> {
        validate_options(&options)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::from)?;
        let (sender, receiver) = mpsc::sync_channel::<Command>(1);
        let identity = options.clone();
        let worker = thread::Builder::new()
            .name("afs-s3-chunks".into())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    let result = runtime.block_on(async {
                        tokio::time::timeout(
                            identity.request_timeout,
                            execute(&*backend, &identity, command.request),
                        )
                        .await
                        .map_err(|_| {
                            Error::coded(afs_error::IO_TIMEOUT, "S3 operation timed out")
                        })?
                    });
                    let _ = command.reply.send(result);
                }
            })
            .map_err(Error::from)?;
        Ok(Self {
            options,
            sender: Some(sender),
            worker: Some(worker),
        })
    }

    fn request(&self, request: Request) -> Result<Reply> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| unavailable("S3 worker stopped"))?;
        sender
            .try_send(Command { request, reply })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => {
                    Error::coded(afs_error::IO_WOULD_BLOCK, "S3 request queue is full")
                }
                mpsc::TrySendError::Disconnected(_) => unavailable("S3 worker stopped"),
            })?;
        // A queued request waits for at most one other bounded operation.
        receiver
            .recv_timeout(self.options.request_timeout * 2 + Duration::from_secs(1))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    Error::coded(afs_error::IO_TIMEOUT, "S3 worker response timed out")
                }
                mpsc::RecvTimeoutError::Disconnected => unavailable("S3 worker stopped"),
            })?
    }

    fn read_verified(&self, chunk: &ChunkObject) -> Result<Vec<u8>> {
        validate_chunk(chunk)?;
        match self.request(Request::Read(chunk.clone()))? {
            Reply::Read(bytes) => Ok(bytes),
            Reply::Put(_) => Err(invalid("S3 worker returned the wrong response")),
        }
    }
}

impl Drop for S3ChunkStore {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl ChunkStore for S3ChunkStore {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        if staged.is_empty() {
            return Ok(Vec::new());
        }
        if staged.len() > MAX_BATCH {
            return Err(invalid("S3 batch allows at most 128 chunks"));
        }
        for value in &staged {
            validate_chunk(&value.chunk)?;
            verify_bytes(&value.chunk, value.bytes())?;
        }
        match self.request(Request::Put(staged))? {
            Reply::Put(receipts) => Ok(receipts),
            Reply::Read(_) => Err(invalid("S3 worker returned the wrong response")),
        }
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        let chunk = chunk_from_id(chunk_id)?;
        if offset >= chunk.length || out.is_empty() {
            return Ok(0);
        }
        let bytes = self.read_verified(&chunk)?;
        let start = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let length = out.len().min(bytes.len() - start);
        out[..length].copy_from_slice(&bytes[start..start + length]);
        Ok(length)
    }
}

impl ExternalChunkReader for S3ChunkStore {
    fn read_chunk(&self, chunk: &ChunkObject, source: &SourceCandidate) -> Result<Vec<u8>> {
        let expected =
            external_object_key(&self.options.store_id, &self.options.generation, &chunk.id)?;
        if source.chunk_id != chunk.id
            || source.role != CopyRole::ExternalCommitted
            || source.state != CopyState::Ready
            || !matches!(&source.location, CopyLocation::External { store_id, object_key, object_revision }
                if store_id == &self.options.store_id && object_key == &expected
                    && object_revision == &self.options.generation)
        {
            return Err(invalid(
                "external source does not match the configured immutable object",
            ));
        }
        self.read_verified(chunk)
    }
}

async fn execute(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    request: Request,
) -> Result<Reply> {
    match request {
        Request::Put(staged) => {
            let mut receipts = Vec::with_capacity(staged.len());
            for value in staged {
                let key =
                    external_object_key(&options.store_id, &options.generation, &value.chunk.id)?;
                // Ordinary PUT only: key identity is content-bound and immutable.
                backend
                    .put(
                        &ObjectPath::from(key.as_str()),
                        value.bytes().to_vec().into(),
                    )
                    .await
                    .map_err(storage_error)?;
                receipts.push(ChunkReceipt {
                    operation_id: value.operation_id,
                    chunk: value.chunk.clone(),
                    placement_revision: 0,
                    placement_epoch: 0,
                    replica_group_id: ReplicaGroupId::new(""),
                    durable_acks: Vec::new(),
                    external_commit: Some(ExternalCommitReceipt {
                        store_id: options.store_id.clone(),
                        generation: options.generation.clone(),
                        object_key: key,
                        persisted_bytes: value.chunk.length,
                        verified_digest: value.chunk.content_digest,
                    }),
                });
            }
            Ok(Reply::Put(receipts))
        }
        Request::Read(chunk) => {
            let key = external_object_key(&options.store_id, &options.generation, &chunk.id)?;
            // No HEAD, versionId or conditional GET in the normal data path.
            let response = backend
                .get(&ObjectPath::from(key.as_str()))
                .await
                .map_err(storage_error)?;
            if response.meta.size != chunk.length || response.range != (0..chunk.length) {
                return Err(corrupt(
                    "S3 object response length does not match Chunk metadata",
                ));
            }
            let limit = usize::try_from(chunk.length).map_err(|_| invalid("chunk is too large"))?;
            let mut bytes = Vec::with_capacity(limit);
            let mut stream = response.into_stream();
            while let Some(part) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                let part = part.map_err(storage_error)?;
                if part.len() > limit.saturating_sub(bytes.len()) {
                    return Err(corrupt(
                        "S3 response body exceeds the immutable Chunk length",
                    ));
                }
                bytes.extend_from_slice(&part);
            }
            verify_bytes(&chunk, &bytes)?;
            Ok(Reply::Read(bytes))
        }
    }
}

fn validate_options(options: &S3ChunkStoreOptions) -> Result<()> {
    let probe = StagedChunk::new(OperationId::new("identity-probe"), Vec::new());
    external_object_key(&options.store_id, &options.generation, &probe.chunk.id)?;
    if options.bucket.is_empty()
        || options.region.is_empty()
        || options.request_timeout.is_zero()
        || options.request_timeout > Duration::from_secs(300)
        || options.max_retries > 10
        || !(options.endpoint.starts_with("https://") || options.endpoint.starts_with("http://"))
        || options.endpoint.contains(['@', '?', '#'])
    {
        return Err(invalid(
            "invalid S3 endpoint, bucket, region or bounded request settings",
        ));
    }
    Ok(())
}

fn load_credentials(source: &S3CredentialsSource) -> Result<Credentials> {
    let credentials = match source {
        S3CredentialsSource::Environment => Credentials {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
                .map_err(|_| invalid("AWS_ACCESS_KEY_ID is required"))?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .map_err(|_| invalid("AWS_SECRET_ACCESS_KEY is required"))?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        },
        S3CredentialsSource::File(path) => {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .map_err(|_| invalid("cannot open S3 credentials file"))?;
            let metadata = file
                .metadata()
                .map_err(|_| invalid("cannot inspect S3 credentials file"))?;
            // SAFETY: geteuid has no arguments or memory preconditions.
            let uid = unsafe { libc::geteuid() };
            if !metadata.is_file() || metadata.mode() & 0o077 != 0 || metadata.uid() != uid {
                return Err(invalid(
                    "S3 credentials require a private file owned by the current UID",
                ));
            }
            let mut bytes = Vec::new();
            file.take(MAX_CREDENTIAL_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid("cannot read S3 credentials file"))?;
            if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
                return Err(invalid("S3 credentials file is too large"));
            }
            serde_json::from_slice::<Credentials>(&bytes)
                .map_err(|_| invalid("invalid S3 credentials file JSON"))?
        }
    };
    if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
        return Err(invalid(
            "S3 credentials must contain nonempty access and secret keys",
        ));
    }
    Ok(credentials)
}

fn chunk_from_id(id: &ChunkId) -> Result<ChunkObject> {
    let remainder =
        id.0.strip_prefix("blake3-")
            .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let (hex, length) = remainder
        .split_once('-')
        .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let length = length
        .parse::<u64>()
        .map_err(|_| invalid("invalid immutable Chunk length"))?;
    if hex.len() != 64
        || length > MAX_STAGED_CHUNK_BYTES as u64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || id.0 != format!("blake3-{hex}-{length}")
    {
        return Err(invalid(
            "noncanonical immutable Chunk ID or oversized Chunk",
        ));
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid immutable Chunk digest"))?;
    }
    Ok(ChunkObject {
        id: id.clone(),
        length,
        encoding: ChunkEncoding::Raw,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes,
        },
    })
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if &chunk_from_id(&chunk.id)? != chunk {
        return Err(invalid(
            "Chunk metadata does not match its immutable identity",
        ));
    }
    Ok(())
}

fn verify_bytes(chunk: &ChunkObject, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != chunk.length
        || blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes
    {
        return Err(corrupt("immutable Chunk length or BLAKE3 mismatch"));
    }
    Ok(())
}

fn storage_error(error: object_store::Error) -> Error {
    let code = match error {
        object_store::Error::NotFound { .. } => afs_error::IO_NOT_FOUND,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => afs_error::IO_PERMISSION_DENIED,
        _ => afs_error::IO_UNAVAILABLE,
    };
    // SDK error text can contain request URLs or credential provider details.
    Error::coded(code, "external S3 operation failed")
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{CopyId, DfsReadGrant, FileVersionId, LayoutRootId, NamespaceId};
    use std::{io::Write, net::TcpListener, os::unix::fs::PermissionsExt};

    fn options() -> S3ChunkStoreOptions {
        S3ChunkStoreOptions {
            endpoint: "http://127.0.0.1:1".into(),
            bucket: "chunks".into(),
            region: "us-east-1".into(),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            allow_http: true,
            credentials: S3CredentialsSource::Environment,
            request_timeout: Duration::from_secs(2),
            max_retries: 0,
        }
    }

    fn source(chunk: &ChunkObject, key: String) -> SourceCandidate {
        SourceCandidate {
            copy_id: CopyId::new("external"),
            chunk_id: chunk.id.clone(),
            role: CopyRole::ExternalCommitted,
            state: CopyState::Ready,
            location: CopyLocation::External {
                store_id: "filesystem-a".into(),
                object_key: key,
                object_revision: "generation-1".into(),
            },
            data_endpoint: None,
            load_hint: 0,
            read_grant: DfsReadGrant {
                namespace_id: NamespaceId::new("namespace"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                caller_node_id: "reader".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "validated-by-engine".into(),
            },
        }
    }

    #[test]
    fn immutable_id_binds_both_digest_and_length() {
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert_eq!(chunk_from_id(&staged.chunk.id).unwrap(), staged.chunk);
        for id in [
            "",
            "../key",
            "blake3-ff-8",
            "blake3-0000000000000000000000000000000000000000000000000000000000000000-4194305",
        ] {
            assert!(chunk_from_id(&ChunkId::new(id)).is_err());
        }
    }

    #[test]
    fn changed_content_never_passes_integrity_validation() {
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert!(verify_bytes(&staged.chunk, b"mutated!").is_err());
        assert!(verify_bytes(&staged.chunk, b"short").is_err());
        verify_bytes(&staged.chunk, staged.bytes()).unwrap();
    }

    #[test]
    fn empty_batch_needs_no_worker_and_nonempty_batches_keep_validation() {
        // Any bridge request would fail because this adapter has no worker.
        let store = S3ChunkStore {
            options: options(),
            sender: None,
            worker: None,
        };
        assert!(store.put_batch(Vec::new()).unwrap().is_empty());

        let valid = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert_eq!(
            store
                .put_batch(vec![valid.clone(); MAX_BATCH + 1])
                .unwrap_err()
                .code(),
            afs_error::IO_INVALID
        );
        let mut invalid_chunk = valid;
        invalid_chunk.chunk.id = ChunkId::new("../invalid");
        assert_eq!(
            store.put_batch(vec![invalid_chunk]).unwrap_err().code(),
            afs_error::IO_INVALID
        );
    }

    #[test]
    fn external_receipt_and_partial_rmw_are_bound_to_the_same_object() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        let receipt = store.put(staged.clone()).unwrap();
        assert!(receipt.durable_acks.is_empty());
        assert_eq!(receipt.placement_revision, 0);
        let external = receipt.external_commit.unwrap();
        let expected =
            external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        assert_eq!(external.object_key, expected);
        assert_eq!(external.persisted_bytes, 8);
        let mut out = [0; 3];
        assert_eq!(store.read_at(&staged.chunk.id, 2, &mut out).unwrap(), 3);
        assert_eq!(&out, b"igi");
        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, expected))
                .unwrap(),
            b"original"
        );
        assert_eq!(
            store.capacity().unwrap_err().code(),
            afs_error::NODE_VFS_UNIMPLEMENTED
        );
    }

    #[test]
    fn untrusted_store_generation_or_key_is_rejected_before_get() {
        let store =
            S3ChunkStore::with_backend(options(), Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let authorized = source(&staged.chunk, key);
        for variant in 0..3 {
            let mut candidate = authorized.clone();
            if let CopyLocation::External {
                store_id,
                object_key,
                object_revision,
            } = &mut candidate.location
            {
                match variant {
                    0 => *store_id = "another-filesystem".into(),
                    1 => *object_revision = "another-generation".into(),
                    _ => object_key.push_str("-forged"),
                }
            }
            // Empty backend would yield NotFound if the binding check were bypassed.
            assert_eq!(
                store
                    .read_chunk(&staged.chunk, &candidate)
                    .unwrap_err()
                    .code(),
                afs_error::IO_INVALID
            );
        }
    }

    #[test]
    fn damaged_s3_bytes_do_not_publish_partial_rmw_output() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        let mut out = [0xa5; 3];
        assert_eq!(
            store
                .read_at(&staged.chunk.id, 2, &mut out)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [0xa5; 3]);
    }

    #[test]
    fn https_s3_client_constructs_with_explicit_tls_provider() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut config = options();
        config.endpoint = "https://127.0.0.1:1".into();
        config.allow_http = false;
        config.credentials = S3CredentialsSource::File(credentials);

        // Client construction initializes rustls; no endpoint connection is made.
        let store = S3ChunkStore::new(config).unwrap();
        drop(store);
    }

    #[test]
    fn real_s3_put_failure_returns_no_external_receipt() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = options();
        config.endpoint = format!("http://{}", listener.local_addr().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        config.credentials = S3CredentialsSource::File(credentials);
        let server = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("S3 fixture accept failed: {error}"),
                }
            };
            connection
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                connection.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
                assert!(header.len() < 16 * 1024);
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("PUT /chunks/afs/v1/filesystem-a/generation-1/chunks/"));
            assert!(!header.contains("If-Match") && !header.contains("If-None-Match"));
            connection.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let store = S3ChunkStore::new(config).unwrap();
        let result = store.put(StagedChunk::new(
            OperationId::new("failure"),
            b"original".to_vec(),
        ));
        assert!(result.is_err());
        server.join().unwrap();
    }

    #[test]
    fn credentials_file_requires_private_permissions_and_hides_secret_parse_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(&path, b"sensitive-invalid-json").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_credentials(&S3CredentialsSource::File(path.clone())).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        match load_credentials(&S3CredentialsSource::File(path)) {
            Err(error) => assert!(!error.message().contains("sensitive")),
            Ok(_) => panic!("malformed credentials accepted"),
        }
    }

    #[test]
    fn sequential_batch_accepts_a_64mib_file_without_node_acknowledgements() {
        let mut config = options();
        config.request_timeout = Duration::from_secs(30);
        let store =
            S3ChunkStore::with_backend(config, Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let staged = (0..16)
            .map(|index| {
                StagedChunk::new(
                    OperationId::new(format!("write-{index}")),
                    vec![index; MAX_STAGED_CHUNK_BYTES],
                )
            })
            .collect();
        let receipts = store.put_batch(staged).unwrap();
        assert_eq!(receipts.len(), 16);
        assert!(receipts.iter().all(|receipt| receipt.durable_acks.is_empty() && receipt.external_commit.is_some()));
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.chunk.length)
                .sum::<u64>(),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn timeout_cancels_the_owned_operation_before_worker_join() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let mut config = options();
        config.request_timeout = Duration::from_millis(20);
        let backend = ThrottledStore::new(
            object_store::memory::InMemory::new(),
            ThrottleConfig {
                wait_put_per_call: Duration::from_secs(5),
                ..Default::default()
            },
        );
        let store = S3ChunkStore::with_backend(config, Arc::new(backend)).unwrap();
        let start = std::time::Instant::now();
        let result = store.put(StagedChunk::new(
            OperationId::new("timeout"),
            b"bytes".to_vec(),
        ));
        assert_eq!(result.unwrap_err().code(), afs_error::IO_TIMEOUT);
        drop(store);
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}

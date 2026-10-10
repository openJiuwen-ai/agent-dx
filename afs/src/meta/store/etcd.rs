//! Etcd backend for the unified Meta Store.
//!
//! One CAS-protected key holds the complete control-plane state. Deployments
//! start with a fresh AFS namespace; there is no legacy data format to read.

use afs_error::{Error, Result};
use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};

use super::{BackendPersistence, MetaFuture, StoreBackend};

const SNAPSHOT_KEY: &[u8] = b"/afs/meta/snapshot";
// The delivery deployment currently accepts snapshots up to 64 MiB. Leave
// room for the protobuf envelope when decoding a backend response.
const SNAPSHOT_RPC_LIMIT: usize = 65 * 1024 * 1024;

/// Etcd storage for an opaque, full Store snapshot.
///
/// The returned version is the etcd `mod_revision` of `SNAPSHOT_KEY`. Pass it
/// back to [`commit`](Self::commit) as `expected_version` to perform a CAS
/// update. Use `expected_version == 0` only for the first create.
pub struct EtcdBackend {
    client: Client,
}

impl EtcdBackend {
    /// Connects to the configured etcd instance and probes its availability.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let mut client = Client::connect([endpoint.as_str()], None)
            .await
            .map_err(etcd_error)?;
        client.status().await.map_err(etcd_error)?;

        Ok(Self { client })
    }

    /// Loads the current full snapshot and its CAS version.
    pub async fn load(&self) -> Result<Option<(u64, Vec<u8>)>> {
        let mut client = self
            .client
            .kv_client()
            .max_decoding_message_size(SNAPSHOT_RPC_LIMIT)
            .max_encoding_message_size(SNAPSHOT_RPC_LIMIT);
        let response = client
            .get(SNAPSHOT_KEY.to_vec(), None)
            .await
            .map_err(etcd_error)?;
        response
            .kvs()
            .first()
            .map(|kv| {
                Ok::<_, Error>((
                    i64_to_u64(
                        kv.mod_revision(),
                        "etcd returned negative snapshot revision",
                    )?,
                    kv.value().to_vec(),
                ))
            })
            .transpose()
    }

    /// Atomically replaces the full snapshot when the observed version matches.
    ///
    /// `expected_version == 0` creates the snapshot only if it is absent.
    /// Non-zero versions compare against the snapshot key's `mod_revision`.
    /// A failed compare is reported as `IO_UNAVAILABLE` so callers fail closed
    /// and re-read rather than assuming their write was not committed.
    pub async fn commit(&self, expected_version: u64, bytes: &[u8]) -> Result<u64> {
        let compare = if expected_version == 0 {
            Compare::version(SNAPSHOT_KEY.to_vec(), CompareOp::Equal, 0)
        } else {
            Compare::mod_revision(
                SNAPSHOT_KEY.to_vec(),
                CompareOp::Equal,
                u64_to_i64(
                    expected_version,
                    "snapshot version exceeds etcd i64 revision",
                )?,
            )
        };
        let txn = Txn::new().when([compare]).and_then([TxnOp::put(
            SNAPSHOT_KEY.to_vec(),
            bytes.to_vec(),
            None,
        )]);
        let mut client = self
            .client
            .kv_client()
            .max_decoding_message_size(SNAPSHOT_RPC_LIMIT)
            .max_encoding_message_size(SNAPSHOT_RPC_LIMIT);
        let response = client.txn(txn).await.map_err(etcd_error)?;
        if !response.succeeded() {
            return Err(cas_mismatch());
        }
        response
            .header()
            .map(|header| {
                i64_to_u64(
                    header.revision(),
                    "etcd returned negative snapshot commit revision",
                )
            })
            .transpose()?
            .ok_or_else(|| invalid_contract("etcd snapshot commit response had no header"))
    }
}

fn etcd_error(error: etcd_client::Error) -> Error {
    Error::coded(
        afs_error::IO_UNAVAILABLE,
        format!("etcd snapshot store unavailable: {error}"),
    )
}

fn cas_mismatch() -> Error {
    Error::coded(
        afs_error::IO_UNAVAILABLE,
        "etcd snapshot CAS mismatch; reload the latest snapshot before retrying",
    )
}

fn invalid_contract(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}

fn i64_to_u64(value: i64, message: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| invalid_contract(message))
}

fn u64_to_i64(value: u64, message: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid_contract(message))
}

impl StoreBackend for EtcdBackend {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        Box::pin(async move { EtcdBackend::load(self).await })
    }

    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
        Box::pin(async move { EtcdBackend::commit(self, expected_version, &bytes).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set this only for a fresh, dedicated etcd instance. The test writes a
    /// fixed production-format key and deliberately leaves it for replay.
    #[tokio::test]
    async fn etcd_ack_reconnect_and_stale_cas_on_dedicated_instance() {
        let Ok(endpoint) = std::env::var("AFS_TEST_ETCD_STORE_ENDPOINT") else {
            return;
        };
        let backend = EtcdBackend::connect(endpoint.clone()).await.unwrap();
        assert!(backend.load().await.unwrap().is_none());
        let version = backend.commit(0, b"first").await.unwrap();
        assert!(backend.commit(0, b"stale").await.is_err());
        let reopened = EtcdBackend::connect(endpoint).await.unwrap();
        assert_eq!(
            reopened.load().await.unwrap(),
            Some((version, b"first".to_vec()))
        );
        let next = reopened.commit(version, b"second").await.unwrap();
        assert!(next > version);
        assert_eq!(
            backend.load().await.unwrap(),
            Some((next, b"second".to_vec()))
        );
    }
}

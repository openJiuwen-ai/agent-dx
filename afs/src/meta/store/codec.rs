//! Canonical binary encoding helpers for operation records and checkpoints.
//! This is not an RPC wire schema; records and checkpoints carry their own
//! version fields (see `log.rs` and `journal.rs`).
use afs_error::Result;
use serde::Serialize;

use super::invalid_contract;

pub(super) fn encoded_size<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    postcard::serialize_with_flavor::<T, _, usize>(value, postcard::ser_flavors::Size::default())
        .map_err(|e| invalid_contract(format!("measure meta encoding: {e}")))
}

/// Hashes the canonical encoding without allocating another encoded payload.
/// Equality is still checked after a digest hit; this is an index, not trust.
pub(super) fn encoded_fingerprint<T: Serialize + ?Sized>(value: &T) -> Result<[u8; 32]> {
    struct Digest(blake3::Hasher);
    impl postcard::ser_flavors::Flavor for Digest {
        type Output = [u8; 32];
        fn try_push(&mut self, value: u8) -> postcard::Result<()> {
            self.0.update(&[value]);
            Ok(())
        }
        fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
            self.0.update(bytes);
            Ok(())
        }
        fn finalize(self) -> postcard::Result<Self::Output> {
            Ok(*self.0.finalize().as_bytes())
        }
    }
    postcard::serialize_with_flavor::<T, _, [u8; 32]>(value, Digest(blake3::Hasher::new()))
        .map_err(|error| invalid_contract(format!("hash meta encoding: {error}")))
}

/// Measures before allocation; the returned Vec never grows beyond the supplied
/// logical encoding bound. Used by canonical records before any WAL append.
pub(super) fn encode_bounded<T: Serialize + ?Sized>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let length = encoded_size(value)?;
    if length > limit {
        return Err(invalid_contract("meta encoding exceeds byte limit"));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| {
        afs_error::Error::coded(afs_error::IO_OUT_OF_MEMORY, "allocate meta encoding")
    })?;
    bytes.resize(length, 0);
    postcard::to_slice(value, &mut bytes)
        .map_err(|e| invalid_contract(format!("encode bounded meta value: {e}")))?;
    Ok(bytes)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::meta::store::MetaEntity;
    pub(in crate::meta::store) fn entity_fixtures() -> Vec<MetaEntity> {
        use serde_json::json;
        let digest = json!({"algorithm":"Blake3", "bytes":vec![0;32]});
        let fixtures = vec![
            json!({"NodeSession":{"node_id":"a","session_id":"s","grpc_addr":"g","data_addr":"d","rest_addr":"r","ownerfs_enabled":true,"storage_devices":[],"lease_epoch":1,"expires_at_unix_ms":42}}),
            json!({"RootReservation":{"root_id":"r","root_epoch":1,"home_node_id":"a","home_session_id":"s","create_intent_id":"c","prepare_token":"p","requester_node_id":"a","requester_session_id":"s","created_at_revision":2}}),
            json!({"Root":{"root_id":"r","root_epoch":1,"home_node_id":"a","home_session_id":"s","local_prepare_id":"p","created_at_revision":2,"updated_at_revision":3}}),
            json!({"RootGrant":{"root_id":"r","root_epoch":1,"home_node_id":"a","home_session_id":"s","holder_node_id":"b","holder_session_id":"t","access_generation":2,"rights":["Read","Write"],"fencing_token":"f","issued_at_revision":3}}),
            json!({"RootCommand":{"command_id":"c","home_node_id":"a","home_session_id":"s","root_id":"r","root_epoch":1,"old_access_generation":2,"command_type":"InvalidateCache"}}),
            json!({"RootCommandAck":{"command_id":"c","node_id":"a","session_id":"s","root_id":"r","root_epoch":1,"access_generation":2,"success":false,"message":"rejected"}}),
            json!({"DfsDentry":{"key":{"namespace_id":"n","parent_inode_id":"p","name":[0,255]},"inode_id":"i"}}),
            json!({"DfsInode":{"namespace_id":"n","inode_id":"i","kind":"Regular","attributes":{"mode":420,"uid":1,"gid":2,"nlink":1,"atime_unix_ms":3,"mtime_unix_ms":4,"ctime_unix_ms":5},"head_version":"v","symlink_target":null,"xattrs":{"00ff":[0,255]},"revision":6}}),
            json!({"DfsFileVersion":{"id":"v","inode_id":"i","parent_version":null,"length":1,"layout_root":"l","created_at_unix_ms":42}}),
            json!({"DfsLayoutRoot":{"id":"l","file_length":1,"inline_extents":[{"file_offset":0,"length":1,"chunk_id":"c","chunk_offset":0}]}}),
            json!({"DfsChunk":{"id":"c","length":1,"content_digest":digest,"encoding":"Raw"}}),
            json!({"DfsReplicationConfig":{"desired_copies":1,"sync_required_copies":1,"min_distinct_nodes":1,"min_distinct_failure_domains":1,"local_copy":"Required"}}),
            json!({"DfsDurabilityPolicy":{"ExternalBackend":{"store_id":"store","generation":"g"}}}),
            json!({"DfsPlacement":{"chunk_id":"c","replica_group_id":"g","placement_epoch":1,"desired_copies":1,"copies":["copy"],"health":"Satisfied"}}),
            json!({"DfsCopy":{"id":"copy","chunk_id":"c","role":"DurableReplica","location":{"External":{"store_id":"store","object_key":"object","object_revision":"g"}},"state":"Ready","persisted_bytes":1,"verified_digest":digest}}),
            json!({"DfsReplicationTask":{"id":"task","chunk_id":"c","placement_epoch":1,"desired_copies":1,"existing_copies":[],"state":"RetryWaiting","attempt":1,"next_retry_unix_ms":42,"last_error":"retry","claim":null}}),
            json!({"DfsWriteLease":{"inode_id":"i","owner_node_id":"a","owner_session_id":"s","lease_epoch":1,"expires_at_unix_ms":42}}),
        ];
        assert_eq!(fixtures.len(), 17);
        fixtures
            .into_iter()
            .map(|fixture| {
                serde_json::from_value(fixture.clone())
                    .unwrap_or_else(|error| panic!("{fixture}: {error}"))
            })
            .collect()
    }
    #[test]
    fn canonical_fingerprint_matches_encoded_bytes_without_payload_buffer() {
        for entity in entity_fixtures() {
            let encoded = encode_bounded(&entity, 8 * 1024 * 1024).unwrap();
            assert_eq!(
                encoded_fingerprint(&entity).unwrap(),
                *blake3::hash(&encoded).as_bytes()
            );
        }
    }
}

//! Versioned binary codec for operation records/checkpoints, not an RPC wire
//! schema or a requirement to persist a backend KV for every entity.
//!
//! Canonical local-file/memory logs use this codec. The legacy snapshot
//! adapters remain only until the P3 backend-removal work item is complete.
use afs_error::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    MetaEntity, MetaKey, OperationResult, RequestKey, RequestOutcome, StoreOperation,
    StoreRevision, invalid_contract,
};
use crate::dfs::{
    ChunkId, CopyId, DentryKey, FileVersionId, InodeId, LayoutRootId, NamespaceId,
    ReplicationTaskId,
};

/// Stable tagged key. Components are length-delimited; path separators and
/// embedded NUL bytes have no special meaning. Dentry names are arbitrary bytes.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct EncodedKey(Vec<u8>);
impl EncodedKey {
    pub fn encode(key: &MetaKey) -> Result<Self> {
        let (tag, fields): (u8, Vec<&[u8]>) = match key {
            MetaKey::NodeSession {
                node_id,
                session_id,
            } => (0x01, vec![node_id.as_bytes(), session_id.as_bytes()]),
            MetaKey::CurrentNodeSession { node_id } => (0x02, vec![node_id.as_bytes()]),
            MetaKey::CurrentNodeSessions => (0x03, vec![]),
            MetaKey::RootEpochWatermark => (0x15, vec![]),
            MetaKey::Root { root_id } => (0x10, vec![root_id.as_bytes()]),
            MetaKey::RootReservation { root_id } => (0x11, vec![root_id.as_bytes()]),
            MetaKey::RootGrant {
                root_id,
                holder_node_id,
                holder_session_id,
            } => (
                0x12,
                vec![
                    root_id.as_bytes(),
                    holder_node_id.as_bytes(),
                    holder_session_id.as_bytes(),
                ],
            ),
            MetaKey::RootCommand { command_id } => (0x13, vec![command_id.as_bytes()]),
            MetaKey::RootCommandAck {
                command_id,
                home_node_id,
                home_session_id,
            } => (
                0x14,
                vec![
                    command_id.as_bytes(),
                    home_node_id.as_bytes(),
                    home_session_id.as_bytes(),
                ],
            ),
            MetaKey::DfsDentry(key) => (
                0x20,
                vec![
                    key.namespace_id.0.as_bytes(),
                    key.parent_inode_id.0.as_bytes(),
                    &key.name,
                ],
            ),
            MetaKey::DfsInode(id) => (0x21, vec![id.0.as_bytes()]),
            MetaKey::DfsFileVersion(id) => (0x22, vec![id.0.as_bytes()]),
            MetaKey::DfsLayoutRoot(id) => (0x23, vec![id.0.as_bytes()]),
            MetaKey::DfsChunk(id) => (0x24, vec![id.0.as_bytes()]),
            MetaKey::DfsPlacement(id) => (0x27, vec![id.0.as_bytes()]),
            MetaKey::DfsCopy(id) => (0x28, vec![id.0.as_bytes()]),
            MetaKey::DfsReplicationTask(id) => (0x29, vec![id.0.as_bytes()]),
            MetaKey::DfsWriteLease(id) => (0x2a, vec![id.0.as_bytes()]),
            MetaKey::DfsReference {
                node_id,
                session_id,
                reference_id,
            } => (
                0x2b,
                vec![
                    node_id.as_bytes(),
                    session_id.as_bytes(),
                    reference_id.as_bytes(),
                ],
            ),
            MetaKey::DfsGc => (0x2c, vec![]),
            MetaKey::DfsReplicationConfig => (0x25, vec![]),
            MetaKey::DfsDurabilityPolicy => (0x26, vec![]),
            MetaKey::RequestOutcome(key) => (
                0x30,
                vec![key.caller_id.as_bytes(), key.request_id.as_bytes()],
            ),
        };
        let mut out = vec![tag];
        for field in fields {
            append_field(&mut out, field)?;
        }
        Ok(Self(out))
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn decode(bytes: &[u8]) -> Result<MetaKey> {
        let (&tag, mut tail) = bytes
            .split_first()
            .ok_or_else(|| invalid_contract("empty meta key"))?;
        let key = match tag {
            0x01 => MetaKey::NodeSession {
                node_id: text(&mut tail)?,
                session_id: text(&mut tail)?,
            },
            0x02 => MetaKey::CurrentNodeSession {
                node_id: text(&mut tail)?,
            },
            0x03 => MetaKey::CurrentNodeSessions,
            0x15 => MetaKey::RootEpochWatermark,
            0x10 => MetaKey::Root {
                root_id: text(&mut tail)?,
            },
            0x11 => MetaKey::RootReservation {
                root_id: text(&mut tail)?,
            },
            0x12 => MetaKey::RootGrant {
                root_id: text(&mut tail)?,
                holder_node_id: text(&mut tail)?,
                holder_session_id: text(&mut tail)?,
            },
            0x13 => MetaKey::RootCommand {
                command_id: text(&mut tail)?,
            },
            0x14 => MetaKey::RootCommandAck {
                command_id: text(&mut tail)?,
                home_node_id: text(&mut tail)?,
                home_session_id: text(&mut tail)?,
            },
            0x20 => MetaKey::DfsDentry(DentryKey {
                namespace_id: NamespaceId(text(&mut tail)?),
                parent_inode_id: InodeId(text(&mut tail)?),
                name: field(&mut tail)?.to_vec(),
            }),
            0x21 => MetaKey::DfsInode(InodeId(text(&mut tail)?)),
            0x22 => MetaKey::DfsFileVersion(FileVersionId(text(&mut tail)?)),
            0x23 => MetaKey::DfsLayoutRoot(LayoutRootId(text(&mut tail)?)),
            0x24 => MetaKey::DfsChunk(ChunkId(text(&mut tail)?)),
            0x27 => MetaKey::DfsPlacement(ChunkId(text(&mut tail)?)),
            0x28 => MetaKey::DfsCopy(CopyId(text(&mut tail)?)),
            0x29 => MetaKey::DfsReplicationTask(ReplicationTaskId(text(&mut tail)?)),
            0x2a => MetaKey::DfsWriteLease(InodeId(text(&mut tail)?)),
            0x2b => MetaKey::DfsReference {
                node_id: text(&mut tail)?,
                session_id: text(&mut tail)?,
                reference_id: text(&mut tail)?,
            },
            0x2c => MetaKey::DfsGc,
            0x25 => MetaKey::DfsReplicationConfig,
            0x26 => MetaKey::DfsDurabilityPolicy,
            0x30 => MetaKey::RequestOutcome(RequestKey {
                caller_id: text(&mut tail)?,
                request_id: text(&mut tail)?,
            }),
            _ => return Err(invalid_contract("unknown meta key tag")),
        };
        if !tail.is_empty() {
            return Err(invalid_contract("trailing meta key bytes"));
        }
        Ok(key)
    }
    pub fn directory_prefix(namespace: &NamespaceId, parent: &InodeId) -> Result<Vec<u8>> {
        let mut out = vec![0x20];
        append_field(&mut out, namespace.0.as_bytes())?;
        append_field(&mut out, parent.0.as_bytes())?;
        Ok(out)
    }
    pub fn root_grants_prefix(root_id: &str) -> Result<Vec<u8>> {
        let mut out = vec![0x12];
        append_field(&mut out, root_id.as_bytes())?;
        Ok(out)
    }
    // CurrentNodeSessions is a selector; stored current sessions use tag 0x02.
    pub const fn sessions_prefix() -> &'static [u8] {
        &[0x02]
    }
    pub const fn placements_prefix() -> &'static [u8] {
        &[0x27]
    }
    pub const fn replication_tasks_prefix() -> &'static [u8] {
        &[0x29]
    }
}
fn append_field(out: &mut Vec<u8>, field: &[u8]) -> Result<()> {
    let len = u32::try_from(field.len())
        .map_err(|_| invalid_contract("meta key component exceeds u32"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(field);
    Ok(())
}
fn field<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let header = input
        .get(..4)
        .ok_or_else(|| invalid_contract("truncated meta key length"))?;
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let rest = &input[4..];
    let value = rest
        .get(..length)
        .ok_or_else(|| invalid_contract("truncated meta key field"))?;
    *input = &rest[length..];
    Ok(value)
}
fn text(input: &mut &[u8]) -> Result<String> {
    std::str::from_utf8(field(input)?)
        .map(str::to_owned)
        .map_err(|_| invalid_contract("non UTF-8 meta identifier"))
}

const SCHEMA: u16 = 1;
const MAX_RESULT_NESTING: usize = 128;
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
enum EntryKind {
    Entity,
    Request,
}

/// Typed schema envelope. A record/frame layer must enforce its own total byte
/// budget before passing a slice here. The per-call limit is checked before
/// decoding. Unknown schema, truncation and trailing bytes are errors.
#[derive(Debug, Deserialize, Serialize)]
pub struct StoredEntry {
    schema_version: u16,
    revision: StoreRevision,
    kind: EntryKind,
    payload: Vec<u8>,
}
impl StoredEntry {
    pub fn entity(revision: StoreRevision, entity: &MetaEntity) -> Result<Self> {
        Ok(Self {
            schema_version: SCHEMA,
            revision,
            kind: EntryKind::Entity,
            payload: encode_payload(entity)?,
        })
    }
    pub fn request(revision: StoreRevision, request: &RequestOutcome) -> Result<Self> {
        let mut result = &request.result;
        let mut depth = 0;
        while let OperationResult::DfsNamespace { result: inner, .. } = result {
            depth += 1;
            if depth > MAX_RESULT_NESTING {
                return Err(invalid_contract("meta result nesting exceeds limit"));
            }
            result = inner;
        }
        Ok(Self {
            schema_version: SCHEMA,
            revision,
            kind: EntryKind::Request,
            payload: encode_payload(request)?,
        })
    }
    pub fn decode_request(&self) -> Result<RequestOutcome> {
        self.check_schema()?;
        if self.kind != EntryKind::Request {
            return Err(invalid_contract("meta entry is not a request"));
        }
        // Schema v1 encodes RequestOutcome as request, operation, result. Guard
        // its only recursive variant before Serde can recurse on corrupt data.
        let (_, tail) = postcard::take_from_bytes::<RequestKey>(&self.payload)
            .map_err(|e| invalid_contract(format!("decode request key: {e}")))?;
        let (_, mut tail) = postcard::take_from_bytes::<StoreOperation>(tail)
            .map_err(|e| invalid_contract(format!("decode operation: {e}")))?;
        for depth in 0..=MAX_RESULT_NESTING {
            let (tag, rest) = postcard::take_from_bytes::<u32>(tail)
                .map_err(|e| invalid_contract(format!("decode result tag: {e}")))?;
            if tag != 0 {
                return decode_payload(&self.payload);
            }
            if depth == MAX_RESULT_NESTING {
                return Err(invalid_contract("meta result nesting exceeds limit"));
            }
            let (_, rest) = postcard::take_from_bytes::<[u8; 32]>(rest)
                .map_err(|e| invalid_contract(format!("decode result digest: {e}")))?;
            tail = rest;
        }
        Err(invalid_contract("meta result nesting exceeds limit"))
    }
    pub fn revision(&self) -> StoreRevision {
        self.revision
    }
    pub fn decode_entity(&self) -> Result<MetaEntity> {
        self.check_schema()?;
        if self.kind != EntryKind::Entity {
            return Err(invalid_contract("meta entry is not an entity"));
        }
        decode_payload(&self.payload)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check_schema()?;
        encode_payload(self)
    }
    pub fn decode(bytes: &[u8], byte_limit: usize) -> Result<Self> {
        if bytes.len() > byte_limit {
            return Err(invalid_contract("meta entry exceeds byte limit"));
        }
        let value: Self = decode_payload(bytes)?;
        value.check_schema()?;
        Ok(value)
    }
    fn check_schema(&self) -> Result<()> {
        if self.schema_version != SCHEMA {
            return Err(invalid_contract("unsupported meta entry schema"));
        }
        Ok(())
    }
}
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

fn encode_payload<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(|e| invalid_contract(format!("encode meta entry: {e}")))
}
fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, tail) = postcard::take_from_bytes(bytes)
        .map_err(|e| invalid_contract(format!("decode meta entry: {e}")))?;
    if !tail.is_empty() {
        return Err(invalid_contract("trailing meta entry bytes"));
    }
    Ok(value)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    #[test]
    fn all_key_shapes_round_trip_and_reject_trailing_bytes() {
        let keys = vec![
            MetaKey::NodeSession {
                node_id: "node_id/中\0".into(),
                session_id: "session_id/中\0".into(),
            },
            MetaKey::CurrentNodeSession {
                node_id: "node_id/中\0".into(),
            },
            MetaKey::CurrentNodeSessions,
            MetaKey::RootEpochWatermark,
            MetaKey::Root {
                root_id: "root_id/中\0".into(),
            },
            MetaKey::RootReservation {
                root_id: "root_id/中\0".into(),
            },
            MetaKey::RootGrant {
                root_id: "root_id/中\0".into(),
                holder_node_id: "holder_node_id/中\0".into(),
                holder_session_id: "holder_session_id/中\0".into(),
            },
            MetaKey::RootCommand {
                command_id: "command_id/中\0".into(),
            },
            MetaKey::RootCommandAck {
                command_id: "command_id/中\0".into(),
                home_node_id: "home_node_id/中\0".into(),
                home_session_id: "home_session_id/中\0".into(),
            },
            MetaKey::DfsDentry(DentryKey {
                namespace_id: NamespaceId("ns".into()),
                parent_inode_id: InodeId("parent".into()),
                name: vec![0, 255, b'/'],
            }),
            MetaKey::DfsInode(InodeId("DfsInode/中".into())),
            MetaKey::DfsFileVersion(FileVersionId("DfsFileVersion/中".into())),
            MetaKey::DfsLayoutRoot(LayoutRootId("DfsLayoutRoot/中".into())),
            MetaKey::DfsChunk(ChunkId("DfsChunk/中".into())),
            MetaKey::DfsPlacement(ChunkId("DfsPlacement/中".into())),
            MetaKey::DfsCopy(CopyId("DfsCopy/中".into())),
            MetaKey::DfsReplicationTask(ReplicationTaskId("DfsReplicationTask/中".into())),
            MetaKey::DfsWriteLease(InodeId("DfsWriteLease/中".into())),
            MetaKey::DfsReference {
                node_id: "node/中".into(),
                session_id: "session\0".into(),
                reference_id: "reference/中".into(),
            },
            MetaKey::DfsGc,
            MetaKey::DfsReplicationConfig,
            MetaKey::DfsDurabilityPolicy,
            MetaKey::RequestOutcome(RequestKey::new("caller", "req")),
        ];
        for key in keys {
            let encoded = EncodedKey::encode(&key).unwrap();
            assert_eq!(EncodedKey::decode(encoded.as_bytes()).unwrap(), key);
            let mut trailing = encoded.as_bytes().to_vec();
            trailing.push(0);
            assert!(EncodedKey::decode(&trailing).is_err());
            for end in 0..encoded.as_bytes().len() {
                assert!(EncodedKey::decode(&encoded.as_bytes()[..end]).is_err());
            }
        }
    }
    #[test]
    fn empty_and_large_components_have_exact_big_endian_lengths() {
        for value in [String::new(), "x".repeat(65_536)] {
            let key = MetaKey::DfsInode(InodeId(value.clone()));
            let encoded = EncodedKey::encode(&key).unwrap();
            assert_eq!(
                &encoded.as_bytes()[1..5],
                &(value.len() as u32).to_be_bytes()
            );
            assert_eq!(EncodedKey::decode(encoded.as_bytes()).unwrap(), key);
        }
        let mut bytes = vec![0x20];
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(EncodedKey::decode(&bytes).is_err());
        // Encoding is length-first, not name-lexicographic. Directory replies
        // keep their explicit name sort independently from storage byte order.
        let one = EncodedKey::encode(&MetaKey::DfsInode(InodeId("z".into()))).unwrap();
        let two = EncodedKey::encode(&MetaKey::DfsInode(InodeId("aa".into()))).unwrap();
        assert!(one < two);
    }
    #[test]
    fn prefixes_are_delimited_and_current_sessions_use_actual_storage_key() {
        let namespace = NamespaceId("n".into());
        let parent = InodeId("a".into());
        let prefix = EncodedKey::directory_prefix(&namespace, &parent).unwrap();
        let key = |id: &str| {
            MetaKey::DfsDentry(DentryKey {
                namespace_id: namespace.clone(),
                parent_inode_id: InodeId(id.into()),
                name: vec![0, 255],
            })
        };
        assert!(
            EncodedKey::encode(&key("a"))
                .unwrap()
                .as_bytes()
                .starts_with(&prefix)
        );
        assert!(
            !EncodedKey::encode(&key("ab"))
                .unwrap()
                .as_bytes()
                .starts_with(&prefix)
        );
        assert!(
            EncodedKey::encode(&MetaKey::CurrentNodeSession {
                node_id: "x".into()
            })
            .unwrap()
            .as_bytes()
            .starts_with(EncodedKey::sessions_prefix())
        );
        assert!(EncodedKey::decode(&[0xff]).is_err());
        assert!(EncodedKey::decode(&[0x21, 0, 0, 0, 1, 255]).is_err());
        assert!(EncodedKey::decode(&[0x21, 255, 255, 255, 255]).is_err());
    }
    #[test]
    fn remaining_result_variants_and_prefix_families_round_trip() {
        let inode: crate::dfs::InodeRecord = serde_json::from_value(serde_json::json!({
            "inode_id":"i", "namespace_id":"n", "kind":"Regular", "attributes":{"mode":420,"uid":1,"gid":2,"nlink":1,"atime_unix_ms":0,"mtime_unix_ms":0,"ctime_unix_ms":0}, "head_version":null,"revision":1
        })).unwrap();
        for result in [
            OperationResult::DfsInodeWithLease {
                inode: inode.clone(),
                lease: crate::dfs::WriteLease {
                    inode_id: InodeId("i".into()),
                    owner_node_id: "a".into(),
                    owner_session_id: "s".into(),
                    lease_epoch: 1,
                    expires_at_unix_ms: 42,
                },
            },
            OperationResult::DfsRename(crate::dfs::RenameOutcome {
                inode: inode.clone(),
                replaced_inode: Some(inode),
            }),
            OperationResult::DfsReplicationClaim {
                request_digest: [7; 32],
                claim: None,
            },
        ] {
            let request = RequestOutcome {
                request: RequestKey::new("c", "r"),
                operation: StoreOperation::DfsCreate,
                result,
            };
            assert_eq!(
                StoredEntry::request(StoreRevision(1), &request)
                    .unwrap()
                    .decode_request()
                    .unwrap(),
                request
            );
        }
        for root in ["r", "rr", "other"] {
            let key = MetaKey::RootGrant {
                root_id: root.into(),
                holder_node_id: "b".into(),
                holder_session_id: "s".into(),
            };
            assert_eq!(
                EncodedKey::encode(&key)
                    .unwrap()
                    .as_bytes()
                    .starts_with(&EncodedKey::root_grants_prefix("r").unwrap()),
                root == "r"
            );
        }
        for (key, prefix) in [
            (
                MetaKey::DfsPlacement(ChunkId("c".into())),
                EncodedKey::placements_prefix(),
            ),
            (
                MetaKey::DfsReplicationTask(ReplicationTaskId("t".into())),
                EncodedKey::replication_tasks_prefix(),
            ),
        ] {
            assert!(
                EncodedKey::encode(&key)
                    .unwrap()
                    .as_bytes()
                    .starts_with(prefix)
            );
            assert!(
                !EncodedKey::encode(&MetaKey::DfsChunk(ChunkId("c".into())))
                    .unwrap()
                    .as_bytes()
                    .starts_with(prefix)
            );
        }
        let malformed = StoredEntry {
            schema_version: SCHEMA,
            revision: StoreRevision(1),
            kind: EntryKind::Entity,
            payload: vec![255, 255],
        };
        assert!(malformed.decode_entity().is_err());
    }
    pub(in crate::meta::store) fn entity_fixtures() -> Vec<MetaEntity> {
        use serde_json::json;
        let digest = json!({"algorithm":"Blake3", "bytes":vec![0;32]});
        let fixtures = vec![
            json!({"NodeSession":{"node_id":"a","session_id":"s","grpc_addr":"g","data_addr":"d","rest_addr":"r","storage_devices":[],"lease_epoch":1,"expires_at_unix_ms":42}}),
            json!({"RootReservation":{"root_id":"r","root_epoch":1,"home_node_id":"a","home_session_id":"s","create_intent_id":"c","prepare_token":"p","created_at_revision":2}}),
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
    fn all_entity_variants_round_trip_without_changing_domain_values() {
        for entity in entity_fixtures() {
            let entry = StoredEntry::entity(StoreRevision(7), &entity).unwrap();
            let bytes = entry.encode().unwrap();
            let restored = StoredEntry::decode(&bytes, bytes.len()).unwrap();
            assert_eq!(restored.decode_entity().unwrap(), entity);
            assert!(restored.decode_request().is_err());
            let result = match entity {
                MetaEntity::NodeSession(value) => OperationResult::NodeSession(value),
                MetaEntity::RootReservation(value) => OperationResult::RootReservation(value),
                MetaEntity::Root(value) => OperationResult::RootRecord(value),
                MetaEntity::RootGrant(value) => OperationResult::RootGrant(value),
                MetaEntity::RootCommand(value) => OperationResult::RootCommand(value),
                MetaEntity::RootCommandAck(value) => OperationResult::RootCommandAck(value),
                MetaEntity::DfsInode(value) => OperationResult::DfsInode(value),
                MetaEntity::DfsWriteLease(value) => OperationResult::DfsWriteLease(value),
                MetaEntity::DfsReplicationTask(value) => OperationResult::DfsReplicationTask(value),
                _ => OperationResult::Empty,
            };
            let request = RequestOutcome {
                request: RequestKey::new("caller", "request"),
                operation: StoreOperation::DfsCreate,
                result,
            };
            let bytes = StoredEntry::request(StoreRevision(8), &request)
                .unwrap()
                .encode()
                .unwrap();
            assert_eq!(
                StoredEntry::decode(&bytes, bytes.len())
                    .unwrap()
                    .decode_request()
                    .unwrap(),
                request
            );
        }
    }
    #[test]
    fn requests_reject_wrong_kind_recursion_and_trailing_payload() {
        let request = RequestOutcome {
            request: RequestKey::new("c", "r"),
            operation: StoreOperation::DfsCreate,
            result: OperationResult::DfsNamespace {
                request_digest: [3; 32],
                result: Box::new(OperationResult::Empty),
            },
        };
        let entry = StoredEntry::request(StoreRevision(9), &request).unwrap();
        assert_eq!(entry.decode_request().unwrap(), request);
        assert!(entry.decode_entity().is_err());
        let mut deep = OperationResult::Empty;
        for _ in 0..=MAX_RESULT_NESTING {
            deep = OperationResult::DfsNamespace {
                request_digest: [0; 32],
                result: Box::new(deep),
            };
        }
        let excessive = RequestOutcome {
            result: deep,
            ..request.clone()
        };
        assert!(StoredEntry::request(StoreRevision(1), &excessive).is_err());
        // Lock the recursive variant discriminant to schema v1's guard.
        assert_eq!(postcard::to_allocvec(&request.result).unwrap()[0], 0);
        let mut malicious = postcard::to_allocvec(&request.request).unwrap();
        malicious.extend(postcard::to_allocvec(&request.operation).unwrap());
        for _ in 0..MAX_RESULT_NESTING + 1 {
            malicious.push(0);
            malicious.extend_from_slice(&[0; 32]);
        }
        let bad = StoredEntry {
            schema_version: SCHEMA,
            revision: StoreRevision(1),
            kind: EntryKind::Request,
            payload: malicious,
        };
        assert!(bad.decode_request().is_err());
        let mut entry = entry;
        entry.payload.push(0);
        assert!(entry.decode_request().is_err());
    }
    #[test]
    fn bounded_malformed_inputs_never_panic() {
        let mut random = 0x1234_5678u64;
        for length in 0..96 {
            for _ in 0..32 {
                let input: Vec<u8> = (0..length)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        random as u8
                    })
                    .collect();
                let _ = EncodedKey::decode(&input);
                if let Ok(entry) = StoredEntry::decode(&input, 96) {
                    let _ = entry.decode_entity();
                    let _ = entry.decode_request();
                }
            }
        }
    }
    #[test]
    fn entry_boundaries_and_binary_xattrs_round_trip() {
        let mut inode: crate::dfs::InodeRecord = serde_json::from_value(serde_json::json!({
            "inode_id":"i", "namespace_id":"n", "kind":"Regular", "attributes":{"mode":420,"uid":1,"gid":2,"nlink":1,"atime_unix_ms":0,"mtime_unix_ms":0,"ctime_unix_ms":0},
            "link_count":1,"head_version":null,"revision":1
        })).unwrap();
        inode.xattrs.insert(vec![0, 255], vec![255, 0, 1]);
        let entity = MetaEntity::DfsInode(inode);
        let bytes = StoredEntry::entity(StoreRevision(7), &entity)
            .unwrap()
            .encode()
            .unwrap();
        let decoded = StoredEntry::decode(&bytes, bytes.len()).unwrap();
        assert_eq!(decoded.revision(), StoreRevision(7));
        assert_eq!(decoded.decode_entity().unwrap(), entity);
        assert!(StoredEntry::decode(&bytes, bytes.len() - 1).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(StoredEntry::decode(&trailing, trailing.len()).is_err());
        for n in 0..bytes.len() {
            assert!(StoredEntry::decode(&bytes[..n], bytes.len()).is_err());
        }
        let future = StoredEntry {
            schema_version: SCHEMA + 1,
            revision: StoreRevision(0),
            kind: EntryKind::Entity,
            payload: vec![],
        };
        assert!(StoredEntry::decode(&postcard::to_allocvec(&future).unwrap(), 100).is_err());
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

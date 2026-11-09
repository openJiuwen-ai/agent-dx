//! Registry of AFS persistent formats and versioned protocols (R17).
//!
//! Rules (also in `docs/architecture/afs.md`, "持久格式版本规则"):
//!
//! 1. Every persistent format or versioned protocol has exactly one
//!    [`FormatKind`] and one current version, defined here. Readers and
//!    writers use these constants instead of local magic/version literals.
//! 2. A reader identifies the version **before** integrity checks (CRC, hash,
//!    identity) and before a full decode: a fixed header is read by its first
//!    six bytes, a postcard payload by its leading varint, JSON by a probe of
//!    its version field. A version this binary does not support is reported
//!    with [`check`] / [`incompatible`] (`STORAGE_FORMAT_INCOMPATIBLE` for
//!    stored data), never as corruption. When the version matches, the
//!    remaining checks and their messages are unchanged.
//! 3. Stored authoritative data ([`Disposition::Refuse`]) is neither migrated
//!    nor deleted: the operator rebuilds the data directory or runs a binary
//!    that matches it. Derived, disposable data ([`Disposition::Discard`]:
//!    Bao sidecars, Peer cache entries) is recognised and then discarded and
//!    rebuilt by its owner, as before.
//! 4. Any change to an encoding (fields or variants added, removed or
//!    reordered, serde attributes, postcard upgrade, hash domain strings)
//!    bumps the owning version here and regenerates the goldens. The variant
//!    index tables (G1) and golden bytes (G2) fail on such a change; formats
//!    without a stored version field (`catalog.wal`, request digests) are
//!    pinned by goldens only.
//! 5. An outer container and its inner payload are versioned independently:
//!    the checkpoint file header versions the file, the `StateImage` schema
//!    versions its payload; the WAL envelope versions a frame, the
//!    `RecordDecision` schema versions a decision. The outer version is
//!    checked before outer integrity; the inner version after outer integrity
//!    and before the inner decode.
//! 6. The system is not yet released: an old format is refused, not migrated.
use afs_error::{Error, ErrorCode, Result};
use std::fmt::Display;

/// One persistent format or versioned protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FormatKind {
    /// M1: postcard `Envelope` of one Meta WAL frame (first field `version`).
    /// The record hash domain `afs.meta.record.v2` belongs to this version.
    MetaWalEnvelope,
    /// M4: `AFSW` Meta WAL segment header (static area, tail slots, frames).
    MetaWalSegment,
    /// M6: `AFSI` Meta store IDENTITY file.
    MetaIdentity,
    /// M7: `AFSM` Meta CURRENT manifest (v3 carries the writer term).
    MetaCurrent,
    /// M8: `AFSC` Meta checkpoint file header (outer container).
    MetaCheckpointFile,
    /// M9: `StateImage` checkpoint payload (`AFSC` magic + schema varint).
    MetaStateImage,
    /// M10: `RecordDecision` schema inside a WAL record (inner payload).
    MetaRecordDecision,
    /// M16: namespace request digest `blake3(serde_json(request))`, stored in
    /// WAL records and checkpoints. No stored version field; pinned by the
    /// request digest goldens.
    MetaRequestDigest,
    /// N1: Node `catalog.wal` JSON lines. No stored version field; pinned by
    /// the chunk goldens.
    NodeChunkCatalogWal,
    /// N2: Node `catalog.snapshot` JSON (`schema`).
    NodeChunkCatalogSnapshot,
    /// N4: CRC32C segment descriptor (`format`), persisted in chunk records.
    NodeCrcSegmentTable,
    /// N5: Bao sidecar `AFSBAO1\0` header (little-endian u16 version).
    NodeChunkSidecar,
    /// N7: Peer cache entry header `AFSPC00<n>`.
    NodePeerCacheEntry,
    /// N8: Peer cache directory marker `.afs-peer-cache-v1`.
    NodePeerCacheMarker,
    /// N9: external object key prefix `afs/v2/` (v2: keys name generational
    /// `blake3-v2-…` chunk ids, R05). Generated; GC parses listed keys back.
    NodeExternalObjectKey,
    /// N11: OwnerFs local root record `.ownerfs-roots/` (`version`).
    OwnerFsRootRecord,
    /// P2: cluster wire protocol generation (Node↔Meta registration, Node↔Node
    /// service layout) carried by `RegisterNodeRequest`. The value is defined by
    /// `afs_protocol::CLUSTER_PROTOCOL_VERSION`.
    NodeMetaProtocol,
    /// P3: Node↔Node RDMA probe handshake.
    RdmaHandshake,
    /// M17: `AFSK` layout key of a shared-service Meta log (etcd/Redis,
    /// `meta::store::kv_log`): key layout and frame/pointer encodings.
    MetaKvLayout,
}

/// What a reader does with a version it does not support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Disposition {
    /// Authoritative stored data: refuse, keep the original, do not migrate.
    Refuse,
    /// Derived, disposable data: recognise, discard and rebuild.
    Discard,
    /// Wire protocol: reject the peer; deploy matching binaries.
    Protocol,
}

/// Every registered kind, in declaration order.
pub const ALL: &[FormatKind] = &[
    FormatKind::MetaWalEnvelope,
    FormatKind::MetaWalSegment,
    FormatKind::MetaIdentity,
    FormatKind::MetaCurrent,
    FormatKind::MetaCheckpointFile,
    FormatKind::MetaStateImage,
    FormatKind::MetaRecordDecision,
    FormatKind::MetaRequestDigest,
    FormatKind::NodeChunkCatalogWal,
    FormatKind::NodeChunkCatalogSnapshot,
    FormatKind::NodeCrcSegmentTable,
    FormatKind::NodeChunkSidecar,
    FormatKind::NodePeerCacheEntry,
    FormatKind::NodePeerCacheMarker,
    FormatKind::NodeExternalObjectKey,
    FormatKind::OwnerFsRootRecord,
    FormatKind::NodeMetaProtocol,
    FormatKind::RdmaHandshake,
    FormatKind::MetaKvLayout,
];

/// Current versions, typed as each format stores them. Values are the versions
/// in use before the registry existed, except `NODE_META_PROTOCOL`, which moved
/// to 2 when `NodeControl` was split by backend.
pub mod version {
    pub const META_WAL_ENVELOPE: u16 = 2;
    pub const META_WAL_SEGMENT: u16 = 1;
    pub const META_IDENTITY: u16 = 1;
    pub const META_CURRENT: u16 = 3;
    pub const META_CHECKPOINT_FILE: u16 = 1;
    pub const META_STATE_IMAGE: u16 = 3;
    pub const META_RECORD_DECISION: u16 = 2;
    pub const META_REQUEST_DIGEST: u32 = 1;
    pub const NODE_CHUNK_CATALOG_WAL: u32 = 1;
    pub const NODE_CHUNK_CATALOG_SNAPSHOT: u32 = 1;
    pub const NODE_CRC_SEGMENT_TABLE: u32 = 1;
    pub const NODE_CHUNK_SIDECAR: u16 = 1;
    pub const NODE_PEER_CACHE_ENTRY: u32 = 2;
    pub const NODE_PEER_CACHE_MARKER: u32 = 1;
    pub const NODE_EXTERNAL_OBJECT_KEY: u32 = 2;
    pub const OWNERFS_ROOT_RECORD: u32 = 1;
    pub const NODE_META_PROTOCOL: u32 = afs_protocol::CLUSTER_PROTOCOL_VERSION;
    pub const RDMA_HANDSHAKE: u32 = 1;
    pub const META_KV_LAYOUT: u16 = 1;
}

/// `magic || version (u16 big-endian)`: the six-byte tag of the fixed Meta
/// headers (`AFSW`, `AFSI`, `AFSM`, `AFSC`).
pub const fn tagged_header(magic: [u8; 4], version: u16) -> [u8; 6] {
    let v = version.to_be_bytes();
    [magic[0], magic[1], magic[2], magic[3], v[0], v[1]]
}
pub const META_WAL_SEGMENT_MAGIC: [u8; 4] = *b"AFSW";
pub const META_IDENTITY_MAGIC: [u8; 4] = *b"AFSI";
pub const META_CURRENT_MAGIC: [u8; 4] = *b"AFSM";
pub const META_CHECKPOINT_MAGIC: [u8; 4] = *b"AFSC";
pub const META_KV_LAYOUT_MAGIC: [u8; 4] = *b"AFSK";
/// `AFSI\0\x01`.
pub const META_IDENTITY_HEADER: [u8; 6] =
    tagged_header(META_IDENTITY_MAGIC, version::META_IDENTITY);
/// `AFSM\0\x03`.
pub const META_CURRENT_HEADER: [u8; 6] = tagged_header(META_CURRENT_MAGIC, version::META_CURRENT);
/// `AFSC\0\x01`.
pub const META_CHECKPOINT_HEADER: [u8; 6] =
    tagged_header(META_CHECKPOINT_MAGIC, version::META_CHECKPOINT_FILE);
/// The `StateImage` payload magic (inner layer; the schema follows as a varint).
pub const META_STATE_IMAGE_MAGIC: [u8; 4] = *b"AFSC";
pub const NODE_CHUNK_SIDECAR_MAGIC: [u8; 8] = *b"AFSBAO1\0";
/// Peer cache entry magic prefix; three ASCII digits of the version follow.
pub const NODE_PEER_CACHE_MAGIC_PREFIX: [u8; 5] = *b"AFSPC";
/// `AFSPC002`.
pub const NODE_PEER_CACHE_MAGIC: [u8; 8] = *b"AFSPC002";
pub const NODE_PEER_CACHE_MARKER_FILE: &str = ".afs-peer-cache-v1";
pub const NODE_PEER_CACHE_MARKER_PREFIX: &str = "AFS disposable Peer cache v";
pub const NODE_PEER_CACHE_MARKER_CONTENTS: &str = "AFS disposable Peer cache v1\n";

impl FormatKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::MetaWalEnvelope => "Meta WAL record envelope",
            Self::MetaWalSegment => "Meta WAL segment header",
            Self::MetaIdentity => "Meta IDENTITY",
            Self::MetaCurrent => "Meta CURRENT manifest",
            Self::MetaCheckpointFile => "Meta checkpoint file header",
            Self::MetaStateImage => "Meta checkpoint state image",
            Self::MetaRecordDecision => "Meta WAL record decision",
            Self::MetaRequestDigest => "Meta namespace request digest",
            Self::NodeChunkCatalogWal => "Node chunk catalog.wal",
            Self::NodeChunkCatalogSnapshot => "Node chunk catalog.snapshot",
            Self::NodeCrcSegmentTable => "Node CRC32C segment descriptor",
            Self::NodeChunkSidecar => "Node Bao sidecar",
            Self::NodePeerCacheEntry => "Node Peer cache entry",
            Self::NodePeerCacheMarker => "Node Peer cache directory marker",
            Self::NodeExternalObjectKey => "external object key",
            Self::OwnerFsRootRecord => "OwnerFs local root record",
            Self::NodeMetaProtocol => "Node-Meta protocol",
            Self::RdmaHandshake => "RDMA handshake",
            Self::MetaKvLayout => "Meta key-value log layout",
        }
    }

    pub const fn current(self) -> u32 {
        match self {
            Self::MetaWalEnvelope => version::META_WAL_ENVELOPE as u32,
            Self::MetaWalSegment => version::META_WAL_SEGMENT as u32,
            Self::MetaIdentity => version::META_IDENTITY as u32,
            Self::MetaCurrent => version::META_CURRENT as u32,
            Self::MetaCheckpointFile => version::META_CHECKPOINT_FILE as u32,
            Self::MetaStateImage => version::META_STATE_IMAGE as u32,
            Self::MetaRecordDecision => version::META_RECORD_DECISION as u32,
            Self::MetaRequestDigest => version::META_REQUEST_DIGEST,
            Self::NodeChunkCatalogWal => version::NODE_CHUNK_CATALOG_WAL,
            Self::NodeChunkCatalogSnapshot => version::NODE_CHUNK_CATALOG_SNAPSHOT,
            Self::NodeCrcSegmentTable => version::NODE_CRC_SEGMENT_TABLE,
            Self::NodeChunkSidecar => version::NODE_CHUNK_SIDECAR as u32,
            Self::NodePeerCacheEntry => version::NODE_PEER_CACHE_ENTRY,
            Self::NodePeerCacheMarker => version::NODE_PEER_CACHE_MARKER,
            Self::NodeExternalObjectKey => version::NODE_EXTERNAL_OBJECT_KEY,
            Self::OwnerFsRootRecord => version::OWNERFS_ROOT_RECORD,
            Self::NodeMetaProtocol => version::NODE_META_PROTOCOL,
            Self::RdmaHandshake => version::RDMA_HANDSHAKE,
            Self::MetaKvLayout => version::META_KV_LAYOUT as u32,
        }
    }

    pub const fn disposition(self) -> Disposition {
        match self {
            Self::NodeChunkSidecar | Self::NodePeerCacheEntry => Disposition::Discard,
            Self::NodeMetaProtocol | Self::RdmaHandshake => Disposition::Protocol,
            _ => Disposition::Refuse,
        }
    }

    /// The error code an unsupported version is reported with.
    pub const fn code(self) -> ErrorCode {
        match self {
            Self::NodeMetaProtocol => afs_error::META_NODE_PROTOCOL_VERSION,
            Self::RdmaHandshake => afs_error::NODE_RDMA_HANDSHAKE_VERSION,
            _ => afs_error::STORAGE_FORMAT_INCOMPATIBLE,
        }
    }
}

/// The fixed message template: format, object, found and supported version,
/// and what the operator does.
pub fn message(kind: FormatKind, object: impl Display, found: u64) -> String {
    let advice = match kind.disposition() {
        Disposition::Refuse => {
            "stored data is retained and not migrated or deleted; rebuild the data directory or run a binary that matches it"
        }
        Disposition::Discard => "derived data; discarded and rebuilt",
        Disposition::Protocol => "deploy Node and Meta binaries of the same release",
    };
    format!(
        "{} {object}: unsupported format version {found}, this binary supports version {}; {advice}",
        kind.name(),
        kind.current()
    )
}

/// The structured error for an unsupported version.
pub fn incompatible(kind: FormatKind, object: impl Display, found: u64) -> Error {
    Error::coded(kind.code(), message(kind, object, found))
}

/// `Ok` when `found` is the current version of `kind`.
pub fn check(kind: FormatKind, object: impl Display, found: u64) -> Result<()> {
    if found == u64::from(kind.current()) {
        Ok(())
    } else {
        Err(incompatible(kind, object, found))
    }
}

/// Version-first check of a fixed `magic || u16 BE version` header. Only a
/// recognised magic with another version is an incompatibility; a short
/// buffer or a foreign magic is left to the caller's integrity checks.
pub fn check_tagged_header(
    kind: FormatKind,
    magic: [u8; 4],
    object: impl Display,
    bytes: &[u8],
) -> Result<()> {
    match bytes.get(..6) {
        Some(tag) if tag[..4] == magic => check(
            kind,
            object,
            u64::from(u16::from_be_bytes([tag[4], tag[5]])),
        ),
        _ => Ok(()),
    }
}

/// Version-first check of a postcard payload whose first field is a `u16`
/// version varint. An undecodable prefix is left to the full decode.
pub fn check_postcard_u16(kind: FormatKind, object: impl Display, payload: &[u8]) -> Result<()> {
    match postcard::take_from_bytes::<u16>(payload) {
        Ok((found, _)) => check(kind, object, u64::from(found)),
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_names_are_unique_and_every_kind_is_listed() {
        let mut names = std::collections::HashSet::new();
        for kind in ALL {
            assert!(names.insert(kind.name()), "duplicate name {}", kind.name());
            check(*kind, "x", u64::from(kind.current())).unwrap();
        }
        assert_eq!(ALL.len(), 19);
    }

    /// The registry only names today's encodings; it must not change a byte.
    #[test]
    fn registry_constants_equal_the_pre_registry_literals() {
        assert_eq!(&META_IDENTITY_HEADER, b"AFSI\0\x01");
        assert_eq!(&META_CURRENT_HEADER, b"AFSM\0\x03");
        assert_eq!(&META_CHECKPOINT_HEADER, b"AFSC\0\x01");
        assert_eq!(
            tagged_header(META_WAL_SEGMENT_MAGIC, version::META_WAL_SEGMENT),
            *b"AFSW\0\x01"
        );
        assert_eq!(&NODE_CHUNK_SIDECAR_MAGIC, b"AFSBAO1\0");
        assert_eq!(&NODE_PEER_CACHE_MAGIC, b"AFSPC002");
        assert_eq!(
            (
                version::META_WAL_ENVELOPE,
                version::META_STATE_IMAGE,
                version::META_RECORD_DECISION,
                version::NODE_CHUNK_CATALOG_SNAPSHOT,
                version::NODE_CRC_SEGMENT_TABLE,
                version::NODE_CHUNK_SIDECAR,
                version::OWNERFS_ROOT_RECORD,
            ),
            // State image 3 and decision 2: GC references name their pinned
            // version, and R12's replica failure entity and keys (S4b).
            (2, 3, 2, 1, 1, 1, 1)
        );
    }

    #[test]
    fn mismatch_message_names_format_object_versions_and_advice() {
        let error = check(FormatKind::MetaCurrent, "/data/CURRENT", 2).unwrap_err();
        assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
        assert_eq!(error.kind(), afs_error::ErrorKind::FailedPrecondition);
        let text = error.message();
        for part in [
            "Meta CURRENT manifest",
            "/data/CURRENT",
            "version 2",
            "supports version 3",
            "not migrated or deleted",
            "rebuild the data directory",
        ] {
            assert!(text.contains(part), "{part}: {text}");
        }
        assert!(message(FormatKind::NodeChunkSidecar, "s", 9).contains("discarded and rebuilt"));
        assert_eq!(
            incompatible(FormatKind::NodeMetaProtocol, "node n", 0).code(),
            afs_error::META_NODE_PROTOCOL_VERSION
        );
    }

    #[test]
    fn tagged_and_postcard_probes_only_flag_recognised_versions() {
        let other = tagged_header(META_CURRENT_MAGIC, 9);
        assert_eq!(
            check_tagged_header(FormatKind::MetaCurrent, META_CURRENT_MAGIC, "c", &other)
                .unwrap_err()
                .code(),
            afs_error::STORAGE_FORMAT_INCOMPATIBLE
        );
        check_tagged_header(
            FormatKind::MetaCurrent,
            META_CURRENT_MAGIC,
            "c",
            &META_CURRENT_HEADER,
        )
        .unwrap();
        check_tagged_header(
            FormatKind::MetaCurrent,
            META_CURRENT_MAGIC,
            "c",
            b"XXXX\0\x09",
        )
        .unwrap();
        check_tagged_header(FormatKind::MetaCurrent, META_CURRENT_MAGIC, "c", b"AFS").unwrap();
        check_postcard_u16(FormatKind::MetaWalEnvelope, "f", &[2, 0xff]).unwrap();
        assert!(check_postcard_u16(FormatKind::MetaWalEnvelope, "f", &[3]).is_err());
        check_postcard_u16(FormatKind::MetaWalEnvelope, "f", &[0x80]).unwrap();
    }
}

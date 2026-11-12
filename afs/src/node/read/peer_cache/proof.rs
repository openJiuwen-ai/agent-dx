//! What a completed Peer decode hands to the cache.
//!
//! The sink only accepts these receipts, never raw bytes plus a claimed root.
//! The concrete receipts are `node::rpc::peer`'s `CompletedPeerObjectProof`
//! and `CompletedPeerStreamProof`, which only EOF-validated decoding can
//! construct (their fields stay private to the decoder). Declaring the
//! receipts here, as traits, keeps the data layer from depending on the
//! transport (R47); `afs/scripts/dep_edges.py` pins the implementor count
//! (`N-peer-proof-impls`) so no other type can quietly mint one.

use super::*;

/// A verified Peer object decode that borrows the caller's private body.
pub(in crate::node) trait PeerObjectProof<'a> {
    /// The body the decode verified; the sink checks it is the caller's.
    fn bytes(&self) -> &'a [u8];
    /// The BLAKE3 root the decode computed.
    fn root_digest(&self) -> &[u8; 32];
    fn into_outboard(self) -> Option<SharedOutboard>;
}

/// A verified Peer stream decode bound to one sink output.
pub(in crate::node) trait PeerStreamProof {
    /// The sink-minted binding carried from first byte through EOF.
    fn binding(&self) -> &PeerStreamBinding;
    fn chunk(&self) -> &ChunkObject;
    /// The BLAKE3 root the decode computed.
    fn root_digest(&self) -> &[u8; 32];
    fn into_outboard(self) -> Option<SharedOutboard>;
}

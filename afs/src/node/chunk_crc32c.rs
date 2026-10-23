//! Versioned native CRC32C descriptors. They are not cryptographic body proofs.
use crate::dfs::{ContentDigest, DigestAlgorithm};
use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};
pub(crate) const SEGMENT: usize = 32 * 1024;
pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: runtime detection guarantees ARM CRC instructions are available.
        return unsafe { crc_arm(bytes) };
    }
    crc32c::crc32c(bytes)
}
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc_arm(mut bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    while bytes.len() >= 8 {
        // SAFETY: at least eight readable bytes remain; unaligned loads are allowed.
        let word = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<u64>()) };
        // SAFETY: caller entered only after detecting ARM CRC support.
        crc = std::arch::aarch64::__crc32cd(crc, word);
        bytes = &bytes[8..];
    }
    for &b in bytes {
        // SAFETY: caller entered only after detecting ARM CRC support.
        crc = std::arch::aarch64::__crc32cb(crc, b);
    }
    !crc
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CrcTable {
    pub(crate) format: u32,
    pub(crate) length: u64,
    pub(crate) segment_bytes: u32,
    pub(crate) checksums: Vec<u32>,
}
impl CrcTable {
    pub(crate) fn build(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 {
            return Err(Error::coded(
                afs_error::NODE_STORAGE_INVALID,
                "CRC32C chunk size is outside native 4MiB bounds",
            ));
        }
        Ok(Self {
            format: 1,
            length: bytes.len() as u64,
            segment_bytes: SEGMENT as u32,
            checksums: bytes.chunks(SEGMENT).map(checksum).collect(),
        })
    }

    pub(crate) fn descriptor_digest(&self) -> ContentDigest {
        let mut h = blake3::Hasher::new();
        h.update(b"AFS-CRC32C-SEGMENTS-v1");
        h.update(&self.format.to_le_bytes());
        h.update(&self.length.to_le_bytes());
        h.update(&self.segment_bytes.to_le_bytes());
        for crc in &self.checksums {
            h.update(&crc.to_le_bytes());
        }
        ContentDigest {
            algorithm: DigestAlgorithm::Crc32cSegmentsV1,
            bytes: *h.finalize().as_bytes(),
        }
    }
    pub(crate) fn valid_shape(&self, length: u64) -> bool {
        length > 0
            && length <= 4 * 1024 * 1024
            && self.format == 1
            && self.length == length
            && self.segment_bytes == SEGMENT as u32
            && self.checksums.len() == (length as usize).div_ceil(SEGMENT)
    }
}

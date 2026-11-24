//! CRC32C of raw transfer bytes.

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

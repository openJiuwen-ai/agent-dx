//! Versioned local FUSE control records for importing an existing POSIX file.
//! File payloads never use these controls; they continue through the shared ring.
#![allow(unsafe_code)]

use std::{
    fs::File,
    io::{self, Read},
    os::fd::{AsRawFd, BorrowedFd},
};

pub const RECORD_BYTES: usize = 64;
pub const VERSION: u32 = 1;
pub const PROBE: u32 = 0xc040_a501;
pub const REGISTER: u32 = 0xc040_a502;
pub const UNREGISTER: u32 = 0xc040_a503;
pub const ACCESS_READ: u32 = 1;
pub const ACCESS_WRITE: u32 = 2;
pub const CACHED: u32 = 4;
pub const PENDING: u32 = 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Control {
    pub flags: u32,
    pub node: [u8; 16],
    pub session: [u8; 16],
    pub operation: [u8; 16],
    pub registration: u32,
}
impl Control {
    pub fn encode(&self) -> [u8; RECORD_BYTES] {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..4].copy_from_slice(&VERSION.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes[8..24].copy_from_slice(&self.node);
        bytes[24..40].copy_from_slice(&self.session);
        bytes[40..56].copy_from_slice(&self.operation);
        bytes[56..60].copy_from_slice(&self.registration.to_le_bytes());
        bytes
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != RECORD_BYTES
            || bytes[..4] != VERSION.to_le_bytes()
            || bytes[60..64] != [0; 4]
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid AFS fd control version or record",
            ));
        }
        Ok(Self {
            flags: u32::from_le_bytes(bytes[4..8].try_into().map_err(io::Error::other)?),
            node: bytes[8..24].try_into().map_err(io::Error::other)?,
            session: bytes[24..40].try_into().map_err(io::Error::other)?,
            operation: bytes[40..56].try_into().map_err(io::Error::other)?,
            registration: u32::from_le_bytes(bytes[56..60].try_into().map_err(io::Error::other)?),
        })
    }
}
/// Execute one fixed-size control on a borrowed descriptor. The caller retains ownership.
/// Returns the kernel/daemon errno or an invalid version/response error.
pub fn control(fd: BorrowedFd<'_>, command: u32, record: &mut Control) -> io::Result<()> {
    if !matches!(command, PROBE | REGISTER | UNREGISTER) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unknown AFS fd control",
        ));
    }
    let mut bytes = record.encode();
    // SAFETY: all commands encode exactly RECORD_BYTES readable/writable bytes;
    // the exclusive stack buffer remains alive and the borrowed fd is not closed.
    let result = unsafe {
        libc::ioctl(
            fd.as_raw_fd(),
            libc::c_ulong::from(command),
            bytes.as_mut_ptr(),
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    *record = Control::decode(&bytes)?;
    Ok(())
}
/// Generate an unguessable local session/operation key without global RNG state.
pub fn random_key() -> io::Result<[u8; 16]> {
    let mut key = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut key)?;
    if key == [0; 16] {
        return Err(io::Error::other("random fd key was zero"));
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_record_round_trips_and_rejects_unversioned_bytes() {
        let value = Control {
            flags: ACCESS_READ | CACHED,
            node: [1; 16],
            session: [2; 16],
            operation: [3; 16],
            registration: u32::MAX,
        };
        assert_eq!(Control::decode(&value.encode()).unwrap(), value);
        let mut bad = value.encode();
        bad[60] = 1;
        assert!(Control::decode(&bad).is_err());
        bad[60] = 0;
        bad[0] = 2;
        assert!(Control::decode(&bad).is_err());
        assert!(Control::decode(&bad[..12]).is_err());
    }
}

//! Immutable source identity shared by the Node and Meta.
use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

pub(crate) const LAZY_FILE_DESCRIPTOR_XATTR: &[u8] = b"user.afs.lazy.v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LazyFillBinding {
    pub kind: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LazyFileSpec {
    pub length: u64,
    pub binding: LazyFillBinding,
}

impl LazyFileSpec {
    pub fn validate(&self) -> Result<()> {
        if self.length > i64::MAX as u64
            || self.binding.kind.is_empty()
            || self.binding.kind.len() > 128
            || !self
                .binding
                .kind
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            || self.binding.payload.len() > 16 * 1024
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_INVALID,
                "invalid or oversized lazy file binding",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct BindLazySourceRequest {
    pub caller_id: String,
    pub caller_session_id: String,
    pub operation_id: super::OperationId,
    pub inode_id: super::InodeId,
    pub expected_inode_revision: u64,
    pub source: LazyFileSpec,
}

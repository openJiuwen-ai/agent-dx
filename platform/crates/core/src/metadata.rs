//! Platform-owned user metadata. This is never execution configuration.
use crate::{Error, Result};
use std::collections::BTreeMap;

pub type UserMetadata = BTreeMap<String, String>;

pub const MAX_USER_METADATA_ENTRIES: usize = 128;
pub const MAX_USER_METADATA_BYTES: usize = 64 * 1024;

pub fn validate_user_metadata(metadata: &UserMetadata) -> Result<()> {
    let bytes = metadata.iter().try_fold(0usize, |total, (key, value)| {
        total
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or(Error::Invalid("user metadata exceeds byte limit".into()))
    })?;
    if metadata.len() > MAX_USER_METADATA_ENTRIES
        || bytes > MAX_USER_METADATA_BYTES
        || metadata
            .iter()
            .any(|(key, value)| key.is_empty() || key.len() > 256 || value.len() > 4096)
    {
        return Err(Error::Invalid("invalid user metadata".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_user_metadata() {
        assert!(validate_user_metadata(&[("team".into(), "core".into())].into()).is_ok());
        assert!(
            validate_user_metadata(&[("team".into(), "line one\nline two".into())].into()).is_ok()
        );
        assert!(validate_user_metadata(&[("".into(), "core".into())].into()).is_err());
        assert!(validate_user_metadata(&[("team".into(), "v".repeat(4097))].into()).is_err());

        let at_entry_limit: UserMetadata = (0..MAX_USER_METADATA_ENTRIES)
            .map(|index| (format!("key-{index}"), "v".into()))
            .collect();
        assert!(validate_user_metadata(&at_entry_limit).is_ok());
        let over_entry_limit: UserMetadata = (0..=MAX_USER_METADATA_ENTRIES)
            .map(|index| (format!("key-{index}"), "v".into()))
            .collect();
        assert!(validate_user_metadata(&over_entry_limit).is_err());

        let at_byte_limit: UserMetadata = (0..16)
            .map(|index| (format!("{index:03}"), "v".repeat(4093)))
            .collect();
        assert_eq!(
            at_byte_limit
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>(),
            MAX_USER_METADATA_BYTES
        );
        assert!(validate_user_metadata(&at_byte_limit).is_ok());
        let mut over_byte_limit = at_byte_limit;
        over_byte_limit.insert("extra".into(), "v".into());
        assert!(validate_user_metadata(&over_byte_limit).is_err());
    }
}

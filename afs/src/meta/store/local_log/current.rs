//! The CURRENT manifest: checkpoint and segment references, encoding and atomic replacement.

use super::*;

pub(super) const CURRENT: &str = "CURRENT";

pub(super) const MANIFEST_MAX: usize = 64 * 1024;

/// CURRENT v3 carries the writer term. Any other version (v2 had no term) is
/// refused by the version-first check, never adopted.
pub(super) const CURRENT_MAGIC: &[u8; 6] = &format::META_CURRENT_HEADER;

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub(super) struct CheckpointRef {
    pub(super) covered: LogTip,
    pub(super) revision: StoreRevision,
    pub(super) bytes: u64,
    pub(super) digest: [u8; 32],
}

/// A sealed segment is immutable. The last (active) segment has no seal;
/// its checked durable tail remains authoritative for appends after CURRENT.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct SegmentRef {
    pub(super) base: LogTip,
    pub(super) base_revision: StoreRevision,
    pub(super) sealed: Option<(Tail, StoreRevision)>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct Current {
    pub(super) identity: StoreIdentity,
    /// Writer term: every writer open durably selects `term + 1` before it
    /// returns; a handle whose term no longer matches is fenced.
    pub(super) term: u64,
    pub(super) current: Option<CheckpointRef>,
    pub(super) previous: Option<CheckpointRef>,
    #[serde(deserialize_with = "segment_refs")]
    pub(super) segments: Vec<SegmentRef>,
    #[serde(deserialize_with = "segment_refs")]
    pub(super) retired_segments: Vec<SegmentRef>,
    #[serde(deserialize_with = "retired_images")]
    pub(super) retired_images: Vec<CheckpointRef>,
}

fn finite_items<'de, D, T, const N: usize>(d: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Items<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>, const N: usize> serde::de::Visitor<'de> for Items<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at most {N} manifest entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Vec<T>, A::Error> {
            let mut result = Vec::new();
            while let Some(item) = seq.next_element()? {
                if result.len() == N {
                    return Err(serde::de::Error::custom("manifest entry count exceeded"));
                }
                result.try_reserve(1).map_err(serde::de::Error::custom)?;
                result.push(item);
            }
            Ok(result)
        }
    }
    d.deserialize_seq(Items::<T, N>(std::marker::PhantomData))
}

pub(super) fn segment_refs<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<SegmentRef>, D::Error> {
    finite_items::<D, SegmentRef, 256>(d)
}

pub(super) fn retired_images<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<CheckpointRef>, D::Error> {
    finite_items::<D, CheckpointRef, 8>(d)
}

impl Current {
    pub(super) fn initial(identity: StoreIdentity) -> Self {
        Self {
            identity,
            term: 0,
            current: None,
            previous: None,
            segments: vec![SegmentRef {
                base: LogTip::EMPTY,
                base_revision: StoreRevision::ZERO,
                sealed: None,
            }],
            retired_segments: Vec::new(),
            retired_images: Vec::new(),
        }
    }
    pub(super) fn validate(&self) -> Result<()> {
        if self.segments.is_empty()
            || self.segments.len() > 256
            || self.retired_segments.len() > 256
            || self.retired_images.len() > 8
            || self.previous.is_some_and(|old| {
                self.current
                    .is_none_or(|new| old.covered.sequence >= new.covered.sequence)
            })
        {
            return Err(invalid_log("CURRENT count/checkpoint ordering"));
        }
        let first = self.segments[0];
        let oldest = self.previous.or(self.current);
        if oldest.is_none()
            && (first.base != LogTip::EMPTY || first.base_revision != StoreRevision::ZERO)
            || oldest.is_some_and(|image| image.covered.sequence < first.base.sequence)
        {
            return Err(invalid_log(
                "CURRENT omits a retained recovery point's required prefix",
            ));
        }
        for (index, segment) in self.segments.iter().enumerate() {
            wal_name(segment.base)?;
            if index + 1 == self.segments.len() {
                if segment.sealed.is_some() {
                    return Err(invalid_log("CURRENT active segment must not be sealed"));
                }
            } else {
                let (tail, revision) = segment
                    .sealed
                    .ok_or_else(|| invalid_log("CURRENT unsealed middle segment"))?;
                let next = self.segments[index + 1];
                if tail.tip.sequence <= segment.base.sequence
                    || tail.end <= LOG_HEADER_BYTES
                    || next.base != tail.tip
                    || next.base_revision != revision
                {
                    return Err(invalid_log("CURRENT segment chain discontinuity"));
                }
            }
        }
        let mut retired_bases = std::collections::BTreeSet::new();
        for segment in &self.retired_segments {
            let (tail, _) = segment
                .sealed
                .ok_or_else(|| invalid_log("CURRENT cannot retire an active segment"))?;
            if tail.tip.sequence > first.base.sequence
                || tail.tip.sequence <= segment.base.sequence
                || !retired_bases.insert(segment.base.sequence)
            {
                return Err(invalid_log(
                    "CURRENT retired segment still required or duplicated",
                ));
            }
        }
        for image in &self.retired_images {
            if self.current == Some(*image) || self.previous == Some(*image) {
                return Err(invalid_log("CURRENT retires a selected image"));
            }
        }
        Ok(())
    }
}

pub(super) fn encode_current(current: &Current) -> Result<Vec<u8>> {
    let payload = super::super::codec::encode_bounded(current, MANIFEST_MAX - 42)?;
    let mut bytes = Vec::with_capacity(payload.len() + 42);
    bytes.extend_from_slice(CURRENT_MAGIC);
    bytes.extend_from_slice(&payload);
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.current.v1");
    hash.update(&bytes);
    bytes.extend_from_slice(hash.finalize().as_bytes());
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    Ok(bytes)
}

pub(super) fn read_current(dir: &Path, identity: StoreIdentity) -> Result<Option<Current>> {
    let path = dir.join(CURRENT);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid_log("CURRENT is not a regular file"));
        }
        Ok(_) => {}
    }
    let mut file = open_file(&path, false)?;
    let length =
        usize::try_from(file.metadata()?.len()).map_err(|_| invalid_log("CURRENT length"))?;
    if !(42..=MANIFEST_MAX).contains(&length) || !file.metadata()?.is_file() {
        return Err(invalid_log("CURRENT length/type"));
    }
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)?;
    // Version first: before the hash, CRC and postcard decode.
    format::check_tagged_header(
        FormatKind::MetaCurrent,
        format::META_CURRENT_MAGIC,
        path.display(),
        &bytes,
    )?;
    let hash_start = length - 36;
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.current.v1");
    hash.update(&bytes[..hash_start]);
    if bytes[..6] != *CURRENT_MAGIC
        || bytes[hash_start..length - 4] != *hash.finalize().as_bytes()
        || bytes[length - 4..] != crc32c::crc32c(&bytes[..length - 4]).to_be_bytes()
    {
        return Err(invalid_log("CURRENT format/hash/CRC"));
    }
    let (current, tail): (Current, _) = postcard::take_from_bytes(&bytes[6..hash_start])
        .map_err(|error| invalid_log(format!("CURRENT decode: {error}")))?;
    if !tail.is_empty() || current.identity != identity {
        return Err(invalid_log("CURRENT identity/trailing bytes"));
    }
    current.validate()?;
    Ok(Some(current))
}

pub(super) fn write_current(
    dir: &Path,
    manifest: &Current,
    limits: LocalCheckpointLimits,
) -> Result<()> {
    manifest.validate()?;
    let bytes = encode_current(manifest)?;
    quarantine_checkpoint_tmp(dir, "CURRENT.tmp", limits)?;
    checkpoint_budget(dir, bytes.len() as u64, 1, limits)?;
    let tmp = dir.join("CURRENT.tmp");
    let mut file = open_file(&tmp, true)?;
    reserve_file(&file, bytes.len() as u64)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(tmp, dir.join(CURRENT))?;
    sync_dir(dir)
}

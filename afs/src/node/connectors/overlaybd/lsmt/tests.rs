use super::*;

fn metadata() -> ([u8; METADATA_SIZE], [u8; METADATA_SIZE]) {
    let mut header = [0; METADATA_SIZE];
    header[..MAGIC.len()].copy_from_slice(&MAGIC);
    header[24..28].copy_from_slice(&390u32.to_le_bytes());
    header[28..32].copy_from_slice(&7u32.to_le_bytes());
    header[48..56].copy_from_slice(&8192u64.to_le_bytes());
    header[132..134].copy_from_slice(&[1, 1]);
    let mut trailer = header;
    trailer[28..32].copy_from_slice(&6u32.to_le_bytes());
    trailer[32..40].copy_from_slice(&8192u64.to_le_bytes());
    trailer[40..48].copy_from_slice(&3u64.to_le_bytes());
    (header, trailer)
}

fn index_entry(start: u64, len: u64, physical: u64, zero: bool) -> [u8; 16] {
    let mut raw = [0; 16];
    raw[..8].copy_from_slice(&(start | (len << 50)).to_le_bytes());
    raw[8..].copy_from_slice(&(physical | (u64::from(zero) << 55)).to_le_bytes());
    raw
}

#[test]
fn trailer_index_and_padding_resolve_data_zero_and_gaps() {
    let (header, trailer) = metadata();
    let metadata = Metadata::parse(&header, &trailer, 16384).unwrap();
    assert_eq!(metadata.virtual_size, 8192);
    assert_eq!(metadata.index_offset, 8192);
    let index = [
        index_entry(1, 2, 8, false),
        index_entry(4, 1, 0, true),
        index_entry(LOGICAL_MASK, 0, 0, false),
    ]
    .concat();
    let index = metadata.parse_index(&index).unwrap();
    assert_eq!(index.lookup(0), (None, 512));
    assert_eq!(index.lookup(1024), (Some(Mapping::Data(4608)), 1536));
    assert_eq!(index.lookup(1536), (None, 2048));
    assert_eq!(index.lookup(2048), (Some(Mapping::Zero), 2560));
    assert_eq!(index.lookup(2560), (None, u64::MAX));
}

#[test]
fn rejects_out_of_bounds_physical_mapping_and_invalid_padding() {
    let (header, trailer) = metadata();
    let metadata = Metadata::parse(&header, &trailer, 16384).unwrap();
    let padding = index_entry(LOGICAL_MASK, 0, 0, false);
    for invalid in [index_entry(1, 2, 15, false), index_entry(1, 0, 0, false)] {
        assert!(
            metadata
                .parse_index(&[invalid, padding, padding].concat())
                .is_err()
        );
    }
    assert!(
        metadata
            .parse_index(&[padding, index_entry(1, 2, 8, false), padding].concat())
            .is_err()
    );
}

#[test]
fn rejects_unsupported_version_and_out_of_bounds_index() {
    let (header, mut trailer) = metadata();
    trailer[133] = 2;
    assert!(Metadata::parse(&header, &trailer, 16384).is_err());
    trailer[133] = 1;
    trailer[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(Metadata::parse(&header, &trailer, 16384).is_err());
}

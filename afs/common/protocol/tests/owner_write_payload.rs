use afs_protocol::node_data::{DataPlane, OwnerHandle, OwnerWriteRequest, RootAccess};
use prost::{Message, bytes::Bytes};

// Published wire contract before changing only the decoded payload representation.
#[derive(Clone, PartialEq, Message)]
struct LegacyOwnerWriteRequest {
    #[prost(message, optional, tag = "1")]
    access: Option<RootAccess>,
    #[prost(message, optional, tag = "2")]
    handle: Option<OwnerHandle>,
    #[prost(uint64, tag = "3")]
    offset: u64,
    #[prost(bytes = "vec", tag = "4")]
    data: Vec<u8>,
    #[prost(uint32, tag = "5")]
    length: u32,
    #[prost(message, optional, tag = "6")]
    plane: Option<DataPlane>,
    #[prost(bool, tag = "7")]
    kill_suidgid: bool,
    #[prost(bytes = "vec", tag = "8")]
    data_checksum: Vec<u8>,
}

fn legacy(data: Vec<u8>) -> LegacyOwnerWriteRequest {
    LegacyOwnerWriteRequest {
        access: Some(RootAccess::default()),
        handle: Some(OwnerHandle::default()),
        offset: 4096,
        length: data.len() as u32,
        data,
        plane: Some(DataPlane::default()),
        kill_suidgid: true,
        data_checksum: vec![0x37; 32],
    }
}

#[test]
fn owner_write_request_preserves_legacy_wire_in_both_directions() {
    for data in [vec![0, 255, 17, 0], b"short".to_vec(), vec![]] {
        let legacy = legacy(data);
        let current = OwnerWriteRequest::decode(Bytes::from(legacy.encode_to_vec())).unwrap();
        assert_eq!(&current.data[..], &legacy.data);
        let decoded_legacy =
            LegacyOwnerWriteRequest::decode(current.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded_legacy, legacy);
    }
}

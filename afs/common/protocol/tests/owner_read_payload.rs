use afs_protocol::node_data::OwnerReadReply;
use prost::{Message, bytes::Bytes};

// The published OwnerReadReply wire contract before the buffer optimization.
#[derive(Clone, PartialEq, Message)]
struct LegacyOwnerReadReply {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
    #[prost(uint32, tag = "2")]
    read: u32,
    #[prost(bool, tag = "3")]
    eof: bool,
    #[prost(bytes = "vec", tag = "4")]
    data_checksum: Vec<u8>,
}

#[test]
fn owner_read_reply_preserves_legacy_wire_in_both_directions() {
    for (data, eof) in [
        (vec![0, 255, 17, 0], false),
        (b"short".to_vec(), true),
        (vec![], true),
    ] {
        let legacy = LegacyOwnerReadReply {
            read: data.len() as u32,
            data,
            eof,
            data_checksum: vec![0x19; 32],
        };
        let current = OwnerReadReply::decode(Bytes::from(legacy.encode_to_vec())).unwrap();
        assert_eq!(&current.data[..], &legacy.data);
        assert_eq!(current.read, legacy.read);
        assert_eq!(current.eof, legacy.eof);
        assert_eq!(current.data_checksum, legacy.data_checksum);
        let decoded_legacy =
            LegacyOwnerReadReply::decode(current.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded_legacy, legacy);
    }
}

#[test]
fn owner_read_reply_decode_shares_owned_wire_payload() {
    let payload: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let legacy = LegacyOwnerReadReply {
        read: payload.len() as u32,
        data: payload.clone(),
        eof: false,
        data_checksum: vec![0x37; 32],
    };
    let wire = Bytes::from(legacy.encode_to_vec());
    let wire_start = wire.as_ptr() as usize;
    let wire_end = wire_start + wire.len();
    let decoded = OwnerReadReply::decode(wire.clone()).unwrap();
    let payload_start = decoded.data.as_ptr() as usize;
    assert!(
        payload_start >= wire_start && payload_start + decoded.data.len() <= wire_end,
        "Owner READ payload was copied outside its owned wire buffer"
    );
    assert_eq!(&decoded.data[..], &payload);
    let retained = decoded.clone();
    drop(decoded);
    drop(wire);
    assert_eq!(&retained.data[..], &payload);
    assert_eq!(retained.data_checksum, vec![0x37; 32]);
}

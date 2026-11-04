use super::*;
#[test]
fn crc_read_modes_server_honours_requested_mode() {
    use crate::config::DfsReadCrcMode;
    for (requested, expected) in [
        ("receiver", DfsReadCrcMode::Receiver),
        ("off", DfsReadCrcMode::Off),
        ("both", DfsReadCrcMode::Both),
    ] {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("x-afs-read-crc-mode", requested.parse().unwrap());
        assert_eq!(requested_crc_read_mode(&metadata).unwrap(), expected);
    }
    assert_eq!(
        requested_crc_read_mode(&tonic::metadata::MetadataMap::new()).unwrap(),
        DfsReadCrcMode::Both,
        "a request without a mode keeps the full policy"
    );
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-afs-read-crc-mode", "unknown".parse().unwrap());
    assert_eq!(
        requested_crc_read_mode(&metadata).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

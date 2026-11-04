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
    let status = requested_crc_read_mode(&metadata).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    // B3: the rejection carries the AFS error detail, not a bare status.
    let error = afs_transport::grpc::error_status::status_to_error(status);
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_INVALID);
    assert!(error.message().contains("unknown read CRC mode"), "{error}");
}

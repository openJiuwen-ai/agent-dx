use adx_protocol::auth::{Peers, Principal};
use tonic::Request;

#[test]
fn network_identity_is_opt_in_and_preserves_node_identity() {
    let mut request = Request::new(());
    request
        .metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    request.metadata_mut().insert_bin(
        "adx-node-id-bin",
        tonic::metadata::MetadataValue::from_bytes(b"worker-7"),
    );
    assert!(Peers::default().authenticate(&request).is_err());
    assert_eq!(
        Peers::network().authenticate(&request).unwrap(),
        Principal::Node("worker-7".into())
    );
}

#[test]
fn network_mode_rejects_missing_or_invalid_identity() {
    let peers = Peers::network();
    let mut request = Request::new(());
    assert!(peers.authenticate(&request).is_err());
    request
        .metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    assert!(peers.authenticate(&request).is_err());
    request
        .metadata_mut()
        .insert("adx-component", "admin".parse().unwrap());
    assert!(peers.authenticate(&request).is_err());
}

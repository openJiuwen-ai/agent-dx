use adx_protocol::auth::Principal;

#[test]
fn exact_node_identity_only_authorizes_its_bound_node() {
    let principal = Principal::Node("node-a".into());

    assert_eq!(principal.authorized_node(""), Some("node-a"));
    assert_eq!(principal.authorized_node("node-a"), Some("node-a"));
    assert_eq!(principal.authorized_node("node-b"), None);
}

#[test]
fn node_pool_identity_requires_an_explicit_node() {
    let principal = Principal::NodePool;

    assert_eq!(principal.authorized_node("node-a"), Some("node-a"));
    assert_eq!(principal.authorized_node("node-b"), Some("node-b"));
    assert_eq!(principal.authorized_node(""), None);
}

#[test]
fn non_node_identity_cannot_authorize_a_node() {
    assert_eq!(Principal::Master.authorized_node("node-a"), None);
    assert_eq!(Principal::ApiServer.authorized_node("node-a"), None);
    assert_eq!(Principal::Edge.authorized_node("node-a"), None);
}

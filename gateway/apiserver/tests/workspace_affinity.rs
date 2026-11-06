use adx_apiserver::{
    config::{WorkspaceAffinity, WorkspaceAffinityErrorPolicy},
    workspace_affinity::{derive_owner, enforce_node, WorkspaceAffinityResolver},
};
use adx_protocol::control as pb;
use std::{
    io::{Read, Write},
    sync::atomic::{AtomicUsize, Ordering},
};
use tonic::Code;

const ROOT: &str = "/home/agentos/users";

fn local(target: &str, source: &str) -> pb::MountSpec {
    pb::MountSpec {
        r#type: "bind".into(),
        target: target.into(),
        mount_source: Some(pb::mount_spec::MountSource::Source(pb::StorageSource {
            source: Some(pb::storage_source::Source::Local(source.into())),
        })),
        ..Default::default()
    }
}

#[test]
fn owner_is_the_first_component_below_the_root() {
    let mounts = vec![local("/workspace", "/home/agentos/users/alice/project")];
    assert_eq!(
        derive_owner(ROOT.as_ref(), &mounts).unwrap().as_deref(),
        Some("alice")
    );
}

#[test]
fn unrelated_prefixes_and_non_workspace_sources_are_ignored() {
    let mounts = vec![
        local("/sibling", "/home/agentos/users2/alice"),
        local("/root-itself", ROOT),
        local("/relative", "home/agentos/users/alice"),
        local("/parent", "/home/agentos/users/alice/../bob"),
        pb::MountSpec {
            r#type: "bind".into(),
            target: "/image".into(),
            mount_source: Some(pb::mount_spec::MountSource::Source(pb::StorageSource {
                source: Some(pb::storage_source::Source::Image("image".into())),
            })),
            ..Default::default()
        },
        pb::MountSpec {
            r#type: "bind".into(),
            target: "/volume".into(),
            mount_source: Some(pb::mount_spec::MountSource::Volume(pb::VolumeRef {
                id: "volume".into(),
                uid: None,
            })),
            ..Default::default()
        },
    ];
    assert_eq!(derive_owner(ROOT.as_ref(), &mounts).unwrap(), None);
}

#[test]
fn a_trailing_root_separator_is_equivalent() {
    let mounts = vec![local("/workspace", "/home/agentos/users/alice")];
    assert_eq!(
        derive_owner("/home/agentos/users/".as_ref(), &mounts)
            .unwrap()
            .as_deref(),
        Some("alice")
    );
    assert_eq!(
        derive_owner("/".as_ref(), &mounts).unwrap(),
        None,
        "a bare root must not turn every absolute mount into a Workspace"
    );
}

#[test]
fn multiple_owners_are_rejected_before_any_lookup() {
    let mounts = vec![
        local("/a", "/home/agentos/users/alice"),
        local("/b", "/home/agentos/users/bob"),
    ];
    assert_eq!(
        derive_owner(ROOT.as_ref(), &mounts).unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[test]
fn empty_policy_gets_one_default_selector() {
    let mut policy = pb::SchedulingPolicy::default();
    enforce_node(&mut policy, "node-a").unwrap();
    assert_eq!(policy.required_node.len(), 1);
    assert_eq!(policy.required_node[0].expressions.len(), 1);
    assert_eq!(policy.required_node[0].expressions[0].key, "NODE_ID");
    assert_eq!(
        policy.required_node[0].expressions[0].op,
        pb::SelectorOp::In as i32
    );
    assert_eq!(policy.required_node[0].expressions[0].values, ["node-a"]);
}

#[test]
fn every_existing_selector_is_narrowed() {
    let mut policy = pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector::default(), pb::LabelSelector::default()],
        ..Default::default()
    };
    enforce_node(&mut policy, "node-a").unwrap();
    assert_eq!(policy.required_node.len(), 2);
    for selector in &policy.required_node {
        assert_eq!(selector.expressions.len(), 1);
        assert_eq!(selector.expressions[0].values, ["node-a"]);
    }
}

#[test]
fn explicit_node_constraints_must_admit_the_owner_node() {
    let requirement = |op: pb::SelectorOp, values: &[&str]| pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            expressions: vec![pb::LabelRequirement {
                key: "NODE_ID".into(),
                op: op as i32,
                values: values.iter().map(|value| (*value).into()).collect(),
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut allowed = requirement(pb::SelectorOp::In, &["node-a", "node-b"]);
    enforce_node(&mut allowed, "node-a").unwrap();
    assert_eq!(allowed.required_node[0].expressions.len(), 2);

    for mut policy in [
        requirement(pb::SelectorOp::In, &["node-b"]),
        requirement(pb::SelectorOp::NotIn, &["node-a"]),
        requirement(pb::SelectorOp::DoesNotExist, &[]),
    ] {
        assert_eq!(
            enforce_node(&mut policy, "node-a").unwrap_err().code(),
            Code::InvalidArgument
        );
    }
}

fn token_file() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "adx-workspace-affinity-{}-{}.token",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&path, "test-token\n").unwrap();
    path.to_string_lossy().into_owned()
}

fn resolver(endpoints: Vec<String>) -> WorkspaceAffinityResolver {
    resolver_with(WorkspaceAffinityErrorPolicy::FailClosed, endpoints)
}

fn resolver_with(
    on_error: WorkspaceAffinityErrorPolicy,
    endpoints: Vec<String>,
) -> WorkspaceAffinityResolver {
    adx_transport::install_crypto_provider();
    WorkspaceAffinityResolver::new(WorkspaceAffinity {
        root: ROOT.into(),
        endpoints,
        token_file: token_file().into(),
        timeout_seconds: 2,
        on_error,
    })
    .unwrap()
}

fn response(status: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn serve(response: Vec<u8>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request);
        let _ = stream.write_all(&response);
    });
    format!("http://{address}")
}

fn unreachable() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{address}")
}

fn node_group(op: pb::SelectorOp, values: &[&str]) -> pb::PlacementGroup {
    pb::PlacementGroup {
        target: pb::PlacementTarget::Node as i32,
        required: true,
        terms: vec![pb::WeightedSelector {
            weight: 1,
            selector: Some(pb::LabelSelector {
                expressions: vec![pb::LabelRequirement {
                    key: "NODE_ID".into(),
                    op: op as i32,
                    values: values.iter().map(|value| (*value).into()).collect(),
                }],
                ..Default::default()
            }),
        }],
        ..Default::default()
    }
}

#[test]
fn required_placement_groups_are_checked_for_conflicts() {
    let mut conflicting = pb::SchedulingPolicy {
        placement_groups: vec![node_group(pb::SelectorOp::In, &["node-b"])],
        ..Default::default()
    };
    assert_eq!(
        enforce_node(&mut conflicting, "node-a").unwrap_err().code(),
        Code::InvalidArgument,
        "explicit node_id is encoded as a required placement group"
    );

    let mut allowed = pb::SchedulingPolicy {
        placement_groups: vec![node_group(pb::SelectorOp::In, &["node-a"])],
        ..Default::default()
    };
    enforce_node(&mut allowed, "node-a").unwrap();
    assert_eq!(allowed.required_node.len(), 1);
    assert_eq!(allowed.required_node[0].expressions[0].values, ["node-a"]);
}

#[tokio::test]
async fn ready_workspace_returns_its_node() {
    let endpoint = serve(response(
        "200 OK",
        r#"{"owner":"alice","node_id":"node-a","state":"ready"}"#,
    ));
    assert_eq!(
        resolver(vec![endpoint]).node_id("alice").await.unwrap(),
        "node-a"
    );
}

#[tokio::test]
async fn unready_workspace_is_a_precondition_failure() {
    let endpoint = serve(response(
        "200 OK",
        r#"{"owner":"alice","node_id":"node-a","state":"pending"}"#,
    ));
    assert_eq!(
        resolver(vec![endpoint])
            .node_id("alice")
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn unregistered_workspace_is_a_precondition_failure() {
    let endpoint = serve(response("404 Not Found", r#"{"error":"missing"}"#));
    assert_eq!(
        resolver(vec![endpoint])
            .node_id("alice")
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn rejected_credentials_are_unavailable_without_failover() {
    let endpoint = serve(response("401 Unauthorized", "{}"));
    assert_eq!(
        resolver(vec![endpoint])
            .node_id("alice")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn malformed_or_nodeless_responses_are_internal() {
    for body in [r#"not-json"#, r#"{"state":"ready"}"#] {
        let endpoint = serve(response("200 OK", body));
        assert_eq!(
            resolver(vec![endpoint])
                .node_id("alice")
                .await
                .unwrap_err()
                .code(),
            Code::Internal
        );
    }
}

#[tokio::test]
async fn transport_and_server_errors_fail_over() {
    let failing = serve(response("500 Internal Server Error", "{}"));
    let ready = serve(response(
        "200 OK",
        r#"{"node_id":"node-b","state":"ready"}"#,
    ));
    assert_eq!(
        resolver(vec![unreachable(), failing, ready])
            .node_id("alice")
            .await
            .unwrap(),
        "node-b"
    );
}

#[tokio::test]
async fn exhausted_endpoints_are_unavailable() {
    let failing = serve(response("503 Service Unavailable", "{}"));
    assert_eq!(
        resolver(vec![unreachable(), failing])
            .node_id("alice")
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn fail_open_downgrades_directory_outages() {
    let resolver = resolver_with(WorkspaceAffinityErrorPolicy::FailOpen, vec![unreachable()]);
    let error = resolver.node_id("alice").await.unwrap_err();
    assert!(resolver.may_fall_back(&error));
}

#[tokio::test]
async fn fail_open_downgrades_every_ownership_failure() {
    let unregistered = resolver_with(
        WorkspaceAffinityErrorPolicy::FailOpen,
        vec![serve(response("404 Not Found", r#"{"error":"missing"}"#))],
    );
    let error = unregistered.node_id("alice").await.unwrap_err();
    assert!(
        unregistered.may_fall_back(&error),
        "an unregistered owner must not block creation under fail_open"
    );

    let unready = resolver_with(
        WorkspaceAffinityErrorPolicy::FailOpen,
        vec![serve(response(
            "200 OK",
            r#"{"node_id":"node-a","state":"pending"}"#,
        ))],
    );
    let error = unready.node_id("alice").await.unwrap_err();
    assert!(unready.may_fall_back(&error));

    let malformed = resolver_with(
        WorkspaceAffinityErrorPolicy::FailOpen,
        vec![serve(response("200 OK", "not-json"))],
    );
    let error = malformed.node_id("alice").await.unwrap_err();
    assert!(malformed.may_fall_back(&error));
}

#[tokio::test]
async fn fail_closed_never_downgrades() {
    let resolver = resolver(vec![unreachable()]);
    let error = resolver.node_id("alice").await.unwrap_err();
    assert!(!resolver.may_fall_back(&error));
}

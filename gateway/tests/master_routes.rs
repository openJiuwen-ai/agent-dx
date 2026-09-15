use adx_protocol::control::{PublishedRoute, RouteFrame};
use data_plane_gateway::edge::{master_routes::RouteConsumer, RouteStore};
use std::sync::Arc;
fn route(id: &str) -> PublishedRoute {
    PublishedRoute {
        instance_id: id.into(),
        tenant_id: "t".into(),
        runtime_id: format!("{id}-1"),
        runtime_ip: "10.0.0.2".into(),
        node_proxy_address: "127.0.0.1:9000".into(),
        generation: 1,
        instance_revision: 2,
    }
}
#[test]
fn full_delta_gap_and_restart_fencing() {
    let store = Arc::new(RouteStore::new());
    let mut c = RouteConsumer::new(store.clone());
    assert!(!store.ready());
    c.apply(RouteFrame {
        epoch: 1,
        revision: 5,
        reset: true,
        upserts: vec![route("a"), route("b")],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(store.len(), 2);
    c.apply(RouteFrame {
        epoch: 1,
        base_revision: 5,
        revision: 6,
        deleted: vec!["a".into()],
        ..Default::default()
    })
    .unwrap();
    assert!(store.get("a").is_none());
    assert!(c
        .apply(RouteFrame {
            epoch: 1,
            base_revision: 4,
            revision: 7,
            ..Default::default()
        })
        .is_err());
    assert!(store.get("b").is_some());
    c.apply(RouteFrame {
        epoch: 2,
        revision: 8,
        reset: true,
        ..Default::default()
    })
    .unwrap();
    assert!(store.is_empty());
    assert!(store.ready());
    assert!(c
        .apply(RouteFrame {
            epoch: 1,
            revision: 9,
            reset: true,
            upserts: vec![route("a")],
            ..Default::default()
        })
        .is_err());
    assert!(store.is_empty());
}

#[tokio::test]
async fn stream_routes_require_auth_and_missing_cache_never_point_gets() {
    use data_plane_gateway::{
        common::route::DataPlaneAuthMode,
        edge::{AccessKind, EdgeRouteResolver, ResolveError},
    };
    let store = Arc::new(RouteStore::new());
    let mut c = RouteConsumer::new(store.clone());
    c.apply(RouteFrame {
        epoch: 1,
        revision: 1,
        reset: true,
        upserts: vec![route("a")],
        ..Default::default()
    })
    .unwrap();
    let resolver = EdgeRouteResolver::new(store).stream_only();
    assert_eq!(
        resolver
            .resolve("a", 8080, AccessKind::PortForwarding, "r")
            .await
            .unwrap()
            .port_forward_auth_mode,
        DataPlaneAuthMode::Token
    );
    assert!(matches!(
        resolver
            .resolve("unknown", 8080, AccessKind::Direct, "r")
            .await,
        Err(ResolveError::Unavailable(_))
    ));
    assert_eq!(resolver.point_get_total(), 0);
}
#[test]
fn malformed_delta_is_atomic_and_does_not_replace_good_routes() {
    let store = Arc::new(RouteStore::new());
    let mut c = RouteConsumer::new(store.clone());
    c.apply(RouteFrame {
        epoch: 1,
        revision: 1,
        reset: true,
        upserts: vec![route("a")],
        ..Default::default()
    })
    .unwrap();
    let mut invalid = route("bad");
    invalid.runtime_ip = "invalid".into();
    assert!(c
        .apply(RouteFrame {
            epoch: 1,
            revision: 2,
            base_revision: 1,
            deleted: vec!["a".into()],
            upserts: vec![invalid],
            ..Default::default()
        })
        .is_err());
    assert!(store.get("a").is_some());
    assert!(store.get("bad").is_none());
}

#[test]
fn full_snapshot_same_cursor_must_be_identical() {
    let store = Arc::new(RouteStore::new());
    let mut consumer = RouteConsumer::new(store.clone());
    let frame = RouteFrame {
        epoch: 1,
        revision: 4,
        reset: true,
        upserts: vec![route("a")],
        ..Default::default()
    };
    consumer.apply(frame.clone()).unwrap();
    consumer.apply(frame.clone()).unwrap();
    let mut conflict = frame;
    conflict.upserts.clear();
    assert!(consumer.apply(conflict).is_err());
    assert!(store.get("a").is_some());
}

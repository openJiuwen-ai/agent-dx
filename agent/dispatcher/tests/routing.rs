use adx_agent_core::Scope;
use adx_agent_store::DispatcherMember;
use adx_dispatcher::routing::{HashRing, RoundRobin};
fn member(id: &str) -> DispatcherMember {
    DispatcherMember {
        node_id: id.into(),
        boot_id: uuid::Uuid::new_v4().to_string(),
        address: format!("https://{id}:8080"),
    }
}
fn scope(id: usize) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "agent".into(),
        version: "1".into(),
        session_id: id.to_string(),
    }
}
#[test]
fn consistent_hash_is_order_independent_and_moves_only_to_added_member() {
    let a = member("a");
    let b = member("b");
    let c = member("c");
    let before = HashRing::new(vec![a.clone(), b.clone()]);
    let reversed = HashRing::new(vec![b.clone(), a.clone()]);
    let after = HashRing::new(vec![a, b, c]);
    let mut changed = 0;
    for n in 0..1000 {
        let s = scope(n);
        let old = before.preferred(&s).unwrap();
        assert_eq!(old, reversed.preferred(&s).unwrap());
        let new = after.preferred(&s).unwrap();
        if new.node_id != old.node_id {
            assert_eq!(new.node_id, "c");
            changed += 1;
        }
    }
    assert!(changed > 0 && changed < 1000);
}
#[test]
fn controlled_initial_cursor_and_boot_replacement_do_not_require_shared_state() {
    let ids = vec!["a".into(), "b".into(), "c".into()];
    let mut a = RoundRobin::with_seed(1);
    let mut b = RoundRobin::with_seed(2);
    assert_eq!(a.choose(&ids), Some("b"));
    assert_eq!(b.choose(&ids), Some("c"));
    assert_eq!(a.choose(&ids), Some("c"));
    assert_eq!(a.choose(&ids), Some("a"));
    let old = member("stable-node");
    let mut new = old.clone();
    new.boot_id = uuid::Uuid::new_v4().to_string();
    let other = member("other");
    let before = HashRing::new(vec![old, other.clone()]);
    let after = HashRing::new(vec![new, other]);
    for n in 0..1000 {
        assert_eq!(
            before.preferred(&scope(n)).unwrap().node_id,
            after.preferred(&scope(n)).unwrap().node_id
        );
    }
}

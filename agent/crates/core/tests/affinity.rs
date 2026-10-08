use adx_agent_core::{
    discovery::{ranked_endpoints, ActivatorEndpoint},
    Scope,
};
fn members() -> Vec<ActivatorEndpoint> {
    ["a", "b", "c"]
        .into_iter()
        .map(|id| ActivatorEndpoint {
            id: id.into(),
            url: format!("http://{id}:8091"),
        })
        .collect()
}
#[test]
fn affinity_is_order_independent_and_membership_changes_only_move_affected_envs() {
    let old = members();
    let mut reversed = old.clone();
    reversed.reverse();
    let mut expanded = old.clone();
    expanded.push(ActivatorEndpoint {
        id: "d".into(),
        url: "http://d:8091".into(),
    });
    let mut moved = 0;
    for i in 0..1000 {
        let scope = Scope {
            tenant: "tenant".into(),
            template: "app".into(),
            version: "1".into(),
            environment_id: i.to_string(),
        };
        let ranked = ranked_endpoints(&scope, &old);
        assert_eq!(ranked, ranked_endpoints(&scope, &reversed));
        let added = ranked_endpoints(&scope, &expanded);
        if added[0] != ranked[0] {
            assert_eq!(added[0].id, "d");
            moved += 1;
        }
        let remaining: Vec<_> = old
            .iter()
            .filter(|e| e.id != ranked[0].id)
            .cloned()
            .collect();
        assert_eq!(ranked_endpoints(&scope, &remaining)[0], ranked[1]);
    }
    assert!(
        (150..350).contains(&moved),
        "unexpected distribution: {moved}"
    );
}

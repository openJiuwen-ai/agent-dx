use adx_core::execution::*;
use adxlet::{jiuwenbox::policy::network_policy, sandboxd::Config};
use serde_json::json;

fn policy() -> NetworkPolicy {
    NetworkPolicy {
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Allow,
            mode: TrafficMode::Stateful,
            rules: vec![NetworkRule {
                action: NetworkAction::Allow,
                direction: NetworkDirection::Ingress,
                protocol: NetworkProtocol::Tcp,
                peer: NetworkPeer::default(),
                environment_port: 8080,
                environment_port_range: None,
                priority: 100,
            }],
        }),
        dns: None,
    }
}

#[test]
fn supervisor_native_acl_keeps_declared_and_control_ports_reachable() {
    let mapped = network_policy(
        "supervisor",
        Some(&policy()),
        &[8081],
        None,
        &Config::default(),
    )
    .unwrap();
    assert_eq!(mapped["ingress"]["default"], "deny");
    assert_eq!(
        mapped["ingress"]["allowed_ports"],
        json!([8080, 8081, 50090])
    );
    assert_eq!(mapped["egress"]["default"], "allow");
}

#[test]
fn conch_maps_egress_ip_allowlist_without_unsupported_port_fields() {
    let mut p = policy();
    let t = p.traffic.as_mut().unwrap();
    t.ingress_default_action = NetworkAction::Allow;
    t.egress_default_action = NetworkAction::Deny;
    t.rules[0].direction = NetworkDirection::Egress;
    t.rules[0].protocol = NetworkProtocol::Any;
    t.rules[0].environment_port = 0;
    t.rules[0].peer.cidr = "10.0.0.0/8".into();
    let mapped = network_policy("conch", Some(&p), &[], None, &Config::default()).unwrap();
    assert_eq!(mapped["egress"]["default"], "deny");
    assert_eq!(mapped["egress"]["allowed_ips"], json!(["10.0.0.0/8"]));
    assert!(mapped["ingress"].get("allowed_ports").is_none());
}

#[test]
fn refuses_policies_that_native_backends_cannot_preserve() {
    assert!(network_policy("conch", Some(&policy()), &[], None, &Config::default()).is_err());
    let mut p = policy();
    p.traffic.as_mut().unwrap().mode = TrafficMode::Stateless;
    assert!(network_policy("supervisor", Some(&p), &[], None, &Config::default()).is_err());
    p = policy();
    p.traffic.as_mut().unwrap().rules[0].environment_port_range = Some(PortRange {
        first: 80,
        last: 90,
    });
    assert!(network_policy("supervisor", Some(&p), &[], None, &Config::default()).is_err());
    p = policy();
    p.traffic.as_mut().unwrap().egress_default_action = NetworkAction::Deny;
    assert!(network_policy("supervisor", Some(&p), &[], None, &Config::default()).is_err());
}

#[test]
fn clear_policy_explicitly_clears_every_native_list() {
    for class in ["supervisor", "conch"] {
        let mapped = network_policy(class, None, &[], None, &Config::default()).unwrap();
        for direction in ["ingress", "egress"] {
            assert_eq!(mapped[direction]["default"], "allow");
            assert_eq!(mapped[direction]["allowed_ips"], json!([]));
            assert_eq!(mapped[direction]["blocked_ips"], json!([]));
        }
    }
}

#[test]
fn supervisor_native_acl_trusts_only_the_derived_tunnel_listener() {
    for http_port in [8766, 18766] {
        let mut config = Config::default();
        config
            .env
            .insert("EXECD_TUNNEL_WS_PORT".into(), "12345".into());
        let mapped =
            network_policy("supervisor", Some(&policy()), &[], Some(http_port), &config).unwrap();
        assert_eq!(
            mapped["ingress"]["allowed_ports"],
            json!([8080, http_port - 1, 50090])
        );
        assert_eq!(mapped["egress"]["allowed_ports"], json!([]));
        assert_eq!(mapped["egress"]["blocked_ports"], json!([]));
    }
}

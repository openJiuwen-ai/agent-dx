//! Optional node-owned permission ceiling for sandboxd network policies.
use std::collections::BTreeMap;

use adx_core::{
    execution::{
        DnsPolicy, NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol,
        NetworkRule, TrafficMode, TrafficPolicy,
    },
    Error, Result,
};

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkBaseline {
    /// Deny-by-default policy describing the maximum permitted traffic.
    pub policy: NetworkPolicy,
    /// Resolve logical traffic peers using the current node's configuration.
    /// Aliases change only peer addresses; the permission ceiling still applies.
    #[serde(default)]
    pub peer_aliases: BTreeMap<String, ipnet::Ipv4Net>,
}

impl NetworkBaseline {
    pub fn validate(&self) -> Result<()> {
        self.policy.validate()?;
        let traffic = self
            .policy
            .traffic
            .as_ref()
            .ok_or_else(|| invalid("network baseline requires traffic policy"))?;
        if traffic.ingress_default_action != NetworkAction::Deny
            || traffic.egress_default_action != NetworkAction::Deny
            || traffic.mode != TrafficMode::Stateful
        {
            return Err(invalid(
                "network baseline requires stateful, deny-by-default traffic",
            ));
        }
        for rule in &traffic.rules {
            if !valid_peer(&rule.peer) {
                return Err(invalid("network baseline requires IPv4 or domain peers"));
            }
        }
        Ok(())
    }

    /// Intersect an explicit request with the node-owned permission ceiling.
    /// Omitting a policy grants no user traffic; platform channels are added later.
    pub fn enforce(&self, requested: Option<&NetworkPolicy>) -> Result<NetworkPolicy> {
        self.validate()?;
        let baseline = self
            .policy
            .traffic
            .as_ref()
            .ok_or_else(|| invalid("missing baseline traffic"))?;
        let mut effective = requested.cloned().unwrap_or_default();
        effective.validate()?;
        let traffic = effective.traffic.get_or_insert_with(|| TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Deny,
            rules: Vec::new(),
            mode: TrafficMode::Stateful,
        });
        if traffic.ingress_default_action != NetworkAction::Deny
            || traffic.egress_default_action != NetworkAction::Deny
            || traffic.mode != TrafficMode::Stateful
        {
            return Err(invalid(
                "requested traffic policy weakens the node network baseline",
            ));
        }
        for rule in &mut traffic.rules {
            if let Some(cidr) = self.peer_aliases.get(&rule.peer.domain) {
                rule.peer.domain.clear();
                rule.peer.cidr = cidr.to_string();
            }
            if !valid_peer(&rule.peer) {
                return Err(invalid("network baseline requires IPv4 or domain peers"));
            }
            if rule.action == NetworkAction::Allow
                && !baseline.rules.iter().any(|ceiling| {
                    ceiling.action == NetworkAction::Allow
                        && rule.priority <= ceiling.priority
                        && rule_subset(rule, ceiling)
                })
            {
                return Err(invalid(
                    "requested allow rule exceeds the node network baseline",
                ));
            }
        }
        for rule in baseline
            .rules
            .iter()
            .filter(|rule| rule.action == NetworkAction::Deny)
        {
            if !traffic.rules.contains(rule) {
                traffic.rules.push(rule.clone());
            }
        }
        let dns = effective.dns.get_or_insert_with(|| DnsPolicy {
            default_action: NetworkAction::Deny,
            rules: Vec::new(),
        });
        if dns.default_action != NetworkAction::Deny {
            return Err(invalid(
                "requested DNS policy weakens the node network baseline",
            ));
        }
        for rule in &dns.rules {
            if rule.action == NetworkAction::Allow
                && !self.policy.dns.as_ref().is_some_and(|baseline| {
                    baseline.default_action == NetworkAction::Allow
                        || baseline.rules.iter().any(|ceiling| {
                            ceiling.action == NetworkAction::Allow
                                && domain_subset(&rule.pattern, &ceiling.pattern)
                        })
                })
            {
                return Err(invalid(
                    "requested DNS rule exceeds the node network baseline",
                ));
            }
        }
        if let Some(baseline) = &self.policy.dns {
            for rule in baseline
                .rules
                .iter()
                .filter(|rule| rule.action == NetworkAction::Deny)
            {
                if !dns.rules.contains(rule) {
                    dns.rules.push(rule.clone());
                }
            }
        }
        Ok(effective)
    }
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}

fn valid_peer(peer: &NetworkPeer) -> bool {
    let selectors = usize::from(!peer.address.is_empty())
        + usize::from(!peer.cidr.is_empty())
        + usize::from(!peer.domain.is_empty());
    selectors <= 1
        && (if !peer.address.is_empty() {
            peer.address.parse::<std::net::Ipv4Addr>().is_ok()
        } else if !peer.cidr.is_empty() {
            peer.cidr.parse::<ipnet::Ipv4Net>().is_ok()
        } else {
            true
        })
}

fn domain_subset(requested: &str, ceiling: &str) -> bool {
    let requested = requested.trim_end_matches('.').to_ascii_lowercase();
    let ceiling = ceiling.trim_end_matches('.').to_ascii_lowercase();
    requested == ceiling
        || ceiling.strip_prefix("*.").is_some_and(|suffix| {
            requested
                .strip_suffix(suffix)
                .is_some_and(|prefix| prefix.ends_with('.') && prefix.len() > 1)
        })
}

fn peer_subset(requested: &NetworkPeer, ceiling: &NetworkPeer) -> bool {
    if !ceiling.domain.is_empty() {
        return !requested.domain.is_empty() && domain_subset(&requested.domain, &ceiling.domain);
    }
    let network = |peer: &NetworkPeer| -> Option<ipnet::Ipv4Net> {
        if !peer.cidr.is_empty() {
            peer.cidr.parse().ok()
        } else if !peer.address.is_empty() {
            peer.address
                .parse::<std::net::Ipv4Addr>()
                .ok()
                .and_then(|ip| ipnet::Ipv4Net::new(ip, 32).ok())
        } else {
            "0.0.0.0/0".parse().ok()
        }
    };
    let Some(allowed) = network(ceiling) else {
        return false;
    };
    if !requested.domain.is_empty() {
        return allowed.prefix_len() == 0;
    }
    network(requested).is_some_and(|requested| {
        requested.prefix_len() >= allowed.prefix_len() && allowed.contains(&requested.network())
    })
}

fn ports_subset(
    value: u32,
    range: Option<&adx_core::execution::PortRange>,
    ceiling: u32,
    bounds: Option<&adx_core::execution::PortRange>,
) -> bool {
    let pair = |value, range: Option<&adx_core::execution::PortRange>| {
        range.map_or_else(
            || {
                if value == 0 {
                    (1, 65535)
                } else {
                    (value, value)
                }
            },
            |range| (range.first, range.last),
        )
    };
    let (first, last) = pair(value, range);
    let (allowed_first, allowed_last) = pair(ceiling, bounds);
    first >= allowed_first && last <= allowed_last
}

fn rule_subset(requested: &NetworkRule, ceiling: &NetworkRule) -> bool {
    (ceiling.direction == NetworkDirection::Both || requested.direction == ceiling.direction)
        && (ceiling.protocol == NetworkProtocol::Any || requested.protocol == ceiling.protocol)
        && peer_subset(&requested.peer, &ceiling.peer)
        && ports_subset(
            requested.peer.port,
            requested.peer.port_range.as_ref(),
            ceiling.peer.port,
            ceiling.peer.port_range.as_ref(),
        )
        && ports_subset(
            requested.environment_port,
            requested.environment_port_range.as_ref(),
            ceiling.environment_port,
            ceiling.environment_port_range.as_ref(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::execution::{
        NetworkAction, NetworkDirection, NetworkPeer, NetworkProtocol, NetworkRule, TrafficMode,
        TrafficPolicy,
    };

    #[test]
    fn configured_peer_alias_uses_the_target_node_and_keeps_port_ceiling() {
        for cidr in ["10.88.4.1/32", "10.99.0.1/32"] {
            let ceiling = policy(vec![rule(cidr, 8080, 300)]);
            let baseline: NetworkBaseline = serde_json::from_value(serde_json::json!({
                "policy": ceiling,
                "peer_aliases": {"node-ingress.example.invalid": cidr}
            }))
            .unwrap();
            let mut requested = rule("", 8080, 300);
            requested.peer.domain = "node-ingress.example.invalid".into();
            let effective = baseline
                .enforce(Some(&policy(vec![requested.clone()])))
                .unwrap();
            let peer = &effective.traffic.unwrap().rules[0].peer;
            assert_eq!(peer.cidr, cidr);
            assert!(peer.domain.is_empty());
            requested.peer.port = 9090;
            assert!(baseline.enforce(Some(&policy(vec![requested]))).is_err());
        }
    }

    fn rule(cidr: &str, port: u32, priority: u32) -> NetworkRule {
        NetworkRule {
            action: NetworkAction::Allow,
            direction: NetworkDirection::Egress,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer {
                cidr: cidr.into(),
                port,
                ..Default::default()
            },
            environment_port: 0,
            environment_port_range: None,
            priority,
        }
    }

    fn policy(rules: Vec<NetworkRule>) -> NetworkPolicy {
        NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                rules,
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        }
    }

    #[test]
    fn dns_ceiling_does_not_grant_queries_until_explicitly_requested() {
        use adx_core::execution::{DnsPolicy, DnsRule};
        let mut ceiling = policy(Vec::new());
        ceiling.dns = Some(DnsPolicy {
            default_action: NetworkAction::Allow,
            rules: Vec::new(),
        });
        let baseline = NetworkBaseline {
            policy: ceiling,
            peer_aliases: BTreeMap::new(),
        };
        let default = baseline.enforce(None).unwrap();
        assert_eq!(default.dns.unwrap().default_action, NetworkAction::Deny);
        let mut requested = policy(Vec::new());
        requested.dns = Some(DnsPolicy {
            default_action: NetworkAction::Deny,
            rules: vec![DnsRule {
                action: NetworkAction::Allow,
                pattern: "api.example".into(),
            }],
        });
        assert!(baseline.enforce(Some(&requested)).is_ok());
        requested.dns.as_mut().unwrap().default_action = NetworkAction::Allow;
        assert!(baseline.enforce(Some(&requested)).is_err());
    }

    #[test]
    fn policy_removal_stays_denied_and_cannot_clear_node_protection() {
        let mut denied = rule("10.0.0.0/8", 0, 200);
        denied.action = NetworkAction::Deny;
        let baseline = NetworkBaseline {
            peer_aliases: BTreeMap::new(),
            policy: policy(vec![rule("0.0.0.0/0", 443, 100), denied.clone()]),
        };
        let effective = baseline.enforce(None).unwrap();
        let traffic = effective.traffic.unwrap();
        assert_eq!(traffic.egress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.ingress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.rules, vec![denied.clone()]);
        let effective = baseline
            .enforce(Some(&policy(vec![rule("203.0.113.9/32", 443, 100)])))
            .unwrap();
        assert!(effective.traffic.unwrap().rules.contains(&denied));
        assert!(baseline
            .enforce(Some(&policy(vec![rule("0.0.0.0/0", 443, 300)])))
            .is_err());
    }

    #[test]
    fn requested_allow_must_fit_baseline_destination_port_and_direction() {
        let baseline = NetworkBaseline {
            peer_aliases: BTreeMap::new(),
            policy: policy(vec![rule("10.88.4.1/32", 8080, 300)]),
        };
        assert!(baseline
            .enforce(Some(&policy(vec![rule("10.88.4.1/32", 8080, 300)])))
            .is_ok());
        for requested in [
            rule("10.88.4.0/24", 8080, 300),
            rule("10.88.4.1/32", 6379, 300),
            rule("10.88.4.1/32", 8080, 301),
        ] {
            assert!(baseline.enforce(Some(&policy(vec![requested]))).is_err());
        }
        let mut requested = rule("10.88.4.1/32", 8080, 300);
        requested.direction = NetworkDirection::Both;
        assert!(baseline.enforce(Some(&policy(vec![requested]))).is_err());
        let mut request = policy(Vec::new());
        request.traffic.as_mut().unwrap().egress_default_action = NetworkAction::Allow;
        assert!(baseline.enforce(Some(&request)).is_err());
    }
}

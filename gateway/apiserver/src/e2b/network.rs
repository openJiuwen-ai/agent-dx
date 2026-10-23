//! E2B's restricted egress policy projection onto the native network model.
//!
//! The E2B update operation replaces its egress configuration, while an ADX
//! network policy also owns ingress and DNS.  Keep that distinction here so a
//! network update cannot silently discard native policy outside E2B's scope.
use adx_core::execution::{
    NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol, NetworkRule,
    TrafficMode, TrafficPolicy,
};
use serde_json::{json, Value};
use std::net::{IpAddr, Ipv4Addr};

const ALLOW_PRIORITY: u32 = 200;
const DENY_PRIORITY: u32 = 100;

/// Typed subset of E2B's network request that ADX currently supports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EgressRequest {
    pub allow_out: Option<Vec<String>>,
    pub deny_out: Option<Vec<String>>,
    pub allow_internet_access: Option<bool>,
}

/// Fields that E2B exposes but which have no compatible ADX implementation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UnsupportedFeatures {
    pub egress_proxy: bool,
    pub rules: bool,
    pub mask_request_host: bool,
    pub https_ports: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConversionError {
    code: &'static str,
    message: &'static str,
}

impl ConversionError {
    pub(crate) fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) fn message(&self) -> &'static str {
        self.message
    }
}

fn unsupported(message: &'static str) -> ConversionError {
    ConversionError {
        code: "capability_not_supported",
        message,
    }
}

/// Replace only the E2B-managed egress part of `baseline`.
///
/// `None` is the create baseline: it receives an allow-by-default ingress
/// policy and stateful traffic mode.  Existing ingress (including `Both`
/// rules), DNS, and traffic mode are retained for updates.
pub(crate) fn convert_egress(
    request: &EgressRequest,
    unsupported_features: UnsupportedFeatures,
    baseline: Option<&NetworkPolicy>,
) -> Result<NetworkPolicy, ConversionError> {
    reject_unsupported(unsupported_features)?;
    let mut policy = baseline.cloned().unwrap_or_default();
    let existing = policy.traffic.take();
    let (ingress_default_action, mode, mut rules) = match existing {
        Some(traffic) => (
            traffic.ingress_default_action,
            traffic.mode,
            traffic
                .rules
                .into_iter()
                .filter_map(project_ingress)
                .collect(),
        ),
        None => (NetworkAction::Allow, TrafficMode::Stateful, Vec::new()),
    };

    rules.extend(rules_for(
        &request.allow_out,
        NetworkAction::Allow,
        ALLOW_PRIORITY,
    )?);
    rules.extend(rules_for(
        &request.deny_out,
        NetworkAction::Deny,
        DENY_PRIORITY,
    )?);
    policy.traffic = Some(TrafficPolicy {
        ingress_default_action,
        egress_default_action: if request.allow_internet_access == Some(false) {
            NetworkAction::Deny
        } else {
            NetworkAction::Allow
        },
        rules,
        mode,
    });
    Ok(policy)
}

fn reject_unsupported(features: UnsupportedFeatures) -> Result<(), ConversionError> {
    if features.egress_proxy {
        return Err(unsupported("egressProxy is not supported"));
    }
    if features.rules {
        return Err(unsupported("network rules are not supported"));
    }
    if features.mask_request_host {
        return Err(unsupported("maskRequestHost is not supported"));
    }
    if features.https_ports {
        return Err(unsupported("httpsPorts is not supported"));
    }
    Ok(())
}

fn project_ingress(mut rule: NetworkRule) -> Option<NetworkRule> {
    match rule.direction {
        NetworkDirection::Ingress => Some(rule),
        NetworkDirection::Both => {
            rule.direction = NetworkDirection::Ingress;
            Some(rule)
        }
        NetworkDirection::Egress => None,
    }
}

fn rules_for(
    values: &Option<Vec<String>>,
    action: NetworkAction,
    priority: u32,
) -> Result<Vec<NetworkRule>, ConversionError> {
    values
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|value| {
            Ok(NetworkRule {
                action,
                direction: NetworkDirection::Egress,
                protocol: NetworkProtocol::Any,
                peer: NetworkPeer {
                    cidr: ipv4_cidr(value)?,
                    ..NetworkPeer::default()
                },
                environment_port: 0,
                environment_port_range: None,
                priority,
            })
        })
        .collect()
}

fn ipv4_cidr(value: &str) -> Result<String, ConversionError> {
    let value = value.trim();
    if let Ok(IpAddr::V4(address)) = value.parse::<IpAddr>() {
        return Ok(format!("{address}/32"));
    }
    if let Ok(IpAddr::V6(_)) = value.parse::<IpAddr>() {
        return Err(unsupported("IPv6 egress rules are not supported"));
    }
    let Some((address, prefix)) = value.split_once('/') else {
        return Err(unsupported(
            "domain and wildcard egress rules are not supported",
        ));
    };
    if address.parse::<Ipv4Addr>().is_err() {
        if address.parse::<IpAddr>().is_ok() {
            return Err(unsupported("IPv6 egress rules are not supported"));
        }
        return Err(unsupported(
            "domain and wildcard egress rules are not supported",
        ));
    }
    if prefix
        .parse::<u8>()
        .ok()
        .filter(|prefix| *prefix <= 32)
        .is_none()
    {
        return Err(ConversionError {
            code: "invalid_request",
            message: "invalid IPv4 CIDR",
        });
    }
    Ok(value.to_owned())
}

/// Encode a native policy with the gateway's schemaVersion 2 JSON contract.
pub(crate) fn native_payload(policy: Option<&NetworkPolicy>) -> Value {
    let Some(policy) = policy else {
        return Value::Null;
    };
    let traffic = policy.traffic.as_ref().map(|traffic| {
        json!({
            "ingressDefaultAction": action(traffic.ingress_default_action),
            "egressDefaultAction": action(traffic.egress_default_action),
            "rules": traffic.rules.iter().map(rule_payload).collect::<Vec<_>>(),
            "mode": mode(traffic.mode),
        })
    });
    let dns = policy.dns.as_ref().map(|dns| {
        json!({
            "defaultAction": action(dns.default_action),
            "rules": dns.rules.iter().map(|rule| json!({
                "action": action(rule.action), "pattern": rule.pattern,
            })).collect::<Vec<_>>(),
        })
    });
    json!({"schemaVersion": 2, "traffic": traffic, "dns": dns})
}

fn rule_payload(rule: &NetworkRule) -> Value {
    json!({
        "action": action(rule.action),
        "direction": direction(rule.direction),
        "protocol": protocol(rule.protocol),
        "peer": {
            "address": rule.peer.address,
            "port": rule.peer.port,
            "cidr": rule.peer.cidr,
            "domain": rule.peer.domain,
            "portRange": rule.peer.port_range.as_ref().map(|range| json!({"first": range.first, "last": range.last})),
        },
        "environmentPort": rule.environment_port,
        "environmentPortRange": rule.environment_port_range.as_ref().map(|range| json!({"first": range.first, "last": range.last})),
        "priority": rule.priority,
    })
}

fn action(value: NetworkAction) -> &'static str {
    match value {
        NetworkAction::Allow => "allow",
        NetworkAction::Deny => "deny",
    }
}

fn direction(value: NetworkDirection) -> &'static str {
    match value {
        NetworkDirection::Ingress => "ingress",
        NetworkDirection::Egress => "egress",
        NetworkDirection::Both => "both",
    }
}

fn protocol(value: NetworkProtocol) -> &'static str {
    match value {
        NetworkProtocol::Any => "any",
        NetworkProtocol::Tcp => "tcp",
        NetworkProtocol::Udp => "udp",
        NetworkProtocol::Icmp => "icmp",
    }
}

fn mode(value: TrafficMode) -> &'static str {
    match value {
        TrafficMode::Stateless => "stateless",
        TrafficMode::Stateful => "stateful",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract;

    #[test]
    fn e2b_egress_rules_preserve_native_ingress_dns_and_mode() {
        let baseline = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                mode: TrafficMode::Stateless,
                rules: vec![NetworkRule {
                    action: NetworkAction::Allow,
                    direction: NetworkDirection::Both,
                    protocol: NetworkProtocol::Tcp,
                    peer: NetworkPeer {
                        cidr: "10.0.0.0/8".into(),
                        ..NetworkPeer::default()
                    },
                    environment_port: 0,
                    environment_port_range: None,
                    priority: 7,
                }],
            }),
            dns: Some(adx_core::execution::DnsPolicy {
                default_action: NetworkAction::Allow,
                rules: Vec::new(),
            }),
        };
        let policy = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["1.2.3.4".into()]),
                deny_out: Some(vec!["10.0.0.0/8".into()]),
                allow_internet_access: Some(false),
            },
            UnsupportedFeatures::default(),
            Some(&baseline),
        )
        .unwrap();
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(traffic.ingress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.egress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.mode, TrafficMode::Stateless);
        assert_eq!(traffic.rules[0].direction, NetworkDirection::Ingress);
        assert_eq!(traffic.rules[1].priority, ALLOW_PRIORITY);
        assert_eq!(traffic.rules[2].priority, DENY_PRIORITY);
        assert_eq!(policy.dns, baseline.dns);
    }

    #[test]
    fn empty_e2b_network_clears_only_egress_and_defaults_allow() {
        let policy = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        let traffic = policy.traffic.unwrap();
        assert_eq!(traffic.ingress_default_action, NetworkAction::Allow);
        assert_eq!(traffic.egress_default_action, NetworkAction::Allow);
        assert!(traffic.rules.is_empty());
    }

    #[test]
    fn unsupported_egress_inputs_return_capability_error() {
        let error = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["*.example.test".into()]),
                ..Default::default()
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code(), "capability_not_supported");
        let error = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures {
                https_ports: true,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.code(), "capability_not_supported");
    }

    #[test]
    fn native_payload_round_trips_through_gateway_contract() {
        let policy = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["1.2.3.4/32".into()]),
                ..Default::default()
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            contract::network_policy(native_payload(Some(&policy))).unwrap(),
            Some(policy)
        );
    }
}

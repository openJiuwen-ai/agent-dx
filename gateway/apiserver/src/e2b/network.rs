//! E2B's restricted egress policy projection onto the native network model.
//!
//! The E2B update operation replaces its egress configuration, while an ADX
//! network policy also owns ingress and DNS.  Keep that distinction here so a
//! network update cannot silently discard native policy outside E2B's scope.
use adx_core::execution::{
    NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol, NetworkRule,
    TrafficMode, TrafficPolicy,
};
use std::net::{IpAddr, Ipv4Addr};

const ALLOW_PRIORITY: u32 = 200;
const DENY_PRIORITY: u32 = 100;
use adx_core::envd::{gateway_ingress_rules, is_gateway_ingress_rule, ENVD_PORT};
#[cfg(test)]
use adx_core::envd::{ENVD_GUARD_ALLOW_PRIORITY, ENVD_GUARD_DENY_PRIORITY};

/// Enable the common service and persist its immutable network/exposure contract.
/// Preparation and instance creation share this exact construction.
pub(crate) fn configure_envd_service(
    spec: &mut adx_protocol::control::EnvironmentSpec,
    service: adx_protocol::control::EnvdServiceConfig,
) -> Result<(), tonic::Status> {
    let config: adx_core::envd::EnvdServiceConfig = service.clone().into();
    config
        .validate()
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut execution = spec.execution.clone().unwrap_or_default();
    if !execution
        .private_service_ports
        .contains(&u32::from(ENVD_PORT))
    {
        execution.private_service_ports.push(u32::from(ENVD_PORT));
    }
    if service.gateway_auth {
        let baseline = execution
            .network
            .take()
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| tonic::Status::data_loss("invalid persisted network policy"))?
            .unwrap_or_default();
        let guarded = with_gateway_envd_ingress_guard(baseline, &service.ingress_sources)?;
        let protected: Vec<adx_protocol::control::NetworkRule> = guarded
            .traffic
            .as_ref()
            .into_iter()
            .flat_map(|traffic| &traffic.rules)
            .filter(|rule| rule.direction != NetworkDirection::Egress)
            .cloned()
            .map(Into::into)
            .collect();
        if !execution.protected_ingress.is_empty() && execution.protected_ingress != protected {
            return Err(tonic::Status::failed_precondition(
                "persisted protected ingress conflicts with envd configuration",
            ));
        }
        execution.protected_ingress = protected;
        execution.network = Some(guarded.into());
    }
    let core: adx_core::execution::ExecutionOptions = execution
        .clone()
        .try_into()
        .map_err(|error: adx_core::Error| tonic::Status::invalid_argument(error.to_string()))?;
    config
        .validate_execution(&core)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    spec.execution = Some(execution);
    spec.envd = Some(service);
    Ok(())
}

/// Adds the deployment-owned ingress policy that limits direct envd access to
/// the Relay and bridge addresses. Callers must validate the source addresses.
pub(crate) fn with_gateway_envd_ingress_guard(
    mut policy: NetworkPolicy,
    sources: &[String],
) -> Result<NetworkPolicy, tonic::Status> {
    let traffic = policy.traffic.get_or_insert(TrafficPolicy {
        ingress_default_action: NetworkAction::Allow,
        egress_default_action: NetworkAction::Allow,
        rules: Vec::new(),
        mode: TrafficMode::Stateful,
    });
    // Split bidirectional rules before ingress becomes immutable. E2B may later
    // replace egress while preserving the independently protected inbound half.
    traffic.rules = std::mem::take(&mut traffic.rules)
        .into_iter()
        .flat_map(|mut rule| {
            let egress = if rule.direction == NetworkDirection::Both {
                let mut egress = rule.clone();
                egress.direction = NetworkDirection::Egress;
                rule.direction = NetworkDirection::Ingress;
                Some(egress)
            } else {
                None
            };
            std::iter::once(rule).chain(egress)
        })
        .collect();
    traffic.rules.retain(|rule| !is_gateway_ingress_rule(rule));
    traffic.rules.extend(
        gateway_ingress_rules(sources)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?,
    );
    Ok(policy)
}

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
                peer: egress_peer(value, action)?,
                environment_port: 0,
                environment_port_range: None,
                priority,
            })
        })
        .collect()
}

fn invalid_destination() -> ConversionError {
    ConversionError {
        code: "invalid_request",
        message: "invalid egress destination",
    }
}

fn egress_peer(value: &str, action: NetworkAction) -> Result<NetworkPeer, ConversionError> {
    let value = value.trim();
    let cidr = match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => Some(format!("{address}/32")),
        Ok(IpAddr::V6(_)) => {
            return Err(unsupported("IPv6 egress rules are not supported"));
        }
        Err(_) => None,
    };
    if let Some(cidr) = cidr {
        return Ok(NetworkPeer {
            cidr,
            ..NetworkPeer::default()
        });
    }
    if let Some((address, prefix)) = value.split_once('/') {
        if matches!(address.parse::<IpAddr>(), Ok(IpAddr::V6(_))) {
            return Err(unsupported("IPv6 egress rules are not supported"));
        }
        if address.parse::<Ipv4Addr>().is_err()
            || prefix
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
        return Ok(NetworkPeer {
            cidr: value.to_owned(),
            ..NetworkPeer::default()
        });
    }
    let domain = domain_pattern(value)?;
    // The E2B contract only accepts domains in allowOut, even though native
    // traffic rules can express domain denies as well.
    if action == NetworkAction::Deny {
        return Err(unsupported("denyOut domain rules are not supported"));
    }
    Ok(NetworkPeer {
        domain,
        ..NetworkPeer::default()
    })
}

fn domain_pattern(value: &str) -> Result<String, ConversionError> {
    let (wildcard, name) = value
        .strip_prefix("*.")
        .map_or((false, value), |name| (true, name));
    let name = name.strip_suffix('.').unwrap_or(name);
    // Host parsing supplies IDNA normalization. Reject URL syntax first so
    // percent escapes, ports and paths cannot become a different destination.
    if name.is_empty()
        || name
            .chars()
            .any(|character| character.is_whitespace() || "*?/#:@\\%".contains(character))
    {
        return Err(invalid_destination());
    }
    let url::Host::Domain(domain) = url::Host::parse(name).map_err(|_| invalid_destination())?
    else {
        return Err(invalid_destination());
    };
    if domain.len() > 253
        || domain.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        })
    {
        return Err(invalid_destination());
    }
    // Preserve the native leading-wildcard matcher: it matches subdomains at
    // any depth and does not add an allow rule for the apex domain.
    Ok(if wildcard {
        format!("*.{domain}")
    } else {
        domain
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envd_service_builder_persists_guard_and_rejects_source_replacement() {
        use adx_protocol::control as pb;
        let service = pb::EnvdServiceConfig {
            version: "0.9.0".into(),
            gateway_auth: true,
            access_token: None,
            ingress_sources: vec!["10.0.0.2".into()],
        };
        let mut spec = pb::EnvironmentSpec::default();
        configure_envd_service(&mut spec, service.clone()).unwrap();
        let execution = spec.execution.as_ref().unwrap();
        assert_eq!(execution.private_service_ports, vec![49_983]);
        assert_eq!(execution.protected_ingress.len(), 2);
        let core: adx_core::execution::ExecutionOptions = execution.clone().try_into().unwrap();
        let config: adx_core::envd::EnvdServiceConfig = service.clone().into();
        config.validate_execution(&core).unwrap();
        let previous = spec.clone();
        configure_envd_service(&mut spec, service.clone()).unwrap();
        assert_eq!(spec, previous);
        let mut changed = service;
        changed.ingress_sources = vec!["10.0.0.3".into()];
        assert!(configure_envd_service(&mut spec, changed).is_err());
        assert_eq!(spec, previous);
    }

    #[test]
    fn guarded_bidirectional_rules_remain_stable_across_egress_replacement() {
        let rule = NetworkRule {
            action: NetworkAction::Allow,
            direction: NetworkDirection::Both,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer::default(),
            environment_port: 8080,
            environment_port_range: None,
            priority: 10,
        };
        let baseline = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Allow,
                egress_default_action: NetworkAction::Allow,
                rules: vec![rule],
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        };
        let guarded = with_gateway_envd_ingress_guard(baseline, &["10.0.0.2".into()]).unwrap();
        let protected: Vec<_> = guarded
            .traffic
            .as_ref()
            .unwrap()
            .rules
            .iter()
            .filter(|rule| rule.direction != NetworkDirection::Egress)
            .cloned()
            .collect();
        let changed = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures::default(),
            Some(&guarded),
        )
        .unwrap();
        let execution = adx_core::execution::ExecutionOptions {
            protected_ingress: protected,
            ..Default::default()
        };
        assert!(execution.validate_protected_ingress(Some(&changed)).is_ok());
    }

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
                allow_out: Some(vec!["*.example.test".into()]),
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
        assert_eq!(traffic.rules[1].peer.domain, "*.example.test");
        assert_eq!(traffic.rules[2].priority, DENY_PRIORITY);
        assert_eq!(policy.dns, baseline.dns);
        let cleared = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures::default(),
            Some(&policy),
        )
        .unwrap();
        assert_eq!(cleared.dns, baseline.dns);
        let cleared_traffic = cleared.traffic.unwrap();
        assert_eq!(cleared_traffic.ingress_default_action, NetworkAction::Deny);
        assert_eq!(cleared_traffic.egress_default_action, NetworkAction::Allow);
        assert_eq!(cleared_traffic.mode, TrafficMode::Stateless);
        assert_eq!(cleared_traffic.rules, vec![traffic.rules[0].clone()]);
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
    fn domain_allow_rules_reuse_native_peers_with_ip_rules() {
        let policy = convert_egress(
            &EgressRequest {
                allow_out: Some(vec![
                    "1.2.3.4".into(),
                    "192.0.2.0/24".into(),
                    " API.Example.test. ".into(),
                    "*.BÜCHER.example.".into(),
                ]),
                deny_out: Some(vec!["0.0.0.0/0".into()]),
                allow_internet_access: Some(false),
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        let traffic = policy.traffic.unwrap();
        assert_eq!(traffic.mode, TrafficMode::Stateful);
        assert_eq!(traffic.egress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.rules.len(), 5);
        for (rule, (cidr, domain)) in traffic.rules.iter().zip([
            ("1.2.3.4/32", ""),
            ("192.0.2.0/24", ""),
            ("", "api.example.test"),
            ("", "*.xn--bcher-kva.example"),
            ("0.0.0.0/0", ""),
        ]) {
            assert_eq!(rule.peer.cidr, cidr);
            assert_eq!(rule.peer.domain, domain);
            assert_eq!(rule.direction, NetworkDirection::Egress);
            assert_eq!(rule.protocol, NetworkProtocol::Any);
            assert_eq!(rule.environment_port, 0);
            assert_eq!(rule.environment_port_range, None);
        }
        for rule in &traffic.rules[..4] {
            assert_eq!(rule.action, NetworkAction::Allow);
            assert_eq!(rule.priority, ALLOW_PRIORITY);
        }
        assert_eq!(traffic.rules[4].action, NetworkAction::Deny);
        assert_eq!(traffic.rules[4].priority, DENY_PRIORITY);
    }

    #[test]
    fn deny_out_domains_remain_unsupported_by_the_e2b_contract() {
        for value in ["example.test", "*.example.test"] {
            let error = convert_egress(
                &EgressRequest {
                    deny_out: Some(vec![value.into()]),
                    ..Default::default()
                },
                UnsupportedFeatures::default(),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code(), "capability_not_supported");
            assert_eq!(error.message(), "denyOut domain rules are not supported");
        }
    }

    #[test]
    fn invalid_egress_destinations_return_invalid_request() {
        for value in [
            "",
            " ",
            "*",
            "*example.test",
            "api.*.example.test",
            "*.*.example.test",
            "?.example.test",
            ".example.test",
            "example..test",
            "-example.test",
            "example-.test",
            "example.test..",
            "https://example.test",
            "example.test/path",
            "example.test:443",
            "user@example.test",
            "example.test?query",
            "example.test#fragment",
            "example%2etest",
            "example test",
            "*.1.2.3.4",
            "999.2.3.4",
            "1.2.3.4/33",
            "1.2.3.4/no-prefix",
            "example.test/24",
        ] {
            for allow in [true, false] {
                let mut request = EgressRequest::default();
                if allow {
                    request.allow_out = Some(vec![value.into()]);
                } else {
                    request.deny_out = Some(vec![value.into()]);
                }
                let error =
                    convert_egress(&request, UnsupportedFeatures::default(), None).unwrap_err();
                assert_eq!(error.code(), "invalid_request", "{value:?}, allow={allow}");
            }
        }
        for value in [format!("{}.test", "a".repeat(64)), "a.".repeat(128)] {
            let error = convert_egress(
                &EgressRequest {
                    allow_out: Some(vec![value]),
                    ..Default::default()
                },
                UnsupportedFeatures::default(),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code(), "invalid_request");
        }
    }

    #[test]
    fn unsupported_egress_inputs_return_capability_error() {
        for value in ["2001:db8::1", "2001:db8::/64"] {
            let error = convert_egress(
                &EgressRequest {
                    allow_out: Some(vec![value.into()]),
                    ..Default::default()
                },
                UnsupportedFeatures::default(),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code(), "capability_not_supported");
        }
        for features in [
            UnsupportedFeatures {
                egress_proxy: true,
                ..Default::default()
            },
            UnsupportedFeatures {
                rules: true,
                ..Default::default()
            },
            UnsupportedFeatures {
                mask_request_host: true,
                ..Default::default()
            },
            UnsupportedFeatures {
                https_ports: true,
                ..Default::default()
            },
        ] {
            let error = convert_egress(&EgressRequest::default(), features, None).unwrap_err();
            assert_eq!(error.code(), "capability_not_supported");
        }
    }

    #[test]
    fn gateway_envd_guard_is_ingress_only_and_preserves_egress() {
        let baseline = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["*.example.test".into()]),
                ..Default::default()
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        let guarded =
            with_gateway_envd_ingress_guard(baseline.clone(), &["10.0.0.2".into()]).unwrap();
        let traffic = guarded.traffic.unwrap();
        assert_eq!(
            traffic.egress_default_action,
            baseline.traffic.unwrap().egress_default_action
        );
        assert!(traffic.rules.iter().any(|rule| {
            rule.direction == NetworkDirection::Egress && rule.peer.domain == "*.example.test"
        }));
        assert!(traffic.rules.iter().any(|rule| {
            rule.direction == NetworkDirection::Ingress
                && rule.action == NetworkAction::Deny
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
                && rule.priority == ENVD_GUARD_DENY_PRIORITY
        }));
        assert!(traffic.rules.iter().any(|rule| {
            rule.direction == NetworkDirection::Ingress
                && rule.action == NetworkAction::Allow
                && rule.peer.cidr == "10.0.0.2/32"
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
                && rule.priority == ENVD_GUARD_ALLOW_PRIORITY
        }));
    }

    #[test]
    fn gateway_envd_guard_replaces_only_prior_canonical_guard_sources() {
        let first = with_gateway_envd_ingress_guard(NetworkPolicy::default(), &["10.0.0.2".into()])
            .unwrap();
        let updated = with_gateway_envd_ingress_guard(first, &["10.0.0.3".into()]).unwrap();
        let rules = &updated.traffic.unwrap().rules;
        assert!(!rules.iter().any(|rule| rule.peer.cidr == "10.0.0.2/32"));
        assert!(rules.iter().any(|rule| {
            rule.peer.cidr == "10.0.0.3/32"
                && rule.priority == ENVD_GUARD_ALLOW_PRIORITY
                && rule.environment_port == 0
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
        }));
    }
}

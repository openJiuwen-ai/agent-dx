//! Translate only policies whose semantics the unmodified native backends preserve.
use crate::sandboxd::Config;
use adx_core::{
    execution::{NetworkAction, NetworkDirection, NetworkPolicy, NetworkProtocol, TrafficMode},
    Error, Result,
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, net::Ipv4Addr};

fn unsupported(message: &str) -> Error {
    Error::Invalid(format!("Jiuwenbox native ACL: {message}"))
}

fn address(text: &str) -> Result<()> {
    let (ip, bits) = text
        .split_once('/')
        .map_or((text, None), |(ip, bits)| (ip, Some(bits)));
    ip.parse::<Ipv4Addr>()
        .map_err(|_| unsupported("IPv4 address/CIDR required"))?;
    if bits.is_some_and(|b| b.parse::<u8>().map_or(true, |n| n > 32)) {
        return Err(unsupported("invalid IPv4 prefix"));
    }
    Ok(())
}

/// Creation and live updates share this exact translation. Unsupported combinations
/// fail before any backend mutation; no host firewall is installed by ADXlet.
pub fn network_policy(
    class: &str,
    policy: Option<&NetworkPolicy>,
    ports: &[u16],
    tunnel_http_port: Option<u16>,
    launch: &Config,
) -> Result<Value> {
    if !matches!(class, "supervisor" | "conch") {
        return Err(unsupported("unknown runtime class"));
    }
    let mut result = json!({"ingress":{"default":"allow"},"egress":{"default":"allow"}});
    for direction in ["ingress", "egress"] {
        result[direction]["allowed_ips"] = json!([]);
        result[direction]["blocked_ips"] = json!([]);
        if class == "supervisor" {
            result[direction]["allowed_ports"] = json!([]);
            result[direction]["blocked_ports"] = json!([]);
        }
    }
    let Some(policy) = policy else {
        return Ok(result);
    };
    policy.validate()?;
    if policy.dns.is_some() {
        return Err(unsupported("DNS policy is unsupported"));
    }
    let Some(traffic) = &policy.traffic else {
        return Ok(result);
    };
    if traffic.mode != TrafficMode::Stateful {
        return Err(unsupported("stateless filtering is unsupported"));
    }
    let execd_port = launch
        .env
        .get("EXECD_HTTP_PORT")
        .map(String::as_str)
        .unwrap_or("50090")
        .parse::<u16>()
        .map_err(|_| unsupported("invalid Execd port"))?;
    // The tunnel WebSocket listener is platform-owned, just like Execd. Native
    // stateful ACLs only need an ingress exception for the inbound handshake.
    let tunnel_ws_port = tunnel_http_port
        .map(|port| {
            port.checked_sub(1)
                .filter(|port| *port != 0)
                .ok_or_else(|| unsupported("invalid tunnel HTTP port"))
        })
        .transpose()?;
    let protected_ports: BTreeSet<_> = ports
        .iter()
        .copied()
        .chain(std::iter::once(execd_port))
        .chain(tunnel_ws_port)
        .collect();
    for (name, direction, default) in [
        (
            "ingress",
            NetworkDirection::Ingress,
            traffic.ingress_default_action,
        ),
        (
            "egress",
            NetworkDirection::Egress,
            traffic.egress_default_action,
        ),
    ] {
        let mut ips = BTreeSet::new();
        let mut port_set = BTreeSet::new();
        if direction == NetworkDirection::Ingress
            && class == "conch"
            && default == NetworkAction::Deny
        {
            return Err(unsupported(
                "Conch cannot express ingress control/public-port exceptions",
            ));
        }
        if direction == NetworkDirection::Egress
            && class == "supervisor"
            && default == NetworkAction::Deny
        {
            // Supervisor unconditionally allows DNS before its default drop. ADX
            // deny-by-default does not imply that exception, so do not broaden it.
            return Err(unsupported(
                "supervisor egress default-deny has implicit DNS exceptions",
            ));
        }
        for rule in traffic
            .rules
            .iter()
            .filter(|r| r.direction == direction || r.direction == NetworkDirection::Both)
        {
            if rule.action == default {
                return Err(unsupported(
                    "ordered allow/deny combinations are unsupported",
                ));
            }
            let peer = &rule.peer;
            if !peer.domain.is_empty()
                || (!peer.address.is_empty() && !peer.cidr.is_empty())
                || peer.port_range.is_some()
                || rule.environment_port_range.is_some()
                || (direction == NetworkDirection::Ingress && peer.port != 0)
                || (direction == NetworkDirection::Egress && rule.environment_port != 0)
            {
                return Err(unsupported(
                    "domain, range or source-port selectors are unsupported",
                ));
            }
            let ip = if peer.address.is_empty() {
                &peer.cidr
            } else {
                &peer.address
            };
            let port = if direction == NetworkDirection::Ingress {
                rule.environment_port
            } else {
                peer.port
            };
            if ip.is_empty() && port == 0 || !ip.is_empty() && port != 0 {
                return Err(unsupported(
                    "a rule must select only an IP/CIDR or a TCP destination port",
                ));
            }
            if direction == NetworkDirection::Ingress {
                // Native IP ACLs cannot give the platform's TCP control/public-port
                // exception precedence over IP rules. Do not widen them to all ports.
                if class == "conch" || !ip.is_empty() {
                    return Err(unsupported(
                        "ingress IP rules cannot preserve platform port exceptions",
                    ));
                }
            } else if launch
                .env
                .get("ADX_RUNTIME_CONTROL_ADDRESS")
                .is_some_and(|v| !v.is_empty())
            {
                return Err(unsupported(
                    "egress rules cannot preserve the runtime-control stream exception",
                ));
            }
            if !ip.is_empty() {
                if rule.protocol != NetworkProtocol::Any {
                    return Err(unsupported("IP rules require protocol Any"));
                }
                address(ip)?;
                ips.insert(ip.to_owned());
            } else {
                if class == "conch" || rule.protocol != NetworkProtocol::Tcp {
                    return Err(unsupported(
                        "native destination-port rules require supervisor TCP",
                    ));
                }
                let port = u16::try_from(port).map_err(|_| unsupported("invalid TCP port"))?;
                if direction != NetworkDirection::Ingress || !protected_ports.contains(&port) {
                    port_set.insert(port);
                }
            }
        }
        if direction == NetworkDirection::Ingress && default == NetworkAction::Deny {
            port_set.extend(&protected_ports);
        }
        let action = if default == NetworkAction::Deny {
            "allowed"
        } else {
            "blocked"
        };
        result[name]["default"] = json!(if default == NetworkAction::Deny {
            "deny"
        } else {
            "allow"
        });
        result[name][format!("{action}_ips")] = json!(ips);
        if class == "supervisor" {
            result[name][format!("{action}_ports")] = json!(port_set);
        }
    }
    if class == "conch" {
        let mut count = 0;
        for d in ["ingress", "egress"] {
            for f in ["allowed_ips", "blocked_ips"] {
                count += result[d][f].as_array().map_or(0, Vec::len);
            }
        }
        if count > 1024 {
            return Err(unsupported("Conch supports at most 1024 destinations"));
        }
    }
    Ok(result)
}

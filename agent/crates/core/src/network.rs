//! Network policy inputs for Agent-managed Environment creation.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateNetwork {
    pub egress: EgressRules<EgressRule>,
}

impl TemplateNetwork {
    pub(crate) fn validate(&self) -> crate::ValidationResult {
        self.egress.validate(128, EgressRule::valid_ports)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, bound(deserialize = "Rule: Deserialize<'de>"))]
pub struct EgressRules<Rule> {
    #[serde(default)]
    pub allow: Vec<Rule>,
    #[serde(default)]
    pub deny: Vec<Rule>,
}

impl<Rule> EgressRules<Rule> {
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Rule)> {
        self.allow
            .iter()
            .map(|rule| ("allow", rule))
            .chain(self.deny.iter().map(|rule| ("deny", rule)))
    }

    fn validate(&self, limit: usize, valid: impl Fn(&Rule) -> bool) -> crate::ValidationResult {
        if self.allow.len().saturating_add(self.deny.len()) > limit {
            return Err(format!(
                "egress accepts at most {limit} rules across allow and deny"
            ));
        }
        if self.iter().any(|(_, rule)| !valid(rule)) {
            return Err("invalid egress port range or priority".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformEgress {
    pub template_priority: Option<u32>,
    pub egress: Option<EgressRules<PlatformEgressRule>>,
}

impl PlatformEgress {
    pub fn validate(&self) -> crate::ValidationResult {
        let valid_priority = |value| (1..u32::MAX).contains(&value);
        if self
            .template_priority
            .is_some_and(|value| !valid_priority(value))
            || self
                .egress
                .as_ref()
                .is_some_and(|rules| rules.iter().next().is_some())
                && self.template_priority.is_none()
        {
            return Err("platform egress requires a valid template_priority".into());
        }
        if let Some(rules) = &self.egress {
            rules.validate(64, |rule| {
                rule.destination.valid_ports() && valid_priority(rule.priority)
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformEgressRule {
    #[serde(flatten)]
    pub destination: EgressRule,
    pub priority: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EgressRule {
    pub cidr: ipnet::Ipv4Net,
    pub protocol: EgressProtocol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<PortRange>,
}

impl EgressRule {
    pub fn valid_ports(&self) -> bool {
        self.ports.as_ref().is_none_or(|ports| {
            matches!(self.protocol, EgressProtocol::Tcp | EgressProtocol::Udp)
                && ports.first > 0
                && ports.first <= ports.last
        })
    }
}
#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EgressProtocol {
    Any,
    Tcp,
    Udp,
    Icmp,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub first: u16,
    pub last: u16,
}

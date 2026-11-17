//! Bounded structural exclusions selected by the subscription owner. No content,
//! query language, provider I/O or semantic mail classification lives here.
use super::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScreeningPolicy {
    version: u8,
    rules: Vec<ExclusionRule>,
}

impl Default for ScreeningPolicy {
    fn default() -> Self {
        Self {
            version: 1,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExclusionRule {
    id: String,
    attribute: Attribute,
    value: String,
    action: ScreeningAction,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Attribute {
    Label,
    Container,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScreeningAction {
    Accept,
    Suppress,
    Quarantine,
}

#[derive(Default)]
pub(crate) struct ScreeningFacts {
    pub labels: Vec<String>,
    pub containers: Vec<String>,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScreeningDecision {
    pub action: ScreeningAction,
    pub rule_id: Option<String>,
    pub reason: String,
    pub policy_digest: String,
    pub policy_version: u8,
    pub policy_revision: u64,
}

impl ScreeningPolicy {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let mut ids = BTreeSet::new();
        if self.version != 1
            || self.rules.len() > 16
            || self.rules.iter().any(|rule| {
                rule.id.is_empty()
                    || rule.id.len() > 64
                    || !rule
                        .id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                    || !ids.insert(&rule.id)
                    || !bounded_value(&rule.value)
                    || rule.action == ScreeningAction::Accept
            })
        {
            return Err(Error::InvalidManifest);
        }
        Ok(())
    }

    pub(crate) fn evaluate(&self, facts: &ScreeningFacts) -> Result<ScreeningDecision, Error> {
        use sha2::{Digest, Sha256};
        // Evaluation only follows validate() at the persisted owner boundary.
        let mut selected = None;
        for rule in &self.rules {
            let values = match rule.attribute {
                Attribute::Label => &facts.labels,
                Attribute::Container => &facts.containers,
            };
            if values.contains(&rule.value) {
                selected = Some(rule);
                if rule.action == ScreeningAction::Quarantine {
                    break;
                }
            }
        }
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidManifest)?;
        Ok(ScreeningDecision {
            action: selected.map_or(ScreeningAction::Accept, |rule| rule.action),
            rule_id: selected.map(|rule| rule.id.clone()),
            reason: if selected.is_some() {
                "user_exclusion"
            } else {
                "no_proven_exclusion"
            }
            .into(),
            policy_digest: hex::encode(Sha256::digest(bytes)),
            policy_version: self.version,
            policy_revision: 0,
        })
    }
}

pub(crate) fn bounded_value(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn policy(action: &str) -> ScreeningPolicy {
        serde_json::from_value(json!({"version":1,"rules":[{
            "id":"junk-choice","attribute":"label","value":"SPAM","action":action
        }]}))
        .unwrap()
    }

    #[test]
    fn only_an_explicit_exact_structural_fact_can_exclude_an_event() {
        let policy = policy("suppress");
        policy.validate().unwrap();
        let facts = ScreeningFacts {
            labels: vec!["SPAM".into()],
            containers: Vec::new(),
        };
        assert_eq!(
            policy.evaluate(&facts).unwrap().action,
            ScreeningAction::Suppress
        );
        assert_eq!(
            policy.evaluate(&facts).unwrap().rule_id.as_deref(),
            Some("junk-choice")
        );
        assert_eq!(
            policy.evaluate(&ScreeningFacts::default()).unwrap().action,
            ScreeningAction::Accept
        );
        for label in ["spam", "CATEGORY_PROMOTIONS", "SOCIAL", "INBOX", "UNREAD"] {
            let facts = ScreeningFacts {
                labels: vec![label.into()],
                containers: Vec::new(),
            };
            assert_eq!(
                policy.evaluate(&facts).unwrap().action,
                ScreeningAction::Accept
            );
        }
        assert_eq!(
            ScreeningPolicy::default().evaluate(&facts).unwrap().action,
            ScreeningAction::Accept
        );
    }

    #[test]
    fn quarantine_wins_over_suppression_without_inventing_a_semantic_verdict() {
        let mut policy = policy("suppress");
        let mut hold = policy.rules[0].clone();
        hold.id = "hold-choice".into();
        hold.action = ScreeningAction::Quarantine;
        policy.rules.push(hold);
        let facts = ScreeningFacts {
            labels: vec!["SPAM".into()],
            containers: Vec::new(),
        };
        assert_eq!(
            policy.evaluate(&facts).unwrap().action,
            ScreeningAction::Quarantine
        );
        assert_eq!(
            policy.evaluate(&facts).unwrap().rule_id.as_deref(),
            Some("hold-choice")
        );
    }

    #[test]
    fn arbitrary_content_paths_regexes_duplicate_ids_and_unbounded_rules_are_refused() {
        for attribute in [
            "subject",
            "body",
            "sender",
            "thread",
            "mime",
            "$.data.labels",
        ] {
            let wire = json!({"version":1,"rules":[{"id":"x","attribute":attribute,
                "value":"invite","action":"suppress"}]});
            assert!(serde_json::from_value::<ScreeningPolicy>(wire).is_err());
        }
        let mut policy = policy("suppress");
        policy.rules.push(policy.rules[0].clone());
        assert!(policy.validate().is_err());
        policy.rules = vec![policy.rules[0].clone(); 17];
        assert!(policy.validate().is_err());
    }

    #[test]
    fn signed_provider_projection_never_uses_body_headers_or_a_callback_policy() {
        use super::super::tests::{body, headers, NOW, SECRET};
        use crate::ingress::digitalmategateway::composio::WebhookVerifier;
        let verify = |data| {
            let mut wire: serde_json::Value =
                serde_json::from_slice(&body("structural", "")).unwrap();
            wire["data"] = data;
            let raw = serde_json::to_vec(&wire).unwrap();
            WebhookVerifier::new(&[SECRET])
                .unwrap()
                .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
                .unwrap()
        };
        let policy = policy("suppress");
        for data in [
            json!({"text":"SPAM","subject":"SPAM","headers":{"List-Unsubscribe":"yes"}}),
            json!({"label_ids":"SPAM"}),
            json!({"label_ids":["SPAM",1]}),
            json!({"labelIds":["SPAM"]}),
            json!({"screening_policy":{"action":"suppress","value":"SPAM"}}),
        ] {
            assert_eq!(
                policy
                    .evaluate(&verify(data).screening_facts())
                    .unwrap()
                    .action,
                ScreeningAction::Accept
            );
        }
        assert_eq!(
            policy
                .evaluate(&verify(json!({"label_ids":["SPAM"]})).screening_facts())
                .unwrap()
                .action,
            ScreeningAction::Suppress
        );
        // No metadata exclusion was selected by the user: provider categories
        // alone cannot suppress any event.
        assert_eq!(
            ScreeningPolicy::default()
                .evaluate(&verify(json!({"label_ids":["SPAM","TRASH"]})).screening_facts())
                .unwrap()
                .action,
            ScreeningAction::Accept
        );
    }

    #[test]
    fn outlook_schema_metadata_is_bound_to_its_message_and_missing_facts_accept() {
        use super::super::tests::{body, headers, NOW, SECRET};
        use crate::ingress::digitalmategateway::composio::WebhookVerifier;
        // GET triggers_types/OUTLOOK_MESSAGE_TRIGGER version20261112_00:
        // current outlook_message/parentFolderId, deprecated message/parent_folder_id.
        let policy: ScreeningPolicy = serde_json::from_value(json!({"version":1,"rules":[{
            "id":"folder-choice","attribute":"container","value":"opaque-folder","action":"quarantine"
        }]})).unwrap();
        for (data, expected) in [
            (
                json!({"id":"message-a","outlook_message":{"id":"message-a","parentFolderId":"opaque-folder"}}),
                ScreeningAction::Quarantine,
            ),
            (
                json!({"id":"message-a","message":{"id":"message-a","parent_folder_id":"opaque-folder"}}),
                ScreeningAction::Quarantine,
            ),
            (
                json!({"id":"message-a","event_type":"created"}),
                ScreeningAction::Accept,
            ),
            (
                json!({"id":"","outlook_message":{"id":"","parentFolderId":"opaque-folder"}}),
                ScreeningAction::Accept,
            ),
            (
                json!({"id":" ","outlook_message":{"id":" ","parentFolderId":"opaque-folder"}}),
                ScreeningAction::Accept,
            ),
            (
                json!({"id":"message-a","parentFolderId":"opaque-folder"}),
                ScreeningAction::Accept,
            ),
            (
                json!({"id":"message-a","outlook_message":{"id":"message-b","parentFolderId":"opaque-folder"}}),
                ScreeningAction::Accept,
            ),
            (
                json!({"id":"message-a","outlook_message":{"id":"message-a","parentFolderId":"opaque-folder"},
                "message":{"id":"message-a","parent_folder_id":"different-folder"}}),
                ScreeningAction::Accept,
            ),
        ] {
            let mut wire: serde_json::Value =
                serde_json::from_slice(&body("outlook-event", "")).unwrap();
            wire["metadata"]["trigger_slug"] = json!("OUTLOOK_MESSAGE_TRIGGER");
            wire["data"] = data;
            let raw = serde_json::to_vec(&wire).unwrap();
            let event = WebhookVerifier::new(&[SECRET])
                .unwrap()
                .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
                .unwrap();
            assert_eq!(
                policy.evaluate(&event.screening_facts()).unwrap().action,
                expected
            );
        }
    }
}

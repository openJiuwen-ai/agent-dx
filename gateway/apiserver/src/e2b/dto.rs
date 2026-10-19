use super::network::{self, ConversionError, EgressRequest, UnsupportedFeatures};
use adx_core::execution::NetworkPolicy;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MAX_TIMEOUT_SECONDS: u64 = 86_400;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateRequest {
    #[serde(rename = "templateID", default = "base_template")]
    pub template_id: String,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub env_vars: BTreeMap<String, String>,
    pub auto_pause: Option<bool>,
    pub auto_pause_memory: Option<bool>,
    pub auto_resume: Option<AutoResume>,
    #[serde(rename = "allow_internet_access")]
    pub allow_internet_access: Option<bool>,
    #[serde(default)]
    pub network: serde_json::Value,
}

fn base_template() -> String {
    "base".into()
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoResume {
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TimeoutRequest {
    pub timeout: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectRequest {
    pub timeout: Option<u64>,
    pub memory: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PauseRequest {
    pub memory: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotRequest {
    pub name: Option<String>,
    pub memory: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateNetwork {
    allow_public_traffic: Option<bool>,
    allow_out: Option<Vec<String>>,
    deny_out: Option<Vec<String>>,
    #[serde(default)]
    egress_proxy: serde_json::Value,
    #[serde(default)]
    rules: serde_json::Value,
    #[serde(default)]
    mask_request_host: serde_json::Value,
    #[serde(default)]
    https_ports: serde_json::Value,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct NetworkUpdateRequest {
    allow_out: Option<Vec<String>>,
    deny_out: Option<Vec<String>>,
    #[serde(rename = "allow_internet_access")]
    allow_internet_access: Option<bool>,
    #[serde(default)]
    egress_proxy: serde_json::Value,
    #[serde(default)]
    rules: serde_json::Value,
}

impl NetworkUpdateRequest {
    pub(super) fn policy(
        &self,
        baseline: Option<&NetworkPolicy>,
    ) -> Result<NetworkPolicy, ValidationError> {
        network::convert_egress(
            &EgressRequest {
                allow_out: self.allow_out.clone(),
                deny_out: self.deny_out.clone(),
                allow_internet_access: self.allow_internet_access,
            },
            UnsupportedFeatures {
                egress_proxy: !self.egress_proxy.is_null(),
                rules: !self.rules.is_null(),
                ..Default::default()
            },
            baseline,
        )
        .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ErrorBody<'a> {
    pub code: u16,
    pub message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    pub message: &'static str,
    pub error_code: &'static str,
}

impl CreateRequest {
    pub(super) fn network_configuration(
        &self,
    ) -> Result<(Option<NetworkPolicy>, Option<bool>), ValidationError> {
        let input: CreateNetwork = if self.network.is_null() {
            CreateNetwork::default()
        } else {
            serde_json::from_value(self.network.clone())
                .map_err(|_| ValidationError::invalid("invalid network configuration"))?
        };
        let has_egress = self.allow_internet_access == Some(false)
            || input
                .allow_out
                .as_ref()
                .is_some_and(|values| !values.is_empty())
            || input
                .deny_out
                .as_ref()
                .is_some_and(|values| !values.is_empty());
        let policy = network::convert_egress(
            &EgressRequest {
                allow_out: input.allow_out,
                deny_out: input.deny_out,
                allow_internet_access: self.allow_internet_access,
            },
            UnsupportedFeatures {
                egress_proxy: !input.egress_proxy.is_null(),
                rules: !input.rules.is_null(),
                mask_request_host: !input.mask_request_host.is_null(),
                https_ports: !input.https_ports.is_null(),
            },
            None,
        )
        .map_err(ValidationError::from)?;
        // Preserve the native no-policy create path for an unrestricted request.
        Ok((has_egress.then_some(policy), input.allow_public_traffic))
    }
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.template_id.trim().is_empty() {
            return Err(ValidationError::invalid("templateID is required"));
        }
        if self.metadata.len() > 128
            || self
                .metadata
                .iter()
                .any(|(key, value)| key.is_empty() || key.len() > 256 || value.len() > 4096)
        {
            return Err(ValidationError::invalid("invalid metadata"));
        }
        if self
            .env_vars
            .iter()
            .any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
        {
            return Err(ValidationError::invalid("invalid environment variable"));
        }
        if self
            .env_vars
            .iter()
            .any(|(key, _)| key.starts_with("ADX_") || key.starts_with("EXECD_"))
        {
            return Err(ValidationError::invalid("reserved environment variable"));
        }
        if self
            .timeout
            .is_some_and(|timeout| timeout == 0 || timeout > MAX_TIMEOUT_SECONDS)
        {
            return Err(ValidationError::invalid(
                "timeout must be between 1 and 86400 seconds",
            ));
        }
        if self
            .auto_resume
            .as_ref()
            .is_some_and(|resume| resume.enabled)
        {
            return Err(ValidationError::unsupported(
                "automatic resume is not supported",
            ));
        }
        if self.auto_pause_memory == Some(false) {
            return Err(ValidationError::unsupported(
                "filesystem-only pause is not supported by this profile",
            ));
        }
        self.network_configuration()?;
        Ok(())
    }
}

impl TimeoutRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.timeout > MAX_TIMEOUT_SECONDS {
            return Err(ValidationError::invalid(
                "timeout must be at most 86400 seconds",
            ));
        }
        Ok(())
    }
}

impl ConnectRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self
            .timeout
            .is_some_and(|timeout| timeout == 0 || timeout > MAX_TIMEOUT_SECONDS)
        {
            return Err(ValidationError::invalid(
                "timeout must be between 1 and 86400 seconds",
            ));
        }
        if self.memory == Some(false) {
            return Err(ValidationError::unsupported(
                "resuming without memory is not supported by this profile",
            ));
        }
        Ok(())
    }
}

impl PauseRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.memory == Some(false) {
            return Err(ValidationError::unsupported(
                "filesystem-only pause is not supported by this profile",
            ));
        }
        Ok(())
    }
}

impl SnapshotRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.memory == Some(false) {
            return Err(ValidationError::unsupported(
                "filesystem-only snapshot is not supported by this profile",
            ));
        }
        Ok(())
    }
}

impl ValidationError {
    pub(super) fn invalid(message: &'static str) -> Self {
        Self {
            message,
            error_code: "invalid_request",
        }
    }

    pub(super) fn unsupported(message: &'static str) -> Self {
        Self {
            message,
            error_code: "capability_not_supported",
        }
    }
}

impl From<ConversionError> for ValidationError {
    fn from(error: ConversionError) -> Self {
        Self {
            message: error.message(),
            error_code: error.code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_uses_rest_seconds_and_rejects_unsupported_lifecycle_modes() {
        let request: CreateRequest = serde_json::from_value(json!({
            "templateID": "base", "timeout": 300, "autoPause": true,
            "autoPauseMemory": true, "metadata": {"purpose": "test"},
            "envVars": {"USER_VALUE": "ok"}
        }))
        .unwrap();
        assert_eq!(request.timeout, Some(300));
        assert!(request.validate().is_ok());

        for request in [
            json!({"timeout": 0}),
            json!({"timeout": 86401}),
            json!({"autoPause": true, "autoPauseMemory": false}),
            json!({"autoResume": {"enabled": true}}),
        ] {
            let request: CreateRequest = serde_json::from_value(request).unwrap();
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn memory_requests_default_to_full_memory_and_reject_filesystem_only() {
        let create: CreateRequest = serde_json::from_value(json!({"autoPause": true})).unwrap();
        assert!(create.validate().is_ok());
        let create: CreateRequest =
            serde_json::from_value(json!({"autoPause": true, "autoPauseMemory": false})).unwrap();
        assert_eq!(
            create.validate().unwrap_err().error_code,
            "capability_not_supported"
        );

        for request in [json!({}), json!({"memory": true})] {
            let connect: ConnectRequest = serde_json::from_value(request.clone()).unwrap();
            assert!(connect.validate().is_ok());
            let snapshot: SnapshotRequest = serde_json::from_value(request).unwrap();
            assert!(snapshot.validate().is_ok());
        }

        let connect: ConnectRequest = serde_json::from_value(json!({"memory": false})).unwrap();
        assert_eq!(
            connect.validate().unwrap_err().error_code,
            "capability_not_supported"
        );
        let snapshot: SnapshotRequest = serde_json::from_value(json!({"memory": false})).unwrap();
        assert_eq!(
            snapshot.validate().unwrap_err().error_code,
            "capability_not_supported"
        );
    }

    #[test]
    fn memory_request_dtos_reject_unknown_fields() {
        assert!(
            serde_json::from_value::<ConnectRequest>(json!({"memory": true, "extra": true}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<SnapshotRequest>(json!({"name": "base", "extra": true}))
                .is_err()
        );
    }

    #[test]
    fn set_timeout_allows_zero_and_pause_requires_memory_checkpoint() {
        assert!(TimeoutRequest { timeout: 0 }.validate().is_ok());
        assert!(TimeoutRequest { timeout: 86401 }.validate().is_err());
        assert_eq!(
            ConnectRequest {
                timeout: Some(0),
                memory: None,
            }
            .validate()
            .unwrap_err()
            .error_code,
            "invalid_request"
        );
        assert!(PauseRequest { memory: None }.validate().is_ok());
        assert!(PauseRequest { memory: Some(true) }.validate().is_ok());
        assert_eq!(
            PauseRequest {
                memory: Some(false)
            }
            .validate()
            .unwrap_err()
            .error_code,
            "capability_not_supported"
        );
    }

    #[test]
    fn pause_defaults_to_memory_checkpoint_for_omitted_or_null_memory() {
        for request in [json!({}), json!({"memory": null}), json!({"memory": true})] {
            let request: PauseRequest = serde_json::from_value(request).unwrap();
            assert!(request.validate().is_ok());
        }
    }

    #[test]
    fn create_accepts_empty_network_and_rejects_unknown_network_fields() {
        let request: CreateRequest = serde_json::from_value(json!({"network": {}})).unwrap();
        assert!(request.validate().is_ok());

        let request: CreateRequest =
            serde_json::from_value(json!({"network": {"unknown": true}})).unwrap();
        assert_eq!(
            request.validate().unwrap_err().error_code,
            "invalid_request"
        );
    }

    #[test]
    fn create_network_keeps_unrestricted_native_path_and_maps_explicit_egress() {
        let empty: CreateRequest = serde_json::from_value(json!({"network": {}})).unwrap();
        assert!(empty.network_configuration().unwrap().0.is_none());
        let input: CreateRequest = serde_json::from_value(json!({
            "network": {"allowPublicTraffic": false, "allowOut": ["10.0.0.0/8"]},
            "allow_internet_access": false,
        }))
        .unwrap();
        let (policy, public) = input.network_configuration().unwrap();
        assert_eq!(public, Some(false));
        let traffic = policy.unwrap().traffic.unwrap();
        assert_eq!(
            traffic.egress_default_action,
            adx_core::execution::NetworkAction::Deny
        );
        assert_eq!(traffic.rules[0].peer.cidr, "10.0.0.0/8");
    }

    #[test]
    fn invalid_process_environment_is_rejected_before_guest_start() {
        for (key, value) in [
            ("", "v"),
            ("A=B", "v"),
            ("A\0", "v"),
            ("A", "v\0"),
            ("ADX_ENVD_PATH", "/untrusted"),
        ] {
            let request: CreateRequest =
                serde_json::from_value(json!({"envVars": BTreeMap::from([(key, value)])})).unwrap();
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn template_alias_resolves_once_to_immutable_base_template() {
        let catalog: crate::e2b::TemplateCatalog = serde_json::from_value(json!({"templates": {
            "base": {"runtimeProfileRef":"e2b-fc-v1", "defaultUser":"user",
              "defaultWorkdir":"/home/user", "cpuMillicores":1000, "memoryMiB":512,
              "capabilities":["envd", "memoryCheckpoint", "reusableSnapshot"]},
            "adx-e2b-base": {"runtimeProfileRef":"unused", "defaultUser":"unused",
              "defaultWorkdir":"/unused", "cpuMillicores":1, "memoryMiB":1,
              "aliasOf":"base"}
        }}))
        .unwrap();
        let (template, resolved) = catalog.resolve("adx-e2b-base").unwrap();
        assert_eq!(resolved, "base");
        assert_eq!(template.runtime_profile_ref, "e2b-fc-v1");
    }

    #[test]
    fn e2b_errors_do_not_use_the_adx_envelope_shape() {
        let value = serde_json::to_value(ErrorBody {
            code: 400,
            message: "unsupported",
            error_code: Some("capability_not_supported"),
        })
        .unwrap();
        assert_eq!(value["code"], 400);
        assert!(value.get("data").is_none());
    }
}

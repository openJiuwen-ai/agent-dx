use super::ValidationError;
use adx_core::runtime_profile::RuntimeProfile;
use serde::Deserialize;
use std::collections::BTreeMap;

/// Protocol aliases only. Execution settings remain owned by native templates.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub domain: String,
    pub envd_version: String,
    pub templates: BTreeMap<String, TemplateRef>,
    /// Deployment-owned selection for workers with the matching trusted profile.
    #[serde(default)]
    pub required_node_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TemplateRef {
    pub name: String,
    pub version: String,
}

impl Config {
    pub fn validate(&self, profile: Option<&RuntimeProfile>) -> Result<(), ValidationError> {
        if self.domain.trim().is_empty()
            || self.envd_version.trim().is_empty()
            || !self.templates.contains_key("base")
        {
            return Err(ValidationError::invalid(
                "E2B domain, envd version and base template reference required",
            ));
        }
        for (alias, target) in &self.templates {
            for value in [alias, &target.name, &target.version] {
                adx_agent_core::identifier(value, "template reference")
                    .map_err(|_| ValidationError::invalid("invalid native template reference"))?;
            }
        }
        adx_core::scheduling::LabelSelector {
            match_labels: self.required_node_labels.clone(),
            expressions: vec![],
        }
        .validate()
        .map_err(|_| ValidationError::invalid("invalid E2B worker selector"))?;
        let profile = profile.ok_or_else(|| {
            ValidationError::invalid("E2B requires the deployment runtime_profile")
        })?;
        profile
            .validate()
            .map_err(|_| ValidationError::invalid("invalid runtime_profile"))?;
        let path = profile
            .env
            .get("ADX_ENVD_PATH")
            .map(String::as_str)
            .unwrap_or("");
        let digest = profile
            .env
            .get("ADX_ENVD_SHA256")
            .map(String::as_str)
            .unwrap_or("");
        let user = profile
            .env
            .get("ADX_ENVD_DEFAULT_USER")
            .map(String::as_str)
            .unwrap_or("");
        let cwd = profile
            .env
            .get("ADX_ENVD_DEFAULT_WORKDIR")
            .map(String::as_str)
            .unwrap_or("");
        if !path.starts_with('/')
            || path.contains('\0')
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || user.trim().is_empty()
            || user.contains('\0')
            || !cwd.starts_with('/')
            || cwd.contains('\0')
        {
            return Err(ValidationError::invalid(
                "runtime_profile requires trusted envd path, digest, user and workdir",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn profile() -> RuntimeProfile {
        serde_json::from_value(json!({
            "rootfs":{"runtime_class":"runc","type":"local","path":"/rootfs","readonly":false},
            "bootstrap":{"type":"erofs","root":"/rootfs","target":"/__adx","entrypoint":["/__adx/init"]},
            "env":{"ADX_ENVD_PATH":"/__adx/envd","ADX_ENVD_SHA256":"a".repeat(64),"ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/home/user"}
        })).unwrap()
    }
    #[test]
    fn aliases_only_reference_native_versions_and_require_trusted_envd() {
        let config: Config = serde_json::from_value(json!({
            "domain":"sandbox.test","envdVersion":"0.9.0",
            "templates":{"base":{"name":"python","version":"1"},"python":{"name":"python","version":"1"}},
            "requiredNodeLabels":{"adx.runtime.profile":"e2b-v1"}
        })).unwrap();
        assert!(config.validate(Some(&profile())).is_ok());
        assert!(config.validate(None).is_err());
        let mut invalid = profile();
        invalid.env.remove("ADX_ENVD_DEFAULT_USER");
        assert!(config.validate(Some(&invalid)).is_err());
        assert!(serde_json::from_value::<Config>(json!({
            "domain":"sandbox.test","envdVersion":"0.9.0",
            "templates":{"base":{"name":"python","version":"1","cpuMillicores":1000}}
        }))
        .is_err());
    }
}

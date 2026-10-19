use super::ValidationError;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub domain: String,
    pub envd_version: String,
    #[serde(default)]
    pub runtime_profiles: HashMap<String, adx_core::runtime_profile::RuntimeProfile>,
    pub templates: TemplateCatalog,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateCatalog {
    #[serde(default)]
    pub templates: HashMap<String, Template>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Template {
    #[serde(default)]
    pub runtime_profile_ref: String,
    #[serde(default)]
    pub default_user: String,
    #[serde(default)]
    pub default_workdir: String,
    #[serde(default)]
    pub cpu_millicores: u64,
    #[serde(default, rename = "memoryMiB")]
    pub memory_mib: u64,
    #[serde(default = "default_pause_retention")]
    pub pause_retention_seconds: u64,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub alias_of: Option<String>,
    #[serde(default = "default_allow_public_traffic")]
    pub allow_public_traffic: bool,
    #[serde(default = "default_timeout_action")]
    pub timeout_action: String,
}

fn default_pause_retention() -> u64 {
    90_000
}

fn default_timeout_action() -> String {
    "delete".into()
}
fn default_allow_public_traffic() -> bool {
    true
}

impl Config {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.domain.trim().is_empty() || self.envd_version.trim().is_empty() {
            return Err(ValidationError::invalid("invalid E2B configuration"));
        }
        self.templates.validate()?;
        for template in self
            .templates
            .templates
            .values()
            .filter(|template| template.alias_of.is_none())
        {
            let profile = self
                .runtime_profiles
                .get(&template.runtime_profile_ref)
                .ok_or_else(|| ValidationError::invalid("unknown E2B runtime profile"))?;
            profile
                .validate()
                .map_err(|_| ValidationError::invalid("invalid E2B runtime profile"))?;
            let path = profile
                .env
                .get("ADX_ENVD_PATH")
                .ok_or_else(|| ValidationError::invalid("missing ADX_ENVD_PATH"))?;
            let digest = profile
                .env
                .get("ADX_ENVD_SHA256")
                .ok_or_else(|| ValidationError::invalid("missing ADX_ENVD_SHA256"))?;
            if !path.starts_with('/')
                || path.contains('\0')
                || digest.len() != 64
                || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(ValidationError::invalid("invalid envd profile"));
            }
        }
        Ok(())
    }

    pub fn runtime_profile(
        &self,
        template: &Template,
    ) -> Result<adx_core::runtime_profile::RuntimeProfile, ValidationError> {
        self.runtime_profiles
            .get(&template.runtime_profile_ref)
            .cloned()
            .ok_or_else(|| ValidationError::invalid("unknown E2B runtime profile"))
    }
}

impl TemplateCatalog {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.templates.is_empty() {
            return Err(ValidationError::invalid("E2B templates are required"));
        }
        self.resolve("base")?;
        for id in self.templates.keys() {
            self.resolve(id)?;
        }
        Ok(())
    }

    pub fn resolve(&self, id: &str) -> Result<(&Template, String), ValidationError> {
        let mut resolved = id.to_owned();
        for _ in 0..2 {
            let template = self
                .templates
                .get(&resolved)
                .ok_or_else(|| ValidationError::invalid("unknown templateID"))?;
            match template.alias_of.as_deref() {
                Some(alias) if alias != resolved => resolved = alias.to_owned(),
                Some(_) => return Err(ValidationError::invalid("invalid template alias")),
                None => {
                    template.validate()?;
                    return Ok((template, resolved));
                }
            }
        }
        Err(ValidationError::invalid(
            "template aliases may only have one level",
        ))
    }
}

impl Template {
    pub fn require_capability(&self, capability: &str) -> Result<(), ValidationError> {
        if self.capabilities.iter().any(|value| value == capability) {
            Ok(())
        } else {
            Err(ValidationError::unsupported(
                "capability is not enabled by this template",
            ))
        }
    }
    fn validate(&self) -> Result<(), ValidationError> {
        if self.runtime_profile_ref.trim().is_empty()
            || self.default_user.trim().is_empty()
            || !self.default_workdir.starts_with('/')
            || self.cpu_millicores == 0
            || self.memory_mib == 0
            || self.pause_retention_seconds == 0
            || !matches!(self.timeout_action.as_str(), "delete" | "pause")
            || !self.capabilities.iter().any(|value| value == "envd")
            || self.default_user.contains('\0')
            || self.default_workdir.contains('\0')
        {
            return Err(ValidationError::invalid(
                "invalid E2B template configuration",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_traffic_defaults_to_enabled_and_envd_is_required() {
        let template: Template = serde_json::from_str(r#"{"runtimeProfileRef":"p","defaultUser":"u","defaultWorkdir":"/w","cpuMillicores":1,"memoryMiB":1,"capabilities":["envd"]}"#).unwrap();
        assert!(template.allow_public_traffic);
        assert!(template.require_capability("envd").is_ok());
        assert!(template.require_capability("memoryCheckpoint").is_err());
    }
}

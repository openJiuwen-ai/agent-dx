//! Declarative version weights. Selection is stable per logical Binding, not per request.
use crate::{identifier, ValidationResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionWeight {
    pub version: String,
    pub weight: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasConfig {
    pub tenant: String,
    pub template: String,
    pub alias: String,
    pub seed: String,
    pub versions: Vec<VersionWeight>,
}
impl AliasConfig {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("alias", &self.alias),
            ("seed", &self.seed),
        ] {
            identifier(value, label)?;
        }
        crate::alias_name(&self.alias)?;
        let mut names = BTreeSet::new();
        let mut total = 0_u64;
        for entry in &self.versions {
            crate::version_number(&entry.version)?;
            if !names.insert(&entry.version) {
                return Err("duplicate alias version".into());
            }
            total = total
                .checked_add(entry.weight)
                .ok_or("alias weight sum overflow")?;
        }
        if total == 0 {
            return Err("alias total weight must be positive".into());
        }
        Ok(())
    }

    /// Returns Invalid input as a validation error; weights may have any positive total.
    /// Canonical version ordering and exact integer ratios make reordering and proportional
    /// scaling invariant. Expanding the second of two sorted versions preserves its cohort.
    pub fn select(&self, binding_id: &str) -> Result<&str, String> {
        self.validate()?;
        identifier(binding_id, "binding_id")?;
        let key = crate::encode_key(&[
            &self.tenant,
            &self.template,
            &self.alias,
            &self.seed,
            binding_id,
        ]);
        let digest = Sha256::digest(key.as_bytes());
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&digest[..8]);
        let total: u64 = self.versions.iter().map(|entry| entry.weight).sum();
        let point = u128::from(u64::from_be_bytes(bytes)) * u128::from(total);
        let mut versions: Vec<_> = self.versions.iter().collect();
        versions.sort_unstable_by(|a, b| {
            a.version
                .len()
                .cmp(&b.version.len())
                .then_with(|| a.version.cmp(&b.version))
        });
        let mut cumulative = 0_u64;
        for entry in versions {
            cumulative += entry.weight; // validate checked the entire nonnegative sum.
            if point < (u128::from(cumulative) << 64) {
                return Ok(&entry.version);
            }
        }
        Err("alias selection has no positive interval".into())
    }
}

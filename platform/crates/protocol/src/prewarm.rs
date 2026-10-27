//! Validated wire conversion for template preparation context.
use crate::control as pb;
use adx_core::{prewarm::*, Error, Result};

impl TryFrom<pb::PreparationContext> for PreparationContext {
    type Error = Error;
    fn try_from(value: pb::PreparationContext) -> Result<Self> {
        if value.preference != "prefer" {
            return Err(Error::Invalid("unsupported preparation preference".into()));
        }
        let context = Self {
            source_ref: SourceRef {
                tenant: value.tenant,
                template: value.template,
                version: value.version,
                template_digest: value.template_digest,
            },
            preference: PreparationPreference::Prefer,
            contract_version: value.contract_version,
        };
        context.validate(&context.source_ref.tenant)?;
        Ok(context)
    }
}
impl From<PreparationContext> for pb::PreparationContext {
    fn from(value: PreparationContext) -> Self {
        Self {
            tenant: value.source_ref.tenant,
            template: value.source_ref.template,
            version: value.source_ref.version,
            template_digest: value.source_ref.template_digest,
            contract_version: value.contract_version,
            preference: "prefer".into(),
        }
    }
}

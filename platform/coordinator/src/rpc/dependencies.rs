//! Resource admission runs before either scheduler exposes a new consumer.
use super::*;

impl State {
    pub(super) async fn persistent_dependencies(&self, spec: &EnvironmentSpec) -> Result<bool> {
        // Redis must own dependencies before runtime I/O, including the interval
        // before the first execution result in either scheduling path.
        Ok(crate::storage::has_volumes(spec)
            || !self
                .session
                .managed_image_dependencies(spec)
                .await?
                .is_empty())
    }

    pub(super) async fn prepare_dependencies(
        &mut self,
        spec: &EnvironmentSpec,
        domains: &BTreeMap<String, adx_core::volumes::StorageDomain>,
    ) -> Result<()> {
        self.session.acquire_managed_images(spec).await?;
        self.prepare_volumes(spec, domains).await
    }
}

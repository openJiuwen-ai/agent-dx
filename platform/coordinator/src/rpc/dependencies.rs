//! Resource admission runs before either scheduler exposes a new consumer.
use super::*;

impl State {
    pub(super) fn persistent_dependencies(spec: &EnvironmentSpec) -> bool {
        // Redis must own dependencies before runtime I/O, including the interval
        // before the first execution result in either scheduling path.
        crate::storage::has_volumes(spec) || !spec.execution.managed_image_publications.is_empty()
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

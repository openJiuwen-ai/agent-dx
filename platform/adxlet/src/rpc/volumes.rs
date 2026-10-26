//! Coordinator grants precede every managed-directory and runtime effect.
use super::*;
use adx_core::{
    execution::{MountSource, StorageSource},
    volumes::{Volume, VolumeSource},
};
use std::collections::BTreeMap;

impl CoordinatorStateSink {
    pub(super) async fn prepare_volume_execution(
        &self,
        record: &EnvironmentRecord,
        previous_runtime_id: &str,
    ) -> Result<adx_core::EnvironmentSpec> {
        if !record
            .spec
            .execution
            .mounts
            .iter()
            .any(|mount| matches!(mount.source, MountSource::Volume(_)))
        {
            return Ok(record.spec.clone());
        }
        let mut client = self
            .volumes
            .read()
            .map_err(|_| Error::Unavailable("volume RPC client unavailable".into()))?
            .clone();
        let mut request = adx_observability::trace::inject(pb::AuthorizeVolumeExecutionRequest {
            record: Some(record.clone().try_into()?),
            previous_runtime_id: previous_runtime_id.into(),
            node_session_id: self.session_id.clone(),
        });
        request.set_timeout(self.timeout);
        let grant = tokio::time::timeout(self.timeout, client.authorize_execution(request))
            .await
            .map_err(|_| Error::Unavailable("volume authorization result unknown".into()))?
            .map_err(dependency_status)?
            .into_inner();
        let assignment: adx_core::Assignment =
            grant.assignment.ok_or(Error::Conflict)?.try_into()?;
        // Coordinator performs authorization. These echoed fields only defend
        // against a mismatched response; they do not independently grant access.
        if assignment != record.assignment
            || grant.runtime_id != record.runtime.id
            || grant.node_session_id != self.session_id
        {
            return Err(Error::Conflict);
        }
        let mut granted = BTreeMap::new();
        for mount in grant.volumes {
            let volume: Volume = mount.volume.ok_or(Error::Conflict)?.try_into()?;
            if volume.spec.tenant_id != record.spec.tenant_id
                || granted
                    .insert(
                        volume.spec.id.clone(),
                        (volume, mount.root, mount.backend_id),
                    )
                    .is_some()
            {
                return Err(Error::Conflict);
            }
        }
        let mut paths = BTreeMap::new();
        for mount in &record.spec.execution.mounts {
            let MountSource::Volume(reference) = &mount.source else {
                continue;
            };
            if paths.contains_key(&reference.id) {
                continue;
            }
            let (volume, root, backend) = granted.get(&reference.id).ok_or(Error::Conflict)?;
            if reference.uid.as_ref().is_some_and(|uid| uid != &volume.uid) {
                return Err(Error::Conflict);
            }
            let VolumeSource::Directory { path, .. } = &volume.spec.source;
            crate::managed_mounts::verify_volume_backend(root, backend).await?;
            let execution = pb::VolumeInitializationRequest {
                volume_uid: volume.uid.clone(),
                record: Some(record.clone().try_into()?),
                node_session_id: self.session_id.clone(),
            };
            let initialization = if volume.spec.create_if_missing {
                let mut request = adx_observability::trace::inject(execution.clone());
                request.set_timeout(self.timeout);
                tokio::time::timeout(self.timeout, client.begin_initialization(request))
                    .await
                    .map_err(|_| {
                        Error::Unavailable("volume initialization admission timed out".into())
                    })?
                    .map_err(dependency_status)?
                    .into_inner()
            } else {
                pb::VolumeInitializationResponse::default()
            };
            let prepared = crate::managed_mounts::prepare_volume_directory(
                root,
                backend,
                path,
                initialization.initialize,
                initialization.verify_existing,
                volume.spec.create_options.as_ref(),
            )
            .await;
            if initialization.initialize {
                let mut request =
                    adx_observability::trace::inject(pb::CompleteVolumeInitializationRequest {
                        execution: Some(execution),
                        success: prepared.is_ok(),
                        error: prepared
                            .as_ref()
                            .err()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    });
                request.set_timeout(self.timeout);
                tokio::time::timeout(self.timeout, client.complete_initialization(request))
                    .await
                    .map_err(|_| {
                        Error::Unavailable("volume initialization publication unknown".into())
                    })?
                    .map_err(dependency_status)?;
            }
            paths.insert(reference.id.clone(), prepared?);
        }
        if paths.len() != granted.len() {
            return Err(Error::Conflict);
        }
        let mut execution = record.spec.clone();
        for mount in &mut execution.execution.mounts {
            if let MountSource::Volume(reference) = &mount.source {
                let source = paths
                    .get(&reference.id)
                    .and_then(|path| path.to_str())
                    .ok_or_else(|| Error::Invalid("volume directory is not UTF-8".into()))?;
                mount.source = MountSource::Inline(StorageSource::Local(source.into()));
                mount.create_if_missing = false;
                mount.create_options = None;
            }
        }
        Ok(execution)
    }
}

use crate::control as pb;
use adx_core::{volumes as core, Error, Result};

impl From<core::VolumeRef> for pb::VolumeRef {
    fn from(value: core::VolumeRef) -> Self {
        Self {
            id: value.id,
            uid: value.uid,
        }
    }
}

impl TryFrom<pb::VolumeRef> for core::VolumeRef {
    type Error = Error;
    fn try_from(value: pb::VolumeRef) -> Result<Self> {
        let reference = Self {
            id: value.id,
            uid: value.uid,
        };
        reference.validate()?;
        Ok(reference)
    }
}

impl From<core::VolumeSpec> for pb::VolumeSpec {
    fn from(value: core::VolumeSpec) -> Self {
        Self {
            tenant_id: value.tenant_id,
            id: value.id,
            source: Some(match value.source {
                core::VolumeSource::Directory {
                    storage_domain,
                    path,
                } => pb::volume_spec::Source::Directory(pb::DirectoryVolumeSource {
                    storage_domain,
                    path,
                }),
            }),
            access_mode: match value.access_mode {
                core::AccessMode::Shared => pb::VolumeAccessMode::Shared,
                core::AccessMode::Exclusive => pb::VolumeAccessMode::Exclusive,
            } as i32,
            create_if_missing: value.create_if_missing,
            create_options: value.create_options.map(Into::into),
        }
    }
}

impl TryFrom<pb::VolumeSpec> for core::VolumeSpec {
    type Error = Error;
    fn try_from(value: pb::VolumeSpec) -> Result<Self> {
        let spec = Self {
            tenant_id: value.tenant_id,
            id: value.id,
            source: match value
                .source
                .ok_or_else(|| Error::Invalid("volume source required".into()))?
            {
                pb::volume_spec::Source::Directory(source) => core::VolumeSource::Directory {
                    storage_domain: source.storage_domain,
                    path: source.path,
                },
            },
            access_mode: match pb::VolumeAccessMode::try_from(value.access_mode) {
                Ok(pb::VolumeAccessMode::Shared) => core::AccessMode::Shared,
                Ok(pb::VolumeAccessMode::Exclusive) => core::AccessMode::Exclusive,
                Err(_) => return Err(Error::Invalid("invalid volume access mode".into())),
            },
            create_if_missing: value.create_if_missing,
            create_options: value.create_options.map(Into::into),
        };
        spec.validate()?;
        Ok(spec)
    }
}

impl From<core::Volume> for pb::Volume {
    fn from(value: core::Volume) -> Self {
        Self {
            uid: value.uid,
            spec: Some(value.spec.into()),
            revision: value.revision,
        }
    }
}

impl TryFrom<pb::Volume> for core::Volume {
    type Error = Error;
    fn try_from(value: pb::Volume) -> Result<Self> {
        let volume = Self {
            uid: value.uid,
            spec: value
                .spec
                .ok_or_else(|| Error::Invalid("volume definition required".into()))?
                .try_into()?,
            revision: value.revision,
        };
        volume.validate()?;
        Ok(volume)
    }
}

impl TryFrom<core::Occupancy> for pb::VolumeOccupancy {
    type Error = Error;
    fn try_from(value: core::Occupancy) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            volume_uid: value.volume_uid,
            assignment: Some(value.assignment.try_into()?),
            runtime_id: value.runtime_id,
            node_session_id: value.node_session_id,
            phase: match value.phase {
                core::OccupancyPhase::Reserved => pb::VolumeOccupancyPhase::Reserved,
                core::OccupancyPhase::Held => pb::VolumeOccupancyPhase::Held,
            } as i32,
        })
    }
}

impl TryFrom<pb::VolumeOccupancy> for core::Occupancy {
    type Error = Error;
    fn try_from(value: pb::VolumeOccupancy) -> Result<Self> {
        let occupancy = Self {
            volume_uid: value.volume_uid,
            assignment: value
                .assignment
                .ok_or_else(|| Error::Invalid("volume assignment required".into()))?
                .try_into()?,
            runtime_id: value.runtime_id,
            node_session_id: value.node_session_id,
            phase: match pb::VolumeOccupancyPhase::try_from(value.phase) {
                Ok(pb::VolumeOccupancyPhase::Reserved) => core::OccupancyPhase::Reserved,
                Ok(pb::VolumeOccupancyPhase::Held) => core::OccupancyPhase::Held,
                _ => return Err(Error::Invalid("volume occupancy phase required".into())),
            },
        };
        occupancy.validate()?;
        Ok(occupancy)
    }
}

impl From<core::StorageMount> for pb::StorageMount {
    fn from(value: core::StorageMount) -> Self {
        Self {
            backend_id: value.backend_id,
            root: value.root,
        }
    }
}

impl TryFrom<pb::StorageMount> for core::StorageMount {
    type Error = Error;
    fn try_from(value: pb::StorageMount) -> Result<Self> {
        let mount = Self {
            backend_id: value.backend_id,
            root: value.root,
        };
        mount.validate()?;
        Ok(mount)
    }
}

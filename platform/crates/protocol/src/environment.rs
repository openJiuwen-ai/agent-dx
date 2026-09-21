use crate::control as pb;
use adx_core::{
    environment::{Bootstrap, Rootfs, RuntimeEnvironment},
    Error, Result,
};
impl From<RuntimeEnvironment> for pb::RuntimeEnvironment {
    fn from(v: RuntimeEnvironment) -> Self {
        Self {
            rootfs: Some(pb::RuntimeRootfs {
                runtime: v.rootfs.runtime,
                r#type: v.rootfs.r#type,
                path: v.rootfs.path,
                readonly: v.rootfs.readonly,
                image: v.rootfs.image,
            }),
            bootstrap: Some(pb::RuntimeBootstrap {
                r#type: v.bootstrap.r#type,
                root: v.bootstrap.root,
                target: v.bootstrap.target,
                entrypoint: v.bootstrap.entrypoint,
                image: v.bootstrap.image,
                image_process_config: v.bootstrap.image_process_config,
            }),
            env: v.env.into_iter().collect(),
        }
    }
}
impl TryFrom<pb::RuntimeEnvironment> for RuntimeEnvironment {
    type Error = Error;
    fn try_from(v: pb::RuntimeEnvironment) -> Result<Self> {
        let r = v
            .rootfs
            .ok_or_else(|| Error::Invalid("runtime rootfs required".into()))?;
        let b = v
            .bootstrap
            .ok_or_else(|| Error::Invalid("runtime bootstrap required".into()))?;
        let value = Self {
            rootfs: Rootfs {
                runtime: r.runtime,
                r#type: r.r#type,
                path: r.path,
                image: r.image,
                readonly: r.readonly,
            },
            bootstrap: Bootstrap {
                r#type: b.r#type,
                root: b.root,
                image: b.image,
                target: b.target,
                entrypoint: b.entrypoint,
                image_process_config: if b.image_process_config.is_empty() {
                    adx_core::environment::DEFAULT_IMAGE_PROCESS_CONFIG.into()
                } else {
                    b.image_process_config
                },
            },
            env: v.env.into_iter().collect(),
        };
        value.validate()?;
        Ok(value)
    }
}

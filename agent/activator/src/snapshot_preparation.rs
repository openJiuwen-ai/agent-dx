//! Freeze the published execution contract and substitute only its anonymous workspace.
use crate::{Activator, Error, Result};
use adx_agent_core::{
    environment::{EnvironmentError, ExecutionSpec},
    mounts::Parameters,
    prewarm::{PrewarmRecipe, SeedExecution, SeedResult, TEMPLATE_HANDOFF_VERSION},
    volumes::{AccessMode, VolumeSource, VolumeSpec},
    TemplateVersion,
};
use adx_agent_store::PreparationJob;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

const WORKSPACE_PARAMETER: &str = "${workspace}";
const PREPARATION_LIFETIME_MS: u64 = 15 * 60 * 1000;
const PREPARATION_ATTEMPT_CAP: Duration = Duration::from_secs(15);
const SEED_PREFIX: &str = "adx-agent-seed-";

fn seed_volume(template: &TemplateVersion, identity: &str) -> Result<VolumeSpec> {
    let ([mount], [volume]) = (template.mounts.as_slice(), template.volumes.as_slice()) else {
        return Err(Error::Invalid(
            "Golden requires one managed workspace mount".into(),
        ));
    };
    let VolumeSource::Directory {
        storage_domain,
        path,
    } = &volume.source;
    if mount.volume_id.as_deref() != Some(volume.id.as_str())
        || mount.volume_uid.is_some()
        || !mount.host_path.is_empty()
        || mount.target.contains("${")
        || mount.options.iter().any(|option| option == "ro")
        || volume.access_mode != AccessMode::Exclusive
        || !volume.create_if_missing
        || !volume.id.ends_with(WORKSPACE_PARAMETER)
        || volume.id.matches("${").count() != 1
        || !path.ends_with("/${workspace}")
        || path.matches("${").count() != 1
        || storage_domain.contains("${")
    {
        return Err(Error::Invalid(
            "Golden requires a writable exclusive directory Volume keyed by workspace".into(),
        ));
    }
    let params = Parameters::from([("workspace".into(), identity.into())]);
    let mut anonymous = volume.render(Some(&params)).map_err(Error::Invalid)?;
    anonymous.id = identity.into();
    Ok(anonymous)
}

pub(super) fn reserved_workspace(parameters: &Parameters) -> bool {
    parameters
        .get("workspace")
        .is_some_and(|value| value.starts_with(SEED_PREFIX))
}

pub(super) fn restore_digest(template: &TemplateVersion, effective_image: &str) -> Result<String> {
    let mut spec = ExecutionSpec::from(template);
    spec.image = effective_image.into();
    let normalized = serde_json::to_vec(&(TEMPLATE_HANDOFF_VERSION, spec, &template.volumes))
        .map_err(|_| Error::Invalid("cannot encode Golden template contract".into()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(normalized)))
}

impl Activator {
    pub(crate) fn seed_job(
        &self,
        tenant: &str,
        template: &TemplateVersion,
        effective_image: &str,
    ) -> Result<PreparationJob> {
        if template.isolation_runtime != "runsc" || template.readiness_probe.is_none() {
            return Err(Error::Invalid(
                "Golden requires runsc and an explicit readiness probe".into(),
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let identity = format!("{SEED_PREFIX}{id}");
        let volume = seed_volume(template, &identity)?;
        let params = Parameters::from([("workspace".into(), identity)]);
        let mut execution = ExecutionSpec::from(template);
        execution.image = effective_image.into();
        execution.env = template.render_env(&params).map_err(Error::Invalid)?;
        execution.lifecycle = Default::default();
        execution.mounts = template.render_mounts(&params).map_err(Error::Invalid)?;
        execution.mounts[0].volume_id = Some(volume.id.clone());
        self.environment.validate_execution(&execution)?;
        let recipe = PrewarmRecipe {
            snapshot_id: "pending".into(),
            image_digest: effective_image
                .rsplit_once('@')
                .map(|(_, digest)| digest.into()),
            restore_profile_digest: restore_digest(template, effective_image)?,
            handoff_contract_version: TEMPLATE_HANDOFF_VERSION.into(),
            restore_env: Default::default(),
        };
        recipe.validate(effective_image).map_err(Error::Invalid)?;
        let mut seed = SeedExecution {
            execution,
            volume,
            native_input: None,
        };
        seed.native_input = self.environment.freeze_preparation(tenant, &id, &seed)?;
        Ok(PreparationJob {
            resolved_snapshot: None,
            discard_reason: None,
            id,
            deadline_unix_ms: adx_agent_core::unix_time_millis()
                .saturating_add(PREPARATION_LIFETIME_MS),
            tenant: tenant.into(),
            template: template.name.clone(),
            version: template.version.clone(),
            seed,
            recipe,
        })
    }

    pub fn start_snapshot_preparations(self: &Arc<Self>) {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(service) = service.upgrade() else {
                    break;
                };
                // Count the scan for the embedded host's shutdown criterion
                // (see OperationScope).
                let _operation = crate::OperationScope::new();
                if let Err(error) = service.reconcile_snapshot_preparations().await {
                    tracing::warn!(%error, "Golden preparation scan incomplete");
                }
            }
        });
    }

    pub async fn reconcile_snapshot_preparations(&self) -> Result<()> {
        let mut cursor = None;
        loop {
            let jobs = self.state.preparation_jobs(cursor.as_deref()).await?;
            if jobs.is_empty() {
                return Ok(());
            }
            for (key, job) in jobs {
                cursor = Some(key);
                match tokio::time::timeout(PREPARATION_ATTEMPT_CAP, self.prepare_golden(&job)).await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(preparation_id = %job.id, tenant = %job.tenant,
                        template = %job.template, version = %job.version, %error, "Golden preparation will retry")
                    }
                    Err(_) => tracing::warn!(preparation_id = %job.id, tenant = %job.tenant,
                        template = %job.template, version = %job.version, "Golden preparation attempt timed out"),
                }
            }
        }
    }

    async fn prepare_golden(&self, job: &PreparationJob) -> Result<()> {
        if job.resolved_snapshot.is_some() {
            return self.finish_resolved_snapshot(job).await;
        }
        let expired = || adx_agent_core::transport::remaining_time(job.deadline_unix_ms).is_zero();
        let snapshot = match self
            .environment
            .prepare_snapshot(&job.tenant, &job.id, &job.seed, job.deadline_unix_ms)
            .await
        {
            Ok(SeedResult::Pending) => return Ok(()),
            Ok(SeedResult::Ready { snapshot_id }) => snapshot_id,
            Ok(SeedResult::Failed) => {
                let reason = if expired() {
                    adx_agent_store::PREPARATION_DEADLINE_EXCEEDED
                } else {
                    "Platform snapshot preparation failed"
                };
                return self
                    .state
                    .finish_preparation(job, Err(reason.into()))
                    .await
                    .map_err(Into::into);
            }
            Err(EnvironmentError::Invalid(_)) => {
                return self
                    .state
                    .finish_preparation(job, Err("Platform rejected the seed specification".into()))
                    .await
                    .map_err(Into::into)
            }
            Err(error) => return Err(error.into()),
        };
        let mut recipe = job.recipe.clone();
        recipe.snapshot_id.clone_from(&snapshot);
        let discard = if expired() {
            Some(adx_agent_store::PREPARATION_DEADLINE_EXCEEDED.into())
        } else if !self
            .environment
            .golden_available(
                &job.tenant,
                &snapshot,
                &job.seed.execution.image,
                &job.seed.execution.isolation_runtime,
            )
            .await?
        {
            Some("prepared Golden is not compatible".into())
        } else {
            recipe.validate(&job.seed.execution.image).err()
        };
        let discard = discard
            .or_else(|| expired().then(|| adx_agent_store::PREPARATION_DEADLINE_EXCEEDED.into()));
        let saved = self
            .state
            .reserve_recipe_owner(job, &snapshot, discard)
            .await?;
        self.finish_resolved_snapshot(&saved).await
    }

    async fn finish_resolved_snapshot(&self, job: &PreparationJob) -> Result<()> {
        let id = job
            .resolved_snapshot
            .as_ref()
            .ok_or_else(|| Error::Conflict("preparation has no resolved snapshot".into()))?;
        let result = if let Some(reason) = &job.discard_reason {
            match self
                .environment
                .update_snapshot_reference(
                    &job.tenant,
                    id,
                    &adx_agent_core::environment::SnapshotChange::Retire,
                )
                .await
            {
                Ok(()) | Err(EnvironmentError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
            Err(reason.clone())
        } else {
            let mut recipe = job.recipe.clone();
            recipe.snapshot_id.clone_from(id);
            self.register_recipe(job, recipe).await?
        };
        self.state
            .finish_preparation(job, result)
            .await
            .map_err(Into::into)
    }

    async fn register_recipe(
        &self,
        job: &PreparationJob,
        recipe: PrewarmRecipe,
    ) -> Result<std::result::Result<PrewarmRecipe, String>> {
        use adx_agent_core::environment::{SnapshotChange, SnapshotOwner};
        let owner = SnapshotOwner::Template {
            name: job.template.clone(),
            version: job.version.clone(),
        };
        // Durable jobs from an earlier handoff still need Catalog protection.
        match self
            .environment
            .update_snapshot_reference(
                &job.tenant,
                &recipe.snapshot_id,
                &SnapshotChange::Acquire { owner },
            )
            .await
        {
            Ok(()) => {}
            Err(EnvironmentError::Conflict(_) | EnvironmentError::NotFound) => {
                return Ok(Err("Golden snapshot is retiring".into()))
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Ok(recipe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_agent_core::environment::{
        CreateEnvironment, Environment, EnvironmentObservation, EnvironmentPhase,
    };
    use adx_agent_store::{AgentState, MemoryRepository};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_rustls::{rustls, TlsAcceptor};

    struct Platform {
        image: String,
    }

    #[async_trait::async_trait]
    impl Environment for Platform {
        async fn prepare_snapshot(
            &self,
            _: &str,
            _: &str,
            seed: &SeedExecution,
            _: u64,
        ) -> std::result::Result<SeedResult, EnvironmentError> {
            assert_eq!(seed.execution.image, self.image);
            Ok(SeedResult::Ready {
                snapshot_id: "golden".into(),
            })
        }

        async fn golden_available(
            &self,
            _: &str,
            id: &str,
            image: &str,
            runtime: &str,
        ) -> std::result::Result<bool, EnvironmentError> {
            Ok(id == "golden" && image == self.image && runtime == "runsc")
        }

        async fn update_snapshot_reference(
            &self,
            _: &str,
            _: &str,
            _: &adx_agent_core::environment::SnapshotChange,
        ) -> std::result::Result<(), EnvironmentError> {
            Ok(())
        }

        async fn snapshot_revision(
            &self,
            _: &str,
            _: &str,
        ) -> std::result::Result<u64, EnvironmentError> {
            Ok(1)
        }

        async fn ensure_volume(
            &self,
            _: &str,
            _: &str,
            spec: &VolumeSpec,
        ) -> std::result::Result<adx_agent_core::volumes::VolumeRef, EnvironmentError> {
            Ok(adx_agent_core::volumes::VolumeRef {
                id: spec.id.clone(),
                uid: "fixture-volume".into(),
            })
        }

        async fn create(
            &self,
            request: &CreateEnvironment,
        ) -> std::result::Result<EnvironmentObservation, EnvironmentError> {
            assert_eq!(request.execution.image, self.image);
            assert_eq!(request.execution.snapshot_id.as_deref(), Some("golden"));
            Ok(EnvironmentObservation {
                id: request.id.clone(),
                tenant: request.tenant.clone(),
                phase: EnvironmentPhase::Running,
                ready: true,
                runtime_id: None,
                message: None,
            })
        }

        async fn get(
            &self,
            _: &str,
            _: &str,
        ) -> std::result::Result<Option<EnvironmentObservation>, EnvironmentError> {
            Ok(None)
        }

        async fn delete(
            &self,
            _: &str,
            _: &str,
        ) -> std::result::Result<EnvironmentObservation, EnvironmentError> {
            Err(EnvironmentError::NotFound)
        }
    }

    #[tokio::test]
    async fn resolved_tag_keeps_the_public_input_and_selects_the_pinned_golden() {
        const CERT: &[u8] = include_bytes!("../../../gateway/tests/fixtures/ingress-cert.pem");
        const KEY: &[u8] = include_bytes!("../../../gateway/tests/fixtures/ingress-key.pem");
        let certs = rustls_pemfile::certs(&mut &CERT[..])
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut &KEY[..]).unwrap().unwrap();
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registry = listener.local_addr().unwrap().to_string();
        let tag = format!("{registry}/app:v1");
        let digest = format!("sha256:{}", "a".repeat(64));
        let image = format!("{registry}/app@{digest}");
        let response = format!("HTTP/1.1 200 OK\r\nDocker-Content-Digest: {digest}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = TlsAcceptor::from(Arc::new(tls))
                .accept(stream)
                .await
                .unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            assert_eq!(line, "HEAD /v2/app/manifests/v1 HTTP/1.1\r\n");
            loop {
                line.clear();
                if stream.read_line(&mut line).await.unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let state = AgentState::new(Arc::new(MemoryRepository::default()));
        let mut activator = Activator::new(
            state.clone(),
            Arc::new(Platform {
                image: image.clone(),
            }),
        );
        activator.image_resolver = Some(crate::image_resolution::ImageResolver::for_test_registry(
            registry, CERT,
        ));
        let draft = serde_json::from_value(serde_json::json!({
            "name":"app", "image":tag, "isolation_runtime":"runsc",
            "auto_prepare_snapshot":true, "resources":{"cpu_millis":1000,"memory_mib":512},
            "readiness_probe":{"http_get":{"path":"/ready","port":8080}},
            "mounts":[{"type":"bind","volume_id":"user-${workspace}","target":"/data","options":["rw"]}],
            "volumes":[{"id":"user-${workspace}","source":{"type":"directory","storage_domain":"sfs","path":"tenant/users/${workspace}"},
                "access_mode":"exclusive","create_if_missing":true}]
        })).unwrap();
        let version = activator
            .publish_with_id("tenant", &draft, Some("publish"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version.image, tag);
        activator.reconcile_snapshot_preparations().await.unwrap();
        let published = state
            .published_template("tenant", "app", "1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.template.image, tag);
        assert_eq!(published.execution().image, image);
        let recipe = published.template.prewarm_recipe.as_ref().unwrap();
        assert_eq!(recipe.image_digest.as_deref(), Some(digest.as_str()));
        assert_eq!(
            activator
                .select_template_golden(
                    "tenant",
                    &published,
                    &Parameters::new(),
                    adx_agent_core::unix_time_millis() + 5_000,
                )
                .await
                .unwrap()
                .as_deref(),
            Some("golden"),
        );
        let binding = adx_agent_core::Scope {
            tenant: "tenant".into(),
            template: "app".into(),
            template_ref: adx_agent_core::TemplateRef::Version("1".into()),
            binding_id: "golden".into(),
        };
        let target = activator
            .activate_with_parameters(
                &binding,
                None,
                adx_agent_core::unix_time_millis() + 5_000,
                false,
                Some(b"workspace=user"),
            )
            .await
            .unwrap();
        assert_eq!(
            target.binding.current.snapshot_id.as_deref(),
            Some("golden")
        );
        let replay = activator
            .publish_with_id("tenant", &draft, Some("publish"))
            .await
            .unwrap();
        assert_eq!(replay, published.template);
    }
}

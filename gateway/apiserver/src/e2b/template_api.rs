//! E2B template protocol projected onto common platform snapshot preparation.
use super::network::configure_envd_service;
use crate::{contract, http::Api};
use adx_core::snapshot_preparation::{PreparationPlan, PreparationStep};
use adx_observability::trace;
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::{Code, Status};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Registration {
    name: Option<String>,
    alias: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    cpu_count: Option<u32>,
    #[serde(rename = "memoryMB")]
    memory_mb: Option<u32>,
    min_free_disk_mb: Option<u64>,
    #[serde(rename = "teamID")]
    team_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Build {
    from_image: Option<String>,
    from_template: Option<String>,
    from_image_registry: Option<Value>,
    #[serde(default)]
    steps: Vec<BuildStep>,
    start_cmd: Option<String>,
    ready_cmd: Option<String>,
    #[serde(default)]
    force: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BuildStep {
    #[serde(rename = "type")]
    kind: String,
    args: Vec<String>,
    #[serde(default)]
    force: bool,
    files_hash: Option<String>,
    force_upload: Option<bool>,
}

fn unsupported(message: &'static str) -> Status {
    super::http::validation(super::ValidationError::unsupported(message))
}

/// Only the SDK default selector is accepted; the platform resolves a family.
pub(super) fn resolution_request(
    reference: &str,
    caller: &pb::CallerContext,
    registration_only: bool,
) -> Result<pb::ResolveSnapshotTemplateRequest, Status> {
    let (family, selector) = reference
        .split_once(':')
        .map_or((reference, None), |(family, selector)| {
            (family, Some(selector))
        });
    if family.trim().is_empty()
        || selector.is_some_and(|selector| selector.is_empty() || selector.contains(':'))
    {
        return Err(Status::invalid_argument("invalid template reference"));
    }
    if selector.is_some_and(|selector| selector != "default") {
        return Err(unsupported(
            "only the default template selector is supported",
        ));
    }
    Ok(pb::ResolveSnapshotTemplateRequest {
        caller: Some(caller.clone()),
        reference: family.into(),
        registration_only: registration_only && selector.is_none(),
    })
}

fn registration_input(
    input: Registration,
    caller: &pb::CallerContext,
) -> Result<(String, pb::Resources), Status> {
    if input
        .team_id
        .as_ref()
        .is_some_and(|tenant| tenant != &caller.tenant_id)
    {
        return Err(Status::permission_denied(
            "template team is outside API key scope",
        ));
    }
    if input.min_free_disk_mb.is_some_and(|size| size != 0) {
        return Err(unsupported("template disk resizing is not supported"));
    }
    let reference = input
        .name
        .or(input.alias)
        .ok_or_else(|| Status::invalid_argument("template name required"))?;
    let (name, selected) = reference
        .rsplit_once(':')
        .map(|(name, tag)| (name, Some(tag)))
        .unwrap_or((&reference, None));
    let name = adx_core::snapshot_preparation::normalize_name(name)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if selected.is_some_and(|tag| tag != "default") || input.tags.iter().any(|tag| tag != "default")
    {
        return Err(unsupported(
            "only the default template selector is supported",
        ));
    }
    let cpu = input.cpu_count.unwrap_or(2);
    let memory = input.memory_mb.unwrap_or(1024);
    if cpu == 0
        || cpu > 32
        || (cpu != 1 && !cpu.is_multiple_of(2))
        || memory < 128
        || !memory.is_multiple_of(2)
    {
        return Err(Status::invalid_argument(
            "template cpuCount or memoryMB is invalid",
        ));
    }
    Ok((
        name,
        pb::Resources {
            cpu_millis: u64::from(cpu) * 1000,
            memory_bytes: u64::from(memory) * 1_048_576,
            disk_bytes: 0,
        },
    ))
}

impl Build {
    fn image(&self) -> Result<&str, Status> {
        if self
            .from_template
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            || self.from_image_registry.is_some()
        {
            return Err(unsupported(
                "fromTemplate and private image registries are not supported",
            ));
        }
        // Every preparation runs afresh; force does not weaken or bypass any checks.
        let _ = self.force;
        for step in &self.steps {
            step.validate()?;
        }
        self.from_image
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Status::invalid_argument("fromImage required"))
    }

    async fn plan(
        &self,
        api: &Api,
        template: &str,
        build: &str,
        caller: &pb::CallerContext,
    ) -> Result<Option<PreparationPlan>, Status> {
        let start_command = self.start_cmd.clone().filter(|value| !value.is_empty());
        let ready_command = self.ready_cmd.clone().filter(|value| !value.is_empty());
        if self.steps.is_empty() && start_command.is_none() && ready_command.is_none() {
            return Ok(None);
        }
        let profile = api
            .clients
            .config
            .runtime_profile
            .as_ref()
            .ok_or_else(|| unsupported("runtime profile required"))?;
        let mut user = profile
            .env
            .get("ADX_ENVD_DEFAULT_USER")
            .cloned()
            .ok_or_else(|| unsupported("default build user missing"))?;
        let mut steps = vec![
            PreparationStep::User { user: user.clone() },
            PreparationStep::Workdir {
                path: profile
                    .env
                    .get("ADX_ENVD_DEFAULT_WORKDIR")
                    .cloned()
                    .ok_or_else(|| unsupported("default build directory missing"))?,
            },
        ];

        for step in &self.steps {
            step.validate()?;
            let args = &step.args;
            steps.push(match step.kind.as_str() {
                "RUN" => PreparationStep::Run {
                    command: args[0].clone(),
                    user: args.get(1).filter(|value| !value.is_empty()).cloned(),
                },
                "USER" => {
                    user = args[0].clone();
                    PreparationStep::User { user: user.clone() }
                }
                "WORKDIR" => PreparationStep::Workdir {
                    path: args[0].clone(),
                },
                "ENV" => PreparationStep::Environment {
                    values: args
                        .chunks_exact(2)
                        .map(|pair| (pair[0].clone(), pair[1].clone()))
                        .collect(),
                },
                "COPY" => {
                    let hash = step
                        .files_hash
                        .as_deref()
                        .ok_or_else(|| Status::invalid_argument("COPY filesHash required"))?;
                    // Validate every step before touching object storage. Replaced by a sealed object below.
                    let file = adx_core::snapshot_preparation::PreparationFile {
                        scope: "pending".into(),
                        location: format!("pending/{hash}"),
                        size_bytes: 1,
                        sha256: hash.to_owned(),
                    };
                    PreparationStep::Copy {
                        file,
                        source: copy_source_prefix(&args[0]),
                        destination: args[1].clone(),
                        user: Some(copy_owner(args.get(2).map(String::as_str), &user)),
                        mode: args
                            .get(3)
                            .filter(|value| !value.is_empty())
                            .map(|value| u32::from_str_radix(value, 8))
                            .transpose()
                            .map_err(|_| Status::invalid_argument("invalid COPY mode"))?,
                    }
                }
                _ => return Err(unsupported("template step is not supported")),
            });
        }
        let mut plan = PreparationPlan {
            steps,
            start_command,
            ready_command,
            timeout_seconds: 1800,
        };
        plan.validate().map_err(adx_protocol::status)?;
        if plan
            .steps
            .iter()
            .any(|v| matches!(v, PreparationStep::Copy { .. }))
        {
            let store = api
                .clients
                .config
                .preparation_files
                .as_ref()
                .ok_or_else(|| unsupported("COPY requires configured preparation file storage"))?
                .build()
                .map_err(adx_protocol::status)?;
            let mut sealed: std::collections::BTreeMap<
                String,
                adx_core::snapshot_preparation::PreparationFile,
            > = Default::default();
            for step in &mut plan.steps {
                if let PreparationStep::Copy { file, .. } = step {
                    let hash = file.sha256.clone();
                    let value = if let Some(value) = sealed.get(&hash) {
                        value.clone()
                    } else {
                        match store.seal(&caller.tenant_id, template, build, &hash).await {
                            Ok(value) => {
                                sealed.insert(hash.clone(), value.clone());
                                value
                            }
                            Err(error) => {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_secs(3),
                                    async {
                                        for value in sealed.values() {
                                            let _ = store.remove(&caller.tenant_id, value).await;
                                        }
                                    },
                                )
                                .await;
                                return Err(adx_protocol::status(error));
                            }
                        }
                    };
                    *file = value;
                }
            }
        }
        Ok(Some(plan))
    }
}

// E2B COPY uses the same-named group when only an owner is supplied.
fn copy_owner(owner: Option<&str>, current_user: &str) -> String {
    let owner = owner
        .filter(|value| !value.is_empty())
        .unwrap_or(current_user);
    if owner.contains(':') {
        owner.to_owned()
    } else {
        format!("{owner}:{owner}")
    }
}

// Upstream expands globs in the SDK, then copies the literal prefix of that selected archive.
fn copy_source_prefix(source: &str) -> String {
    let parts: Vec<_> = source
        .split('/')
        .take_while(|part| !part.contains(['*', '?', '[', '{']))
        .collect();
    let prefix = parts.join("/");
    if prefix.is_empty() {
        ".".into()
    } else {
        prefix
    }
}

impl BuildStep {
    fn validate(&self) -> Result<(), Status> {
        let _ = (self.force, self.force_upload);
        let valid = match self.kind.as_str() {
            "RUN" => (1..=2).contains(&self.args.len()),
            "COPY" => {
                (2..=4).contains(&self.args.len())
                    && self
                        .files_hash
                        .as_deref()
                        .is_some_and(adx_preparation_files::valid_files_hash)
            }
            "ENV" => !self.args.is_empty() && self.args.len().is_multiple_of(2),
            "USER" | "WORKDIR" => self.args.len() == 1,
            _ => return Err(unsupported("template step is not supported")),
        };
        if !valid || self.args.iter().any(|value| value.contains('\0')) {
            return Err(Status::invalid_argument("invalid template step arguments"));
        }
        let path = match self.kind.as_str() {
            "WORKDIR" => self.args.first(),
            "COPY" => self.args.get(1),
            _ => None,
        };
        if path.is_some_and(|value| value.contains(['$', '`'])) {
            return Err(unsupported("template path interpolation is not supported"));
        }
        Ok(())
    }
}

pub(super) async fn upload_link(
    api: &Api,
    template: &str,
    hash: &str,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    let registered = api
        .clients
        .rpc(
            "apiserver.resolve_upload_preparation",
            client.resolve_snapshot_template(trace::inject(pb::ResolveSnapshotTemplateRequest {
                caller: Some(caller.clone()),
                reference: template.into(),
                registration_only: true,
            })),
        )
        .await?;
    if registered.phase != pb::SnapshotPreparationPhase::Registered as i32 {
        return Err(Status::failed_precondition(
            "template preparation already started",
        ));
    }
    let config = api
        .clients
        .config
        .preparation_files
        .as_ref()
        .ok_or_else(|| unsupported("COPY requires configured preparation file storage"))?;
    let url = config
        .build()
        .map_err(adx_protocol::status)?
        .upload_url(
            &caller.tenant_id,
            &registered.template_id,
            &registered.preparation_id,
            hash,
        )
        .await
        .map_err(adx_protocol::status)?;
    Ok(json!({"present":false,"url":url,"headers":{}}))
}

pub(super) async fn register(
    api: &Api,
    input: Registration,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let (name, resources) = registration_input(input, caller)?;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    let value = api
        .clients
        .rpc(
            "apiserver.register_snapshot_preparation",
            client.register_snapshot_preparation(trace::inject(
                pb::RegisterSnapshotPreparationRequest {
                    caller: Some(caller.clone()),
                    name,
                    resources: Some(resources),
                },
            )),
        )
        .await?;
    Ok(
        json!({"templateID":value.template_id,"buildID":value.preparation_id,"public":false,"names":[value.name.clone()],"aliases":[value.name],"tags":["default"]}),
    )
}

async fn get(
    api: &Api,
    template: &str,
    build: &str,
    caller: &pb::CallerContext,
) -> Result<pb::SnapshotPreparationView, Status> {
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    api.clients
        .rpc(
            "apiserver.get_snapshot_preparation",
            client.get_snapshot_preparation(trace::inject(pb::GetSnapshotPreparationRequest {
                caller: Some(caller.clone()),
                template_id: template.into(),
                preparation_id: build.into(),
            })),
        )
        .await
}

pub(super) async fn start(
    api: &Api,
    template: &str,
    build: &str,
    input: Build,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let image = match input.image() {
        Ok(image) => image,
        Err(error) => return Err(error),
    };
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    let current = get(api, template, build, caller).await?;
    if current.phase != pb::SnapshotPreparationPhase::Registered as i32 {
        return Err(Status::invalid_argument(
            "snapshot preparation has already started",
        ));
    }
    let plan = input.plan(api, template, build, caller).await?;
    let resources = current
        .resources
        .ok_or_else(|| Status::data_loss("template resources missing"))?;
    let memory_mb = resources.memory_bytes / 1_048_576;
    let mut spec = contract::create_spec_with_environment(
        json!({
            "namespace":"golden", "name":build, "image":image,
            "cpu":resources.cpu_millis, "memory":memory_mb,
            "idleTimeoutSeconds":0, "inheritEntrypoint":false,
        }),
        caller,
        api.clients.config.runtime_profile.as_ref(),
    )?;
    if !config.required_node_labels.is_empty() {
        let scheduling = spec.scheduling.get_or_insert_with(Default::default);
        if scheduling.required_node.is_empty() {
            scheduling.required_node.push(Default::default());
        }
        for selector in &mut scheduling.required_node {
            selector
                .match_labels
                .extend(config.required_node_labels.clone());
        }
    }
    if plan.is_some() {
        // A prepared build may start an application before capture. Reuse private
        // routing; Golden capture removes this temporary SDK route identity.
        spec.compatibility = Some(pb::EnvironmentCompatibility {
            template_id: template.into(),
            envd_access_token: uuid::Uuid::new_v4().to_string(),
            traffic_access_token: uuid::Uuid::new_v4().to_string(),
            allow_public_traffic: false,
        });
    }
    configure_envd_service(
        &mut spec,
        pb::EnvdServiceConfig {
            version: config.envd_version.clone(),
            gateway_auth: true,
            access_token: None,
            ingress_sources: config.envd_ingress_sources.clone(),
        },
    )?;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    api.clients
        .rpc(
            "apiserver.start_snapshot_preparation",
            client.start_snapshot_preparation(trace::inject(pb::StartSnapshotPreparationRequest {
                plan: plan.map(Into::into),
                caller: Some(caller.clone()),
                template_id: template.into(),
                preparation_id: build.into(),
                spec: Some(spec),
            })),
        )
        .await?;
    Ok(Value::Null)
}

fn sdk_status(phase: i32) -> Result<&'static str, Status> {
    use pb::SnapshotPreparationPhase as Phase;
    match Phase::try_from(phase) {
        Ok(Phase::Registered) => Ok("waiting"),
        Ok(Phase::Creating | Phase::Preparing | Phase::Snapshotting | Phase::Cleaning) => {
            Ok("building")
        }
        Ok(Phase::Ready) => Ok("ready"),
        Ok(Phase::Failed) => Ok("error"),
        _ => Err(Status::data_loss("invalid snapshot preparation phase")),
    }
}

fn status_value_at(value: pb::SnapshotPreparationView, offset: u64) -> Result<Value, Status> {
    let mut response = json!({"templateID":value.template_id,"buildID":value.preparation_id,
        "status":sdk_status(value.phase)?,"logs":[],"logEntries":[]});
    if let Some(preparation) = value.source_preparation {
        let logs: Vec<_> = preparation
            .logs
            .into_iter()
            .filter(|v| v.offset >= offset)
            .take(100)
            .map(|entry| {
                let timestamp = i64::try_from(entry.timestamp_unix_ms)
                    .ok()
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map(|v| v.to_rfc3339())
                    .unwrap_or_default();
                json!({"timestamp":timestamp,"level":entry.level,"message":entry.message})
            })
            .collect();
        response["logs"] = json!(logs
            .iter()
            .map(|v| v["message"].clone())
            .collect::<Vec<_>>());
        response["logEntries"] = json!(logs);
    }
    if !value.error.is_empty() {
        response["reason"] = json!({"message":value.error});
    }
    Ok(response)
}

pub(super) async fn status(
    api: &Api,
    template: &str,
    build: &str,
    caller: &pb::CallerContext,
    query: &str,
) -> Result<Value, Status> {
    let offset = url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "logsOffset")
        .map(|(_, value)| value.parse::<u64>())
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid logsOffset"))?
        .unwrap_or(0);
    let view = get(api, template, build, caller).await?;
    if matches!(
        pb::SnapshotPreparationPhase::try_from(view.phase),
        Ok(pb::SnapshotPreparationPhase::Ready | pb::SnapshotPreparationPhase::Failed)
    ) {
        if let Some(config) = &api.clients.config.preparation_files {
            if let Ok(store) = config.build() {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    store.cleanup(&caller.tenant_id, template, build),
                )
                .await;
            }
        }
    }
    status_value_at(view, offset)
}

#[derive(Debug)]
struct DetailQuery {
    limit: usize,
    cursor: String,
}

fn detail_query(query: &str) -> Result<DetailQuery, Status> {
    let mut result = DetailQuery {
        limit: 100,
        cursor: String::new(),
    };
    let mut seen = std::collections::BTreeSet::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if !seen.insert(key.to_string()) {
            return Err(Status::invalid_argument("duplicate template detail query"));
        }
        match key.as_ref() {
            "limit" => {
                result.limit = value
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid limit"))?;
                if !(1..=100).contains(&result.limit) {
                    return Err(Status::invalid_argument("invalid limit"));
                }
            }
            "nextToken" => result.cursor = value.into_owned(),
            _ => return Err(Status::invalid_argument("unknown template detail query")),
        }
    }
    Ok(result)
}

fn detail_resolution_request(
    id: &str,
    caller: &pb::CallerContext,
    registration_only: bool,
) -> Result<pb::ResolveSnapshotTemplateRequest, Status> {
    if !adx_core::snapshot_preparation::valid_preparation_id(id) {
        return Err(Status::invalid_argument("invalid template ID"));
    }
    Ok(pb::ResolveSnapshotTemplateRequest {
        caller: Some(caller.clone()),
        reference: id.into(),
        registration_only,
    })
}

fn retained_success(
    result: Result<pb::SnapshotPreparationView, Status>,
) -> Result<Option<pb::SnapshotPreparationView>, Status> {
    match result {
        Ok(value) => Ok(Some(value)),
        // No usable success must not hide an owned, existing registration.
        Err(error) if error.code() == Code::FailedPrecondition => Ok(None),
        Err(error) => Err(error),
    }
}

fn detail_resources(
    response: &mut Value,
    value: &pb::SnapshotPreparationView,
) -> Result<(), Status> {
    let resources = value
        .resources
        .as_ref()
        .ok_or_else(|| Status::data_loss("template resources missing"))?;
    response["cpuCount"] = json!(resources.cpu_millis / 1000);
    response["memoryMB"] = json!(resources.memory_bytes / 1_048_576);
    response["diskSizeMB"] = json!(resources.disk_bytes / 1_048_576);
    if !value.envd_version.is_empty() {
        response["envdVersion"] = json!(value.envd_version);
    }
    Ok(())
}

fn detail_build_status(value: &pb::SnapshotPreparationView) -> Result<&'static str, Status> {
    if value.phase == pb::SnapshotPreparationPhase::Ready as i32 && !value.snapshot_available {
        return Ok("error");
    }
    sdk_status(value.phase)
}

fn detail_timestamp(timestamp: u64) -> Result<String, Status> {
    if timestamp == 0 {
        return Err(Status::failed_precondition(
            "historical template metadata missing; create a new template family",
        ));
    }
    i64::try_from(timestamp)
        .ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .ok_or_else(|| Status::data_loss("invalid template timestamp"))
}

fn detail_value(
    current: pb::SnapshotPreparationView,
    success: Option<pb::SnapshotPreparationView>,
    query: DetailQuery,
) -> Result<Value, Status> {
    if current.template_id.is_empty() || current.preparation_id.is_empty() {
        return Err(Status::data_loss("template detail identity missing"));
    }
    if success.as_ref().is_some_and(|value| {
        value.template_id != current.template_id
            || value.preparation_id.is_empty()
            || value.phase != pb::SnapshotPreparationPhase::Ready as i32
            || !value.snapshot_available
    }) {
        return Err(Status::data_loss("invalid successful template preparation"));
    }
    let created_at = detail_timestamp(current.created_at_unix_ms)?;
    let updated_at = detail_timestamp(current.template_updated_at_unix_ms)?;
    let spawn_count = current.spawn_count.ok_or_else(|| {
        Status::failed_precondition(
            "historical template metadata missing; create a new template family",
        )
    })?;
    let last_spawned_at = current
        .last_spawned_at_unix_ms
        .map(detail_timestamp)
        .transpose()?;
    if spawn_count > 0 && last_spawned_at.is_none() {
        return Err(Status::failed_precondition(
            "historical template metadata missing; create a new template family",
        ));
    }
    if spawn_count == 0 && last_spawned_at.is_some() {
        return Err(Status::data_loss("inconsistent template usage metadata"));
    }
    let selected = success.as_ref().unwrap_or(&current);
    let mut response = json!({
        "templateID": current.template_id,
        "public": false,
        "names": [current.name.clone()],
        "aliases": [current.name],
        "createdAt": created_at,
        "updatedAt": updated_at,
        "spawnCount": spawn_count,
        "lastSpawnedAt": last_spawned_at,
        "buildStatus": detail_build_status(selected)?,
    });
    detail_resources(&mut response, selected)?;
    if let Some(success) = &success {
        response["buildID"] = json!(success.preparation_id);
    }

    // The platform retains only current and latest usable success, not build history.
    let mut retained = vec![&current];
    if let Some(success) = success
        .as_ref()
        .filter(|value| value.preparation_id != current.preparation_id)
    {
        retained.push(success);
    }
    for build in &retained {
        detail_timestamp(build.preparation_created_at_unix_ms)?;
        detail_timestamp(build.preparation_updated_at_unix_ms)?;
    }
    let offset = if query.cursor.is_empty() {
        0
    } else {
        retained
            .iter()
            .position(|value| value.preparation_id == query.cursor)
            .map(|index| index + 1)
            .ok_or_else(|| Status::invalid_argument("invalid or stale template build cursor"))?
    };
    let end = (offset + query.limit).min(retained.len());
    let builds = retained[offset..end]
        .iter()
        .map(|value| {
            let mut build = json!({
                "buildID": value.preparation_id,
                "status": detail_build_status(value)?,
                "createdAt": detail_timestamp(value.preparation_created_at_unix_ms)?,
                "updatedAt": detail_timestamp(value.preparation_updated_at_unix_ms)?,
            });
            detail_resources(&mut build, value)?;
            Ok(build)
        })
        .collect::<Result<Vec<_>, Status>>()?;
    response["builds"] = json!(builds);
    if end < retained.len() {
        // Validated only after resolving the caller-owned family; no persisted cursor state.
        response["nextToken"] = json!(retained[end - 1].preparation_id);
    }
    Ok(response)
}

/// Project only retained preparations; missing or foreign families remain NotFound.
pub(super) async fn detail(
    api: &Api,
    id: &str,
    query: &str,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let query = detail_query(query)?;
    let current_request = detail_resolution_request(id, caller, true)?;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    let current = api
        .clients
        .rpc(
            "apiserver.template_detail_registration",
            client.resolve_snapshot_template(trace::inject(current_request)),
        )
        .await?;
    if current.template_id != id {
        return Err(Status::not_found("snapshot template not found"));
    }
    let success_request = detail_resolution_request(id, caller, false)?;
    let success = retained_success(
        api.clients
            .rpc(
                "apiserver.template_detail_success",
                client.resolve_snapshot_template(trace::inject(success_request)),
            )
            .await,
    )?;
    detail_value(current, success, query)
}

/// A missing platform template can still be an existing E2B runtime snapshot ID.
pub(super) async fn delete(
    api: &Api,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<bool, Status> {
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    match api
        .clients
        .rpc(
            "apiserver.delete_snapshot_template",
            client.delete_snapshot_template(trace::inject(pb::DeleteSnapshotTemplateRequest {
                caller: Some(caller.clone()),
                template_id: id.into(),
                require_unused: true,
            })),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.code() == Code::NotFound => Ok(false),
        // Upstream template deletion reports active/paused references as HTTP 400.
        Err(error) if error.code() == Code::FailedPrecondition => {
            Err(Status::invalid_argument(error.message()))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE_ID: &str = "00000000-0000-4000-8000-000000000001";
    const CURRENT_ID: &str = "00000000-0000-4000-8000-000000000002";
    const SUCCESS_ID: &str = "00000000-0000-4000-8000-000000000003";

    fn detail_view(id: &str, phase: pb::SnapshotPreparationPhase) -> pb::SnapshotPreparationView {
        pb::SnapshotPreparationView {
            template_id: TEMPLATE_ID.into(),
            name: "demo".into(),
            preparation_id: id.into(),
            phase: phase as i32,
            resources: Some(pb::Resources {
                cpu_millis: 2000,
                memory_bytes: 1024 * 1_048_576,
                disk_bytes: 64 * 1_048_576,
            }),
            envd_version: "0.9.0".into(),
            created_at_unix_ms: 1000,
            template_updated_at_unix_ms: 5000,
            preparation_created_at_unix_ms: 2000,
            preparation_updated_at_unix_ms: 3000,
            spawn_count: Some(0),
            snapshot_available: phase == pb::SnapshotPreparationPhase::Ready,
            ..Default::default()
        }
    }

    #[test]
    fn sdk_copy_globs_use_the_selected_archive_prefix() {
        assert_eq!(copy_source_prefix("src/**/*.py"), "src");
        assert_eq!(copy_source_prefix("*.py"), ".");
        assert_eq!(copy_source_prefix("src/file.py"), "src/file.py");
    }

    #[test]
    fn sdk_copy_owner_defaults_include_the_same_named_group() {
        for (owner, expected) in [
            (None, "builder:builder"),
            (Some(""), "builder:builder"),
            (Some("65534"), "65534:65534"),
            (Some("user:other"), "user:other"),
        ] {
            assert_eq!(copy_owner(owner, "builder"), expected);
        }
    }

    #[test]
    fn interpolated_build_paths_are_rejected_before_source_creation() {
        for step in [
            json!({"type":"WORKDIR", "args":["$APP_DIR"]}),
            json!({"type":"COPY", "args":["src", "${APP_DIR}/"], "filesHash":"a".repeat(64)}),
        ] {
            let step: BuildStep = serde_json::from_value(step).unwrap();
            let error = step.validate().unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert!(error.message().contains("interpolation"));
        }
    }

    #[test]
    fn terminal_build_log_pages_are_drained_once() {
        let value = pb::SnapshotPreparationView {
            phase: pb::SnapshotPreparationPhase::Ready as i32,
            source_preparation: Some(pb::SourcePreparationResult {
                logs: (0..120)
                    .map(|offset| pb::PreparationLogEntry {
                        offset,
                        timestamp_unix_ms: 1,
                        level: "info".into(),
                        message: format!("step {offset}"),
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            status_value_at(value.clone(), 0).unwrap()["logEntries"]
                .as_array()
                .unwrap()
                .len(),
            100
        );
        assert_eq!(
            status_value_at(value.clone(), 100).unwrap()["logEntries"]
                .as_array()
                .unwrap()
                .len(),
            20
        );
        assert!(status_value_at(value, 120).unwrap()["logEntries"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn detail_does_not_publish_a_missing_snapshot_as_ready() {
        let mut current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Ready);
        current.snapshot_available = false;
        let value = detail_value(current, None, detail_query("").unwrap()).unwrap();
        assert_eq!(value["buildStatus"], "error");
        assert_eq!(value["builds"][0]["status"], "error");
        assert!(value.get("buildID").is_none());
    }

    #[test]
    fn detail_query_rejects_unknown_duplicate_and_invalid_pagination() {
        let query = detail_query("").unwrap();
        assert_eq!(query.limit, 100);
        assert!(query.cursor.is_empty());
        for query in ["limit=1", "limit=100", "nextToken="] {
            assert!(detail_query(query).is_ok(), "{query}");
        }
        for query in [
            "limit=0",
            "limit=101",
            "limit=-1",
            "limit=abc",
            "limit=",
            "limit=1&limit=2",
            "nextToken=a&nextToken=b",
            "teamID=other",
            "name=demo",
        ] {
            assert_eq!(
                detail_query(query).unwrap_err().code(),
                Code::InvalidArgument
            );
        }
    }

    #[test]
    fn detail_resolution_accepts_only_family_ids_and_keeps_caller_scope() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        };
        for registration_only in [true, false] {
            let request =
                detail_resolution_request(TEMPLATE_ID, &caller, registration_only).unwrap();
            assert_eq!(request.reference, TEMPLATE_ID);
            assert_eq!(request.caller, Some(caller.clone()));
            assert_eq!(request.registration_only, registration_only);
        }
        for reference in [
            "",
            "demo",
            "demo:default",
            "00000000-0000-4000-8000-000000000001:default",
        ] {
            assert_eq!(
                detail_resolution_request(reference, &caller, true)
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
    }

    #[test]
    fn detail_projects_current_first_and_successful_defaults_without_fabricated_history() {
        let mut current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Failed);
        current.resources.as_mut().unwrap().cpu_millis = 4000;
        current.envd_version = "next".into();
        current.preparation_created_at_unix_ms = 4000;
        current.preparation_updated_at_unix_ms = 5000;
        let success = detail_view(SUCCESS_ID, pb::SnapshotPreparationPhase::Ready);
        let value = detail_value(current, Some(success), detail_query("").unwrap()).unwrap();
        assert_eq!(value["templateID"], TEMPLATE_ID);
        assert_eq!(value["public"], false);
        assert_eq!(value["aliases"], json!(["demo"]));
        assert_eq!(value["names"], json!(["demo"]));
        assert_eq!(value["createdAt"], "1970-01-01T00:00:01.000Z");
        assert_eq!(value["buildID"], SUCCESS_ID);
        assert_eq!(value["buildStatus"], "ready");
        assert_eq!(value["cpuCount"], 2);
        assert_eq!(value["memoryMB"], 1024);
        assert_eq!(value["diskSizeMB"], 64);
        assert_eq!(value["envdVersion"], "0.9.0");
        assert_eq!(value["builds"][0]["buildID"], CURRENT_ID);
        assert_eq!(value["builds"][0]["status"], "error");
        assert_eq!(value["builds"][0]["cpuCount"], 4);
        assert_eq!(value["builds"][0]["createdAt"], "1970-01-01T00:00:04.000Z");
        assert_eq!(value["builds"][0]["updatedAt"], "1970-01-01T00:00:05.000Z");
        assert_eq!(value["builds"][1]["buildID"], SUCCESS_ID);
        assert_eq!(value["builds"][1]["status"], "ready");
        assert_eq!(value["builds"][1]["createdAt"], "1970-01-01T00:00:02.000Z");
        assert_eq!(value["builds"][1]["updatedAt"], "1970-01-01T00:00:03.000Z");
        assert!(value.get("nextToken").is_none());
        assert_eq!(value["spawnCount"], 0);
        assert!(value.get("lastSpawnedAt").unwrap().is_null());
        assert_eq!(value["updatedAt"], "1970-01-01T00:00:05.000Z");
        for build in value["builds"].as_array().unwrap() {
            for unavailable in ["finishedAt", "buildDuration"] {
                assert!(build.get(unavailable).is_none());
            }
        }
    }

    #[test]
    fn detail_without_success_preserves_registration_and_omits_unavailable_envd() {
        let mut current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Registered);
        current.envd_version.clear();
        let value = detail_value(current, None, detail_query("").unwrap()).unwrap();
        assert_eq!(value["buildStatus"], "waiting");
        assert_eq!(value["cpuCount"], 2);
        assert_eq!(value["builds"].as_array().unwrap().len(), 1);
        assert!(value.get("buildID").is_none());
        assert!(value.get("envdVersion").is_none());
        assert!(value["builds"][0].get("envdVersion").is_none());
        assert!(
            retained_success(Err(Status::failed_precondition("no ready preparation")))
                .unwrap()
                .is_none()
        );
        for code in [Code::NotFound, Code::PermissionDenied, Code::Unavailable] {
            assert_eq!(
                retained_success(Err(Status::new(code, "failure")))
                    .unwrap_err()
                    .code(),
                code
            );
        }
    }

    #[test]
    fn detail_deduplicates_the_current_success_and_pages_retained_builds() {
        let current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Ready);
        let value = detail_value(
            current.clone(),
            Some(current),
            detail_query("limit=1").unwrap(),
        )
        .unwrap();
        assert_eq!(value["builds"].as_array().unwrap().len(), 1);
        assert!(value.get("nextToken").is_none());

        let project = |query: &str| {
            detail_value(
                detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Creating),
                Some(detail_view(SUCCESS_ID, pb::SnapshotPreparationPhase::Ready)),
                detail_query(query).unwrap(),
            )
        };
        let first = project("limit=1").unwrap();
        assert_eq!(first["buildID"], SUCCESS_ID);
        assert_eq!(first["buildStatus"], "ready");
        assert_eq!(first["builds"][0]["buildID"], CURRENT_ID);
        assert_eq!(first["builds"][0]["status"], "building");
        assert_eq!(first["nextToken"], CURRENT_ID);
        let next = project(&format!("limit=1&nextToken={CURRENT_ID}")).unwrap();
        assert_eq!(next["builds"][0]["buildID"], SUCCESS_ID);
        assert_eq!(next["builds"][0]["status"], "ready");
        assert!(next.get("nextToken").is_none());
        let end = project(&format!("nextToken={SUCCESS_ID}")).unwrap();
        assert_eq!(end["builds"], json!([]));
        assert!(end.get("nextToken").is_none());
        for cursor in ["stale-build", TEMPLATE_ID, "foreign-build"] {
            assert_eq!(
                project(&format!("nextToken={cursor}")).unwrap_err().code(),
                Code::InvalidArgument
            );
        }
    }

    #[test]
    fn detail_rejects_unknown_history_and_inconsistent_usage() {
        for missing in 0..7 {
            let mut current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Failed);
            let mut success = detail_view(SUCCESS_ID, pb::SnapshotPreparationPhase::Ready);
            match missing {
                0 => current.created_at_unix_ms = 0,
                1 => current.template_updated_at_unix_ms = 0,
                2 => current.preparation_created_at_unix_ms = 0,
                3 => current.preparation_updated_at_unix_ms = 0,
                4 => current.spawn_count = None,
                5 => current.spawn_count = Some(1),
                _ => success.preparation_created_at_unix_ms = 0,
            }
            let error =
                detail_value(current, Some(success), detail_query("limit=1").unwrap()).unwrap_err();
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert!(error
                .message()
                .contains("historical template metadata missing"));
        }
        let mut current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Ready);
        current.last_spawned_at_unix_ms = Some(4000);
        assert_eq!(
            detail_value(current.clone(), None, detail_query("").unwrap())
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
        current.spawn_count = Some(2);
        let value = detail_value(current, None, detail_query("").unwrap()).unwrap();
        assert_eq!(value["spawnCount"], 2);
        assert_eq!(value["lastSpawnedAt"], "1970-01-01T00:00:04.000Z");
    }

    #[test]
    fn detail_rejects_inconsistent_or_unrepresentable_platform_values() {
        let current = detail_view(CURRENT_ID, pb::SnapshotPreparationPhase::Registered);
        let mut missing = current.clone();
        missing.resources = None;
        let mut timestamp = current.clone();
        timestamp.created_at_unix_ms = u64::MAX;
        for value in [missing, timestamp] {
            assert_eq!(
                detail_value(value, None, detail_query("").unwrap())
                    .unwrap_err()
                    .code(),
                Code::DataLoss
            );
        }
        let mut foreign = detail_view(SUCCESS_ID, pb::SnapshotPreparationPhase::Ready);
        foreign.template_id = "another-family".into();
        assert_eq!(
            detail_value(current, Some(foreign), detail_query("").unwrap())
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
    }

    #[test]
    fn sdk_references_accept_only_default_and_reject_other_selectors() {
        let caller = pb::CallerContext::default();
        for reference in [
            "demo",
            "demo:default",
            "550e8400-e29b-41d4-a716-446655440010:default",
        ] {
            let request = resolution_request(reference, &caller, false).unwrap();
            assert!(!request.registration_only);
            assert!(!request.reference.contains(':'));
        }
        assert!(
            resolution_request("demo", &caller, true)
                .unwrap()
                .registration_only
        );
        assert!(
            !resolution_request("demo:default", &caller, true)
                .unwrap()
                .registration_only
        );
        for reference in ["demo:v1", "demo:550e8400-e29b-41d4-a716-446655440011"] {
            let error = resolution_request(reference, &caller, false).unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
        for invalid in ["", ":default", "demo:", "demo:a:b"] {
            assert!(resolution_request(invalid, &caller, false).is_err());
        }
    }

    #[test]
    fn common_preparation_phases_map_to_sdk_build_states_only_at_the_adapter() {
        use pb::SnapshotPreparationPhase as Phase;
        for (phase, expected) in [
            (Phase::Registered, "waiting"),
            (Phase::Creating, "building"),
            (Phase::Snapshotting, "building"),
            (Phase::Cleaning, "building"),
            (Phase::Ready, "ready"),
            (Phase::Failed, "error"),
        ] {
            assert_eq!(sdk_status(phase as i32).unwrap(), expected);
        }
        assert!(sdk_status(Phase::Unspecified as i32).is_err());
        assert!(sdk_status(99).is_err());
    }

    #[test]
    fn dockerfile_recipe_accepts_ordered_steps_and_startup_commands() {
        let input: Build = serde_json::from_value(json!({
            "fromImage": "python:3.11",
            "steps": [
                {"type":"USER", "args":["root"], "force":false},
                {"type":"WORKDIR", "args":["/app"], "force":false},
                {"type":"RUN", "args":["python -c 'print(42)'"], "force":false}
            ],
            "startCmd": "python /app/app.py",
            "readyCmd": "test -f /tmp/app-ready"
        }))
        .unwrap();
        assert_eq!(input.image().unwrap(), "python:3.11");
    }

    #[test]
    fn from_image_accepts_empty_default_steps_and_rejects_unimplemented_operations() {
        let input: Build =
            serde_json::from_value(json!({"fromImage":"python:3.11","steps":[],"force":false}))
                .unwrap();
        assert_eq!(input.image().unwrap(), "python:3.11");
        for extra in [
            json!({"fromTemplate":"parent"}),
            json!({"fromImageRegistry":{}}),
        ] {
            let mut input = json!({"fromImage":"python:3.11"});
            input
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let error = serde_json::from_value::<Build>(input)
                .unwrap()
                .image()
                .unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
    }

    #[test]
    fn registration_accepts_only_default_and_preserves_tenant_scope() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        };
        for settings in [
            json!({"name":" Demo "}),
            json!({"name":"demo:default"}),
            json!({"name":"demo","tags":["default"]}),
        ] {
            let (name, resources) =
                registration_input(serde_json::from_value(settings).unwrap(), &caller).unwrap();
            assert_eq!(name, "demo");
            assert_eq!(resources.cpu_millis, 2000);
            assert_eq!(resources.memory_bytes, 1024 * 1_048_576);
        }
        for settings in [
            json!({"name":"demo:v1"}),
            json!({"name":"demo","tags":["release"]}),
        ] {
            let error =
                registration_input(serde_json::from_value(settings).unwrap(), &caller).unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
        for invalid in [
            json!({"name":"demo","cpuCount":3}),
            json!({"name":"demo","cpuCount":33}),
            json!({"name":"demo","memoryMB":129}),
        ] {
            assert_eq!(
                registration_input(serde_json::from_value(invalid).unwrap(), &caller)
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
        let input = serde_json::from_value(json!({"name":"demo","teamID":"other"})).unwrap();
        assert_eq!(
            registration_input(input, &caller).unwrap_err().code(),
            Code::PermissionDenied
        );
    }
}

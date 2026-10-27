//! HTTP framing only; Platform owns policy, replay and lifecycle decisions.
use super::*;
use adx_core::prewarm::{management::*, PoolPolicy, PreparationProfile};
use hyper::header::{HeaderMap, HeaderValue};

const POOLS: &str = "/api/environment/v1/prewarm-pools";
const OPERATIONS: &str = "/api/environment/v1/prewarm-operations/";

pub(super) fn matches(path: &str) -> bool {
    path == POOLS
        || path.starts_with(&format!("{POOLS}/"))
        || path.starts_with(OPERATIONS)
        || agent_path(path).is_some()
}

fn agent_path(path: &str) -> Option<(&str, &str, &str)> {
    let parts: Vec<_> = path.split('/').collect();
    match parts.as_slice() {
        ["", "api", "agent", "v2", "templates", name, "versions", version, "prewarm", rest @ ..] => {
            Some((
                name,
                version,
                if rest.is_empty() {
                    ""
                } else {
                    path.splitn(10, '/').nth(9).unwrap_or("")
                },
            ))
        }
        _ => None,
    }
}

#[allow(clippy::result_large_err)] // HTTP validation shares the API Server's native Tonic error contract.
fn condition(headers: &HeaderMap) -> Result<Condition, Status> {
    if headers.get_all("if-match").iter().count() > 1
        || headers.get_all("if-none-match").iter().count() > 1
    {
        return Err(Status::invalid_argument(
            "one conditional header is required",
        ));
    }
    match (headers.get("if-match"), headers.get("if-none-match")) {
        (None, None) => Ok(Condition::Missing),
        (None, Some(value)) if value == "*" => Ok(Condition::Absent),
        (Some(value), None) => {
            let raw = value
                .to_str()
                .map_err(|_| Status::invalid_argument("invalid ETag"))?;
            let revision = raw
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .and_then(|v| v.strip_prefix("revision-"))
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .ok_or_else(|| Status::invalid_argument("expected a strong revision ETag"))?;
            if raw != format!("\"revision-{revision}\"") {
                return Err(Status::invalid_argument("noncanonical revision ETag"));
            }
            Ok(Condition::Revision(revision))
        }
        _ => Err(Status::invalid_argument(
            "conflicting or unsupported conditional headers",
        )),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Put {
    profile: PreparationProfile,
    policy: PoolPolicy,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionInput {
    action: Action,
}

impl Api {
    async fn preparation_template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<adx_agent_core::TemplateVersion, Status> {
        let managed = self
            .agent_templates
            .get_or_try_init(|| async {
                let path = std::env::var_os("ADX_AGENT_CONFIG")
                    .ok_or_else(|| Status::unavailable("Agent configuration unavailable"))?;
                let bytes = tokio::fs::read(path)
                    .await
                    .map_err(|_| Status::unavailable("Agent configuration unavailable"))?;
                let config: data_plane_gateway::ingress::agent_api::AgentConfig =
                    serde_json::from_slice(&bytes)
                        .map_err(|_| Status::unavailable("invalid Agent configuration"))?;
                let agent = if let Some(settings) = config.embedded.as_ref() {
                    let environment = adx_activator::local_environment::LocalEnvironment::new(
                        self.environment_service.clone(),
                        self.clients.config.timeout(),
                        settings.sandbox_network.clone(),
                    )
                    .map_err(|_| Status::unavailable("Agent Environment capability unavailable"))?;
                    data_plane_gateway::ingress::agent_api::AgentApi::new_embedded(
                        config,
                        Arc::new(environment),
                    )
                    .await
                } else {
                    data_plane_gateway::ingress::agent_api::AgentApi::new(config)
                }
                .map_err(|_| Status::unavailable("Agent template capability unavailable"))?;
                Ok::<_, Status>(agent.managed)
            })
            .await?;
        managed
            .template(
                &adx_agent_api::request::RequestContext::new(self.clients.config.timeout()),
                tenant,
                name,
                version,
            )
            .await
            .map_err(|e| match e {
                adx_agent_api::Error::NotFound => Status::not_found("template not found"),
                adx_agent_api::Error::Invalid(_) => {
                    Status::invalid_argument("invalid template identity")
                }
                _ => Status::unavailable("template service unavailable"),
            })
    }
    pub(super) async fn prewarm(
        &self,
        request: Request<Incoming>,
        caller: &pb::CallerContext,
        path: &str,
    ) -> Response<Body> {
        let request_id = RequestContext::from_headers(request.headers()).request_id;
        if !caller.administrator {
            return error_response(
                Status::permission_denied("prewarm management requires administrator"),
                &request_id,
                None,
                None,
                false,
            );
        }
        let mut write = false;
        let result = async {
            let mut compiled_profile = None;
            let request_path = path.to_owned();
            let mut required_pool = None;
            let mut platform_path = path.to_owned();
            if let Some((name,version,action)) = agent_path(path) {
                identifier(name)?;identifier(version)?;
                let template = self.preparation_template(&caller.tenant_id,name,version).await?;
                let context = adx_agent_core::environment::PreparationContext::for_template(&caller.tenant_id,&template).map_err(|_|Status::invalid_argument("invalid template preparation context"))?;
                let digest = adx_core::prewarm::fingerprint(&(&caller.tenant_id,name,version)).map_err(adx_protocol::status)?;
                let pool_id = format!("template-{}",digest.trim_start_matches("sha256:"));
                required_pool = Some(pool_id.clone());
                platform_path = if let Some(operation) = action.strip_prefix("operations/") { format!("{OPERATIONS}{operation}") }
                    else {format!("{POOLS}/{pool_id}{}",if action.is_empty(){String::new()}else{format!("/{action}")})};
                let architecture = match std::env::consts::ARCH { "x86_64"=>"amd64","aarch64"=>"arm64",_=>return Err(Status::unimplemented("unsupported preparation architecture")) };
                let profile = adx_agent_core::environment::CompiledPreparationProfile::for_template(&caller.tenant_id,&template,architecture,
                    adx_core::prewarm::fingerprint(&self.clients.config.runtime_profile).map_err(adx_protocol::status)?)
                    .map_err(|_|Status::invalid_argument("template cannot compile a preparation profile"))?;
                let value = serde_json::to_value(profile).map_err(|_|Status::internal("invalid template profile"))?;
                let profile:PreparationProfile = serde_json::from_value(value).map_err(|_|Status::internal("Agent/Platform preparation profile contract mismatch"))?;
                if profile.source_ref.template_digest != context.source_ref.template_digest { return Err(Status::internal("template digest mismatch")); }
                compiled_profile = Some(profile);
            }
            let path = platform_path.as_str();
            if request.uri().query().is_some() && !path.ends_with("/artifacts") { return Err(Status::invalid_argument("query parameters are not supported for this view")); }
            let method = request.method().clone();
            let caller = Some(caller.clone());
            let channel = self.clients.coordinator().await?;
            let mut client = pb::prewarm_service_client::PrewarmServiceClient::new(channel);
            if method == hyper::Method::GET && path == POOLS {
                let data = client.list_pools(trace::inject(pb::PrewarmQuery { caller, id: String::new() })).await?.into_inner();
                return response(200, data, None);
            }
            if let Some(id) = path.strip_prefix(OPERATIONS) {
                identifier(id)?;
                if method != hyper::Method::GET { return Err(Status::not_found("unknown prewarm endpoint")); }
                let data = client.get_operation(trace::inject(pb::PrewarmQuery { caller, id: id.into() })).await?.into_inner();
                if let Some(pool) = &required_pool {
                    let operation:ManagementOperation = serde_json::from_slice(&data.json).map_err(|_|Status::data_loss("invalid operation response"))?;
                    if &operation.pool_id != pool { return Err(Status::not_found("operation not found in template scope")); }
                }
                return response(200, data, None);
            }
            let rest = path.strip_prefix(&format!("{POOLS}/")).ok_or_else(|| Status::not_found("unknown prewarm endpoint"))?;
            let (id, action) = rest.split_once('/').unwrap_or((rest, ""));
            identifier(id)?;
            if method == hyper::Method::GET && action == "artifacts" {
                let mut query=pb::PrewarmArtifactsQuery {caller,pool_id:id.into(),page_size:50,page_token:String::new(),state:String::new()};
                let mut seen=std::collections::BTreeSet::new();
                for (key,value) in url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes()) {
                    if !seen.insert(key.to_string()) {return Err(Status::invalid_argument("duplicate artifact pagination parameter"));}
                    match key.as_ref() {
                        "page_size"=>{query.page_size=value.parse().map_err(|_|Status::invalid_argument("invalid page_size"))?;if !(1..=100).contains(&query.page_size){return Err(Status::invalid_argument("page_size must be 1-100"));}},
                        "page_token"=>query.page_token=value.into_owned(),"state"=>query.state=value.into_owned(),
                        _=>return Err(Status::invalid_argument("unknown artifact pagination parameter")),
                    }
                }
                let data=client.list_artifacts(trace::inject(query)).await?.into_inner();return response(200,data,None);
            }
            if method == hyper::Method::GET {
                if !matches!(action, "" | "status" | "slots" | "artifacts") { return Err(Status::not_found("unknown prewarm view")); }
                let mut data = client.get_pool(trace::inject(pb::PrewarmQuery { caller, id: id.into() })).await?.into_inner();
                if !action.is_empty() {
                    let pool: PoolRecord = serde_json::from_slice(&data.json).map_err(|_| Status::data_loss("invalid prewarm pool response"))?;
                    let mut inventory = std::collections::BTreeMap::<String,u64>::new();
                    for state in ["preparing","ready","reserved","attaching","quarantined","retiring","live"] {inventory.insert(state.into(),0);}
                    let mut idle_bytes = 0u64;
                    for artifact in &pool.artifacts {
                        if !artifact.counts_as_live() {continue;}
                        *inventory.entry("live".into()).or_default() += 1;
                        let state = serde_json::to_value(artifact.state).map_err(|_|Status::internal("invalid artifact state"))?;
                        if let Some(state) = state.as_str() {*inventory.entry(state.into()).or_default() += 1;}
                        idle_bytes = idle_bytes.checked_add(artifact.size_bytes).ok_or_else(||Status::data_loss("artifact byte ledger overflow"))?;
                    }
                    let ready = pool.artifacts.iter().filter(|a|a.claimable(now_seconds(),&pool.policy)).count();
                    let value = match action {
                        "slots" => return Err(Status::unimplemented("runtime slot preparation is unavailable")),
                        "artifacts" => json!({"items":pool.artifacts, "next_page_token":null}),
                        _ => json!({"pool_id":pool.pool_id,"phase":pool.phase,"desired_revision":pool.revision,
                            "observed_revision":pool.observed_revision,"observed_at":pool.observed_at,
                            "inventory":inventory,"idle_disk_bytes":idle_bytes,"claimable":ready,
                            "conditions":[{"type":"BackendQualified","status":pool.policy.enabled || !pool.artifacts.is_empty(),"reason":if pool.policy.enabled || !pool.artifacts.is_empty(){"QualifiedErofsContract"}else{"DisabledOrUnqualified"}},
                                {"type":"CapacitySatisfied","status":ready >= pool.policy.capacity.min_ready as usize,"reason":if ready >= pool.policy.capacity.min_ready as usize{"Satisfied"}else{"RefillingOrUnavailable"}}],
                            "unresolved_operations":pool.artifacts.iter().filter(|a|matches!(a.state,adx_core::prewarm::SlotState::Preparing|adx_core::prewarm::SlotState::Quarantined|adx_core::prewarm::SlotState::Retiring)).count()}),
                    };
                    data.json = serde_json::to_vec(&value).map_err(|_| Status::internal("invalid prewarm response"))?;
                }
                return response(200, data, None);
            }
            let condition = condition(request.headers())?;
            let logical_id = request.headers().get("x-adx-request-id").and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
            if request.headers().get_all("x-adx-request-id").iter().count() != 1 || logical_id.is_empty() || logical_id.len() > 256 {
                return Err(Status::invalid_argument("one X-ADX-Request-ID of 1-256 bytes is required"));
            }
            let input = body(request.into_body(), 65_536).await?;
            let command = match (method.as_str(), action) {
                ("PUT", "") => {
                    if let Some(profile) = compiled_profile {
                        let policy:PoolPolicy = serde_json::from_value(input).map_err(|_|Status::invalid_argument("expected strict prewarm policy"))?;
                        Command::Put {profile:Box::new(profile),policy}
                    } else { let input: Put = serde_json::from_value(input).map_err(|_| Status::invalid_argument("expected strict profile and policy"))?;
                        Command::Put { profile: Box::new(input.profile), policy: input.policy } }
                },
                ("POST", "actions") => { let input: ActionInput = serde_json::from_value(input).map_err(|_| Status::invalid_argument("expected prewarm action"))?;
                    Command::Action { action: input.action } },
                ("DELETE", "") if input.is_null() || input == json!({}) => Command::Delete,
                _ => return Err(Status::invalid_argument("unsupported prewarm method, action or payload")),
            };
            let mutation = Mutation { pool_id: id.into(), request_id: logical_id, condition, command, request_path };
            let json = serde_json::to_vec(&mutation).map_err(|_| Status::internal("invalid prewarm mutation"))?;
            write = true;
            let data = client.mutate_pool(trace::inject(pb::PrewarmMutationRequest { caller, mutation_json: json, contract_version: 1 })).await?.into_inner();
            let accepted: Accepted = serde_json::from_slice(&data.json).map_err(|_| Status::data_loss("invalid prewarm acceptance"))?;
            response(202, data, Some(format!("{OPERATIONS}{}", accepted.operation.operation_id)))
        }.await;
        match result {
            Ok(response) => response,
            Err(error) => {
                let override_status = match error
                    .metadata()
                    .get("adx-prewarm-error")
                    .and_then(|v| v.to_str().ok())
                {
                    Some("precondition_required") => Some(StatusCode::PRECONDITION_REQUIRED),
                    Some("precondition_failed") => Some(StatusCode::PRECONDITION_FAILED),
                    _ => None,
                };
                let mut response = error_response(error, &request_id, None, None, write);
                if let Some(status) = override_status {
                    *response.status_mut() = status;
                }
                response
            }
        }
    }
}
#[allow(clippy::result_large_err)] // Keep the domain-to-HTTP status mapping consistent with other routes.
fn identifier(id: &str) -> Result<(), Status> {
    adx_core::prewarm::validate_identifier(id).map_err(adx_protocol::status)
}
fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |v| v.as_secs())
}
#[allow(clippy::result_large_err)] // This HTTP response adapter preserves the shared Tonic status type.
fn response(
    status: u16,
    data: pb::PrewarmView,
    location: Option<String>,
) -> Result<Response<Body>, Status> {
    let value: Value = serde_json::from_slice(&data.json)
        .map_err(|_| Status::data_loss("invalid prewarm response"))?;
    let mut response = plain(status, value);
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    if data.revision > 0 {
        response.headers_mut().insert(
            "etag",
            HeaderValue::from_str(&format!("\"revision-{}\"", data.revision))
                .map_err(|_| Status::internal("invalid revision"))?,
        );
    }
    if let Some(location) = location {
        response.headers_mut().insert(
            "location",
            HeaderValue::from_str(&location)
                .map_err(|_| Status::internal("invalid operation location"))?,
        );
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conditional_headers_require_one_strong_revision_or_absent_marker() {
        let mut headers = HeaderMap::new();
        assert_eq!(condition(&headers).unwrap(), Condition::Missing);
        headers.insert("if-none-match", HeaderValue::from_static("*"));
        assert_eq!(condition(&headers).unwrap(), Condition::Absent);
        headers.insert("if-match", HeaderValue::from_static("\"revision-7\""));
        assert!(condition(&headers).is_err());
        headers.remove("if-none-match");
        assert_eq!(condition(&headers).unwrap(), Condition::Revision(7));
        for bad in [
            "*",
            "7",
            "W/\"revision-7\"",
            "\"revision-0\"",
            "\"revision-18446744073709551616\"",
        ] {
            headers.insert("if-match", HeaderValue::from_str(bad).unwrap());
            assert!(condition(&headers).is_err(), "{bad}");
        }
    }
}

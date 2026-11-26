use adx_agent_core::{AgentBinding, Scope};
use serde_json::Value;

/// Add narrow latest-version configuration summaries; failed rows remain available without a summary.
pub async fn template_summaries(clients:&crate::clients::Clients,endpoint:&crate::config::Endpoint,tenant:&crate::config::Tenant,catalog:&mut Value) {
    use futures_util::{stream,StreamExt};
    let Some(items)=catalog["templates"].as_array_mut() else {return;};
    let queries:Vec<_>=items.iter().enumerate().map(|(index,item)|(index,item["name"].as_str().map(str::to_owned),item["latest_version"].as_str().map(str::to_owned))).collect();
    let summaries:Vec<_>=stream::iter(queries).map(|(index,name,version)|async move {
        let summary=async {
            let name=name?;let version=version?;
            let url=crate::clients::Clients::url(&endpoint.base_url,&["templates",&name,"versions",&version]).ok()?;
            let value=clients.json(tenant,reqwest::Method::GET,url,None,endpoint.timeout_ms).await.ok()?;
            let template=&value["template"];
            if template["name"].as_str()!=Some(&name)||template["version"].as_str()!=Some(&version){return None;}
            let cpu=template["resources"]["cpu_millis"].as_u64()?;
            let memory=template["resources"]["memory_mib"].as_u64()?;
            let services=match template.get("service") {None=>0,Some(value)=>value.as_array()?.len()};
            Some(serde_json::json!({"cpu_millis":cpu,"memory_mib":memory,"service_count":services}))
        }.await;
        (index,summary.unwrap_or(Value::Null))
    }).buffer_unordered(8).collect().await;
    for (index,summary) in summaries {items[index]["configuration_summary"]=summary;}
}

pub struct OperationContext<'a> {
    pub target: &'a crate::authorization::AuthorizedTenant,
    pub actor: &'a crate::session::SessionRecord,
}

pub fn resume_result(wire: Value, expected_id: &str) -> Result<Value, &'static str> {
    use base64::Engine;
    let encoded = wire.get("data").and_then(Value::as_str).ok_or("upstream_response_invalid")?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_| "upstream_response_invalid")?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| "upstream_response_invalid")?;
    if value["environmentId"].as_str() != Some(expected_id) { return Err("environment_context_conflict"); }
    let status = value.get("state").and_then(Value::as_str).ok_or("upstream_response_invalid")?;
    Ok(serde_json::json!({"environment_id":expected_id,"status":status}))
}

pub fn resume_rejection(wire: Value, id: &str) -> Value {
    let (kind,message) = match wire["error"]["code"].as_str() {
        Some("INVALID_ARGUMENT") => ("invalid_argument","恢复参数无效"),
        Some("UNAUTHENTICATED") => ("unauthenticated","上游认证失败"),
        Some("PERMISSION_DENIED") => ("permission_denied","平台拒绝访问目标环境"),
        Some("NOT_FOUND") => ("not_found","目标环境不存在"),
        Some("CONFLICT") => ("conflict","平台条件不满足，当前环境无法恢复"),
        Some("OUTCOME_UNKNOWN") => ("outcome_unknown","恢复结果未知，请重新查询原环境"),
        Some("DEADLINE_EXCEEDED") => ("deadline_exceeded","平台恢复请求超时，请重新查询原环境"),
        Some("RESOURCE_EXHAUSTED") => ("resource_exhausted","平台资源不足"),
        Some("UNSUPPORTED") => ("unsupported","平台不支持恢复操作"),
        Some("UNAVAILABLE") => ("unavailable","平台暂不可用"),
        Some("DATA_LOSS") => ("data_loss","平台记录不完整"),
        _ => ("wake_rejected","平台拒绝恢复，请刷新原环境核实"),
    };
    let outcome = match wire["error"]["outcome"].as_str() { Some("not_started") => "not_started", Some("terminal") => "terminal", _ => "unknown" };
    serde_json::json!({"environment_id":id,"error":{"kind":kind,"message":message,"outcome":outcome}})
}

pub fn wake_precondition(binding: &AgentBinding, scope: &Scope, generation: &str, slot: &str, id: &str, status: &str) -> Result<(), &'static str> {
    check_context(binding,scope,generation,Some((slot,id)))?;
    match status {
        "absent" => Err("environment_absent"),
        "unknown" => Err("environment_status_unknown"),
        _ => Ok(()),
    }
}

pub fn check_context(binding: &AgentBinding, scope: &Scope, generation: &str, environment: Option<(&str, &str)>) -> Result<(), &'static str> {
    if &binding.scope != scope { return Err("binding_scope_mismatch"); }
    if binding.generation != generation { return Err("generation_conflict"); }
    if let Some((slot, expected)) = environment {
        let actual = match slot {
            "current" => Some(&binding.current.environment_id),
            "secondary" => binding.secondary.as_ref().map(|value| &value.environment.environment_id),
            _ => None,
        };
        if actual.map(String::as_str) != Some(expected) { return Err("environment_context_conflict"); }
    }
    Ok(())
}
pub fn sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    if key == "next_page_token" { return false; }
    ["secret", "password", "token", "api_key", "apikey", "credential", "private_key"].iter().any(|part| key.contains(part))
}
pub fn redact(mut value: Value) -> Value {
    match &mut value {
        Value::Object(items) => { for (key, item) in items { *item = if sensitive(key) { Value::String("********".into()) } else { redact(item.take()) }; } },
        Value::Array(items) => { for item in items { *item = redact(item.take()); } },
        _ => {},
    }
    value
}

/// Restore only explicitly retained masked fields from the selected immutable snapshot.
pub fn restore_secrets(original: &Value, mut edited: Value) -> Result<Value, &'static str> {
    match &mut edited {
        Value::Object(items) => {
            for (key,value) in items {
                let old = original.get(key).unwrap_or(&Value::Null);
                if sensitive(key) && value.as_str() == Some("********") {
                    if old.is_null() { return Err("secret_source_missing"); }
                    *value = old.clone();
                } else { *value = restore_secrets(old,value.take())?; }
            }
        }
        Value::Array(items) => {
            for (index,value) in items.iter_mut().enumerate() {
                *value = restore_secrets(original.get(index).unwrap_or(&Value::Null),value.take())?;
            }
        }
        _ => {},
    }
    Ok(edited)
}

/// Publish through the existing API; the Agent allocates the immutable version.
pub async fn publish_version(clients: &crate::clients::Clients, endpoint: &crate::config::Endpoint,
    tenant: &crate::config::Tenant, context: OperationContext<'_>,
    name: &str, draft: Value, source_version: Option<&str>) -> Result<Value, crate::clients::UpstreamError> {
    let OperationContext { target, actor } = context;
    use crate::{audit::{audit, AuditEvent, AuditTarget, Outcome}, clients::UpstreamError};
    use reqwest::{Method, StatusCode};
    let result = async {
        if tenant.tenant_id != target.tenant_id { return Err(UpstreamError { status:StatusCode::FORBIDDEN,kind:"tenant_forbidden" }); }
        adx_agent_core::identifier(name,"template").map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_template" })?;
        let original = if let Some(version) = source_version {
            adx_agent_core::version_number(version).map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_version" })?;
            let url = crate::clients::Clients::url(&endpoint.base_url,&["templates",name,"versions",version])?;
            let value = clients.json(tenant,Method::GET,url,None,endpoint.timeout_ms).await?;
            let snapshot: adx_agent_core::TemplateVersion = serde_json::from_value(value.get("template").cloned().unwrap_or(Value::Null))
                .map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" })?;
            if snapshot.name != name || snapshot.version != version { return Err(UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" }); }
            serde_json::to_value(adx_agent_core::TemplateDraft::from(&snapshot))
                .map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" })?
        } else { Value::Null };
        let restored = restore_secrets(&original,draft).map_err(|kind| UpstreamError { status:StatusCode::BAD_REQUEST,kind })?;
        let draft: adx_agent_core::TemplateDraft = serde_json::from_value(restored)
            .map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_template" })?;
        if draft.name != name { return Err(UpstreamError { status:StatusCode::BAD_REQUEST,kind:"template_name_mismatch" }); }
        draft.validate().map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_template" })?;
        let body = serde_json::to_value(draft).map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_template" })?;
        let url = crate::clients::Clients::url(&endpoint.base_url,&["templates"])?;
        clients.json(tenant,Method::POST,url,Some(&body),endpoint.timeout_ms).await
    }.await;
    let outcome = match &result { Ok(_) => Outcome::Success, Err(error) if error.kind == "upstream_outcome_unknown" => Outcome::Unknown, Err(_) => Outcome::Failure };
    audit(AuditEvent::new(actor,&target.tenant_id,"console.template.publish",AuditTarget { template:Some(name.into()),..Default::default() },outcome));
    result.map(redact)
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AliasInput {
    pub seed: String,
    pub versions: Vec<adx_agent_core::alias::VersionWeight>,
    pub expected_revision: Option<String>,
}
/// Validate the page revision and seed, then use the existing upstream revision CAS.
pub async fn update_alias(clients: &crate::clients::Clients, endpoint: &crate::config::Endpoint,
    tenant: &crate::config::Tenant, context: OperationContext<'_>,
    template: &str, alias: &str, input: &AliasInput) -> Result<Value, crate::clients::UpstreamError> {
    let OperationContext { target, actor } = context;
    use crate::{audit::{audit, AuditEvent, AuditTarget, Outcome}, clients::UpstreamError};
    use reqwest::{Method,StatusCode};
    let result = async {
        if tenant.tenant_id != target.tenant_id { return Err(UpstreamError { status:StatusCode::FORBIDDEN,kind:"tenant_forbidden" }); }
        let config = adx_agent_core::alias::AliasConfig { tenant:target.tenant_id.clone(),template:template.into(),alias:alias.into(),seed:input.seed.clone(),versions:input.versions.clone() };
        config.validate().map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_alias" })?;
        let url = crate::clients::Clients::url(&endpoint.base_url,&["templates",template,"aliases",alias])?;
        match clients.json(tenant,Method::GET,url.clone(),None,endpoint.timeout_ms).await {
            Ok(value) => {
                let snapshot: adx_agent_core::activator::AliasSnapshot = serde_json::from_value(value.get("alias").cloned().unwrap_or(Value::Null))
                    .map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" })?;
                if snapshot.config.tenant != target.tenant_id || snapshot.config.template != template || snapshot.config.alias != alias {
                    return Err(UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" });
                }
                if input.expected_revision.as_deref() != Some(snapshot.revision.as_str()) { return Err(UpstreamError { status:StatusCode::CONFLICT,kind:"revision_conflict" }); }
                if input.seed != snapshot.config.seed { return Err(UpstreamError { status:StatusCode::CONFLICT,kind:"seed_immutable" }); }
            }
            Err(error) if error.status == StatusCode::NOT_FOUND && input.expected_revision.is_none() => {},
            Err(error) => return Err(error),
        }
        for entry in &input.versions {
            let version_url = crate::clients::Clients::url(&endpoint.base_url,&["templates",template,"versions",&entry.version])?;
            let value = clients.json(tenant,Method::GET,version_url,None,endpoint.timeout_ms).await?;
            if value["template"]["name"].as_str() != Some(template) || value["template"]["version"].as_str() != Some(entry.version.as_str()) {
                return Err(UpstreamError { status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid" });
            }
        }
        let body = serde_json::to_value(input).map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST,kind:"invalid_alias" })?;
        clients.json(tenant,Method::PUT,url,Some(&body),endpoint.timeout_ms).await
    }.await;
    let outcome = match &result { Ok(_) => Outcome::Success, Err(error) if error.kind == "upstream_outcome_unknown" => Outcome::Unknown, Err(_) => Outcome::Failure };
    audit(AuditEvent::new(actor,&target.tenant_id,"console.alias.update",AuditTarget { template:Some(template.into()),ref_kind:Some("alias".into()),reference:Some(alias.into()),..Default::default() },outcome));
    result.map(redact)
}

/// Query only the public instance view; upstream failures never prove absence.
pub async fn environment_status(clients: &crate::clients::Clients, endpoint: &crate::config::Endpoint,
    tenant: &crate::config::Tenant, id: &str, version: &str) -> Value {
    let unknown = || serde_json::json!({"environment_id":id,"version":version,"status":"unknown","error":"实例状态查询失败，请重新查询"});
    let Ok(mut url) = crate::clients::Clients::url(&endpoint.base_url, &["api","instances"]) else { return unknown(); };
    url.query_pairs_mut().append_pair("instance_id",id);
    match clients.json(tenant, reqwest::Method::GET, url, None, endpoint.timeout_ms).await {
        Err(error) if error.status == reqwest::StatusCode::NOT_FOUND => serde_json::json!({"environment_id":id,"version":version,"status":"absent"}),
        Err(_) => unknown(),
        Ok(value) => {
            let Some(detail) = value.as_array().filter(|items| items.len() == 1).and_then(|items| items.first()) else { return unknown(); };
            if detail.get("id").and_then(Value::as_str) != Some(id) || detail.get("tenant_id").and_then(Value::as_str) != Some(tenant.tenant_id.as_str()) { return unknown(); }
            let Some(status) = detail.get("status").and_then(Value::as_str) else { return unknown(); };
            serde_json::json!({"environment_id":id,"version":version,"status":status,"detail":redact(detail.clone())})
        }
    }
}

/// Re-read the exact scope before deletion. This is not an atomic generation CAS.
pub async fn delete_binding(clients: &crate::clients::Clients, endpoint: &crate::config::Endpoint,
    tenant: &crate::config::Tenant, target: &crate::authorization::AuthorizedTenant,
    actor: &crate::session::SessionRecord, scope: &Scope, generation: &str) -> Result<Value, crate::clients::UpstreamError> {
    use crate::{audit::{audit, AuditEvent, AuditTarget, Outcome}, clients::UpstreamError};
    use reqwest::{Method, StatusCode};
    let result = async {
        if scope.tenant != target.tenant_id || tenant.tenant_id != target.tenant_id {
            return Err(UpstreamError { status:StatusCode::FORBIDDEN, kind:"tenant_forbidden" });
        }
        scope.validate().map_err(|_| UpstreamError { status:StatusCode::BAD_REQUEST, kind:"invalid_scope" })?;
        let kind = match scope.template_ref { adx_agent_core::TemplateRef::Version(_) => "versions", adx_agent_core::TemplateRef::Alias(_) => "aliases" };
        let url = crate::clients::Clients::url(&endpoint.base_url, &["templates", &scope.template, kind, scope.template_ref.name(), "bindings", &scope.binding_id])?;
        let value = clients.json(tenant, Method::GET, url.clone(), None, endpoint.timeout_ms).await?;
        let binding: AgentBinding = serde_json::from_value(value.get("binding").cloned().unwrap_or(Value::Null))
            .map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY, kind:"upstream_response_invalid" })?;
        check_context(&binding, scope, generation, None)
            .map_err(|kind| UpstreamError { status:StatusCode::CONFLICT, kind })?;
        clients.json(tenant, Method::DELETE, url, None, endpoint.timeout_ms).await
    }.await;
    let outcome = match &result {
        Ok(_) => Outcome::Success,
        Err(error) if error.kind == "upstream_outcome_unknown" => Outcome::Unknown,
        Err(_) => Outcome::Failure,
    };
    audit(AuditEvent::new(actor, &target.tenant_id, "console.binding.delete", AuditTarget {
        template:Some(scope.template.clone()), ref_kind:Some(scope.template_ref.kind().into()),
        reference:Some(scope.template_ref.name().into()), binding_id:Some(scope.binding_id.clone()), generation:Some(generation.into()),..Default::default()
    }, outcome));
    match result {
        Err(error) if error.kind == "upstream_outcome_unknown" => Ok(serde_json::json!({
            "status":"unknown", "message":"删除结果未知，请按 Binding ID 重新查询"
        })),
        result => result,
    }
}

use crate::{config::{ConsoleConfig, Tenant, TenantType}, session::{LoginInput, Role, SessionRecord, SessionRegistry}};
use axum::{extract::{Path, State}, http::{header, HeaderMap, HeaderValue, StatusCode}, middleware::{self, Next}, response::{IntoResponse, Response}, routing::get, Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;

const COOKIE: &str = "adx_console_session";
struct App { config: ConsoleConfig, sessions: SessionRegistry, clients: crate::clients::Clients, profiles: Vec<crate::terminal::Profile> }
type Shared = Arc<App>;
struct Failure(StatusCode, &'static str, &'static str);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":{"kind":self.1,"message":self.2}}))).into_response()
    }
}
fn authenticated(app: &App, headers: &HeaderMap) -> Result<SessionRecord, Failure> {
    let mut cookies = headers.get_all(header::COOKIE).iter().filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';')).filter_map(|pair| pair.trim().split_once('='))
        .filter_map(|(key, value)| (key == COOKIE).then_some(value));
    let cookie = cookies.next().filter(|_| cookies.next().is_none());
    cookie.and_then(|value| app.sessions.verify(value)).ok_or(Failure(StatusCode::UNAUTHORIZED, "unauthenticated", "未登录或会话已过期"))
}
fn target<'a>(app: &'a App, session: &SessionRecord, id: &str) -> Result<&'a Tenant, Failure> {
    if session.role == Role::TenantAdmin && session.tenant_id.as_deref() != Some(id) {
        return Err(Failure(StatusCode::FORBIDDEN, "tenant_forbidden", "无权访问该租户"));
    }
    app.config.tenants.iter().find(|tenant| tenant.tenant_id == id)
        .ok_or(Failure(StatusCode::NOT_FOUND, "tenant_not_found", "租户不存在"))
        .and_then(|tenant| {
            crate::authorization::authorize_tenant(session, id, tenant.tenant_type)
                .map_err(|kind| Failure(StatusCode::FORBIDDEN, kind, "无权访问该租户"))?;
            Ok(tenant)
        })
}
fn view(tenant: &Tenant) -> Value {
    json!({"tenant_id":tenant.tenant_id,"display_name":tenant.display_name,"type":tenant.tenant_type,
        "modules":{"agent":true,"business":tenant.tenant_type == TenantType::Digitalmate}})
}
fn session_view(session: &SessionRecord) -> Value {
    let landing = session.tenant_id.as_ref().map_or_else(|| "#/tenants".into(), |tenant| {
        let encoded: String = url::form_urlencoded::byte_serialize(tenant.as_bytes()).collect();
        format!("#/tenants/{}/templates", encoded.replace('+', "%20"))
    });
    json!({"subject_id":session.subject_id,"role":session.role,"tenant_id":session.tenant_id,
        "expires_at":session.expires_at,"landing_path":landing})
}
fn check_origin(app: &App, headers: &HeaderMap) -> Result<(), Failure> {
    let mut origins = headers.get_all(header::ORIGIN).iter();
    if origins.next().and_then(|value| value.to_str().ok()) != Some(app.config.public_origin.as_str()) || origins.next().is_some() {
        return Err(Failure(StatusCode::FORBIDDEN, "origin_forbidden", "请求来源无效"));
    }
    Ok(())
}
async fn login(State(app): State<Shared>, headers: HeaderMap, input: Result<Json<LoginInput>, axum::extract::rejection::JsonRejection>) -> Result<Response, Failure> {
    check_origin(&app, &headers)?;
    let invalid = || Failure(StatusCode::UNAUTHORIZED, "invalid_credentials", "身份或管理凭据无效");
    let Json(input) = input.map_err(|_| invalid())?;
    let (session, cookie) = app.sessions.login(&input).map_err(|_| invalid())?;
    let cookie = HeaderValue::from_str(&format!("{COOKIE}={cookie}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={}", app.config.session_ttl_seconds)).map_err(|_| invalid())?;
    Ok(([(header::SET_COOKIE, cookie)], Json(session_view(&session))).into_response())
}
async fn get_session(State(app): State<Shared>, headers: HeaderMap) -> Result<Json<Value>, Failure> {
    Ok(Json(session_view(&authenticated(&app, &headers)?)))
}
async fn logout(State(app): State<Shared>, headers: HeaderMap) -> Result<Response, Failure> {
    check_origin(&app, &headers)?;
    app.sessions.logout(&authenticated(&app, &headers)?.session_id);
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, "adx_console_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0")]).into_response())
}
async fn tenants(State(app): State<Shared>, headers: HeaderMap,axum::extract::RawQuery(raw):axum::extract::RawQuery) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let mut filters=std::collections::BTreeMap::new();
    for(key,value)in url::form_urlencoded::parse(raw.as_deref().unwrap_or_default().as_bytes()){if !matches!(key.as_ref(),"name"|"tenant_id"|"type"|"page_size"|"page_token")||filters.insert(key.into_owned(),value.into_owned()).is_some(){return Err(Failure(StatusCode::BAD_REQUEST,"invalid_query","租户查询无效"));}}
    let size=filters.get("page_size").map_or(Ok(50),|v|v.parse::<usize>()).ok().filter(|v|*v>0&&*v<=200).ok_or(Failure(StatusCode::BAD_REQUEST,"invalid_query","分页大小无效"))?;
    let mut items:Vec<_>=app.config.tenants.iter().filter(|tenant|session.role==Role::PlatformAdmin||session.tenant_id.as_ref()==Some(&tenant.tenant_id)).map(view).filter(|tenant|filters.iter().all(|(key,value)|match key.as_str(){"tenant_id"=>tenant[key]==*value,"name"=>tenant["display_name"].as_str().is_some_and(|name|name.contains(value)),"type"=>tenant[key]==*value,_=>true})).collect();
    items.sort_by(|a,b|a["tenant_id"].as_str().cmp(&b["tenant_id"].as_str()));let total=items.len();
    use base64::{Engine,engine::general_purpose::URL_SAFE_NO_PAD};use sha2::{Digest,Sha256};
    let context=hex::encode(Sha256::digest(serde_json::to_vec(&(&session.subject_id,session.role,&session.tenant_id,filters.iter().filter(|(key,_)|key.as_str()!="page_token").collect::<Vec<_>>())).map_err(|_|Failure(StatusCode::BAD_REQUEST,"invalid_query","查询无效"))?));
    if let Some(token)=filters.get("page_token"){let value:Value=URL_SAFE_NO_PAD.decode(token).ok().and_then(|bytes|serde_json::from_slice(&bytes).ok()).ok_or(Failure(StatusCode::BAD_REQUEST,"invalid_page_token","游标无效"))?;if value["context"]!=context{return Err(Failure(StatusCode::BAD_REQUEST,"invalid_page_token","游标无效"));}let after=value["after"].as_str().ok_or(Failure(StatusCode::BAD_REQUEST,"invalid_page_token","游标无效"))?;items.retain(|item|item["tenant_id"].as_str().is_some_and(|id|id>after));}
    let more=items.len()>size;items.truncate(size);let next=if more {items.last().map(|last|URL_SAFE_NO_PAD.encode(json!({"context":context,"after":last["tenant_id"]}).to_string()))}else{None};
    Ok(Json(json!({"total":total,"tenants":items,"next_page_token":next})))
}
async fn tenant(State(app): State<Shared>, Path(id): Path<String>, headers: HeaderMap) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    Ok(Json(view(target(&app, &session, &id)?)))
}
async fn users(State(app): State<Shared>, Path(id): Path<String>, headers: HeaderMap,axum::extract::RawQuery(raw):axum::extract::RawQuery) -> Result<Response, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let authorized = crate::authorization::authorize_tenant(&session, &id, tenant.tenant_type)
        .map_err(|kind| Failure(StatusCode::FORBIDDEN, kind, "无权访问该租户"))?;
    crate::authorization::require_digitalmate(&authorized)
        .map_err(|kind| Failure(StatusCode::FORBIDDEN, kind, "该租户未启用业务模块"))?;
    let query=crate::users::UserQuery::parse(raw.as_deref().unwrap_or_default()).map_err(|kind|Failure(StatusCode::BAD_REQUEST,kind,"用户查询参数无效"))?;
    let business=tenant.business.as_ref().ok_or(Failure(StatusCode::SERVICE_UNAVAILABLE,"business_unavailable","业务配置不可用"))?;
    let actor=format!("{:?}:{}",session.role,session.subject_id);
    let _ = query;
    let mut value=app.clients.accounts(&business.accounts_api,reqwest::Method::GET,&["users"],raw.as_deref().unwrap_or_default(),None,&actor).await
        .map_err(|error|Failure(error.status,error.kind,"业务用户查询失败，请重试"))?;
    value=crate::users::project_list(value).map_err(|kind|Failure(StatusCode::BAD_GATEWAY,kind,"用户响应无效"))?;
    let mut agent_failed=false;
    if let Some(users)=value["users"].as_array_mut() {
        for user in users {
            let user_id=user["user_id"].as_str().ok_or(Failure(StatusCode::SERVICE_UNAVAILABLE,"database_unavailable","用户记录无效"))?;
            let items=crate::business::associations(&app.clients,&app.config.agent_api,tenant,user_id,&business.bindings).await.map_err(|kind|Failure(StatusCode::SERVICE_UNAVAILABLE,kind,"业务映射无效"))?;
            let (state,exists)=crate::business::association_summary(&items);
            agent_failed|=items.iter().any(|item|item["existence"]=="uncertain");
            user["binding_state"]=json!(state);
            user["has_binding"]=json!(exists);
        }
    }
    if agent_failed { if let Some(items)=value["degraded"].as_array_mut() {items.push(json!("agent"));} }
    Ok(Json(value).into_response())
}
async fn user_detail(State(app):State<Shared>,Path((id,user_id)):Path<(String,String)>,headers:HeaderMap)->Result<Json<Value>,Failure> {
    let session=authenticated(&app,&headers)?;
    let tenant=target(&app,&session,&id)?;
    let authorized=crate::authorization::authorize_tenant(&session,&id,tenant.tenant_type).map_err(|kind|Failure(StatusCode::FORBIDDEN,kind,"无权访问"))?;
    crate::authorization::require_digitalmate(&authorized).map_err(|kind|Failure(StatusCode::FORBIDDEN,kind,"该租户未启用业务模块"))?;
    let business=tenant.business.as_ref().ok_or(Failure(StatusCode::SERVICE_UNAVAILABLE,"business_unavailable","业务配置不可用"))?;
    let actor=format!("{:?}:{}",session.role,session.subject_id);
    let mut detail=app.clients.accounts(&business.accounts_api,reqwest::Method::GET,&["users",&user_id],"",None,&actor).await.map_err(|error|Failure(error.status,error.kind,"业务用户查询失败"))?;
    detail=crate::users::project_detail(detail).map_err(|kind|Failure(StatusCode::BAD_GATEWAY,kind,"用户响应无效"))?;
    let associations=crate::business::associations(&app.clients,&app.config.agent_api,tenant,&user_id,&business.bindings).await.map_err(|kind|Failure(StatusCode::SERVICE_UNAVAILABLE,kind,"业务关联配置无效"))?;
    if associations.iter().any(|item|item["existence"]=="uncertain") {if let Some(items)=detail["degraded"].as_array_mut() {items.push(json!("agent"));}}
    detail["agent_associations"]=json!(associations);
    Ok(Json(detail))
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RevealInput {key_id:String,purpose:String}
async fn reveal_key(State(app):State<Shared>,Path((id,user_id)):Path<(String,String)>,headers:HeaderMap,Json(input):Json<RevealInput>)->Result<Json<Value>,Failure> {
    let session=authenticated(&app,&headers)?;
    let result=async {
    let tenant=target(&app,&session,&id)?;
    check_origin(&app,&headers)?;
    let authorized=crate::authorization::authorize_tenant(&session,&id,tenant.tenant_type).map_err(|k|Failure(StatusCode::FORBIDDEN,k,"无权访问"))?;
    crate::authorization::require_digitalmate(&authorized).map_err(|k|Failure(StatusCode::FORBIDDEN,k,"业务模块不可用"))?;
    if !matches!(input.purpose.as_str(),"view"|"copy") || input.key_id.is_empty() {return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reveal_request","查看或复制请求无效"));}
    let business=tenant.business.as_ref().ok_or(Failure(StatusCode::SERVICE_UNAVAILABLE,"business_unavailable","业务配置不可用"))?;
    let actor=format!("{:?}:{}",session.role,session.subject_id);
    let value=app.clients.accounts(&business.accounts_api,reqwest::Method::POST,&["users",&user_id,"key","reveal"],"",Some(&json!({"key_id":input.key_id,"purpose":input.purpose})),&actor).await.map_err(|error|Failure(error.status,error.kind,"Key 无法恢复或已变化，请刷新"))?;
    let key=value["key"].as_str().filter(|key|!key.is_empty()).ok_or(Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","Key 响应无效"))?;
    Ok(Json(json!({"key":key})))
    }.await;
    crate::audit::audit(crate::audit::AuditEvent::new(&session,&id,"console.key.reveal",crate::audit::AuditTarget {user_id:Some(user_id),key_id:Some(input.key_id),purpose:Some(input.purpose),..Default::default()},if result.is_ok() {crate::audit::Outcome::Success} else {crate::audit::Outcome::Failure}));
    result
}
async fn templates(State(app): State<Shared>, Path(id): Path<String>, headers: HeaderMap, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let mut value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["templates"], query.as_deref().unwrap_or_default(), None).await
        .map_err(|error| Failure(error.status, error.kind, "上游资源查询失败"))?;
    crate::agent_view::template_summaries(&app.clients,&app.config.agent_api,tenant,&mut value).await;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn template_latest(State(app): State<Shared>, Path((id,name)): Path<(String,String)>, headers: HeaderMap) -> Result<Json<Value>,Failure> {
    let session = authenticated(&app,&headers)?;
    let tenant = target(&app,&session,&id)?;
    let query = {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("name",&name).append_pair("page_size","1");
        query.finish()
    };
    let value = app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates"],&query,None).await
        .map_err(|error| Failure(error.status,error.kind,"模板列表查询失败"))?;
    let summary = value["templates"].as_array().and_then(|items| items.iter().find(|item| item["name"].as_str() == Some(name.as_str())))
        .ok_or(Failure(StatusCode::NOT_FOUND,"template_not_found","模板不存在"))?;
    let version = summary["latest_version"].as_str().ok_or(Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","模板响应无效"))?;
    let value = app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates",&name,"versions",version],"",None).await
        .map_err(|error| Failure(error.status,error.kind,"模板详情查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn version_bindings(State(app): State<Shared>, Path((id,name,version)): Path<(String,String,String)>, headers: HeaderMap, uri: axum::http::Uri, axum::extract::RawQuery(raw): axum::extract::RawQuery) -> Result<Json<Value>,Failure> {
    let session = authenticated(&app,&headers)?;
    let tenant = target(&app,&session,&id)?;
    adx_agent_core::version_number(&version).map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_version","版本无效"))?;
    let mut params = std::collections::BTreeMap::new();
    for (key,value) in url::form_urlencoded::parse(raw.as_deref().unwrap_or_default().as_bytes()) {
        if !matches!(key.as_ref(),"page_size"|"page_token"|"binding_id") || params.insert(key.into_owned(),value.into_owned()).is_some() {
            return Err(Failure(StatusCode::BAD_REQUEST,"invalid_query","查询参数无效或重复"));
        }
    }
    let query = {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (key,value) in params { query.append_pair(&key,&value); }
    query.append_pair("template",&name);
    if uri.path().ends_with("/alias-usage") {
        query.append_pair("actual_version",&version).append_pair("ref_kind","aliases");
    } else { query.append_pair("ref_kind","versions").append_pair("ref",&version); }
    query.finish()
    };
    let value = app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["bindings"],&query,None).await
        .map_err(|error| Failure(error.status,error.kind,"Binding 查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn catalog(State(app): State<Shared>, Path((id, name)): Path<(String, String)>, headers: HeaderMap, uri: axum::http::Uri, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let collection = if uri.path().ends_with("/versions") { "versions" } else { "aliases" };
    adx_agent_core::identifier(&name, "template").map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_template","模板标识无效"))?;
    let value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["templates", &name, collection], query.as_deref().unwrap_or_default(), None).await
        .map_err(|error| Failure(error.status, error.kind, "上游资源查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn reference(State(app): State<Shared>, Path((id, name, reference)): Path<(String, String, String)>, headers: HeaderMap, uri: axum::http::Uri) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let collection = if uri.path().rsplit('/').nth(1) == Some("versions") { "versions" } else { "aliases" };
    let template_ref = if collection == "versions" { adx_agent_core::TemplateRef::Version(reference.clone()) } else { adx_agent_core::TemplateRef::Alias(reference.clone()) };
    template_ref.validate().map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用无效"))?;
    let value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["templates", &name, collection, &reference], "", None).await
        .map_err(|error| Failure(error.status, error.kind, "上游资源查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn bindings(State(app): State<Shared>, Path(id): Path<String>, headers: HeaderMap, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["bindings"], query.as_deref().unwrap_or_default(), None).await
        .map_err(|error| Failure(error.status, error.kind, "上游资源查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
async fn binding(State(app): State<Shared>, Path((id, template, kind, reference, binding)): Path<(String, String, String, String, String)>, headers: HeaderMap) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let template_ref = match kind.as_str() {
        "versions" => adx_agent_core::TemplateRef::Version(reference.clone()),
        "aliases" => adx_agent_core::TemplateRef::Alias(reference.clone()),
        _ => return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用类型无效")),
    };
    let scope = adx_agent_core::Scope { tenant:id, template:template.clone(), template_ref, binding_id:binding.clone() };
    scope.validate().map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_scope","资源标识无效"))?;
    let value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["templates", &template, &kind, &reference, "bindings", &binding], "", None).await
        .map_err(|error| Failure(error.status, error.kind, "上游资源查询失败"))?;
    Ok(Json(crate::agent_view::redact(value)))
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteInput { expected_generation: String }
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WakeInput { expected_generation: String, environment_id: String }
async fn wake(State(app): State<Shared>, Path((id, template, kind, reference, binding_id, slot)): Path<(String,String,String,String,String,String)>, headers: HeaderMap, Json(input): Json<WakeInput>) -> Result<Response,Failure> {
    let session = authenticated(&app,&headers)?;
    let tenant = target(&app,&session,&id)?;
    check_origin(&app,&headers)?;
    if input.expected_generation.is_empty() || input.environment_id.is_empty() || !matches!(slot.as_str(),"current"|"secondary") {
        return Err(Failure(StatusCode::BAD_REQUEST,"invalid_context","缺少或非法目标上下文"));
    }
    let template_ref = match kind.as_str() {
        "versions" => adx_agent_core::TemplateRef::Version(reference.clone()),
        "aliases" => adx_agent_core::TemplateRef::Alias(reference.clone()),
        _ => return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用类型无效")),
    };
    let scope = adx_agent_core::Scope { tenant:id.clone(),template:template.clone(),template_ref,binding_id:binding_id.clone() };
    scope.validate().map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_scope","资源标识无效"))?;
    let value = app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates",&template,&kind,&reference,"bindings",&binding_id],"",None).await
        .map_err(|error| Failure(error.status,error.kind,"Binding 查询失败"))?;
    let binding: adx_agent_core::AgentBinding = serde_json::from_value(value.get("binding").cloned().unwrap_or(Value::Null))
        .map_err(|_| Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","Binding 响应无效"))?;
    crate::agent_view::check_context(&binding,&scope,&input.expected_generation,Some((&slot,&input.environment_id)))
        .map_err(|kind| Failure(StatusCode::CONFLICT,kind,"目标已变化，请刷新"))?;
    let version = if slot == "current" { &binding.current.version } else {
        &binding.secondary.as_ref().ok_or(Failure(StatusCode::CONFLICT,"environment_context_conflict","目标已变化，请刷新"))?.environment.version
    };
    let status = crate::agent_view::environment_status(&app.clients,&app.config.apiserver,tenant,&input.environment_id,version).await;
    crate::agent_view::wake_precondition(&binding,&scope,&input.expected_generation,&slot,&input.environment_id,status["status"].as_str().unwrap_or("unknown"))
        .map_err(|kind| Failure(if kind == "environment_absent" { StatusCode::NOT_FOUND } else { StatusCode::SERVICE_UNAVAILABLE },kind,"环境不存在或状态未知，请重新查询"))?;
    let url = crate::clients::Clients::url(&app.config.apiserver.base_url,&["api","environment","v1","environments",&input.environment_id,"resume"])
        .map_err(|error| Failure(error.status,error.kind,"恢复地址配置无效"))?;
    let token = crate::clients::Clients::token(tenant).map_err(|error| Failure(error.status,error.kind,"上游凭据不可用"))?;
    let response = app.clients.http.post(url).bearer_auth(token)
        .header("x-adx-request-id",format!("resume-{}",uuid::Uuid::new_v4()))
        .timeout(std::time::Duration::from_millis(app.config.apiserver.timeout_ms)).send().await
        .map_err(|error| Failure(if error.is_timeout() { StatusCode::GATEWAY_TIMEOUT } else { StatusCode::BAD_GATEWAY },"wake_outcome_unknown","恢复结果未知，请重新查询原环境"))?;
    let code = response.status();
    if !code.is_success() {
        let wire = response.json().await.unwrap_or(Value::Null);
        let body = crate::agent_view::resume_rejection(wire,&input.environment_id);
        return Ok((code,Json(body)).into_response());
    }
    let wire = response.json().await.map_err(|_| Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","恢复响应无效，请重新查询原环境"))?;
    let result = crate::agent_view::resume_result(wire,&input.environment_id)
        .map_err(|kind| Failure(StatusCode::BAD_GATEWAY,kind,"恢复响应无效，请重新查询原环境"))?;
    Ok(Json(result).into_response())
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishInput { template: Value, source_version: Option<String> }
async fn publish(State(app): State<Shared>, Path((id,name)): Path<(String,String)>, headers: HeaderMap, Json(input): Json<PublishInput>) -> Result<Json<Value>,Failure> {
    let session = authenticated(&app,&headers)?;
    let tenant = target(&app,&session,&id)?;
    check_origin(&app,&headers)?;
    let target = crate::authorization::authorize_tenant(&session,&id,tenant.tenant_type)
        .map_err(|kind| Failure(StatusCode::FORBIDDEN,kind,"无权访问该租户"))?;
    let value = crate::agent_view::publish_version(&app.clients,&app.config.agent_api,tenant,crate::agent_view::OperationContext { target:&target,actor:&session },&name,input.template,input.source_version.as_deref()).await
        .map_err(|error| Failure(error.status,error.kind,"版本发布失败或结果未知，请刷新核实"))?;
    Ok(Json(value))
}
async fn put_alias(State(app): State<Shared>, Path((id,name,alias)): Path<(String,String,String)>, headers: HeaderMap, Json(input): Json<crate::agent_view::AliasInput>) -> Result<Json<Value>,Failure> {
    let session = authenticated(&app,&headers)?;
    let tenant = target(&app,&session,&id)?;
    check_origin(&app,&headers)?;
    let target = crate::authorization::authorize_tenant(&session,&id,tenant.tenant_type)
        .map_err(|kind| Failure(StatusCode::FORBIDDEN,kind,"无权访问该租户"))?;
    let value = crate::agent_view::update_alias(&app.clients,&app.config.agent_api,tenant,crate::agent_view::OperationContext { target:&target,actor:&session },&name,&alias,&input).await
        .map_err(|error| Failure(error.status,error.kind,"别名保存失败，请刷新核实"))?;
    Ok(Json(value))
}
#[derive(Default,serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EnvironmentQuery { expected_generation:Option<String>, environment_id:Option<String> }
impl EnvironmentQuery {
    pub(crate) fn context(&self)->Result<Option<(&str,&str)>, &'static str> {
        match (&self.expected_generation,&self.environment_id) {
            (None,None)=>Ok(None),
            (Some(generation),Some(environment)) if !generation.is_empty()&&!environment.is_empty()=>Ok(Some((generation,environment))),
            _=>Err("invalid_context"),
        }
    }
}
async fn environment(State(app): State<Shared>, Path((id, template, kind, reference, binding_id, slot)): Path<(String, String, String, String, String, String)>, headers: HeaderMap, axum::extract::Query(input):axum::extract::Query<EnvironmentQuery>) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    let template_ref = match kind.as_str() {
        "versions" => adx_agent_core::TemplateRef::Version(reference.clone()),
        "aliases" => adx_agent_core::TemplateRef::Alias(reference.clone()),
        _ => return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用类型无效")),
    };
    let scope = adx_agent_core::Scope { tenant:id.clone(), template:template.clone(), template_ref, binding_id:binding_id.clone() };
    scope.validate().map_err(|_| Failure(StatusCode::BAD_REQUEST,"invalid_scope","资源标识无效"))?;
    let value = app.clients.agent(&app.config, tenant, reqwest::Method::GET, &["templates",&template,&kind,&reference,"bindings",&binding_id],"",None).await
        .map_err(|error| Failure(error.status,error.kind,"Binding 查询失败"))?;
    let binding: adx_agent_core::AgentBinding = serde_json::from_value(value.get("binding").cloned().unwrap_or(Value::Null))
        .map_err(|_| Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","Binding 响应无效"))?;
    if binding.scope != scope { return Err(Failure(StatusCode::BAD_GATEWAY,"binding_scope_mismatch","Binding 身份不匹配")); }
    if let Some((generation,environment))=input.context().map_err(|kind|Failure(StatusCode::BAD_REQUEST,kind,"查询上下文无效"))? {
        crate::agent_view::check_context(&binding,&scope,generation,Some((&slot,environment))).map_err(|kind|Failure(StatusCode::CONFLICT,kind,"绑定已变化，请刷新"))?;
    }
    let environment = match slot.as_str() {
        "current" => &binding.current,
        "secondary" => &binding.secondary.as_ref().ok_or(Failure(StatusCode::NOT_FOUND,"environment_absent","次要实例不存在"))?.environment,
        _ => return Err(Failure(StatusCode::BAD_REQUEST,"invalid_slot","实例槽位无效")),
    };
    let status = crate::agent_view::environment_status(&app.clients,&app.config.apiserver,tenant,&environment.environment_id,&environment.version).await;
    Ok(Json(json!({"environment":status})))
}
async fn delete_binding(State(app): State<Shared>, Path((id, template, kind, reference, binding_id)): Path<(String, String, String, String, String)>, headers: HeaderMap, Json(input): Json<DeleteInput>) -> Result<Json<Value>, Failure> {
    let session = authenticated(&app, &headers)?;
    let tenant = target(&app, &session, &id)?;
    check_origin(&app, &headers)?;
    if input.expected_generation.is_empty() { return Err(Failure(StatusCode::BAD_REQUEST,"invalid_generation","缺少页面代次")); }
    let target = crate::authorization::authorize_tenant(&session, &id, tenant.tenant_type)
        .map_err(|kind| Failure(StatusCode::FORBIDDEN, kind, "无权访问该租户"))?;
    let template_ref = match kind.as_str() {
        "versions" => adx_agent_core::TemplateRef::Version(reference),
        "aliases" => adx_agent_core::TemplateRef::Alias(reference),
        _ => return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用类型无效")),
    };
    let scope = adx_agent_core::Scope { tenant:id, template, template_ref, binding_id };
    let value = crate::agent_view::delete_binding(&app.clients, &app.config.agent_api, tenant, &target, &session, &scope, &input.expected_generation).await
        .map_err(|error| Failure(error.status, error.kind, "删除失败或结果未知，请刷新原资源核实"))?;
    Ok(Json(value))
}
async fn no_store(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
async fn file_route(State(app):State<Shared>,Path((id,template,kind,reference,binding_id)):Path<(String,String,String,String,String)>,headers:HeaderMap,uri:axum::http::Uri,axum::extract::RawQuery(raw):axum::extract::RawQuery)->Result<Response,Failure> {
    let session=authenticated(&app,&headers)?;
    let tenant=target(&app,&session,&id)?;
    let fail=|kind|Failure(match kind {"path_escape"|"invalid_query"=>StatusCode::BAD_REQUEST,"symlink_not_allowed"|"mount_boundary_crossed"=>StatusCode::FORBIDDEN,"preview_too_large"|"directory_too_large"=>StatusCode::PAYLOAD_TOO_LARGE,"binary_file"=>StatusCode::UNSUPPORTED_MEDIA_TYPE,"generation_conflict"|"environment_context_conflict"|"mount_context_conflict"|"volume_identity_conflict"|"shared_mount_unsupported"=>StatusCode::CONFLICT,_=>StatusCode::SERVICE_UNAVAILABLE},kind,"文件读取不可用或上下文已变化，请刷新");
    let mut params=std::collections::BTreeMap::new();
    for (key,value) in url::form_urlencoded::parse(raw.as_deref().unwrap_or_default().as_bytes()) {
        if !matches!(key.as_ref(),"slot"|"expected_generation"|"environment_id"|"mount_id"|"path"|"mode") || params.insert(key.into_owned(),value.into_owned()).is_some() {return Err(fail("invalid_query"));}
    }
    let required=|key|params.get(key).filter(|v|!v.is_empty()).map(String::as_str).ok_or_else(||fail("invalid_query"));
    let slot=required("slot")?;let generation=required("expected_generation")?;let environment=required("environment_id")?;
    let template_ref=match kind.as_str() {"versions"=>adx_agent_core::TemplateRef::Version(reference),"aliases"=>adx_agent_core::TemplateRef::Alias(reference),_=>return Err(fail("invalid_query"))};
    let scope=adx_agent_core::Scope {tenant:id,template,template_ref,binding_id};
    scope.validate().map_err(|_|fail("invalid_query"))?;
    let ctx=crate::storage_http::context(&app.clients,&app.config,tenant,&scope,generation,slot,environment).await.map_err(fail)?;
    if uri.path().ends_with("/mounts") {
        let mut mounts=Vec::new();
        for (index,mount) in ctx.mounts.iter().enumerate() {
            let value=match crate::storage_http::resolve(&app.clients,&app.config,tenant,&ctx,index,slot).await {
                Ok(resolved)=>json!({"mount_id":resolved.id,"slot":slot,"source_kind":"volume","target":mount.target,"volume_id":mount.volume_id,"volume_uid":ctx.binding.volumes.get(mount.volume_id.as_deref().unwrap_or_default()),"read_only":true,"availability":"available","error":null}),
                Err(kind)=>json!({"mount_id":null,"slot":slot,"target":mount.target,"read_only":true,"availability":if kind=="shared_mount_unsupported" {"unsupported"} else {"unavailable"},"error":{"kind":kind}}),
            };mounts.push(value);
        }
        return Ok(Json(json!({"access_mode":"shared_storage","mounts":mounts})).into_response());
    }
    let mount_id=required("mount_id")?;
    let mut selected=None;
    for index in 0..ctx.mounts.len() {
        if let Ok(resolved)=crate::storage_http::resolve(&app.clients,&app.config,tenant,&ctx,index,slot).await {if resolved.id==mount_id {selected=Some((index,resolved));break;}}
    }
    let (index,resolved)=selected.ok_or_else(||fail("mount_context_conflict"))?;
    let path=params.get("path").map_or("",String::as_str);
    let excluded=crate::files::excluded_subtrees(&resolved.target,&ctx.mounts);
    crate::files::check_excluded(path,&excluded).map_err(fail)?;
    let file=crate::storage_http::open(&resolved,path).map_err(fail)?;
    let fresh=crate::storage_http::context(&app.clients,&app.config,tenant,&scope,generation,slot,environment).await.map_err(fail)?;
    let fresh=crate::storage_http::resolve(&app.clients,&app.config,tenant,&fresh,index,slot).await.map_err(fail)?;
    if fresh.identity!=resolved.identity {return Err(fail("mount_context_conflict"));}
    let mut response=if uri.path().ends_with("/download") {
        if !file.metadata().map_err(|_|fail("shared_storage_unavailable"))?.is_file() {return Err(fail("not_regular_file"));}
        let audit=crate::audit::DownloadAudit::new(session,scope.tenant.clone(),crate::audit::AuditTarget {
            template:Some(scope.template.clone()),ref_kind:Some(kind),reference:Some(scope.template_ref.name().into()),binding_id:Some(scope.binding_id.clone()),generation:Some(generation.into()),slot:Some(slot.into()),environment_id:Some(environment.into()),mount_id:Some(mount_id.into()),path:Some(path.into()),..Default::default()
        });
        let stream=futures_util::stream::try_unfold((tokio::fs::File::from_std(file),audit),|(mut file,mut audit)|async move {
            use tokio::io::AsyncReadExt;
            let mut bytes=vec![0u8;65536];let length=match file.read(&mut bytes).await {Ok(length)=>length,Err(error)=>{audit.failed();return Err(error);}};bytes.truncate(length);
            if length==0 {audit.finished();}
            Ok::<_,std::io::Error>(if length==0 {None} else {Some((bytes,(file,audit)))})
        });
        ([(header::CONTENT_TYPE,"application/octet-stream"),(header::CONTENT_DISPOSITION,"attachment; filename=\"download\"")],axum::body::Body::from_stream(stream)).into_response()
    } else if params.get("mode").map(String::as_str)==Some("list") {
        Json(json!({"entries":crate::files::list_directory(file,app.config.shared_files.as_ref().map_or(1000,|c|c.directory_entries_limit),path,&excluded).map_err(fail)?})).into_response()
    } else if params.get("mode").map(String::as_str)==Some("read") {
        Json(json!({"text":crate::files::preview(file,app.config.file_preview_bytes_limit).map_err(fail)?})).into_response()
    } else {return Err(fail("invalid_query"));};
    response.headers_mut().insert("x-adx-console-file-source",HeaderValue::from_static("shared-storage"));
    Ok(response)
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalQuery { expected_generation:String, environment_id:String }
async fn terminal(State(app): State<Shared>, Path((id,template,kind,reference,binding_id,slot)): Path<(String,String,String,String,String,String)>, headers:HeaderMap,
    axum::extract::RawQuery(raw):axum::extract::RawQuery,
    ws:Result<axum::extract::WebSocketUpgrade,axum::extract::ws::rejection::WebSocketUpgradeRejection>) -> Result<Response,Failure> {
    let session=authenticated(&app,&headers)?;
    let tenant=target(&app,&session,&id)?;
    check_origin(&app,&headers)?;
    let mut query=serde_json::Map::new();
    for (key,value) in url::form_urlencoded::parse(raw.as_deref().unwrap_or_default().as_bytes()) {
        if query.insert(key.into_owned(),Value::String(value.into_owned())).is_some() { return Err(Failure(StatusCode::BAD_REQUEST,"invalid_query","查询参数重复")); }
    }
    let input:TerminalQuery=serde_json::from_value(Value::Object(query)).map_err(|_|Failure(StatusCode::BAD_REQUEST,"invalid_context","终端上下文无效"))?;
    if input.expected_generation.is_empty() || input.environment_id.is_empty() || !matches!(slot.as_str(),"current"|"secondary") { return Err(Failure(StatusCode::BAD_REQUEST,"invalid_context","终端上下文无效")); }
    let template_ref=match kind.as_str() { "versions"=>adx_agent_core::TemplateRef::Version(reference.clone()),"aliases"=>adx_agent_core::TemplateRef::Alias(reference.clone()),_=>return Err(Failure(StatusCode::BAD_REQUEST,"invalid_reference","引用无效")) };
    let scope=adx_agent_core::Scope { tenant:id.clone(),template:template.clone(),template_ref,binding_id:binding_id.clone() };
    scope.validate().map_err(|_|Failure(StatusCode::BAD_REQUEST,"invalid_scope","资源标识无效"))?;
    let value=app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates",&template,&kind,&reference,"bindings",&binding_id],"",None).await.map_err(|e|Failure(e.status,e.kind,"Binding 查询失败"))?;
    let binding:adx_agent_core::AgentBinding=serde_json::from_value(value["binding"].clone()).map_err(|_|Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","Binding 响应无效"))?;
    crate::agent_view::check_context(&binding,&scope,&input.expected_generation,Some((&slot,&input.environment_id))).map_err(|k|Failure(StatusCode::CONFLICT,k,"目标已变化，请刷新"))?;
    let environment=if slot=="current" { &binding.current } else { &binding.secondary.as_ref().ok_or(Failure(StatusCode::CONFLICT,"environment_context_conflict","实例已变化"))?.environment };
    let status=crate::agent_view::environment_status(&app.clients,&app.config.apiserver,tenant,&input.environment_id,&environment.version).await;
    if status["status"]!="running" { return Err(Failure(StatusCode::CONFLICT,"terminal_environment_not_running","环境未运行，请先查询状态或显式恢复")); }
    let snapshot=app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates",&template,"versions",&environment.version],"",None).await.map_err(|e|Failure(e.status,e.kind,"模板查询失败"))?;
    let snapshot:adx_agent_core::TemplateVersion=serde_json::from_value(snapshot.get("template").cloned().unwrap_or(snapshot)).map_err(|_|Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","模板响应无效"))?;
    let profile=app.profiles.iter().find(|p|p.tenant==id && p.template==template && p.version==environment.version).ok_or(Failure(StatusCode::SERVICE_UNAVAILABLE,"terminal_profile_unavailable","终端配置不可用"))?.clone();
    let cwd=crate::terminal::terminal_cwd(&profile,&id,&snapshot,&binding.parameters).map_err(|k|Failure(StatusCode::CONFLICT,k,"终端工作目录不可用"))?;
    let protocol=crate::terminal::PtyProtocol::new(&profile,&cwd,80,24).map_err(|k|Failure(StatusCode::SERVICE_UNAVAILABLE,k,"终端配置无效"))?;
    let url=crate::terminal::pty_url(&app.config.ingress,&input.environment_id,&profile.adapter,80,24).map_err(|k|Failure(StatusCode::SERVICE_UNAVAILABLE,k,"终端地址无效"))?;
    let token=crate::clients::Clients::token(tenant).map_err(|e|Failure(e.status,e.kind,"上游凭据不可用"))?;
    let upgrade=ws.map_err(|_|Failure(StatusCode::BAD_REQUEST,"websocket_required","需要 WebSocket 升级"))?;
    let fresh=app.clients.agent(&app.config,tenant,reqwest::Method::GET,&["templates",&template,&kind,&reference,"bindings",&binding_id],"",None).await.map_err(|e|Failure(e.status,e.kind,"Binding 复核失败"))?;
    let fresh:adx_agent_core::AgentBinding=serde_json::from_value(fresh["binding"].clone()).map_err(|_|Failure(StatusCode::BAD_GATEWAY,"upstream_response_invalid","Binding 响应无效"))?;
    crate::agent_view::check_context(&fresh,&scope,&input.expected_generation,Some((&slot,&input.environment_id))).map_err(|k|Failure(StatusCode::CONFLICT,k,"目标已变化，请刷新"))?;
    let meta=json!({"type":"meta","tenant":id,"scope":scope,"slot":slot,"environment_id":input.environment_id,"profile_revision":profile.revision,"backend":"pty_adapter","method":"execd_pty"});
    Ok(upgrade.max_message_size(262144).on_upgrade(move |socket| async move {
        let headers=headers;
        let active=||authenticated(&app,&headers).is_ok();
        let _=crate::terminal::bridge(socket,url,token,protocol,meta,crate::terminal::BridgeLimits { connect_ms:app.config.ingress.connect_timeout_ms,idle_seconds:app.config.terminal_idle_timeout_seconds,lost_seconds:app.config.terminal_lost_timeout_seconds,tls:Some(app.clients.trust.websocket.clone()) },active).await;
        crate::audit::audit(crate::audit::AuditEvent::new(&session,&id,"terminal_closed",crate::audit::AuditTarget { template:Some(template),binding_id:Some(binding_id),generation:Some(input.expected_generation),..Default::default() },crate::audit::Outcome::Unknown));
    }).into_response())
}
pub fn router(config: ConsoleConfig, secret: Vec<u8>) -> Result<Router, String> {
    config.validate()?;
    build_router(config,secret)
}
pub(crate) fn build_router(config: ConsoleConfig, secret: Vec<u8>) -> Result<Router, String> {
    let clients = crate::clients::Clients::with_ca(config.upstream_ca_file.as_deref())?;
    let profiles:Vec<crate::terminal::Profile>=match &config.terminal_profiles_file {
        Some(path)=>serde_json::from_slice(&std::fs::read(path).map_err(|_|"terminal profiles unreadable")?).map_err(|_|"terminal profiles invalid")?,
        None=>Vec::new(),
    };
    let mut identities=std::collections::BTreeSet::new();
    for profile in &profiles { profile.validate()?; if !identities.insert((&profile.tenant,&profile.template,&profile.version)) { return Err("duplicate terminal profile".into()); } }
    let sessions = SessionRegistry::new(secret, config.session_ttl_seconds,
        config.platform_admins.iter().map(|admin| (admin.admin_id.clone(), admin.token_sha256.clone())).collect(),
        config.tenants.iter().map(|tenant| (tenant.tenant_id.clone(), tenant.admin_token_sha256.clone())).collect())?;
    Ok(Router::new()
        .route("/", get(|| async { axum::response::Redirect::temporary("/console/") }))
        .route("/console/", get(|| async { axum::response::Html(include_str!("../frontend/dist/index.html")) }))
        .route("/console/console.js", get(|| async { ([(header::CONTENT_TYPE,"text/javascript; charset=utf-8")],include_str!("../frontend/dist/console.js")) }))
        .route("/console/console.css", get(|| async { ([(header::CONTENT_TYPE,"text/css; charset=utf-8")],include_str!("../frontend/dist/console.css")) }))
        .route("/api/console/v1/session", get(get_session).post(login).delete(logout))
        .route("/api/console/v1/tenants", get(tenants))
        .route("/api/console/v1/tenants/{id}", get(tenant))
        .route("/api/console/v1/tenants/{id}/users", get(users))
        .route("/api/console/v1/tenants/{id}/users/{user_id}",get(user_detail))
        .route("/api/console/v1/tenants/{id}/users/{user_id}/key/reveal",axum::routing::post(reveal_key))
        .route("/api/console/v1/tenants/{id}/templates", get(templates))
        .route("/api/console/v1/tenants/{id}/templates/{name}", get(template_latest))
        .route("/api/console/v1/tenants/{id}/templates/{name}/versions", get(catalog).post(publish))
        .route("/api/console/v1/tenants/{id}/templates/{name}/aliases", get(catalog))
        .route("/api/console/v1/tenants/{id}/templates/{name}/versions/{reference}", get(reference))
        .route("/api/console/v1/tenants/{id}/templates/{name}/versions/{reference}/bindings", get(version_bindings))
        .route("/api/console/v1/tenants/{id}/templates/{name}/versions/{reference}/alias-usage", get(version_bindings))
        .route("/api/console/v1/tenants/{id}/templates/{name}/aliases/{reference}", get(reference).put(put_alias))
        .route("/api/console/v1/tenants/{id}/bindings", get(bindings))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}", get(binding).delete(delete_binding))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/environment/{slot}", get(environment))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/environment/{slot}/wake", axum::routing::post(wake))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/terminal/{slot}", get(terminal))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/files/mounts",get(file_route))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/files",get(file_route))
        .route("/api/console/v1/tenants/{id}/bindings/{template}/{kind}/{reference}/{binding}/files/download",get(file_route))
        .layer(middleware::from_fn(no_store)).with_state(Arc::new(App { config, sessions, clients,profiles })))
}

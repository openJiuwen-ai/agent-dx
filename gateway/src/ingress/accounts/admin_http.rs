//! Accounts management HTTP contract; tenant authentication is supplied by Ingress.
use super::{admin::{AdminError, AdminQuery}, AccountService, Error};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde::Deserialize;

pub enum Operation {
    List,
    Detail(String),
    Reveal(String),
}

pub fn matches(path: &str) -> bool {
    path == "/api/accounts/v1/admin" || path.starts_with("/api/accounts/v1/admin/")
}

pub fn operation(method: &str, path: &str) -> Result<Operation, StatusCode> {
    let tail = path.strip_prefix("/api/accounts/v1/admin/").ok_or(StatusCode::NOT_FOUND)?;
    let parts: Vec<_> = tail.split('/').collect();
    let (required, user) = match parts.as_slice() {
        ["users"] => ("GET", None),
        ["users", user] if !user.is_empty() => ("GET", Some(*user)),
        ["users", user, "key", "reveal"] if !user.is_empty() => ("POST", Some(*user)),
        _ => return Err(StatusCode::NOT_FOUND),
    };
    if method != required { return Err(StatusCode::METHOD_NOT_ALLOWED); }
    let Some(user) = user else { return Ok(Operation::List); };
    // Parse as a form value solely for strict percent and UTF-8 validation.
    let query = AdminQuery::parse(&format!("user_id={}",user.replace('+',"%2B").replace('&',"%26"))).map_err(|_|StatusCode::BAD_REQUEST)?;
    let user = query.filters.get("user_id").cloned().ok_or(StatusCode::BAD_REQUEST)?;
    if required == "POST" { Ok(Operation::Reveal(user)) } else { Ok(Operation::Detail(user)) }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevealInput {
    pub key_id: String,
    pub purpose: String,
}
impl RevealInput {
    pub fn validate(&self) -> Result<(), Error> {
        if self.key_id.is_empty() || self.key_id.len() > 512 || !matches!(self.purpose.as_str(),"view"|"copy") {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

pub fn response(status: StatusCode, value: &serde_json::Value) -> Response<crate::ingress::server::ProxyBody> {
    if serde_json::to_vec(value).map_or(true,|bytes|bytes.len()>4*1024*1024) {
        return failure(StatusCode::SERVICE_UNAVAILABLE,"response_limit_exceeded");
    }
    let mut response = crate::ingress::agent_response::json_response(status,value,None);
    response.headers_mut().insert("cache-control",http::HeaderValue::from_static("no-store"));
    response
}

pub fn failure(status: StatusCode, kind: &str) -> Response<crate::ingress::server::ProxyBody> {
    response(status,&serde_json::json!({"error":{"kind":kind,"message":"Accounts management request failed"}}))
}

pub async fn handle(service: &AccountService, request: Request<hyper::body::Incoming>, tenant: &str) -> Response<crate::ingress::server::ProxyBody> {
    let is_reveal = request.uri().path().ends_with("/key/reveal");
    if service.config.admin.as_ref().is_none_or(|admin| admin.authorize(tenant).is_err()) {
        if is_reveal { tracing::info!(event="accounts.key.reveal",authenticated_tenant=tenant,success=false,reason="forbidden","Accounts management audit"); }
        return failure(StatusCode::FORBIDDEN,"forbidden");
    }
    let operation = match operation(request.method().as_str(),request.uri().path()) {
        Ok(operation) => operation,
        Err(status) => return failure(status,"invalid_management_request"),
    };
    let actor = request.headers().get("x-adx-console-actor").and_then(|v|v.to_str().ok()).filter(|v|v.len() <= 256).unwrap_or("unspecified").to_owned();
    let reveal = matches!(&operation,Operation::Reveal(_));
    let user = match &operation { Operation::Detail(user)|Operation::Reveal(user) => Some(user.clone()),_=>None };
    let result = tokio::time::timeout(std::time::Duration::from_secs(30),async {
        match operation {
            Operation::List => {
                let query = AdminQuery::parse(request.uri().query().unwrap_or_default())?;
                service.admin_list(tenant,&actor,&query).await
            }
            Operation::Detail(user) => {
                if request.uri().query().is_some() { return Err(Error::Invalid.into()); }
                service.admin_detail(tenant,&user).await
            }
            Operation::Reveal(user) => {
                if request.uri().query().is_some() { return Err(Error::Invalid.into()); }
                let bytes = http_body_util::Limited::new(request.into_body(),4096).collect().await.map_err(|_|Error::Invalid)?.to_bytes();
                let input: RevealInput = serde_json::from_slice(&bytes).map_err(|_|Error::Invalid)?;
                input.validate()?;
                tracing::info!(event="accounts.key.reveal.request",authenticated_tenant=tenant,forwarded_actor=%actor,actor_source="untrusted_forwarded",user_id=%user,key_id=%input.key_id,purpose=%input.purpose,"Accounts management audit");
                let key = service.admin_reveal(tenant,&user,&input.key_id).await?;
                Ok(serde_json::json!({"key":key}))
            }
        }
    }).await.unwrap_or(Err(Error::Unavailable.into()));
    if reveal {
        tracing::info!(event="accounts.key.reveal",authenticated_tenant=tenant,forwarded_actor=%actor,actor_source="untrusted_forwarded",user_id=?user,success=result.is_ok(),"Accounts management audit");
    }
    match result {
        Ok(value) => response(StatusCode::OK,&value),
        Err(error) => {
            let (status,kind) = match error {
                AdminError::NotFound => (StatusCode::NOT_FOUND,"user_not_found"),
                AdminError::NotRecoverable => (StatusCode::NOT_FOUND,"not_recoverable"),
                AdminError::IdentityConflict => (StatusCode::CONFLICT,"key_identity_conflict"),
                AdminError::Account(Error::Forbidden) => (StatusCode::FORBIDDEN,"forbidden"),
                AdminError::Account(Error::Invalid) => (StatusCode::BAD_REQUEST,"invalid_query"),
                _ => (StatusCode::SERVICE_UNAVAILABLE,"accounts_unavailable"),
            };
            failure(status,kind)
        }
    }
}

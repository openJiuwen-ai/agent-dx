//! Team-bound API key management using the ADX administrator credential profile.
use super::http::decode;
use crate::http::Api;
use adx_observability::trace;
use adx_protocol::control as pb;
use chrono::{DateTime, SecondsFormat, Utc};
use hyper::{body::Incoming, HeaderMap, Request};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;

pub(crate) fn route(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    path == "/api-keys" || path.starts_with("/api-keys/")
}

fn unique<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, Status> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(Status::unauthenticated("ambiguous management credentials"));
    }
    value
        .map(|value| {
            value
                .to_str()
                .map(str::trim)
                .map_err(|_| Status::unauthenticated("invalid management credentials"))
        })
        .transpose()
}

/// This profile accepts existing ADX administrator keys, not provider JWTs.
pub(crate) fn token(headers: &HeaderMap) -> Result<&str, Status> {
    let admin = unique(headers, "x-admin-token")?;
    let bearer = unique(headers, "authorization")?;
    match (admin, bearer) {
        (Some(key), None) if !key.is_empty() => Ok(key),
        (None, Some(value)) => value
            .strip_prefix("Bearer ")
            .filter(|key| !key.is_empty())
            .ok_or_else(|| Status::unauthenticated("administrator credential required")),
        _ => Err(Status::unauthenticated("administrator credential required")),
    }
}

fn authorize(headers: &HeaderMap, caller: &pb::CallerContext) -> Result<(), Status> {
    if !caller.administrator {
        return Err(Status::permission_denied("administrator required"));
    }
    let team = unique(headers, "x-team-id")?
        .ok_or_else(|| Status::invalid_argument("X-Team-ID is required"))?;
    if team.is_empty() || team != caller.tenant_id {
        return Err(Status::permission_denied(
            "team does not match authenticated tenant",
        ));
    }
    Ok(())
}

pub(super) async fn handle(
    api: &Api,
    request: Request<Incoming>,
    path: &str,
    caller: &pb::CallerContext,
) -> Result<(u16, Value), Status> {
    authorize(request.headers(), caller)?;
    if request.uri().query().is_some_and(|value| !value.is_empty()) {
        return Err(Status::invalid_argument(
            "API key query parameters are not supported",
        ));
    }
    let mut client = pb::credential_service_client::CredentialServiceClient::new(
        api.clients.coordinator().await?,
    );
    match (request.method().as_str(), path.strip_prefix("/api-keys/")) {
        ("POST", None) => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                name: String,
            }
            let input: Input = decode(request).await?;
            if input.name.trim().is_empty()
                || input.name.chars().count() > 256
                || input.name.chars().any(char::is_control)
            {
                return Err(Status::invalid_argument(
                    "name must contain 1..256 non-control characters",
                ));
            }
            let result = api
                .clients
                .rpc(
                    "apiserver.e2b_create_key",
                    client.create_tenant_key(trace::inject(pb::CreateTenantKeyRequest {
                        caller: Some(caller.clone()),
                        tenant_id: caller.tenant_id.clone(),
                        expires_at_unix_seconds: 0,
                        name: Some(input.name),
                    })),
                )
                .await?;
            let key = result
                .key
                .ok_or_else(|| Status::data_loss("missing key metadata"))?;
            let mut value = public_key(&key, &caller.tenant_id)?;
            if result.api_key.len() < 32 || result.api_key.len() > 512 {
                return Err(Status::data_loss("invalid created key"));
            }
            value["key"] = Value::String(result.api_key);
            Ok((201, value))
        }
        ("GET", None) => {
            let mut items = Vec::new();
            let mut token = String::new();
            loop {
                let page = api
                    .clients
                    .rpc(
                        "apiserver.e2b_list_keys",
                        client.list_tenant_keys(trace::inject(pb::ListTenantKeysRequest {
                            caller: Some(caller.clone()),
                            tenant_id: caller.tenant_id.clone(),
                            page_token: token.clone(),
                            page_size: 1000,
                        })),
                    )
                    .await?;
                for key in page.keys {
                    if key.tenant_id != caller.tenant_id {
                        return Err(Status::data_loss("key page tenant mismatch"));
                    }
                    if key.metadata.is_some() {
                        items.push(public_key(&key, &caller.tenant_id)?);
                    }
                }
                if page.next_page_token.is_empty() {
                    break;
                }
                if page.next_page_token <= token {
                    return Err(Status::data_loss("invalid key pagination"));
                }
                token = page.next_page_token;
            }
            Ok((200, Value::Array(items)))
        }
        ("DELETE", Some(id)) if !id.contains('/') => {
            let public_id = uuid::Uuid::parse_str(id)
                .map_err(|_| Status::invalid_argument("invalid API key ID"))?
                .to_string();
            api.clients
                .rpc(
                    "apiserver.e2b_revoke_key",
                    client.revoke_tenant_key(trace::inject(pb::RevokeTenantKeyRequest {
                        caller: Some(caller.clone()),
                        id: String::new(),
                        public_id,
                        tenant_id: caller.tenant_id.clone(),
                    })),
                )
                .await?;
            Ok((204, Value::Null))
        }
        ("PATCH", Some(_)) => Err(Status::unimplemented("API key rename is not supported")),
        _ => Err(Status::not_found("route not found")),
    }
}

fn public_key(key: &pb::TenantKey, tenant: &str) -> Result<Value, Status> {
    let metadata = key
        .metadata
        .as_ref()
        .filter(|_| key.tenant_id == tenant)
        .ok_or_else(|| Status::data_loss("invalid key metadata"))?;
    uuid::Uuid::parse_str(&metadata.public_id)
        .map_err(|_| Status::data_loss("invalid public key ID"))?;
    let created = i64::try_from(metadata.created_at_unix_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| Status::data_loss("invalid key creation time"))?;
    Ok(json!({
        "id":metadata.public_id, "name":metadata.name,
        "createdAt":created.to_rfc3339_opts(SecondsFormat::Millis, true),
        "createdBy":null, "lastUsed":null,
        "mask": { "prefix":metadata.prefix, "valueLength":metadata.value_length,
            "maskedValuePrefix":metadata.masked_value_prefix,
            "maskedValueSuffix":metadata.masked_value_suffix }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_credentials_cannot_be_ambiguous_or_promote_tenant_keys() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "ordinary-tenant-key".parse().unwrap());
        assert_eq!(
            token(&headers).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        headers.insert("x-admin-token", "admin-key".parse().unwrap());
        assert_eq!(token(&headers).unwrap(), "admin-key");
        headers.append("x-admin-token", "second-key".parse().unwrap());
        assert!(token(&headers).is_err());
        headers.remove("x-admin-token");
        headers.insert("authorization", "Bearer admin-key".parse().unwrap());
        assert_eq!(token(&headers).unwrap(), "admin-key");
        headers.insert("x-admin-token", "admin-key".parse().unwrap());
        assert!(token(&headers).is_err());
    }

    #[test]
    fn team_is_bound_to_the_authenticated_administrator() {
        let mut headers = HeaderMap::new();
        let mut caller = pb::CallerContext {
            tenant_id: "tenant-a".into(),
            administrator: true,
        };
        assert!(authorize(&headers, &caller).is_err());
        headers.insert("x-team-id", "tenant-b".parse().unwrap());
        assert_eq!(
            authorize(&headers, &caller).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        headers.insert("x-team-id", "tenant-a".parse().unwrap());
        assert!(authorize(&headers, &caller).is_ok());
        caller.administrator = false;
        assert_eq!(
            authorize(&headers, &caller).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
}

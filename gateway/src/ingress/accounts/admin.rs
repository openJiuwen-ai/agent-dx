//! Explicit server-side authorization for read-only accounts administration.
use super::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub fn model_namespace<'a>(business: Option<&'a str>, explicit: Option<&'a str>) -> Result<&'a str> {
    if matches!((business,explicit),(Some(a),Some(b)) if a != b) { return Err(Error::Configuration); }
    let namespace = explicit.or(business).ok_or(Error::Configuration)?;
    adx_agent_core::identifier(namespace,"accounts model namespace").map_err(|_|Error::Configuration)?;
    Ok(namespace)
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    pub grants: Vec<AdminGrant>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminGrant {
    pub tenant: String,
    pub identity_scope: IdentityScope,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityScope {
    Exclusive { exclusive_tenant: String },
    Scoped { rules: Vec<IdentityRule> },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct IdentityRule {
    pub provider: String,
    pub developer_scope: String,
    pub client_id: String,
}

#[derive(Default, Serialize)]
pub struct AdminQuery {
    pub filters: std::collections::BTreeMap<String, String>,
    pub page_size: usize,
    pub page_token: Option<String>,
}

impl AdminQuery {
    pub fn parse(raw: &str) -> Result<Self> {
        let mut decoded = Vec::with_capacity(raw.len());
        let mut bytes = raw.as_bytes().iter().copied();
        while let Some(byte) = bytes.next() {
            if byte == b'%' {
                let high = bytes.next().and_then(|b| (b as char).to_digit(16)).ok_or(Error::Invalid)?;
                let low = bytes.next().and_then(|b| (b as char).to_digit(16)).ok_or(Error::Invalid)?;
                decoded.push((high * 16 + low) as u8);
            } else {
                decoded.push(byte);
            }
        }
        std::str::from_utf8(&decoded).map_err(|_| Error::Invalid)?;
        let mut result = Self { page_size: 50, ..Self::default() };
        let mut seen = BTreeSet::new();
        for (key, value) in url::form_urlencoded::parse(raw.as_bytes()) {
            if !seen.insert(key.to_string()) || value.len() > 2048 {
                return Err(Error::Invalid);
            }
            match key.as_ref() {
                "page_size" => result.page_size = value.parse().ok().filter(|n| (1..=200).contains(n)).ok_or(Error::Invalid)?,
                "page_token" => result.page_token = Some(value.into_owned()),
                "provider" if !matches!(value.as_ref(), "huawei" | "agc") => return Err(Error::Invalid),
                "status" if !matches!(value.as_ref(), "active" | "disabled" | "deleted") => return Err(Error::Invalid),
                "user_id" | "display_name" | "provider" | "status" | "litellm_user_id" | "key_id" | "key_alias" => {
                    result.filters.insert(key.into_owned(), value.into_owned());
                }
                _ => return Err(Error::Invalid),
            }
        }
        Ok(result)
    }

    pub fn requires_model_lookup(&self) -> bool {
        self.filters.keys().any(|key| matches!(key.as_str(), "litellm_user_id" | "key_id" | "key_alias"))
    }
}

pub struct QueryPlan {
    pub predicate: String,
    pub parameters: Vec<String>,
    pub after: Option<String>,
    pub page_size: usize,
    context: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    context: String,
    after: String,
}

impl QueryPlan {
    pub fn new(grant: &AdminGrant, actor: &str, query: &AdminQuery) -> Result<Self> {
        use base64::Engine;
        let mut parameters = Vec::new();
        let mut bind = |value: &str| {
            parameters.push(value.to_owned());
            format!("${}", parameters.len())
        };
        let mut clauses = vec![match &grant.identity_scope {
            IdentityScope::Exclusive { exclusive_tenant } if exclusive_tenant == &grant.tenant => "TRUE".to_owned(),
            IdentityScope::Scoped { rules } if !rules.is_empty() => {
                let rules: Vec<_> = rules.iter().map(|rule| {
                    let provider = bind(&rule.provider);
                    let scope = bind(&rule.developer_scope);
                    let client = bind(&rule.client_id);
                    format!("(u.provider={provider} AND u.developer_scope={scope} AND u.client_id={client})")
                }).collect();
                format!("({})", rules.join(" OR "))
            }
            _ => return Err(Error::Configuration),
        }];
        for (key, value) in &query.filters {
            match key.as_str() {
                "display_name" => {
                    let literal = value.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
                    clauses.push(format!("u.display_name LIKE {} ESCAPE E'\\\\'", bind(&format!("%{literal}%"))));
                }
                "user_id" | "provider" | "status" => clauses.push(format!("u.{key}={}", bind(value))),
                "litellm_user_id" | "key_id" | "key_alias" => {}
                _ => return Err(Error::Invalid),
            }
        }
        let context = super::digest(&serde_json::to_string(&(
            &grant.tenant, actor, &grant.identity_scope, &query.filters, query.page_size, "user_id:C:asc",
        )).map_err(|_| Error::Invalid)?);
        let after = query.page_token.as_ref().map(|token| {
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token).map_err(|_| Error::Invalid)?;
            let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
            if cursor.context != context || cursor.after.len() > 512 {
                return Err(Error::Invalid);
            }
            Ok(cursor.after)
        }).transpose()?;
        Ok(Self { predicate: clauses.join(" AND "), parameters, after, page_size: query.page_size, context })
    }

    pub fn page_token(&self, after: &str) -> Result<String> {
        use base64::Engine;
        let cursor = Cursor { context: self.context.clone(), after: after.into() };
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).map_err(|_| Error::Invalid)?))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("accounts management operation failed")]
    Account(#[from] Error),
    #[error("account was not found in the authorized scope")]
    NotFound,
    #[error("managed credential is not recoverable")]
    NotRecoverable,
    #[error("managed credential identity changed")]
    IdentityConflict,
}

impl super::AccountService {
    fn admin_grant(&self, tenant: &str) -> std::result::Result<&AdminGrant, AdminError> {
        Ok(self.config.admin.as_ref().ok_or(Error::Forbidden)?.authorize(tenant)?)
    }

    pub async fn admin_list(&self, tenant: &str, actor: &str, query: &AdminQuery) -> std::result::Result<serde_json::Value, AdminError> {
        let grant = self.admin_grant(tenant)?;
        if !query.requires_model_lookup() {
            return Ok(self.store.admin_list(grant, actor, query).await?);
        }
        // Resolve every candidate before counting: a dependency failure never returns a partial total.
        let scan = AdminQuery { filters: query.filters.iter().filter(|(key,_)| !matches!(key.as_str(),"litellm_user_id"|"key_id"|"key_alias")).map(|(k,v)|(k.clone(),v.clone())).collect(), page_size: 10000, page_token: None };
        let page = self.store.admin_list(grant, actor, &scan).await?;
        if !page["next_page_token"].is_null() { return Err(Error::Unavailable.into()); }
        let mut rows = Vec::new();
        for row in page["users"].as_array().ok_or(Error::Unavailable)? {
            let user = row["user_id"].as_str().ok_or(Error::Unavailable)?;
            let owner = self.cipher.model_owner(user);
            let mut matches = true;
            if let Some(expected) = query.filters.get("litellm_user_id") {
                if expected != &owner { continue; }
                self.models.admin_user_info(&owner).await?.ok_or(Error::Unavailable)?;
            }
            if query.filters.contains_key("key_id") || query.filters.contains_key("key_alias") {
                let Some(record) = self.store.admin_credential(grant, user).await? else { continue; };
                let plain = key_material(&self.cipher,user,&record)?;
                let info = self.models.admin_key_info(&owner,&plain).await?.ok_or(Error::Unavailable)?;
                let view = key_view(&record,&plain,&info,&owner)?;
                if let Some(expected) = query.filters.get("key_id") { matches &= view["key_id"].as_str() == Some(expected); }
                if let Some(expected) = query.filters.get("key_alias") { matches &= view["alias"].as_str().is_some_and(|alias| alias.contains(expected)); }
            }
            if matches { rows.push(row.clone()); }
        }
        let plan = QueryPlan::new(grant,actor,query)?;
        let total = rows.len();
        rows.retain(|row| plan.after.as_ref().is_none_or(|after| row["user_id"].as_str().is_some_and(|id| id > after.as_str())));
        let more = rows.len() > query.page_size;
        rows.truncate(query.page_size);
        let next = if more { rows.last().and_then(|row| row["user_id"].as_str()).map(|id| plan.page_token(id)).transpose()? } else { None };
        Ok(serde_json::json!({"users":rows,"total":total,"next_page_token":next,"degraded":[]}))
    }

    pub async fn admin_detail(&self, tenant: &str, user_id: &str) -> std::result::Result<serde_json::Value, AdminError> {
        let grant = self.admin_grant(tenant)?;
        let user = self.store.admin_user(grant,user_id).await.map_err(scoped_error)?;
        let owner = self.cipher.model_owner(user_id);
        let mut quota = serde_json::json!({"total":null,"used":null,"balance":null,"state":"failed","unit":"points","source_unit":"budget_unit","points_per_budget_unit":self.config.litellm.points_per_budget_unit.to_string(),"field_states":{"total":"failed","used":"failed","balance":"failed"},"reset_period":null,"reset_at":null,"error":"Model quota lookup failed"});
        let mut degraded = Vec::new();
        let record = self.store.admin_credential(grant,user_id).await.map_err(scoped_error)?;
        let mut key = serde_json::Value::Null;
        let key_state = if let Some(record) = record {
            key = serde_json::json!({"key_id":record.version,"key_display":null,"config_status":if record.ready {"ready"} else {"pending"},"summary_only":false,"recoverable":false,"disabled":null,"expires_at":null,"alias":null});
            match key_material(&self.cipher,user_id,&record) {
                Ok(plain) => {
                    if let Ok(Some(info)) = self.models.admin_key_info(&owner,&plain).await {
                        if let Ok(view) = key_view(&record,&plain,&info,&owner) {
                            key = view;
                            if let Ok(value) = admin_quota(&info,self.config.litellm.points_per_budget_unit) {
                                quota = value;
                            }
                        }
                    }
                    if quota["state"] == "failed" { degraded.push("litellm"); }
                }
                Err(_) => {
                    quota["state"] = serde_json::json!("not_recoverable");
                    quota["field_states"] = serde_json::json!({"total":"not_recoverable","used":"not_recoverable","balance":"not_recoverable"});
                    quota["error"] = serde_json::json!("Managed credential is not recoverable");
                    degraded.push("litellm");
                }
            }
            "exists"
        } else {
            quota["state"] = serde_json::json!("absent");
            quota["field_states"] = serde_json::json!({"total":"absent","used":"absent","balance":"absent"});
            quota["error"] = serde_json::Value::Null;
            "absent"
        };
        quota["queried_at"] = serde_json::json!(chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()).to_rfc3339_opts(chrono::SecondsFormat::Secs,true));
        Ok(serde_json::json!({"user":user,"litellm":{"user_id":owner,"key":key,"key_state":key_state},"quota":quota,"degraded":degraded}))
    }

    pub async fn admin_reveal(&self, tenant: &str, user: &str, key_id: &str) -> std::result::Result<String, AdminError> {
        let grant = self.admin_grant(tenant)?;
        let record = self.store.admin_credential(grant,user).await.map_err(scoped_error)?.ok_or(AdminError::NotRecoverable)?;
        verify_reveal(&record,&record,key_id)?;
        let plain = key_material(&self.cipher,user,&record)?;
        let owner = self.cipher.model_owner(user);
        let info = self.models.admin_key_info(&owner,&plain).await?.ok_or(AdminError::NotRecoverable)?;
        key_view(&record,&plain,&info,&owner)?;
        let fresh = self.store.admin_credential(grant,user).await.map_err(scoped_error)?.ok_or(AdminError::IdentityConflict)?;
        verify_reveal(&record,&fresh,key_id)?;
        Ok(plain)
    }
}

fn scoped_error(error: Error) -> AdminError {
    match error { Error::Forbidden => AdminError::NotFound, error => AdminError::Account(error) }
}

pub(crate) fn admin_quota(info: &serde_json::Value, multiplier: f64) -> Result<serde_json::Value> {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    let factor = Decimal::from_str(&multiplier.to_string()).map_err(|_| Error::Configuration)?;
    if factor <= Decimal::ZERO { return Err(Error::Configuration); }
    let amount = |field: &str| -> Result<Option<Decimal>> {
        match info.get(field) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => {
                let text = match value { serde_json::Value::String(text) => text.clone(), serde_json::Value::Number(number) => number.to_string(), _ => return Err(Error::Unavailable) };
                let value = Decimal::from_str(&text).or_else(|_| Decimal::from_scientific(&text)).map_err(|_| Error::Unavailable)?;
                Ok(Some(value.checked_mul(factor).ok_or(Error::Unavailable)?))
            }
        }
    };
    let total = amount("max_budget")?;
    let used = amount("spend")?;
    let balance = match (total,used) { (Some(total),Some(used)) => Some(total.checked_sub(used).ok_or(Error::Unavailable)?), _ => None };
    let unconfigured = info.get("max_budget") == Some(&serde_json::Value::Null);
    let state = if unconfigured { "unconfigured" } else if balance.is_none() { "unknown" } else if balance == Some(Decimal::ZERO) { "zero" } else { "ok" };
    let field_state = |value: Option<Decimal>| if value.is_some() { "known" } else if unconfigured { "unconfigured" } else { "unknown" };
    Ok(serde_json::json!({"total":total.map(|n|n.normalize().to_string()),"used":used.map(|n|n.normalize().to_string()),"balance":balance.map(|n|n.normalize().to_string()),"state":state,"unit":"points","source_unit":"budget_unit","points_per_budget_unit":factor.normalize().to_string(),"field_states":{"total":field_state(total),"used":field_state(used),"balance":field_state(balance)},"reset_period":info.get("budget_duration").filter(|v|v.is_string()),"reset_at":info.get("budget_reset_at").filter(|v|v.is_string()),"error":null}))
}

pub(crate) fn key_material(
    cipher: &super::CredentialCipher,
    user: &str,
    record: &super::store::CredentialRecord,
) -> std::result::Result<String, AdminError> {
    if !record.ready {
        return Err(AdminError::NotRecoverable);
    }
    let aad = cipher.associated_data(user).map_err(|_| AdminError::NotRecoverable)?;
    let bytes = cipher.open(&aad, &record.encrypted_key).map_err(|_| AdminError::NotRecoverable)?;
    let text = String::from_utf8(bytes).map_err(|_| AdminError::NotRecoverable)?;
    if text.is_empty() || text.chars().any(char::is_control) {
        return Err(AdminError::NotRecoverable);
    }
    Ok(text)
}

pub(crate) fn key_view(
    record: &super::store::CredentialRecord,
    plain: &str,
    info: &serde_json::Value,
    owner: &str,
) -> std::result::Result<serde_json::Value, AdminError> {
    if info["user_id"].as_str() != Some(owner) {
        return Err(AdminError::IdentityConflict);
    }
    let digest = super::digest(plain);
    Ok(serde_json::json!({"key_id":record.version,"key_display":format!("{}…",&digest[..8]),
        "alias":info["key_alias"],"config_status":if record.ready {"ready"} else {"pending"},
        "disabled":info["blocked"].as_bool(),"expires_at":info["expires"],
        "summary_only":false,"recoverable":record.ready}))
}

pub(crate) fn verify_reveal(
    original: &super::store::CredentialRecord,
    fresh: &super::store::CredentialRecord,
    expected: &str,
) -> std::result::Result<(), AdminError> {
    if original.version != expected || fresh.version != expected || original.encrypted_key != fresh.encrypted_key {
        return Err(AdminError::IdentityConflict);
    }
    if !original.ready || !fresh.ready {
        return Err(AdminError::NotRecoverable);
    }
    Ok(())
}

impl AdminConfig {
    /// Reject ambiguous ownership before serving any management requests.
    pub fn validate(&self) -> Result<()> {
        let mut tenants = BTreeSet::new();
        let mut ranges = BTreeSet::new();
        for grant in &self.grants {
            adx_agent_core::identifier(&grant.tenant, "accounts admin tenant")
                .map_err(|_| Error::Configuration)?;
            if !tenants.insert(&grant.tenant) {
                return Err(Error::Configuration);
            }
            match &grant.identity_scope {
                IdentityScope::Exclusive { exclusive_tenant } => {
                    if exclusive_tenant != &grant.tenant || self.grants.len() != 1 {
                        return Err(Error::Configuration);
                    }
                }
                IdentityScope::Scoped { rules } => {
                    if rules.is_empty() {
                        return Err(Error::Configuration);
                    }
                    for rule in rules {
                        if !matches!(rule.provider.as_str(), "huawei" | "agc")
                            || !ranges.insert(rule)
                        {
                            return Err(Error::Configuration);
                        }
                        for value in [&rule.developer_scope, &rule.client_id] {
                            adx_agent_core::identifier(value, "accounts admin scope")
                                .map_err(|_| Error::Configuration)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The tenant must be supplied by Gateway authentication, never request input.
    pub fn authorize(&self, authenticated_tenant: &str) -> Result<&AdminGrant> {
        self.validate()?;
        self.grants
            .iter()
            .find(|grant| grant.tenant == authenticated_tenant)
            .ok_or(Error::Forbidden)
    }
}

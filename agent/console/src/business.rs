//! Read-only business identity adapters, independent of Gateway assembly.
use crate::config::{BindingRule,TenantType};
use adx_agent_core::{Scope,TemplateRef};
use sha2::{Digest,Sha256};

/// Preserve the existing accounts JSON-tuple billing namespace contract.
pub fn model_owner(namespace:&str,user:&str)->Result<String,&'static str> {
    adx_agent_core::identifier(namespace,"credential namespace").map_err(|_|"invalid_business_mapping")?;
    adx_agent_core::identifier(user,"user identity").map_err(|_|"invalid_business_identity")?;
    let tuple=serde_json::to_vec(&[namespace,user]).map_err(|_|"invalid_business_identity")?;
    Ok(format!("adx-{}",hex::encode(Sha256::digest(tuple))))
}

pub fn association_summary(items:&[serde_json::Value])->(&'static str,Option<bool>) {
    if items.iter().any(|item|item["existence"]=="exists") {("exists",Some(true))}
    else if !items.is_empty() && items.iter().all(|item|item["existence"]=="not_created") {("not_created",Some(false))}
    else {("uncertain",None)}
}

pub fn binding_scope(tenant_type: TenantType, tenant: &str, user: &str, rule: &BindingRule) -> Result<Scope,&'static str> {
    if tenant_type == TenantType::Standard { return Err("feature_not_enabled"); }
    for value in [tenant,user] {
        adx_agent_core::identifier(value,"business identity").map_err(|_| "invalid_business_identity")?;
        if value.len() > 256 { return Err("invalid_business_identity"); }
    }
    let tuple = match rule.kind.as_str() {
        "jiuwen" => {
            let agent_type = rule.agent_type.as_deref().ok_or("invalid_business_mapping")?;
            adx_agent_core::identifier(agent_type,"agent type").map_err(|_| "invalid_business_mapping")?;
            serde_json::to_vec(&[tenant,user,agent_type])
        }
        "orbit"|"digitalmate" => serde_json::to_vec(&[tenant,user]),
        _ => return Err("invalid_business_mapping"),
    }.map_err(|_| "invalid_business_identity")?;
    let template_ref = match rule.reference.kind.as_str() {
        "version" => TemplateRef::Version(rule.reference.name.clone()),
        "alias" => TemplateRef::Alias(rule.reference.name.clone()),
        _ => return Err("invalid_business_mapping"),
    };
    let scope = Scope { tenant:tenant.into(),template:rule.template.clone(),template_ref,
        binding_id:format!("{}-{}",rule.kind,hex::encode(Sha256::digest(tuple))) };
    scope.validate().map_err(|_| "invalid_business_mapping")?;
    Ok(scope)
}

#[derive(serde::Deserialize)]
struct AssociationBinding { scope: Scope }
#[derive(serde::Deserialize)]
struct AssociationPage {
    bindings: Vec<AssociationBinding>,
    next_page_token: Option<String>,
}

/// Query all references for each configured business identity without activating it.
/// Invalid mappings return an error; upstream failures are represented as uncertain rows.
pub async fn associations(clients:&crate::clients::Clients,endpoint:&crate::config::Endpoint,tenant:&crate::config::Tenant,user:&str,rules:&[BindingRule]) -> Result<Vec<serde_json::Value>,&'static str> {
    if tenant.tenant_type==TenantType::Standard { return Err("feature_not_enabled"); }
    let mut result=Vec::with_capacity(rules.len());
    for rule in rules {
        let scope=binding_scope(tenant.tenant_type,&tenant.tenant_id,user,rule)?;
        let mut refs=std::collections::BTreeSet::new();
        let mut cursors=std::collections::BTreeSet::new();
        let mut cursor:Option<String>=None;
        let error=loop {
            let mut url=crate::clients::Clients::url(&endpoint.base_url,&["bindings"]).map_err(|_|"invalid_business_mapping")?;
            url.query_pairs_mut().append_pair("template",&scope.template).append_pair("binding_id",&scope.binding_id).append_pair("page_size",&adx_agent_core::limits::BINDING_PAGE_SIZE.to_string());
            if let Some(cursor)=&cursor { url.query_pairs_mut().append_pair("page_token",cursor); }
            let value=match clients.json(tenant,reqwest::Method::GET,url,None,endpoint.timeout_ms).await {
                Ok(value)=>value,
                Err(_)=>break Some("agent_query_failed"),
            };
            let page=match serde_json::from_value::<AssociationPage>(value) {
                Ok(page)=>page,
                Err(_)=>break Some("binding_identity_unverified"),
            };
            if page.bindings.iter().any(|binding| binding.scope.validate().is_err()
                || binding.scope.tenant!=scope.tenant || binding.scope.template!=scope.template
                || binding.scope.binding_id!=scope.binding_id) {
                break Some("binding_identity_unverified");
            }
            for binding in page.bindings { refs.insert(binding.scope.template_ref); }
            match page.next_page_token {
                None=>break None,
                Some(next) if next.is_empty() || !cursors.insert(next.clone())=>break Some("agent_query_failed"),
                Some(next)=>cursor=Some(next),
            }
        };
        let existence=if error.is_some() {"uncertain"}else if refs.is_empty() {"not_created"}else{"exists"};
        let mut refs:Vec<_>=refs.into_iter().collect();
        refs.sort_by(|left,right|(left.kind(),left.name()).cmp(&(right.kind(),right.name())));
        let refs:Vec<_>=refs.into_iter().map(|reference|serde_json::json!({"kind":reference.kind(),"name":reference.name()})).collect();
        let mut item=serde_json::json!({"kind":rule.kind,"template":scope.template,"refs":refs,"binding_id":scope.binding_id,"existence":existence});
        if let Some(error)=error {item["error"]=serde_json::json!({"kind":error,"message":"关联暂无法确认，请重试"});}
        result.push(item);
    }
    Ok(result)
}

pub struct QuotaInput {
    pub configured: Option<bool>,
    pub total: Option<String>,
    pub used: Option<String>,
    /// Adapter-normalized balance: field-specific sentinels must already be removed.
    pub balance: Option<String>,
    pub failed: bool,
}
/// Normalize LiteLLM budget fields without floating-point arithmetic or clamping debt.
pub fn litellm_quota(info:&serde_json::Value,multiplier:&str)->Result<serde_json::Value,&'static str> {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    let amount=|key:&str|->Result<Option<Decimal>,&'static str> {
        match info.get(key) {
            None|Some(serde_json::Value::Null)=>Ok(None),
            Some(value)=> {
                let raw=if let Some(text)=value.as_str() {text.to_owned()} else if value.is_number() {value.to_string()} else {return Err("invalid_quota_value");};
                Decimal::from_str(&raw).or_else(|_|Decimal::from_scientific(&raw)).map(Some).map_err(|_|"invalid_quota_value")
            }
        }
    };
    let total=amount("max_budget")?;
    let used=amount("spend")?;
    let balance=match (total,used) { (Some(total),Some(used))=>Some(total.checked_sub(used).ok_or("quota_overflow")?),_=>None };
    let configured=match info.get("max_budget") {None=>None,Some(serde_json::Value::Null)=>Some(false),Some(_)=>Some(true)};
    let mut value=quota(QuotaInput {configured,total:total.map(|v|v.to_string()),used:used.map(|v|v.to_string()),balance:balance.map(|v|v.to_string()),failed:false},multiplier)?;
    value["reset_period"]=info.get("budget_duration").filter(|v|v.is_string()).cloned().unwrap_or(serde_json::Value::Null);
    value["reset_at"]=info.get("budget_reset_at").filter(|v|v.is_string()).cloned().unwrap_or(serde_json::Value::Null);
    value["queried_at"]=serde_json::json!(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0,|v|v.as_secs()));
    Ok(value)
}
pub fn quota(input: QuotaInput, multiplier: &str) -> Result<serde_json::Value,&'static str> {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    let multiplier = Decimal::from_str(multiplier).map_err(|_| "invalid_points_multiplier")?;
    if multiplier <= Decimal::ZERO { return Err("invalid_points_multiplier"); }
    let convert = |value: &Option<String>| -> Result<Option<String>,&'static str> {
        value.as_deref().map(|value| Decimal::from_str(value).map_err(|_| "invalid_quota_value")?
            .checked_mul(multiplier).map(|value| value.normalize().to_string()).ok_or("quota_overflow")).transpose()
    };
    let total = convert(&input.total)?;
    let used = convert(&input.used)?;
    let balance = convert(&input.balance)?;
    let state = if input.failed { "failed" } else if input.configured == Some(false) { "unconfigured" }
        else if input.configured.is_none() || total.is_none() || used.is_none() || balance.is_none() { "unknown" }
        else if balance.as_deref() == Some("0") { "zero" } else { "ok" };
    let field_state = |value: &Option<String>| if value.is_some() { "known" } else if input.failed { "failed" }
        else if input.configured == Some(false) { "unconfigured" } else { "unknown" };
    Ok(serde_json::json!({"total":total,"used":used,"balance":balance,"state":state,
        "unit":"points","source_unit":"budget_unit","points_per_budget_unit":multiplier.normalize().to_string(),
        "field_states":{"total":field_state(&total),"used":field_state(&used),"balance":field_state(&balance)},
        "reset_period":null,"reset_at":null,"error":if input.failed { Some("额度上游查询失败") } else { None }}))
}

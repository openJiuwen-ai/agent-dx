//! Browser query validation; ownership and pagination belong to Accounts.
#[derive(Default, serde::Serialize)]
pub struct UserQuery {
    pub filters: std::collections::BTreeMap<String,String>,
    pub page_size: usize,
    pub page_token: Option<String>,
}
impl UserQuery {
    pub fn parse(raw:&str)->Result<Self,&'static str> {
        let mut decoded = Vec::new();
        let mut bytes = raw.as_bytes().iter().copied();
        while let Some(byte) = bytes.next() {
            if byte == b'%' {
                let high = bytes.next().and_then(|b|(b as char).to_digit(16)).ok_or("invalid_query")?;
                let low = bytes.next().and_then(|b|(b as char).to_digit(16)).ok_or("invalid_query")?;
                decoded.push((high*16+low) as u8);
            } else { decoded.push(byte); }
        }
        std::str::from_utf8(&decoded).map_err(|_|"invalid_query")?;
        let mut query = Self {page_size:50,..Default::default()};
        let mut seen = std::collections::BTreeSet::new();
        for (key,value) in url::form_urlencoded::parse(raw.as_bytes()) {
            if !seen.insert(key.to_string()) || value.len()>2048 {return Err("invalid_query");}
            match key.as_ref() {
                "page_size"=>query.page_size=value.parse().ok().filter(|n|*n>0&&*n<=200).ok_or("invalid_query")?,
                "page_token"=>query.page_token=Some(value.into_owned()),
                "provider" if !matches!(value.as_ref(),"huawei"|"agc")=>return Err("invalid_query"),
                "status" if !matches!(value.as_ref(),"active"|"disabled"|"deleted")=>return Err("invalid_query"),
                "user_id"|"display_name"|"provider"|"status"|"litellm_user_id"|"key_id"|"key_alias"=>{query.filters.insert(key.into_owned(),value.into_owned());},
                _=>return Err("invalid_query"),
            }
        }
        Ok(query)
    }
}

fn fields(value: &serde_json::Value, names: &[&str]) -> serde_json::Value {
    serde_json::Value::Object(names.iter().filter_map(|name|value.get(*name).map(|value|((*name).to_owned(),value.clone()))).collect())
}

fn user(value:&serde_json::Value)->Result<serde_json::Value,&'static str> {
    if value["user_id"].as_str().is_none() {return Err("upstream_response_invalid");}
    let mut user=fields(value,&["user_id","provider","provider_label","display_name","status","created_at","has_key","key_state","has_binding","binding_state"]);
    if let Some(scope)=value.get("identity_scope") {user["identity_scope"]=fields(scope,&["developer_scope","client_id","exclusive_tenant"]);}
    Ok(user)
}

pub fn project_list(value:serde_json::Value)->Result<serde_json::Value,&'static str> {
    let users=value["users"].as_array().ok_or("upstream_response_invalid")?.iter().map(user).collect::<Result<Vec<_>,_>>()?;
    if value["total"].as_u64().is_none() || !value["degraded"].is_array() {return Err("upstream_response_invalid");}
    Ok(serde_json::json!({"users":users,"total":value["total"],"next_page_token":value["next_page_token"],"degraded":value["degraded"]}))
}

pub fn project_detail(value:serde_json::Value)->Result<serde_json::Value,&'static str> {
    if !value["litellm"].is_object() || !value["quota"].is_object() || !value["degraded"].is_array() {return Err("upstream_response_invalid");}
    let mut model=fields(&value["litellm"],&["user_id","key_state"]);
    model["key"]=if value["litellm"]["key"].is_null() {serde_json::Value::Null} else {fields(&value["litellm"]["key"],&["key_id","key_display","config_status","summary_only","recoverable","disabled","expires_at","alias"])};
    let mut quota=fields(&value["quota"],&["total","used","balance","state","unit","source_unit","points_per_budget_unit","reset_period","reset_at","queried_at","error"]);
    if let Some(states)=value["quota"].get("field_states") {quota["field_states"]=fields(states,&["total","used","balance"]);}
    Ok(serde_json::json!({"user":user(&value["user"])?,"litellm":model,"quota":quota,"degraded":value["degraded"]}))
}

use crate::{config::{BindingRule, Reference, TenantType}, business::binding_scope};

#[test]
fn three_application_namespaces_match_gateway_tuple_rules() {
    for (kind,expected) in [
        ("jiuwen","jiuwen-f5a344ea6b5108c958203f03b6cb7c755e299a712f2fdad6ca9682c868b611c8"),
        ("orbit","orbit-51caeb32bf20ebc625e22bd3b6b93a488ceedf385159770501f6b6edf93498aa"),
        ("digitalmate","digitalmate-51caeb32bf20ebc625e22bd3b6b93a488ceedf385159770501f6b6edf93498aa"),
    ] {
        let rule = BindingRule { kind:kind.into(),agent_type:Some("assistant".into()),template:kind.into(),reference:Reference { kind:"alias".into(),name:"production".into() } };
        let scope = binding_scope(TenantType::Digitalmate,"tenant","user",&rule).unwrap();
        assert_eq!(scope.binding_id,expected);
        assert_eq!(scope.tenant,"tenant");
        assert_eq!(scope.template_ref,adx_agent_core::TemplateRef::Alias("production".into()));
        assert_ne!(binding_scope(TenantType::Digitalmate,"other","user",&rule).unwrap().binding_id,scope.binding_id);
        assert_ne!(binding_scope(TenantType::Digitalmate,"tenant","other",&rule).unwrap().binding_id,scope.binding_id);
        assert_eq!(binding_scope(TenantType::Standard,"tenant","user",&rule).err(),Some("feature_not_enabled"));
    }
}

#[test]
fn quota_states_preserve_negative_balance_and_decimal_multiplier() {
    use crate::business::{QuotaInput,quota};
    for (input,state,balance) in [
        (QuotaInput { configured:Some(true),total:Some("30".into()),used:Some("31".into()),balance:Some("-1".into()),failed:false },"ok",Some("-10")),
        (QuotaInput { configured:Some(true),total:Some("0".into()),used:Some("0".into()),balance:Some("0".into()),failed:false },"zero",Some("0")),
        (QuotaInput { configured:Some(false),total:None,used:None,balance:None,failed:false },"unconfigured",None),
        (QuotaInput { configured:Some(true),total:None,used:Some("1".into()),balance:None,failed:false },"unknown",None),
        (QuotaInput { configured:Some(false),total:None,used:None,balance:None,failed:true },"failed",None),
    ] {
        let value = quota(input,"10").unwrap();
        assert_eq!(value["state"],state);
        assert_eq!(value["balance"].as_str(),balance);
        assert_eq!(value["points_per_budget_unit"],"10");
    }
    let value = quota(QuotaInput { configured:Some(true),total:Some("0.000000000000001".into()),used:Some("0".into()),balance:Some("-0.000000000000001".into()),failed:false },"0.1").unwrap();
    assert_eq!(value["balance"],"-0.0000000000000001");
    assert!(quota(QuotaInput { configured:None,total:None,used:None,balance:None,failed:false },"0").is_err());
}
#[tokio::test]
async fn association_queries_distinguish_absence_failure_and_verified_scope() {
    use axum::{Router,routing::get,Json,http::StatusCode};
    use serde_json::json;
    let server=Router::new().route("/bindings",get(|axum::extract::Query(query):axum::extract::Query<std::collections::HashMap<String,String>>|async move {
        let template=query["template"].clone();let id=query["binding_id"].clone();
        if query["page_size"]!="100" { return (StatusCode::BAD_REQUEST,Json(json!({}))); }
        match template.as_str() {
            "missing"=>(StatusCode::OK,Json(json!({"bindings":[],"next_page_token":null}))),
            "failed"=>(StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"secret"}))),
            _=>(StatusCode::OK,Json(json!({"bindings":[{"scope":{"tenant":"t","template":template,"template_ref":{"alias":"stable"},"binding_id":id},"phase":"active","current":{"environment_id":"hidden-env"}}],"next_page_token":null}))),
        }
    }));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let task=tokio::spawn(async move {axum::serve(listener,server).await.unwrap();});
    let token=std::env::temp_dir().join(format!("console-associations-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token,"tenant-secret").unwrap();
    let tenant:crate::config::Tenant=serde_json::from_value(json!({"tenant_id":"t","display_name":"T","type":"digitalmate","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token})).unwrap();
    let rules:Vec<crate::config::BindingRule>=serde_json::from_value(json!([
        {"kind":"jiuwen","agent_type":"assistant","template":"found","ref":{"kind":"alias","name":"stable"}},
        {"kind":"orbit","template":"missing","ref":{"kind":"alias","name":"stable"}},
        {"kind":"digitalmate","template":"failed","ref":{"kind":"alias","name":"stable"}}
    ])).unwrap();
    let endpoint=crate::config::Endpoint {base_url:format!("http://{address}"),timeout_ms:1000};
    let result=crate::business::associations(&crate::clients::Clients::new().unwrap(),&endpoint,&tenant,"user",&rules).await.unwrap();
    assert_eq!(result[0]["existence"],"exists");
    assert_eq!(result[1]["existence"],"not_created");
    assert_eq!(result[2]["existence"],"uncertain");
    assert_eq!(result[0]["refs"],json!([{"kind":"alias","name":"stable"}]));
    assert_eq!(result[1]["refs"],json!([]));
    let raw=serde_json::to_string(&result).unwrap();
    for forbidden in ["hidden-env","phase","current","secret","detail_path"] {assert!(!raw.contains(forbidden));}
    task.abort();
    std::fs::remove_file(token).unwrap();
}
#[tokio::test]
async fn association_catalog_collects_all_pages_and_preserves_verified_partial_results() {
    use axum::{Router,routing::get,Json,http::StatusCode};
    use serde_json::json;
    let server=Router::new().route("/bindings",get(|axum::extract::Query(query):axum::extract::Query<std::collections::HashMap<String,String>>|async move {
        let template=&query["template"];
        let later=query.contains_key("page_token");
        if template=="partial" && later { return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({}))); }
        if template=="malformed" { return (StatusCode::OK,Json(json!({}))); }
        let reference=if later {json!({"version":"2"})}else{json!({"alias":"stable"})};
        let tenant=if template=="wrong" {"other"}else{"t"};
        let binding=json!({"scope":{"tenant":tenant,"template":template,"template_ref":reference,"binding_id":query["binding_id"]},"current":{"environment_id":"private-env"}});
        let token=if !later || template=="loop" {Some("next")}else{None};
        (StatusCode::OK,Json(json!({"bindings":[binding],"next_page_token":token})))
    }));
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let task=tokio::spawn(async move {axum::serve(listener,server).await.unwrap();});
    let token=std::env::temp_dir().join(format!("console-association-pages-{}",uuid::Uuid::new_v4()));
    std::fs::write(&token,"tenant-secret").unwrap();
    let tenant:crate::config::Tenant=serde_json::from_value(json!({"tenant_id":"t","display_name":"T","type":"digitalmate","admin_token_sha256":"00".repeat(32),"agent":{"enabled":true},"upstream_token_file":token})).unwrap();
    let endpoint=crate::config::Endpoint {base_url:format!("http://{address}"),timeout_ms:1000};
    for template in ["complete","partial","wrong","malformed","loop"] {
        let rule:BindingRule=serde_json::from_value(json!({"kind":"jiuwen","agent_type":"assistant","template":template,"ref":{"kind":"alias","name":"configured-only"}})).unwrap();
        let result=crate::business::associations(&crate::clients::Clients::new().unwrap(),&endpoint,&tenant,"user",&[rule]).await.unwrap();
        let item=&result[0];
        assert_eq!(item["existence"],if template=="complete" {"exists"}else{"uncertain"},"{template}");
        if template=="complete" {
            assert_eq!(item["refs"],json!([{"kind":"alias","name":"stable"},{"kind":"version","name":"2"}]));
        } else if template=="partial" {
            assert_eq!(item["refs"],json!([{"kind":"alias","name":"stable"}]));
        } else if matches!(template,"wrong"|"malformed") { assert_eq!(item["refs"],json!([])); }
        let raw=item.to_string();
        assert!(!raw.contains("private-env"));
        assert!(!raw.contains("configured-only"));
    }
    task.abort();
    std::fs::remove_file(token).unwrap();
}
#[test]
fn association_summary_only_asserts_absence_when_every_scope_is_confirmed_absent() {
    use serde_json::json;
    assert_eq!(crate::business::association_summary(&[json!({"existence":"not_created"}),json!({"existence":"uncertain"})]),("uncertain",None));
    assert_eq!(crate::business::association_summary(&[json!({"existence":"not_created"}),json!({"existence":"exists"})]),("exists",Some(true)));
    assert_eq!(crate::business::association_summary(&[json!({"existence":"not_created"})]),("not_created",Some(false)));
    assert_eq!(crate::business::association_summary(&[]),("uncertain",None));
}
#[test]
fn litellm_budget_adapter_preserves_overdrawn_balance_and_unconfigured_budget() {
    use serde_json::json;
    let value=crate::business::litellm_quota(&json!({"max_budget":"0.3","spend":"0.4","budget_duration":"1d","budget_reset_at":"2026-11-12T00:00:00Z"}),"10").unwrap();
    assert_eq!(value["balance"],"-1");
    assert_eq!(value["total"],"3");
    assert_eq!(value["state"],"ok");
    assert_eq!(value["reset_period"],"1d");
    let value=crate::business::litellm_quota(&json!({"max_budget":null,"spend":"0"}),"10").unwrap();
    assert_eq!(value["state"],"unconfigured");
    assert_eq!(value["total"],serde_json::Value::Null);
    assert_eq!(value["used"],"0");
    let value=crate::business::litellm_quota(&json!({"spend":0}),"10").unwrap();
    assert_eq!(value["state"],"unknown");
    assert!(crate::business::litellm_quota(&json!({"max_budget":"bad","spend":0}),"10").is_err());
}

#[test]
fn billing_owner_uses_explicit_accounts_namespace_not_tenant_id() {
    use sha2::{Digest,Sha256};
    assert_eq!(crate::business::model_owner("jiuwen","alice").unwrap(),format!("adx-{}",hex::encode(Sha256::digest(br#"["jiuwen","alice"]"#))));
    assert_ne!(crate::business::model_owner("other","alice").unwrap(),crate::business::model_owner("jiuwen","alice").unwrap());
    assert!(crate::business::model_owner("","alice").is_err());
}

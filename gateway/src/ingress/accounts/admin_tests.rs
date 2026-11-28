use super::admin::AdminConfig;
use serde_json::{json, Value};

#[test]
fn admin_configuration_is_opt_in_and_validated_at_service_boundary() {
    let value = json!({"developer_scope":"developer","agreement_version":"1","database_url_env":"UNUSED",
        "litellm":{"management_url":"https://model.test","api_base":"https://model.test/v1","admin_key_env":"UNUSED","max_budget":1.0,"rpm_limit":10,"tpm_limit":1000}});
    let absent: super::AccountConfig = serde_json::from_value(value.clone()).unwrap();
    assert!(absent.admin.is_none());
    let mut enabled = value;
    enabled["admin"] = json!({"grants":[scoped("tenant-a", "project-a")]});
    let enabled: super::AccountConfig = serde_json::from_value(enabled).unwrap();
    assert!(enabled.admin.unwrap().authorize("tenant-a").is_ok());
}

#[test]
fn admin_http_routes_enforce_methods_and_reveal_input() {
    use super::admin_http::{operation, Operation, RevealInput};
    assert!(matches!(operation("GET","/api/accounts/v1/admin/users").unwrap(),Operation::List));
    assert!(operation("POST","/api/accounts/v1/admin/users").is_err());
    assert!(operation("GET","/api/accounts/v1/admin/unknown").is_err());
    assert!(matches!(operation("GET","/api/accounts/v1/admin/users/a%2Fb").unwrap(),Operation::Detail(user) if user=="a/b"));
    assert!(serde_json::from_value::<RevealInput>(json!({"key_id":"k","purpose":"rotate"})).unwrap().validate().is_err());
    assert!(serde_json::from_value::<RevealInput>(json!({"key_id":"k","purpose":"copy"})).unwrap().validate().is_ok());
}

#[test]
fn admin_namespace_is_explicit_without_business_entrypoint() {
    use super::admin::model_namespace;
    assert!(model_namespace(None,None).is_err());
    assert_eq!(model_namespace(Some("business"),None).unwrap(),"business");
    assert_eq!(model_namespace(None,Some("standalone")).unwrap(),"standalone");
    assert!(model_namespace(Some("business"),Some("different")).is_err());
}

#[tokio::test]
async fn admin_http_sensitive_responses_are_not_cached() {
    let response = super::admin_http::response(http::StatusCode::OK,&json!({"key":"test-only"}));
    assert_eq!(response.headers()["cache-control"],"no-store");
    let error = super::admin_http::failure(http::StatusCode::FORBIDDEN,"forbidden");
    assert_eq!(error.headers()["cache-control"],"no-store");
}

fn config(grants: Value) -> AdminConfig {
    serde_json::from_value(json!({"grants": grants})).unwrap()
}

fn scoped(tenant: &str, scope: &str) -> Value {
    json!({"tenant":tenant,"identity_scope":{"mode":"scoped","rules":[
        {"provider":"agc","developer_scope":scope,"client_id":"client"}
    ]}})
}

#[test]
fn admin_requires_explicit_tenant_grant() {
    let empty = config(json!([]));
    assert!(empty.validate().is_ok());
    assert!(empty.authorize("tenant-a").is_err());
    let grants = config(json!([scoped("tenant-a", "project-a")]));
    assert!(grants.validate().is_ok());
    assert!(grants.authorize("tenant-a").is_ok());
    assert!(grants.authorize("tenant-b").is_err());
}

#[test]
fn admin_rejects_empty_or_invalid_scope() {
    for scope in [
        json!({"mode":"scoped","rules":[]}),
        json!({"mode":"scoped","rules":[{"provider":"other","developer_scope":"p","client_id":"c"}]}),
        json!({"mode":"scoped","rules":[{"provider":"agc","developer_scope":"","client_id":"c"}]}),
        json!({"mode":"exclusive","exclusive_tenant":"tenant-b"}),
    ] {
        assert!(config(json!([{"tenant":"tenant-a","identity_scope":scope}])).validate().is_err());
    }
}

#[test]
fn admin_rejects_duplicate_tenants_and_overlapping_ranges() {
    for grants in [
        json!([scoped("tenant-a","p1"),scoped("tenant-a","p2")]),
        json!([scoped("tenant-a","p1"),scoped("tenant-b","p1")]),
        json!([{"tenant":"tenant-a","identity_scope":{"mode":"exclusive","exclusive_tenant":"tenant-a"}},scoped("tenant-b","p2")]),
    ] {
        assert!(config(grants).validate().is_err());
    }
    assert!(config(json!([scoped("tenant-a","p1"),scoped("tenant-b","p2")])).validate().is_ok());
}

#[test]
fn admin_rejects_mixed_scope_modes_and_unknown_fields() {
    for scope in [
        json!({"mode":"scoped","exclusive_tenant":"tenant-a","rules":[]}),
        json!({"mode":"exclusive","exclusive_tenant":"tenant-a","rules":[{"provider":"agc","developer_scope":"p","client_id":"c"}]}),
        json!({"mode":"exclusive","exclusive_tenant":"tenant-a","request_override":true}),
    ] {
        let value = json!({"grants":[{"tenant":"tenant-a","identity_scope":scope}]});
        assert!(serde_json::from_value::<AdminConfig>(value).is_err());
    }
}

#[test]
fn admin_query_enforces_scope_and_binds_pagination_context() {
    use super::admin::{AdminQuery, QueryPlan};
    let grants = config(json!([scoped("tenant-a", "project-a")]));
    let grant = grants.authorize("tenant-a").unwrap();
    let query = AdminQuery::parse("provider=agc&display_name=a%25_&page_size=1").unwrap();
    let plan = QueryPlan::new(grant, "operator", &query).unwrap();
    assert_eq!(plan.parameters, ["agc", "project-a", "client", "%a\\%\\_%", "agc"]);
    assert!(plan.predicate.contains("u.developer_scope=$2"));
    assert!(plan.predicate.contains(" AND "));
    let token = plan.page_token("user-1").unwrap();
    let mut next = query;
    next.page_token = Some(token);
    assert!(QueryPlan::new(grant, "operator", &next).is_ok());
    assert!(QueryPlan::new(grant, "another-operator", &next).is_err());
    next.page_size = 2;
    assert!(QueryPlan::new(grant, "operator", &next).is_err());
}

#[test]
fn admin_query_rejects_request_scope_override_and_lossy_identities() {
    use super::admin::AdminQuery;
    for raw in ["tenant=other", "developer_scope=other", "page_size=0", "page_size=201",
        "provider=agc&provider=huawei", "user_id=%ff", "user_id=%zz", "status=unknown"] {
        assert!(AdminQuery::parse(raw).is_err(), "{raw}");
    }
}

#[test]
fn admin_quota_preserves_decimal_debt_and_unconfigured_values() {
    let quota = super::admin::admin_quota(&json!({"max_budget":"0.1","spend":"0.3"}),2.0).unwrap();
    assert_eq!(quota["total"],"0.2");
    assert_eq!(quota["balance"],"-0.4");
    let quota = super::admin::admin_quota(&json!({"max_budget":null,"spend":0}),2.0).unwrap();
    assert_eq!(quota["state"],"unconfigured");
    assert!(quota["total"].is_null());
}

#[test]
fn admin_key_recovery_rejects_pending_wrong_owner_and_stale_identity() {
    use super::admin::{key_material, key_view, verify_reveal};
    use super::store::CredentialRecord;
    let cipher = super::CredentialCipher::new([7;32], "test").unwrap();
    let encrypted = cipher.seal(&cipher.associated_data("user").unwrap(), b"sk-existing").unwrap();
    let mut record = CredentialRecord { encrypted_key: encrypted, version: "v1".into(), ready: false };
    assert!(key_material(&cipher, "user", &record).is_err());
    record.ready = true;
    assert!(key_material(&cipher, "other", &record).is_err());
    let plain = key_material(&cipher, "user", &record).unwrap();
    assert_eq!(plain, "sk-existing");
    assert!(key_view(&record, &plain, &json!({"user_id":"foreign"}), "owner").is_err());
    let view = key_view(&record, &plain, &json!({"user_id":"owner","blocked":false,"key_alias":"existing"}), "owner").unwrap();
    assert_eq!(view["key_id"], "v1");
    assert!(!view.to_string().contains("sk-existing"));
    assert!(verify_reveal(&record, &record, "v1").is_ok());
    assert!(verify_reveal(&record, &record, "old").is_err());
    let fresh = CredentialRecord { encrypted_key: vec![0;32], version: "v1".into(), ready: true };
    assert!(verify_reveal(&record, &fresh, "v1").is_err());
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL with accounts/schema.sql applied"]
async fn admin_database_scopes_counts_pages_and_never_creates_credentials() {
    use super::admin::AdminQuery;
    let db = std::env::var("ADX_ACCOUNT_TEST_DATABASE_URL").unwrap();
    let store = super::AccountStore::from_url(&db, true, None).await.unwrap();
    let (client, connection) = tokio_postgres::connect(&db, tokio_postgres::NoTls).await.unwrap();
    let worker = tokio::spawn(async move { let _ = connection.await; });
    client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES ('admin-a','agc','project-a','client','a','1'),('admin-b','agc','project-a','client','b','1'),('admin-foreign','agc','project-b','client','f','1') ON CONFLICT DO NOTHING", &[]).await.unwrap();
    let grants = config(json!([scoped("tenant-a", "project-a")]));
    let grant = grants.authorize("tenant-a").unwrap();
    let query = AdminQuery::parse("page_size=1&provider=agc").unwrap();
    let page = store.admin_list(grant, "operator", &query).await.unwrap();
    assert_eq!(page["total"], 2);
    assert_eq!(page["users"][0]["user_id"], "admin-a");
    let mut next = query;
    next.page_token = Some(page["next_page_token"].as_str().unwrap().into());
    let page = store.admin_list(grant, "operator", &next).await.unwrap();
    assert_eq!(page["total"], 2);
    assert_eq!(page["users"][0]["user_id"], "admin-b");
    assert!(store.admin_user(grant, "admin-foreign").await.is_err());
    assert!(store.admin_credential(grant, "admin-foreign").await.is_err());
    assert!(store.admin_credential(grant, "admin-a").await.unwrap().is_none());
    let count: i64 = client.query_one("SELECT count(*) FROM adx_accounts.model_credentials WHERE user_id LIKE 'admin-%'", &[]).await.unwrap().get(0);
    assert_eq!(count, 0);
    client.execute("INSERT INTO adx_accounts.model_credentials(user_id,credential_version,encrypted_key,ready) VALUES ('admin-a','test-a',decode('00','hex'),false),('admin-b','test-b',decode('00','hex'),true)", &[]).await.unwrap();
    let page = store.admin_list(grant, "operator", &AdminQuery::parse("").unwrap()).await.unwrap();
    assert_eq!(page["users"][0]["key_state"], "pending");
    assert_eq!(page["users"][0]["has_key"], true);
    assert_eq!(page["users"][1]["key_state"], "ready");
    client.execute("DELETE FROM adx_accounts.model_credentials WHERE user_id IN ('admin-a','admin-b')", &[]).await.unwrap();
    let page = store.admin_list(grant, "operator", &AdminQuery::parse("").unwrap()).await.unwrap();
    assert_eq!(page["users"][0]["key_state"], "absent");
    assert_eq!(page["users"][0]["has_key"], false);
    worker.abort();
}

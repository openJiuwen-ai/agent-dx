#![cfg(feature = "test-memory")]
use adx_agent_core::activator::{ListPage, PublishRequest};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::Arc;

async fn publish(state: &AgentState, tenant: &str, name: &str) {
    let request: PublishRequest = serde_json::from_value(serde_json::json!({
        "tenant":tenant,
        "template":{"name":name,"image":"app:1","isolation_runtime":"runc",
        "resources":{"cpu_millis":100,"memory_mib":128}}
    })).unwrap();
    state.publish(tenant, &request.template).await.unwrap();
}

#[tokio::test]
async fn list_templates_filters_before_count_and_scopes_cursor() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    publish(&state, "tenant", "alpha").await;
    publish(&state, "tenant", "alpha").await;
    publish(&state, "tenant", "beta").await;
    publish(&state, "other", "hidden").await;
    let mut query = ListPage { page_size: 1, ..Default::default() };
    let first = state.list_templates("tenant", &query).await.unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.templates[0].name, "alpha");
    assert_eq!(first.templates[0].latest_version, "2");
    assert_eq!(first.templates[0].published_at, None);
    query.page_token = first.next_page_token;
    let second = state.list_templates("tenant", &query).await.unwrap();
    assert_eq!(second.templates[0].name, "beta");
    assert_eq!(second.total, 2);
    assert!(second.next_page_token.is_none());
    assert!(state.list_templates("other", &query).await.is_err());
    query.name = Some("al".into());
    assert!(state.list_templates("tenant", &query).await.is_err());
    query.page_token = None;
    let filtered = state.list_templates("tenant", &query).await.unwrap();
    assert_eq!(filtered.total, 1);
}

#[tokio::test]
async fn list_versions_matches_exact_numeric_version_not_prefix() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    for _ in 0..10 { publish(&state, "tenant", "agent").await; }
    let page = state.list_versions("tenant", "agent", &ListPage {
        name: Some("1".into()), ..Default::default()
    }).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.versions[0].version, "1");
    let all = state.list_versions("tenant", "agent", &ListPage::default()).await.unwrap();
    assert_eq!(all.total, 10);
    assert_eq!(all.versions.last().unwrap().version, "10");
}

#[tokio::test]
async fn list_aliases_prefix_filter_preserves_config_and_revision() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    publish(&state, "tenant", "agent").await;
    for alias in ["production", "preview", "stable"] {
        let config = serde_json::from_value(serde_json::json!({
            "tenant":"tenant","template":"agent","alias":alias,
            "seed":"seed", "versions":[{"version":"1","weight":100}]
        })).unwrap();
        state.put_alias(&config, None).await.unwrap();
    }
    let query = ListPage { name: Some("pr".into()), page_size: 1, ..Default::default() };
    let first = state.list_aliases("tenant", "agent", &query).await.unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.aliases[0].config.alias, "preview");
    let stored = state.alias("tenant", "agent", "preview").await.unwrap().unwrap();
    assert_eq!(first.aliases[0].revision, stored.0.as_str());
    let next = state.list_aliases("tenant", "agent", &ListPage {
        page_token: first.next_page_token, ..query
    }).await.unwrap();
    assert_eq!(next.aliases[0].config.alias, "production");
    assert!(next.next_page_token.is_none());
}

#[tokio::test]
async fn list_tenant_bindings_filters_before_pagination_and_rejects_other_scope_cursor() {
    use adx_agent_core::{activator::BindingCatalog, Scope, TemplateRef};
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    for tenant in ["tenant", "other"] {
        publish(&state, tenant, "agent").await;
        for id in ["a", "b"] {
            state.create_binding(Scope { tenant: tenant.into(), template: "agent".into(),
                template_ref: TemplateRef::Version("1".into()), binding_id: id.into() }).await.unwrap();
        }
    }
    let mut query = BindingCatalog { tenant: "tenant".into(), page: ListPage { page_size: 1, ..Default::default() }, ..Default::default() };
    let first = state.list_bindings_tenant(&query).await.unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.bindings[0].scope.binding_id, "a");
    query.page.page_token = first.next_page_token;
    query.tenant = "other".into();
    assert!(state.list_bindings_tenant(&query).await.is_err());
    query.tenant = "tenant".into();
    query.page.page_token = None;
    query.binding_id = Some("b".into());
    let page = state.list_bindings_tenant(&query).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.bindings[0].scope.binding_id, "b");
}

#[tokio::test]
async fn list_actual_version_includes_secondary_once_and_keeps_historical_version() {
    use adx_agent_core::{activator::BindingCatalog, Scope, TemplateRef};
    use adx_agent_store::{Repository, Key, Record, Check, Put, Transaction};
    let repository = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repository.clone());
    publish(&state,"tenant","app").await;
    let alias = serde_json::from_value(serde_json::json!({"tenant":"tenant","template":"app","alias":"stable","seed":"seed","versions":[{"version":"1","weight":100}]})).unwrap();
    state.put_alias(&alias,None).await.unwrap();
    for id in ["both","secondary","unmatched"] {
        let scope = Scope { tenant:"tenant".into(), template:"app".into(),template_ref:TemplateRef::Alias("stable".into()),binding_id:id.into() };
        state.create_binding(scope).await.unwrap();
        let key = Key::new("binding", &["tenant","app","alias","stable",id]).unwrap();
        let old = repository.get(&key).await.unwrap().unwrap();
        let mut binding: adx_agent_core::AgentBinding = old.decode().unwrap();
        binding.current.version = if id == "both" { "1" } else { "2" }.into();
        if id != "unmatched" { binding.secondary = Some(adx_agent_core::SecondaryEnv {
            environment:adx_agent_core::EnvironmentRef::new("1".into(),format!("old-{id}")),intent:adx_agent_core::SecondaryIntent::Retiring }); }
        let transaction = Transaction::new(vec![Check { key:key.clone(),expected:Some(old.revision) }],vec![Put { key,record:Record::new(&binding).unwrap() }]).unwrap();
        assert!(repository.commit(&transaction).await.unwrap());
    }
    let page = state.list_bindings_tenant(&BindingCatalog { tenant:"tenant".into(),ref_kind:Some("alias".into()),actual_version:Some("1".into()),..Default::default() }).await.unwrap();
    assert_eq!(page.total,2);
    assert_eq!(page.bindings.iter().map(|b| b.scope.binding_id.as_str()).collect::<Vec<_>>(),vec!["both","secondary"]);
}

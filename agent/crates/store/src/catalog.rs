//! Read-only catalog enumeration over existing records, with scope-bound cursors.
use crate::{AgentState, Error, Key, Result};
use adx_agent_core::{activator::*, TemplateVersion};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
struct Cursor { scope: Vec<String>, after: String }

fn page<T>(items: Vec<(String, T)>, scope: Vec<String>, query: &ListPage) -> Result<(Vec<T>, usize, Option<String>)> {
    query.validate().map_err(Error::Invalid)?;
    let after = query.page_token.as_ref().map(|raw| {
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| Error::Invalid("invalid catalog cursor".into()))?;
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid("invalid catalog cursor".into()))?;
        if cursor.scope != scope { return Err(Error::Invalid("catalog cursor scope mismatch".into())); }
        Ok(cursor.after)
    }).transpose()?;
    let total = items.len();
    let mut selected: Vec<_> = items.into_iter().filter(|(key, _)| after.as_ref().is_none_or(|after| key > after)).take(query.page_size + 1).collect();
    let more = selected.len() > query.page_size;
    selected.truncate(query.page_size);
    let token = if more {
        selected.last().map(|(key, _)| serde_json::to_vec(&Cursor { scope, after: key.clone() })
            .map(|v| URL_SAFE_NO_PAD.encode(v)).map_err(|e| Error::Invalid(e.to_string()))).transpose()?
    } else { None };
    Ok((selected.into_iter().map(|(_, item)| item).collect(), total, token))
}

impl AgentState {
    pub async fn list_bindings_tenant(&self, query: &BindingCatalog) -> Result<BindingCatalogPage> {
        adx_agent_core::identifier(&query.tenant, "tenant").map_err(Error::Invalid)?;
        query.page.validate().map_err(Error::Invalid)?;
        if query.page.name.is_some() { return Err(Error::Invalid("binding name filter is unsupported".into())); }
        if let Some(template) = &query.template { adx_agent_core::identifier(template, "template").map_err(Error::Invalid)?; }
        if let Some(reference) = &query.template_ref { reference.validate().map_err(Error::Invalid)?; }
        if query.ref_kind.as_ref().is_some_and(|kind| !matches!(kind.as_str(),"version"|"alias")) { return Err(Error::Invalid("invalid reference kind filter".into())); }
        if let Some(version) = &query.actual_version { adx_agent_core::version_number(version).map_err(Error::Invalid)?; }
        let mut bindings = BTreeMap::new();
        for (key, record) in self.store.scan("binding", &[&query.tenant]).await? {
            let value: adx_agent_core::AgentBinding = record.decode()?;
            if value.scope.tenant != query.tenant || crate::state::binding_key(&value.scope)?.as_str() != key {
                return Err(Error::Corrupt("binding catalog identity mismatch".into()));
            }
            if query.template.as_ref().is_some_and(|v| v != &value.scope.template)
                || query.template_ref.as_ref().is_some_and(|v| v != &value.scope.template_ref)
                || query.ref_kind.as_ref().is_some_and(|kind| kind != value.scope.template_ref.kind())
                || query.binding_id.as_ref().is_some_and(|v| v != &value.scope.binding_id)
                || query.actual_version.as_ref().is_some_and(|v| v != &value.current.version && value.secondary.as_ref().is_none_or(|s| &s.environment.version != v)) { continue; }
            bindings.insert(key, value);
        }
        let mut scope_query = query.clone();
        scope_query.page.page_token = None;
        let scope = serde_json::to_string(&scope_query).map_err(|e| Error::Invalid(e.to_string()))?;
        let (bindings, total, next_page_token) = page(bindings.into_iter().collect(), vec!["bindings".into(), scope], &query.page)?;
        Ok(BindingCatalogPage { bindings, total, next_page_token })
    }
    pub async fn list_aliases(&self, tenant: &str, template: &str, query: &ListPage) -> Result<AliasPage> {
        for value in [tenant, template] { adx_agent_core::identifier(value, "catalog scope").map_err(Error::Invalid)?; }
        query.validate().map_err(Error::Invalid)?;
        let mut aliases = BTreeMap::new();
        for (key, record) in self.store.scan("alias", &[tenant, template]).await? {
            let config: adx_agent_core::alias::AliasConfig = record.decode()?;
            config.validate().map_err(Error::Corrupt)?;
            if config.tenant != tenant || config.template != template
                || Key::new("alias", &[tenant, template, &config.alias])?.as_str() != key {
                return Err(Error::Corrupt("alias catalog identity mismatch".into()));
            }
            if query.name.as_ref().is_none_or(|name| config.alias.starts_with(name)) {
                aliases.insert(config.alias.clone(), AliasSnapshot { config, revision: record.revision.as_str().into() });
            }
        }
        let (aliases, total, next_page_token) = page(aliases.into_iter().collect(),
            vec!["aliases".into(), tenant.into(), template.into(), query.name.clone().unwrap_or_default()], query)?;
        Ok(AliasPage { aliases, total, next_page_token })
    }
    pub async fn list_templates(&self, tenant: &str, query: &ListPage) -> Result<TemplateSummaryPage> {
        adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
        query.validate().map_err(Error::Invalid)?;
        let mut latest: BTreeMap<String, (u64, TemplateSummary)> = BTreeMap::new();
        for (key, record) in self.store.scan("template", &[tenant]).await? {
            let value: TemplateVersion = record.decode()?;
            if Key::new("template", &[tenant, &value.name, &value.version])?.as_str() != key {
                return Err(Error::Corrupt("template catalog identity mismatch".into()));
            }
            let version = adx_agent_core::version_number(&value.version).map_err(Error::Corrupt)?;
            if query.name.as_ref().is_some_and(|name| !value.name.starts_with(name)) { continue; }
            let entry = latest.entry(value.name.clone()).or_insert_with(|| (0, TemplateSummary {
                name: value.name, latest_version: value.version.clone(), published_at: None,
            }));
            if version > entry.0 { entry.0 = version; entry.1.latest_version = value.version; }
        }
        let (templates, total, next_page_token) = page(latest.into_iter().map(|(key, (_, value))| (key, value)).collect(),
            vec!["templates".into(), tenant.into(), query.name.clone().unwrap_or_default()], query)?;
        Ok(TemplateSummaryPage { templates, total, next_page_token })
    }

    pub async fn list_versions(&self, tenant: &str, template: &str, query: &ListPage) -> Result<VersionSummaryPage> {
        for value in [tenant, template] { adx_agent_core::identifier(value, "catalog scope").map_err(Error::Invalid)?; }
        query.validate().map_err(Error::Invalid)?;
        if let Some(version) = &query.name { adx_agent_core::version_number(version).map_err(Error::Invalid)?; }
        let mut versions = BTreeMap::new();
        for (key, record) in self.store.scan("template", &[tenant, template]).await? {
            let value: TemplateVersion = record.decode()?;
            if value.name != template || Key::new("template", &[tenant, template, &value.version])?.as_str() != key {
                return Err(Error::Corrupt("version catalog identity mismatch".into()));
            }
            let number = adx_agent_core::version_number(&value.version).map_err(Error::Corrupt)?;
            if query.name.as_ref().is_none_or(|name| name == &value.version) {
                versions.insert(format!("{number:020}"), VersionSummary { version: value.version, published_at: None });
            }
        }
        let (versions, total, next_page_token) = page(versions.into_iter().collect(),
            vec!["versions".into(), tenant.into(), template.into(), query.name.clone().unwrap_or_default()], query)?;
        Ok(VersionSummaryPage { versions, total, next_page_token })
    }
}

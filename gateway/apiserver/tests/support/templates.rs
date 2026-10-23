use adx_agent_api::{
    activator::Control, managed::ManagedService, request::RequestContext, Error, Result,
};
use adx_agent_core::{
    activator::{BindingList, BindingPage, Target},
    AgentBinding, Scope, TemplateVersion,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Read-only template source for API Server E2B fixtures. Any lifecycle call is a test failure.
pub struct TemplateSource {
    templates: BTreeMap<(String, String, String), TemplateVersion>,
    calls: Mutex<Vec<(String, String, String)>>,
}

impl TemplateSource {
    pub fn managed(
        entries: impl IntoIterator<Item = (String, TemplateVersion)>,
    ) -> (Arc<ManagedService>, Arc<Self>) {
        let templates = entries
            .into_iter()
            .map(|(tenant, template)| {
                (
                    (tenant, template.name.clone(), template.version.clone()),
                    template,
                )
            })
            .collect();
        let source = Arc::new(Self {
            templates,
            calls: Mutex::new(Vec::new()),
        });
        (Arc::new(ManagedService::new(source.clone())), source)
    }

    pub fn calls(&self) -> Vec<(String, String, String)> {
        self.calls.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl Control for TemplateSource {
    async fn publish(&self, _: &RequestContext, _: &str, _: &TemplateVersion) -> Result<()> {
        Err(Error::Unsupported(
            "fixture template source is read-only".into(),
        ))
    }

    async fn template(
        &self,
        _: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        let key = (tenant.into(), name.into(), version.into());
        self.calls.lock().unwrap().push(key.clone());
        self.templates.get(&key).cloned().ok_or(Error::NotFound)
    }

    async fn binding(&self, _: &RequestContext, _: &Scope) -> Result<AgentBinding> {
        Err(Error::Unsupported("fixture must not read bindings".into()))
    }

    async fn list_bindings(&self, _: &RequestContext, _: &BindingList) -> Result<BindingPage> {
        Err(Error::Unsupported("fixture must not list bindings".into()))
    }

    async fn delete_binding(&self, _: &RequestContext, _: &Scope) -> Result<()> {
        Err(Error::Unsupported(
            "fixture must not delete bindings".into(),
        ))
    }

    async fn activate(&self, _: &RequestContext, _: &Scope, _: Option<&str>) -> Result<Target> {
        Err(Error::Unsupported(
            "fixture must not activate bindings".into(),
        ))
    }

    async fn activate_with_cache(
        &self,
        _: &RequestContext,
        _: &Scope,
        _: Option<&str>,
        _: bool,
    ) -> Result<Target> {
        Err(Error::Unsupported(
            "fixture must not activate bindings".into(),
        ))
    }
}

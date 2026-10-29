//! Gateway product facade. Product state and Environment lifecycle stay behind the Activator.
use crate::request::RequestContext;
use crate::{activator::Control, Error, Result};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{activator::Target, target::Target as AccessTarget, *};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
type TemplateKey = (String, String, String);

#[derive(Default)]
struct ActivationInput<'a> {
    generation: Option<&'a str>,
    bypass_cache: bool,
    parameters: Option<&'a [u8]>,
}

pub struct ManagedService {
    control: Arc<dyn Control>,
    templates: Mutex<BoundedCache<TemplateKey, Arc<OnceCell<TemplateVersion>>>>,
}
impl ManagedService {
    pub fn new(control: Arc<dyn Control>) -> Self {
        Self {
            control,
            templates: Mutex::new(BoundedCache::new(1024)),
        }
    }
    /// Select an AgentBinding identity without creating product or Environment state.
    /// Returns Invalid for a direct instance target or invalid scope components.
    pub fn binding_scope(tenant: &str, target: &AccessTarget) -> Result<Scope> {
        let (name, template_ref, id) = match target {
            AccessTarget::Template { name, template_ref } => {
                (name, template_ref.clone(), uuid::Uuid::new_v4().to_string())
            }
            AccessTarget::Binding {
                name,
                template_ref,
                id,
            } => (name, template_ref.clone(), id.clone()),
            AccessTarget::Instance(_) => {
                return Err(Error::Invalid(
                    "managed access requires a template or binding target".into(),
                ));
            }
        };
        let scope = Scope {
            tenant: tenant.into(),
            template: name.clone(),
            template_ref,
            binding_id: id,
        };
        scope.validate().map_err(Error::Invalid)?;
        Ok(scope)
    }
    pub async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &adx_agent_core::TemplateDraft,
    ) -> Result<TemplateVersion> {
        self.control.publish(ctx, tenant, template).await
    }
    pub async fn template(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        for value in [tenant, name, version] {
            identifier(value, "template scope").map_err(Error::Invalid)?;
        }
        ctx.run(async {
            let key = (tenant.to_owned(), name.to_owned(), version.to_owned());
            let cell = {
                let mut templates = self.templates.lock().await;
                if let Some(cell) = templates.get(&key) {
                    cell.clone()
                } else {
                    let cell = Arc::new(OnceCell::new());
                    templates.insert(key, cell.clone());
                    cell
                }
            };
            cell.get_or_try_init(|| async {
                let value = self.control.template(ctx, tenant, name, version).await?;
                if value.name != name || value.version != version {
                    return Err(Error::Unavailable("template identity mismatch".into()));
                }
                value.validate().map_err(Error::Invalid)?;
                Ok(value)
            })
            .await
            .cloned()
        })
        .await
    }

    pub async fn binding_template(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
    ) -> Result<TemplateVersion> {
        match &scope.template_ref {
            TemplateRef::Version(version) => {
                self.template(ctx, &scope.tenant, &scope.template, version)
                    .await
            }
            TemplateRef::Alias(_) => Ok(self.binding_view(ctx, scope).await?.template),
        }
    }
    /// Return metadata selected by Activator, with no activation side effects.
    pub async fn binding_view(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
    ) -> Result<activator::BindingView> {
        scope.validate().map_err(Error::Invalid)?;
        let view = ctx.run(self.control.binding_view(ctx, scope)).await?;
        if view.template.name != scope.template
            || matches!(&scope.template_ref, TemplateRef::Version(version) if version != &view.template.version)
            || view.binding.as_ref().is_some_and(|binding| {
                binding.scope != *scope || binding.current.version != view.template.version
            })
        {
            return Err(Error::Unavailable(
                "Binding/template identity mismatch".into(),
            ));
        }
        view.template.validate().map_err(Error::Invalid)?;
        Ok(view)
    }
    pub async fn update_alias(
        &self,
        ctx: &RequestContext,
        request: &activator::AliasUpdate,
    ) -> Result<activator::AliasSnapshot> {
        request.config.validate().map_err(Error::Invalid)?;
        self.control.update_alias(ctx, request).await
    }

    pub async fn alias_config(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &str,
        alias: &str,
    ) -> Result<activator::AliasSnapshot> {
        for value in [tenant, template, alias] {
            identifier(value, "alias scope").map_err(Error::Invalid)?;
        }
        let snapshot = self
            .control
            .alias_config(
                ctx,
                &activator::AliasRequest {
                    tenant: tenant.into(),
                    template: template.into(),
                    alias: alias.into(),
                },
            )
            .await?;
        if snapshot.config.tenant != tenant
            || snapshot.config.template != template
            || snapshot.config.alias != alias
        {
            return Err(Error::Unavailable("alias identity mismatch".into()));
        }
        snapshot.config.validate().map_err(Error::Unavailable)?;
        Ok(snapshot)
    }

    pub async fn binding(&self, ctx: &RequestContext, scope: &Scope) -> Result<AgentBinding> {
        self.control.binding(ctx, scope).await
    }
    pub async fn list_bindings(
        &self,
        ctx: &RequestContext,
        query: &activator::BindingList,
    ) -> Result<activator::BindingPage> {
        query.validate().map_err(Error::Invalid)?;
        self.control.list_bindings(ctx, query).await
    }
    pub async fn delete_binding(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        self.control.delete_binding(ctx, scope).await
    }

    /// Each request validates the service; the Activator may reuse a cached AgentBinding binding.
    pub async fn resolve(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
    ) -> Result<(Target, u16)> {
        self.resolve_with_cache(ctx, scope, protocol, port, false)
            .await
    }
    /// Resolve with an explicit successful-binding cache bypass; immutable templates stay cached.
    pub async fn resolve_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        bypass_cache: bool,
    ) -> Result<(Target, u16)> {
        self.resolve_generation(
            ctx,
            scope,
            protocol,
            port,
            ActivationInput {
                bypass_cache,
                ..Default::default()
            },
        )
        .await
    }
    /// A retry of the same incoming request must not start a new lifecycle after deletion.
    pub async fn retry_resolve(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        generation: &str,
    ) -> Result<(Target, u16)> {
        self.resolve_generation(
            ctx,
            scope,
            protocol,
            port,
            ActivationInput {
                generation: Some(generation),
                bypass_cache: true,
                parameters: None,
            },
        )
        .await
    }
    pub async fn resolve_with_parameters(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        parameters: Option<&[u8]>,
        bypass_cache: bool,
    ) -> Result<(Target, u16)> {
        self.resolve_generation(
            ctx,
            scope,
            protocol,
            port,
            ActivationInput {
                parameters,
                bypass_cache,
                ..Default::default()
            },
        )
        .await
    }
    async fn resolve_generation(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        input: ActivationInput<'_>,
    ) -> Result<(Target, u16)> {
        scope.validate().map_err(Error::Invalid)?;
        let request = activator::ResolveRequest {
            activation: activator::ActivationRequest {
                scope: scope.clone(),
                expected_generation: input.generation.map(str::to_owned),
                bypass_cache: input.bypass_cache,
                parameters: input.parameters.map(<[u8]>::to_vec),
            },
            service: activator::ServiceSelector { protocol, port },
        };
        let resolved = ctx.run(self.control.resolve(ctx, &request)).await?;
        let target = resolved.target;
        let selected_port = resolved.port;
        if matches!(&scope.template_ref, TemplateRef::Version(version) if version != &target.binding.current.version)
        {
            return Err(Error::Unavailable(
                "Activator returned another template version".into(),
            ));
        }
        if target.binding.scope != *scope
            || target.binding.phase != BindingPhase::Active
            || target.binding.generation.is_empty()
            || target.binding.current.environment_id.is_empty()
            || port.is_some_and(|port| port != selected_port)
            || !target
                .service
                .iter()
                .any(|service| service.protocol == protocol && service.port == selected_port)
        {
            return Err(Error::Unavailable(
                "Activator target identity or service mismatch".into(),
            ));
        }
        Ok((target, selected_port))
    }
}

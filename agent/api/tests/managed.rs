use adx_agent_api::{managed::ManagedService, Error};
use adx_agent_core::{target::Target, Protocol, Scope, TemplateVersion};

#[test]
fn binding_selection_generates_an_identity_and_reuses_an_explicit_one() {
    let template = Target::Template {
        name: "demo".into(),
        version: "1".into(),
    };
    let selected = ManagedService::binding_scope("tenant-a", &template).unwrap();
    uuid::Uuid::parse_str(&selected.binding_id).unwrap();
    assert_eq!(
        selected,
        Scope {
            tenant: "tenant-a".into(),
            template: "demo".into(),
            version: "1".into(),
            binding_id: selected.binding_id.clone(),
        }
    );
    let explicit = Target::Binding {
        name: "demo".into(),
        version: "1".into(),
        id: selected.binding_id.clone(),
    };
    assert_eq!(
        ManagedService::binding_scope("tenant-a", &explicit).unwrap(),
        selected
    );
    assert_ne!(
        ManagedService::binding_scope("tenant-a", &template)
            .unwrap()
            .binding_id,
        selected.binding_id
    );
    assert_eq!(
        ManagedService::binding_scope("tenant-b", &explicit)
            .unwrap()
            .tenant,
        "tenant-b"
    );
    assert!(matches!(
        ManagedService::binding_scope("", &explicit),
        Err(Error::Invalid(_))
    ));
}

#[test]
fn service_selection_matches_protocol_and_requires_an_unambiguous_port() {
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"demo", "version":"1", "image":"app:1", "isolation_runtime":"runc",
        "resources":{"cpu_millis":1000,"memory_mib":512},
        "service":[
            {"protocol":"http","port":8080}, {"protocol":"ws","port":8080},
            {"protocol":"ssh","port":22}, {"protocol":"ssh","port":2222}
        ]
    }))
    .unwrap();
    for (protocol, port, expected) in [
        (Protocol::Http, None, 8080),
        (Protocol::Ws, Some(8080), 8080),
        (Protocol::Ssh, Some(22), 22),
        (Protocol::Ssh, Some(2222), 2222),
    ] {
        assert_eq!(
            ManagedService::select_service(&template, protocol, port).unwrap(),
            expected
        );
    }
    for (protocol, port) in [(Protocol::Ssh, None), (Protocol::Http, Some(22))] {
        assert!(matches!(
            ManagedService::select_service(&template, protocol, port),
            Err(Error::Invalid(_))
        ));
    }
}

mod post_write_outcome {
    use adx_agent_api::{
        activator::Control, managed::ManagedService, request::RequestContext, Error,
    };
    use adx_agent_core::{
        activator::{BindingList, BindingPage, Target},
        AgentBinding, BindingPhase, Scope, TemplateVersion,
    };
    use async_trait::async_trait;
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    struct MismatchedPrepare;

    #[async_trait]
    impl Control for MismatchedPrepare {
        async fn prepare_binding(
            &self,
            ctx: &RequestContext,
            scope: &Scope,
            _launch: &adx_agent_core::launch::LaunchConfig,
            _parameters: &adx_agent_core::mounts::Parameters,
        ) -> adx_agent_api::Result<AgentBinding> {
            ctx.start_write();
            Ok(AgentBinding {
                scope: Scope {
                    binding_id: "different-binding".into(),
                    ..scope.clone()
                },
                generation: "generation-1".into(),
                environment_id: "environment-1".into(),
                phase: BindingPhase::Active,
                parameters: BTreeMap::new(),
            })
        }
        async fn publish(
            &self,
            _ctx: &RequestContext,
            _tenant: &str,
            _template: &TemplateVersion,
        ) -> adx_agent_api::Result<()> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn template(
            &self,
            _ctx: &RequestContext,
            _tenant: &str,
            _name: &str,
            _version: &str,
        ) -> adx_agent_api::Result<TemplateVersion> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn binding(
            &self,
            _ctx: &RequestContext,
            _scope: &Scope,
        ) -> adx_agent_api::Result<AgentBinding> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn list_bindings(
            &self,
            _ctx: &RequestContext,
            _query: &BindingList,
        ) -> adx_agent_api::Result<BindingPage> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn delete_binding(
            &self,
            _ctx: &RequestContext,
            _scope: &Scope,
        ) -> adx_agent_api::Result<()> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn activate(
            &self,
            _ctx: &RequestContext,
            _scope: &Scope,
            _expected_generation: Option<&str>,
        ) -> adx_agent_api::Result<Target> {
            Err(Error::Unsupported("unused".into()))
        }
        async fn activate_with_cache(
            &self,
            _ctx: &RequestContext,
            _scope: &Scope,
            _expected_generation: Option<&str>,
            _bypass_cache: bool,
        ) -> adx_agent_api::Result<Target> {
            Err(Error::Unsupported("unused".into()))
        }
    }

    #[tokio::test]
    async fn committed_prepare_with_mismatched_identity_is_outcome_unknown() {
        let service = ManagedService::new(Arc::new(MismatchedPrepare));
        let ctx = RequestContext::new(Duration::from_secs(5));
        let scope = Scope {
            tenant: "tenant-a".into(),
            template: "demo".into(),
            version: "1".into(),
            binding_id: "binding-a".into(),
        };
        let launch = adx_agent_core::launch::LaunchConfig {
            credential_version: "v1".into(),
            env: BTreeMap::new(),
        };
        let error = service
            .prepare_binding(&ctx, &scope, &launch, &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::OutcomeUnknown(_)),
            "committed mismatch must not be retry-safe: {error:?}"
        );
    }
}

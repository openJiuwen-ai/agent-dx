use adx_agent_api::{managed::ManagedService, Error};
use adx_agent_core::{target::Target, Protocol, Scope, TemplateVersion};

#[test]
fn binding_selection_generates_an_identity_and_reuses_an_explicit_one() {
    let template = Target::Template {
        name: "demo".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
    };
    let selected = ManagedService::binding_scope("tenant-a", &template).unwrap();
    uuid::Uuid::parse_str(&selected.binding_id).unwrap();
    assert_eq!(
        selected,
        Scope {
            tenant: "tenant-a".into(),
            template: "demo".into(),
            template_ref: adx_agent_core::TemplateRef::Version("1".into()),
            binding_id: selected.binding_id.clone(),
        }
    );
    let explicit = Target::Binding {
        name: "demo".into(),
        template_ref: adx_agent_core::TemplateRef::Version("1".into()),
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
            adx_agent_core::activator::ServiceSelector { protocol, port }
                .select(&template.service)
                .map_err(Error::Invalid)
                .unwrap(),
            expected
        );
    }
    for (protocol, port) in [(Protocol::Ssh, None), (Protocol::Http, Some(22))] {
        assert!(matches!(
            adx_agent_core::activator::ServiceSelector { protocol, port }
                .select(&template.service)
                .map_err(Error::Invalid),
            Err(Error::Invalid(_))
        ));
    }
}

#[test]
fn unified_binding_targets_select_numeric_versions_and_named_aliases() {
    for (reference, expected) in [
        ("12", adx_agent_core::TemplateRef::Version("12".into())),
        (
            "production",
            adx_agent_core::TemplateRef::Alias("production".into()),
        ),
        (
            "v1-workspace",
            adx_agent_core::TemplateRef::Alias("v1-workspace".into()),
        ),
    ] {
        let target = format!("urn:adx:binding:orbit:{reference}:user")
            .parse()
            .unwrap();
        let scope = ManagedService::binding_scope("tenant", &target).unwrap();
        assert_eq!(scope.template_ref, expected);
        assert_eq!(scope.binding_id, "user");
        assert_eq!(Target::from(&scope), target);
    }
}

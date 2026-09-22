use adx_agent_api::{managed::ManagedService, Error};
use adx_agent_core::{target::Target, Protocol, Scope, TemplateVersion};

#[test]
fn environment_selection_generates_an_identity_and_reuses_an_explicit_one() {
    let template = Target::Template {
        name: "demo".into(),
        version: "1".into(),
    };
    let selected = ManagedService::environment_scope("tenant-a", &template).unwrap();
    uuid::Uuid::parse_str(&selected.environment_id).unwrap();
    assert_eq!(
        selected,
        Scope {
            tenant: "tenant-a".into(),
            template: "demo".into(),
            version: "1".into(),
            environment_id: selected.environment_id.clone(),
        }
    );
    let explicit = Target::Environment {
        name: "demo".into(),
        version: "1".into(),
        id: selected.environment_id.clone(),
    };
    assert_eq!(
        ManagedService::environment_scope("tenant-a", &explicit).unwrap(),
        selected
    );
    assert_ne!(
        ManagedService::environment_scope("tenant-a", &template)
            .unwrap()
            .environment_id,
        selected.environment_id
    );
    assert_eq!(
        ManagedService::environment_scope("tenant-b", &explicit)
            .unwrap()
            .tenant,
        "tenant-b"
    );
    assert!(matches!(
        ManagedService::environment_scope("", &explicit),
        Err(Error::Invalid(_))
    ));
}

#[test]
fn service_selection_matches_protocol_and_requires_an_unambiguous_port() {
    let template: TemplateVersion = serde_json::from_value(serde_json::json!({
        "name":"demo", "version":"1", "image":"app:1", "isolation_runtime":"runc",
        "entrypoint":["/start"], "resources":{"cpu_millis":1000,"memory_mib":512},
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

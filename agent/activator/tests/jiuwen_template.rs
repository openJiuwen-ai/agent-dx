use adx_agent_core::{probes::ProbeAction, TemplateDraft};

#[test]
fn jiuwen_template_waits_for_agentserver_port() {
    let template: TemplateDraft =
        serde_json::from_str(include_str!("../examples/jiuwen-sfs-template.json")).unwrap();
    template.validate().unwrap();
    let template = template.with_version("1".into());
    assert_eq!(template.init_timeout_seconds, Some(30));
    assert!(matches!(
        template.readiness_probe.unwrap().action().unwrap(),
        ProbeAction::TcpSocket { port: 18092 }
    ));
}

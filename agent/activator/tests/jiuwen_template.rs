use adx_agent_core::{probes::ProbeAction, TemplateDraft};

#[test]
fn jiuwen_template_probes_agentserver_port_without_initial_delay() {
    let template: TemplateDraft =
        serde_json::from_str(include_str!("../examples/jiuwen-sfs-template.json")).unwrap();
    template.validate().unwrap();
    let template = template.with_version("1".into());
    assert_eq!(template.init_timeout_seconds, Some(30));
    let readiness = template.readiness_probe.unwrap();
    assert_eq!(readiness.initial_delay_seconds, 0);
    assert!(matches!(
        readiness.action().unwrap(),
        ProbeAction::TcpSocket { port: 18092 }
    ));
}

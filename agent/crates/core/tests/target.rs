use adx_agent_core::target::{SshRoute, Target};

#[test]
fn urn_and_ssh_routes_keep_target_identity_and_options() {
    for value in [
        "urn:adx:instance:instance-1",
        "urn:adx:template:demo:1",
        "urn:adx:environment:demo:1:env%3Aone",
    ] {
        let target: Target = value.parse().unwrap();
        assert_eq!(target.to_string(), value);
        let route = SshRoute {
            target,
            port: Some(22),
            trace: Some("trace:one".into()),
        };
        assert_eq!(route.to_string().parse::<SshRoute>().unwrap(), route);
    }
    let legacy: SshRoute = "yr:instance:instance-1:port=2222:trace=trace%3Aone"
        .parse()
        .unwrap();
    assert_eq!(legacy.target, Target::Instance("instance-1".into()));
    assert_eq!(legacy.port, Some(2222));
    assert_eq!(legacy.trace.as_deref(), Some("trace:one"));
    let template: SshRoute = "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1"
        .parse()
        .unwrap();
    assert_eq!(template.target.to_string(), "urn:adx:template:demo:1");
}

#[test]
fn malformed_or_ambiguous_routes_are_rejected() {
    for value in [
        "urn:adx:template:demo",
        "urn:adx:template::1",
        "urn:adx:instance:id%GG",
        "urn:adx:environment:a:b:x:y",
    ] {
        assert!(value.parse::<Target>().is_err(), "{value}");
    }
    for value in [
        "yr:instance:id:port=0",
        "yr:instance:id:port=22:port=23",
        "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1:instance=id",
    ] {
        assert!(value.parse::<SshRoute>().is_err(), "{value}");
    }
}

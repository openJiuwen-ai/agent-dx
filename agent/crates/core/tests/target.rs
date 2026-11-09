use adx_agent_core::target::{SshRoute, Target};

#[test]
fn urn_and_ssh_routes_keep_target_identity_and_options() {
    for value in [
        "urn:adx:instance:instance-1",
        "urn:adx:template:demo:1",
        "urn:adx:template:demo:production",
        "urn:adx:template:demo%3Aone:production",
    ] {
        let target: Target = value.parse().unwrap();
        assert_eq!(target.to_string(), value);
        let route = SshRoute {
            binding_id: matches!(target, Target::Template { .. })
                .then(|| "user:a/中文 %,plus+".into()),
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
        "urn:adx:alias-binding:demo:production:user",
        "urn:adx:template::1",
        "urn:adx:instance:id%GG",
        "urn:adx:binding:a:b:x:y",
        "urn:adx:binding:demo:1:user",
    ] {
        assert!(value.parse::<Target>().is_err(), "{value}");
    }
    for value in [
        "yr:instance:id:binding_id=user",
        "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1:binding_id=",
        "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1:binding_id=x:binding_id=y",
        "yr:instance:id:port=0",
        "yr:instance:id:port=22:port=23",
        "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1:instance=id",
    ] {
        assert!(value.parse::<SshRoute>().is_err(), "{value}");
    }
}

#[test]
fn numeric_qualifiers_reject_zero_leading_zeroes_and_overflow() {
    for reference in ["0", "01", "18446744073709551616"] {
        assert!(format!("urn:adx:template:agent:{reference}")
            .parse::<Target>()
            .is_err());
    }
}

#[test]
fn binding_header_codec_preserves_identity_and_rejects_invalid_values() {
    use adx_agent_core::target::{decode_binding_id, encode_binding_id};
    for id in ["simple", "user:a/中文 %,plus+", "%2F", &"a".repeat(512)] {
        assert_eq!(
            decode_binding_id(&encode_binding_id(id).unwrap()).unwrap(),
            id
        );
    }
    assert_eq!(decode_binding_id("%252F").unwrap(), "%2F");
    assert_eq!(decode_binding_id("a+b").unwrap(), "a+b");
    for value in [
        "",
        "%",
        "%GG",
        "%FF",
        "%0A",
        "%20",
        "a,b",
        "a b",
        &"a".repeat(513),
    ] {
        assert!(decode_binding_id(value).is_err(), "{value}");
    }
}

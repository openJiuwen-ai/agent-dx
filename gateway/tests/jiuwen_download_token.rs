#![cfg(feature = "agent-api")]
use base64::{engine::general_purpose::URL_SAFE, Engine};
use data_plane_gateway::ingress::jiuwen::download_token::{
    DownloadVerifier, FileMetadata, TokenError,
};
use ring::hmac;
use serde_json::{json, Value};
fn vectors() -> Value {
    serde_json::from_str(include_str!("fixtures/jiuwen_download_tokens.json")).unwrap()
}
fn verifier() -> DownloadVerifier {
    DownloadVerifier::new(vectors()["secret"].as_str().unwrap()).unwrap()
}
fn token(name: &str) -> String {
    vectors()["vectors"][name]["token"].as_str().unwrap().into()
}
fn signed_raw(raw: &str) -> String {
    let data = URL_SAFE.encode(raw);
    let key = hmac::Key::new(
        hmac::HMAC_SHA256,
        vectors()["secret"].as_str().unwrap().as_bytes(),
    );
    format!(
        "{}.{}",
        data,
        hex::encode(hmac::sign(&key, data.as_bytes()))
    )
}
fn signed(payload: Value) -> String {
    signed_raw(&payload.to_string())
}
fn metadata() -> FileMetadata {
    FileMetadata {
        regular_file: true,
        size: 123,
    }
}
fn registration() -> Value {
    let p = &vectors()["vectors"]["verified"]["payload"];
    json!({"asset_id":p["asset_id"],"sealed_path":p["path"],"expires_at":p["exp"],"size_bytes":p["size"],"content_digest":p["digest"],"state":"committed"})
}

#[test]
fn python_generated_tokens_verify_without_reserializing_the_signed_payload() {
    let claims = verifier()
        .verify(&token("ordinary"), 100.0, Some("session-a"))
        .unwrap();
    assert_eq!(claims.path(), "/data/报告.txt");
    assert_eq!(claims.registration_path("/assets").unwrap(), None);
    let checked = claims
        .check_file("/assets", None, metadata(), 100.0)
        .unwrap();
    assert_eq!(checked.name(), "报告.txt");
    assert_eq!(checked.size(), 123);
    // A valid signature over noncanonical JSON is still valid.
    let reordered = signed_raw("{ \"sid\": \"session-a\", \"path\": \"/data/报告.txt\" }");
    assert!(verifier().verify(&reordered, 100.0, None).is_ok());
}

#[test]
fn tampering_wrong_key_and_wrong_session_fail_before_file_access() {
    let original = token("ordinary");
    let (body, sig) = original.split_once('.').unwrap();
    let changed = format!(
        "{}.{sig}",
        URL_SAFE.encode(br#"{"path":"/other","sid":"session-a"}"#)
    );
    assert!(matches!(
        verifier().verify(&changed, 100.0, None),
        Err(TokenError::InvalidSignature)
    ));
    assert!(DownloadVerifier::new(&"x".repeat(32))
        .unwrap()
        .verify(&original, 100.0, None)
        .is_err());
    assert!(matches!(
        verifier().verify(&original, 100.0, Some("session-b")),
        Err(TokenError::SessionMismatch)
    ));
    assert!(verifier()
        .verify(&format!("{body}.{}", sig.to_uppercase()), 100.0, None)
        .is_err());
}

#[test]
fn only_the_exact_delivery_schemas_can_omit_expiry() {
    let routed = json!({"path":"/data/file","sid":"session-a","download_http_base":"https://ignored.example"});
    assert!(verifier()
        .verify(&signed(routed.clone()), 100.0, None)
        .is_ok());
    for extra in [
        json!({"exp":null}),
        json!({"kind":"other"}),
        json!({"purpose":"skill_content_image"}),
        json!({"name":"x"}),
    ] {
        let mut p = routed.clone();
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(verifier().verify(&signed(p), 100.0, None).is_err());
    }
    assert!(verifier()
        .verify(&signed(json!({"path":"/data/file","sid":""})), 100.0, None)
        .is_err());
}

#[test]
fn expiry_matches_integer_ordinary_and_precise_verified_boundaries() {
    assert!(verifier().verify(&token("expiring"), 110.9, None).is_ok());
    assert!(matches!(
        verifier().verify(&token("expiring"), 111.0, None),
        Err(TokenError::Expired)
    ));
    assert!(verifier().verify(&token("verified"), 160.5, None).is_ok());
    assert!(matches!(
        verifier().verify(&token("verified"), 160.6, None),
        Err(TokenError::Expired)
    ));
    assert!(verifier()
        .verify(&token("ordinary"), f64::NAN, None)
        .is_err());
    for exp in [json!(true), json!("999"), json!(-1)] {
        assert!(verifier()
            .verify(
                &signed(json!({"path":"/data/file","sid":"s","exp":exp})),
                100.0,
                None
            )
            .is_err());
    }
}

#[test]
fn verified_requires_matching_registration_and_actual_file_metadata() {
    let claims = verifier().verify(&token("verified"), 100.0, None).unwrap();
    assert_eq!(
        claims.registration_path("/assets").unwrap(),
        Some(format!("/assets/{}.json", "ab".repeat(16)))
    );
    assert!(claims.registration_path("/other").is_err());
    assert!(claims
        .check_file("/assets", None, metadata(), 100.0)
        .is_err());
    for state in ["staged", "committed"] {
        let mut record = registration();
        record["state"] = json!(state);
        let checked = claims
            .check_file(
                "/assets",
                Some(&serde_json::to_vec(&record).unwrap()),
                metadata(),
                100.0,
            )
            .unwrap();
        assert_eq!(checked.name(), "报告.pdf");
        assert_eq!(checked.size(), 123);
    }
    for (key, value) in [
        ("state", json!("revoked")),
        ("asset_id", json!("00".repeat(16))),
        ("sealed_path", json!("/other/file")),
        ("expires_at", json!(161)),
        ("size_bytes", json!(124)),
        (
            "content_digest",
            json!("sha256:".to_owned() + &"00".repeat(32)),
        ),
    ] {
        let mut record = registration();
        record[key] = value;
        assert!(
            claims
                .check_file(
                    "/assets",
                    Some(&serde_json::to_vec(&record).unwrap()),
                    metadata(),
                    100.0
                )
                .is_err(),
            "{key}"
        );
    }
    let bytes = serde_json::to_vec(&registration()).unwrap();
    assert!(claims
        .check_file(
            "/assets",
            Some(&bytes),
            FileMetadata {
                regular_file: false,
                size: 123
            },
            100.0
        )
        .is_err());
    assert!(claims
        .check_file(
            "/assets",
            Some(&bytes),
            FileMetadata {
                regular_file: true,
                size: 124
            },
            100.0
        )
        .is_err());
    assert!(matches!(
        claims.check_file("/assets", Some(&bytes), metadata(), 161.0),
        Err(TokenError::Expired)
    ));
}

#[test]
fn signed_malformed_claims_cannot_select_a_sidecar_outside_the_asset_root() {
    for (field, value) in [
        ("asset_id", json!("../../secret")),
        ("digest", json!("bad")),
        ("size", json!(-1)),
        ("path", json!("/assets/../other")),
        ("path", json!("relative")),
    ] {
        let mut p = vectors()["vectors"]["verified"]["payload"].clone();
        p[field] = value;
        assert!(
            verifier().verify(&signed(p), 100.0, None).is_err(),
            "{field}"
        );
    }
    let mut p = vectors()["vectors"]["verified"]["payload"].clone();
    p["path"] = json!("/assets/.hidden");
    assert!(verifier()
        .verify(&signed(p), 100.0, None)
        .unwrap()
        .registration_path("/assets")
        .is_err());
}

#[test]
fn shared_config_uses_exact_env_key_or_trimmed_sandbox_file_without_generating_a_key() {
    use data_plane_gateway::ingress::jiuwen::download_config::DownloadConfig;
    let mut template:adx_agent_core::TemplateVersion=serde_json::from_value(json!({"name":"jiuwen","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":1,"memory_mib":1},"env":{"JIUWENSWARM_WORKSPACE":"/work","JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/assets"}})).unwrap();
    let config = DownloadConfig::from_template(&template).unwrap();
    assert!(DownloadVerifier::from_config(&config, None).is_err());
    assert!(DownloadVerifier::from_config(&config, Some(b"short")).is_err());
    assert!(DownloadVerifier::from_config(&config, Some(&[255; 32])).is_err());
    let file = format!(" \n{}\n", "s".repeat(32));
    let key = DownloadVerifier::from_config(&config, Some(file.as_bytes())).unwrap();
    assert!(key.verify(&token("ordinary"), 100.0, None).is_ok());
    assert!(!format!("{key:?}").contains(&"s".repeat(32)));
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), "s".repeat(32));
    let config = DownloadConfig::from_template(&template).unwrap();
    assert!(
        DownloadVerifier::from_config(&config, Some(b"ignored-file"))
            .unwrap()
            .verify(&token("ordinary"), 100.0, None)
            .is_ok()
    );
}

#[test]
fn verification_buffers_are_bounded_and_final_check_enforces_the_trusted_root() {
    use data_plane_gateway::ingress::jiuwen::download_token::{
        MAX_REGISTRATION_BYTES, MAX_SECRET_BYTES, MAX_TOKEN_BYTES,
    };
    assert!(matches!(
        verifier().verify(&"a".repeat(MAX_TOKEN_BYTES + 1), 100.0, None),
        Err(TokenError::TooLarge)
    ));
    assert!(matches!(
        DownloadVerifier::new(&"a".repeat(MAX_SECRET_BYTES + 1)),
        Err(TokenError::TooLarge)
    ));
    let claims = verifier().verify(&token("verified"), 100.0, None).unwrap();
    assert!(matches!(
        claims.check_file(
            "/assets",
            Some(&vec![b' '; MAX_REGISTRATION_BYTES + 1]),
            metadata(),
            100.0
        ),
        Err(TokenError::TooLarge)
    ));
    let registration = serde_json::to_vec(&registration()).unwrap();
    assert!(claims
        .check_file("/other", Some(&registration), metadata(), 100.0)
        .is_err());
}

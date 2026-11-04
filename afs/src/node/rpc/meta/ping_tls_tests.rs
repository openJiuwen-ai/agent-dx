use super::*;

#[tokio::test]
async fn ping_applies_tls_policy_before_connecting() {
    let error = ping(
        "https://127.0.0.1:1",
        "node-a",
        Duration::from_millis(10),
        TlsConfig::MutualTls {
            encryption_enabled: true,
            ca_certificate: "missing-ca.pem".into(),
            identity_certificate: "missing-client.pem".into(),
            identity_private_key: "missing-client-key.pem".into(),
            server_name: String::new(),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), CLIENT_ARGUMENT_INVALID);
    assert!(error.to_string().contains("server_name"));
}

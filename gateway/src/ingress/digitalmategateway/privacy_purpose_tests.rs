//! Purpose separation in the existing identity signer, with synthetic claims only.
use super::*;

#[test]
fn delivery_bootstrap_connector_cannot_outlive_its_signed_deadline() {
    let signer = TokenSigner::new(&[7; 32], "tenant");
    let current = now().unwrap();
    let mut claims = Claims {
        id: "a".repeat(64),
        tenant: "tenant".into(),
        workspace: "workspace".into(),
        user: "user".into(),
        exp: current + 30,
        recovery_until: current + 30,
        bootstrap_until: Some(current + 30),
    };
    let token = signer.sign_connector(&claims).unwrap();
    assert!(signer.verify_connector(&token, false).is_ok());
    assert!(signer.verify(&token, false).is_err());
    claims.exp = current + 60;
    assert!(signer
        .verify_connector(&signer.sign_connector(&claims).unwrap(), false)
        .is_err());
    claims.exp = current + 30;
    claims.recovery_until = current + 60;
    assert!(signer
        .verify_connector(&signer.sign_connector(&claims).unwrap(), true)
        .is_err());
    claims.exp = current - 1;
    claims.recovery_until = current - 1;
    claims.bootstrap_until = Some(current - 1);
    assert!(signer
        .verify_connector(&signer.sign_connector(&claims).unwrap(), true)
        .is_err());
}

#[test]
fn connector_identity_cannot_be_used_as_a_privacy_bearer() {
    let signer = TokenSigner::new(&[7; 32], "tenant");
    let current = now().unwrap();
    let mut claims = Claims {
        id: "a".repeat(64),
        tenant: "tenant".into(),
        workspace: "workspace".into(),
        user: "user".into(),
        exp: current + 60,
        recovery_until: current + 3600,
        bootstrap_until: None,
    };
    let connector = signer.sign_connector(&claims).unwrap();
    assert!(connector.starts_with("dm-connector-v1."));
    assert!(signer.verify(&connector, false).is_err());
    assert!(signer.verify_connector(&connector, false).is_ok());
    let privacy = signer.sign(&claims).unwrap();
    assert!(signer.verify_connector(&privacy, false).is_err());
    assert!(TokenSigner::new(&[8; 32], "tenant")
        .verify_connector(&connector, false)
        .is_err());
    assert!(TokenSigner::new(&[7; 32], "other")
        .verify_connector(&connector, false)
        .is_err());
    claims.exp = current - 1;
    let old = signer.sign_connector(&claims).unwrap();
    assert!(signer.verify_connector(&old, false).is_err());
    assert!(signer.verify_connector(&old, true).is_ok());
}

#[test]
fn delivery_identity_is_short_lived_and_fixes_the_batch_and_wire_size() {
    let signer = TokenSigner::new(&[7; 32], "tenant");
    let digest = "b".repeat(64);
    let mut claims = DeliveryClaims {
        grant_id: "a".repeat(64),
        tenant: "tenant".into(),
        batch_id: format!("batch_{digest}"),
        digest,
        wire_bytes: 1024,
        exp: now().unwrap() + 60,
    };
    let token = signer.sign_delivery(&claims).unwrap();
    assert!(token.starts_with("dm-ingestion-v1."));
    assert!(signer.verify(&token, false).is_err());
    assert!(signer.verify_connector(&token, false).is_err());
    assert_eq!(signer.verify_delivery(&token).unwrap().wire_bytes, 1024);
    assert!(TokenSigner::new(&[8; 32], "tenant")
        .verify_delivery(&token)
        .is_err());
    assert!(TokenSigner::new(&[7; 32], "other")
        .verify_delivery(&token)
        .is_err());
    claims.batch_id = format!("batch_{}", "c".repeat(64));
    assert!(signer
        .verify_delivery(&signer.sign_delivery(&claims).unwrap())
        .is_err());
    claims.batch_id = format!("batch_{}", claims.digest);
    claims.exp = now().unwrap() - 1;
    assert!(signer
        .verify_delivery(&signer.sign_delivery(&claims).unwrap())
        .is_err());
    claims.exp = now().unwrap() + ACCESS_SECONDS;
    assert!(signer
        .verify_delivery(&signer.sign_delivery(&claims).unwrap())
        .is_err());
    claims.exp = now().unwrap() + 60;
    for bytes in [0, 2_097_153] {
        claims.wire_bytes = bytes;
        assert!(signer
            .verify_delivery(&signer.sign_delivery(&claims).unwrap())
            .is_err());
    }
}

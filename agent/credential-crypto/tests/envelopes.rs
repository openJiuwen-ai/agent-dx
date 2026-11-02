use adx_credential_crypto::{Context, CryptoError, Keyring};
use base64::{engine::general_purpose::STANDARD, Engine};
use std::collections::BTreeMap;

fn context(workspace: &str) -> Context {
    Context {
        tenant: "digitalmate".into(),
        workspace: workspace.into(),
        purpose: "orbit-config".into(),
    }
}

fn ring(active: &str) -> Keyring {
    Keyring::new(
        active.into(),
        BTreeMap::from([("v1".into(), [7; 32]), ("v2".into(), [8; 32])]),
    )
    .unwrap()
}

#[test]
fn ciphertext_is_randomized_bound_to_workspace_and_authenticates_every_layer() {
    let keys = ring("v1");
    let owner = context("alice-workspace");
    let a = keys.seal(&owner, b"private-refresh-token").unwrap();
    let b = keys.seal(&owner, b"private-refresh-token").unwrap();
    assert_ne!(a.ciphertext, b.ciphertext);
    assert_ne!(a.wrapped_key, b.wrapped_key);
    assert_eq!(
        keys.open(&owner, &a).unwrap().as_slice(),
        b"private-refresh-token"
    );
    assert!(keys.open(&context("bob-workspace"), &a).is_err());
    assert!(!serde_json::to_string(&a)
        .unwrap()
        .contains("private-refresh-token"));
    for component in ["data", "key", "id", "context", "version"] {
        let mut damaged = a.clone();
        match component {
            "data" => damaged.ciphertext = STANDARD.encode([1; 40]),
            "key" => damaged.wrapped_key = STANDARD.encode([2; 60]),
            "id" => damaged.key_id = "v2".into(),
            "context" => damaged.context.workspace = "bob-workspace".into(),
            _ => damaged.version = 99,
        }
        assert!(keys.open(&owner, &damaged).is_err(), "{component}");
    }
}

#[test]
fn rewrapping_changes_only_the_key_wrapper_and_supports_retiring_an_old_key() {
    let owner = context("alice-workspace");
    let original = ring("v1").seal(&owner, b"secret").unwrap();
    let upgraded = ring("v2").rewrap(&owner, &original).unwrap();
    assert_eq!(upgraded.ciphertext, original.ciphertext);
    assert_ne!(upgraded.wrapped_key, original.wrapped_key);
    assert_eq!(upgraded.key_id, "v2");
    let new_only = Keyring::new("v2".into(), BTreeMap::from([("v2".into(), [8; 32])])).unwrap();
    assert_eq!(
        new_only.open(&owner, &upgraded).unwrap().as_slice(),
        b"secret"
    );
    assert!(new_only.open(&owner, &original).is_err());
}

#[test]
fn invalid_context_and_oversized_plaintext_are_rejected() {
    assert!(Context {
        tenant: "".into(),
        ..context("a")
    }
    .validate()
    .is_err());
    assert!(Context {
        purpose: "../../file".into(),
        ..context("a")
    }
    .validate()
    .is_err());
    assert!(matches!(
        ring("v1").seal(&context("a"), &vec![0; 131_073]),
        Err(CryptoError::InvalidInput)
    ));
}

#[test]
fn persisted_v1_envelope_keeps_its_wire_context_and_aad() {
    let envelope = serde_json::from_str(r#"{"version":1,"algorithm":"AES-256-GCM","key_backend":"local","key_id":"v1","context":{"tenant":"digitalmate","workspace":"alice-workspace","purpose":"orbit-config"},"wrapped_key":"AQEBAQEBAQEBAQEBc+SMspW66QvU0dkkdTJ1NK3+EfEdkj4WXTRGZ2OxM77zhpWEwExA4LXGDYhQ79Nu","ciphertext":"AgICAgICAgICAgICrj6Lz7//T4WAifF7/FexfSVNWyG6Pzba5TrCKAGavkySzf9l8qfr6g=="}"#).unwrap();
    assert_eq!(
        ring("v1")
            .open(&context("alice-workspace"), &envelope)
            .unwrap()
            .as_slice(),
        b"persisted-provider-token"
    );
}

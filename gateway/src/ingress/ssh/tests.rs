use super::*;
use adx_agent_core::target::{SshRoute, Target};
use russh::keys::{Algorithm, PrivateKey};

fn key() -> PrivateKey {
    PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()
}

#[test]
fn ssh_credentials_select_the_requested_target_family() {
    let inline = key();
    let managed = key();
    let grants = Grants::load(vec![KeyGrant {
        public_key: managed.public_key().to_openssh().unwrap(),
        tenant_id: "tenant-a".into(),
    }])
    .unwrap();
    assert_eq!(grants.tenant(managed.public_key()), Some("tenant-a"));
    assert_eq!(grants.tenant(inline.public_key()), None);
    let duplicate = KeyGrant {
        public_key: managed.public_key().to_openssh().unwrap(),
        tenant_id: "tenant-b".into(),
    };
    assert!(Grants::load(vec![duplicate.clone(), duplicate]).is_err());
}

#[test]
fn terminal_notice_returns_a_reusable_environment_identity() {
    let route: SshRoute = "adx:target:urn%3Aadx%3Atemplate%3Ademo%3A1"
        .parse()
        .unwrap();
    let target = SessionTarget::new(&route, "tenant-a").unwrap();
    let SessionTarget::Managed { scope, .. } = &target else {
        panic!("managed target expected")
    };
    let notice = target.notice().unwrap();
    assert!(notice.contains(&scope.environment_id));
    let urn = notice
        .lines()
        .find_map(|line| line.strip_prefix("Environment URN: "))
        .unwrap();
    assert_eq!(
        urn.parse::<Target>().unwrap(),
        Target::Environment {
            name: "demo".into(),
            version: "1".into(),
            id: scope.environment_id.clone()
        }
    );
    let again = SessionTarget::new(
        &SshRoute {
            target: urn.parse().unwrap(),
            port: None,
            trace: None,
        },
        "tenant-a",
    )
    .unwrap();
    assert_eq!(again.notice(), Some(notice));
    let inline = SessionTarget::new(&"yr:instance:inline-1".parse().unwrap(), "tenant-a").unwrap();
    assert!(matches!(inline, SessionTarget::Inline { ref id, port: 22, .. } if id == "inline-1"));
}

#[tokio::test]
async fn backend_host_verification_accepts_only_configured_keys() {
    use russh::client::Handler;
    let pinned = key();
    let different = key();
    let mut verifier = BackendVerifier {
        keys: vec![pinned.public_key().clone()],
    };
    assert!(verifier
        .check_server_key(&PublicKeyOrCertificate::PublicKey {
            key: pinned.public_key().clone(),
            hash_alg: None
        })
        .await
        .unwrap());
    assert!(!verifier
        .check_server_key(&PublicKeyOrCertificate::PublicKey {
            key: different.public_key().clone(),
            hash_alg: None
        })
        .await
        .unwrap());
}

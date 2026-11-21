//! PS256 JWT signing for Huawei Push V3. The JWT is itself the bearer token;
//! there is no separate token-exchange request. The private key stays here.
use super::Error;
use base64::Engine;
use ring::{
    rand,
    signature::{RsaKeyPair, RSA_PSS_SHA256},
};
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

const AUDIENCE: &str = "https://oauth-login.cloud.huawei.com/oauth2/v3/token";
const URL_SAFE_NO_PAD: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

const JWT_LIFETIME_SECONDS: i64 = 3600;
const REFRESH_AHEAD_SECONDS: i64 = 60;

struct CachedJwt {
    issued_at: i64,
    expires_at: i64,
    token: Arc<Zeroizing<String>>,
}

pub(super) struct Signer {
    key: RsaKeyPair,
    key_id: String,
    issuer: String,
    cached: Mutex<Option<CachedJwt>>,
}

impl Signer {
    pub(super) fn from_pem(
        private_key: &str,
        key_id: String,
        issuer: String,
    ) -> Result<Self, Error> {
        let der = Zeroizing::new(pem_to_der(private_key).ok_or(Error::NotConfigured)?);
        let key = RsaKeyPair::from_pkcs8(&der).map_err(|_| Error::NotConfigured)?;
        Ok(Self {
            key,
            key_id,
            issuer,
            cached: Mutex::new(None),
        })
    }

    pub(super) fn jwt(
        &self,
        clock: impl FnOnce() -> SystemTime,
    ) -> Result<Arc<Zeroizing<String>>, Error> {
        // Sample time under the same lock as signing so concurrent refreshes share one JWT.
        let mut cached = self.cached.lock().map_err(|_| Error::UpstreamUnavailable)?;
        let elapsed = clock()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::UpstreamUnavailable)?;
        let now = i64::try_from(elapsed.as_secs()).map_err(|_| Error::UpstreamUnavailable)?;
        if let Some(entry) = cached.as_ref().filter(|entry| {
            (entry.issued_at..entry.expires_at - REFRESH_AHEAD_SECONDS).contains(&now)
        }) {
            return Ok(Arc::clone(&entry.token));
        }
        let expires_at = now
            .checked_add(JWT_LIFETIME_SECONDS)
            .ok_or(Error::UpstreamUnavailable)?;
        let token = Arc::new(Zeroizing::new(self.sign(now, expires_at)?));
        *cached = Some(CachedJwt {
            issued_at: now,
            expires_at,
            token: Arc::clone(&token),
        });
        Ok(token)
    }

    fn sign(&self, now: i64, expires_at: i64) -> Result<String, Error> {
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::json!({"kid": self.key_id, "typ": "JWT", "alg": "PS256"})
                .to_string()
                .as_bytes(),
        );
        let claims = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                "aud": AUDIENCE, "iss": self.issuer, "iat": now, "exp": expires_at,
            })
            .to_string()
            .as_bytes(),
        );
        let input = format!("{header}.{claims}");
        let rng = rand::SystemRandom::new();
        let mut signature = vec![0u8; self.key.public().modulus_len()];
        self.key
            .sign(&RSA_PSS_SHA256, &rng, input.as_bytes(), &mut signature)
            .map_err(|_| Error::UpstreamUnavailable)?;
        Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(&signature)))
    }
}

/// Strip the PEM armor and decode the PKCS#8 DER body. Returns None for an
/// empty or non-base64 body; key validity is checked by `RsaKeyPair::from_pkcs8`.
fn pem_to_der(pem: &str) -> Option<Vec<u8>> {
    let body: String = pem
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    if body.is_empty() {
        return None;
    }
    base64::engine::general_purpose::STANDARD.decode(body).ok()
}

#[cfg(test)]
mod pem_tests {
    use super::{pem_to_der, Signer, AUDIENCE, URL_SAFE_NO_PAD};
    use base64::Engine;
    use ring::signature::{UnparsedPublicKey, RSA_PSS_2048_8192_SHA256};
    use serde_json::{json, Value};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn at(seconds: u64) -> impl FnOnce() -> SystemTime {
        move || UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[test]
    fn huawei_jwt_has_ps256_claims_and_verifiable_signature() {
        let signer = test_signer();
        let jwt = signer.jwt(at(1000)).unwrap();
        let (signed, signature) = jwt.rsplit_once('.').unwrap();
        let (header, claims) = signed.split_once('.').unwrap();
        let decode = |value| URL_SAFE_NO_PAD.decode(value).unwrap();
        let header: Value = serde_json::from_slice(&decode(header)).unwrap();
        let claims: Value = serde_json::from_slice(&decode(claims)).unwrap();
        assert_eq!(header, json!({"kid":"test-key","typ":"JWT","alg":"PS256"}));
        assert_eq!(
            claims,
            json!({"aud":AUDIENCE,"iss":"test-issuer","iat":1000,"exp":4600})
        );
        UnparsedPublicKey::new(&RSA_PSS_2048_8192_SHA256, signer.key.public().as_ref())
            .verify(signed.as_bytes(), &decode(signature))
            .unwrap();
    }

    fn test_signer() -> Signer {
        Signer::from_pem(
            include_str!("../../../../tests/fixtures/huawei-test-key.pem"),
            "test-key".into(),
            "test-issuer".into(),
        )
        .unwrap()
    }

    #[test]
    fn reuses_jwt_while_more_than_a_minute_remains() {
        let signer = test_signer();
        let first = signer.jwt(at(1000)).unwrap();
        let reused = signer.jwt(at(4539)).unwrap();
        assert!(first.as_str() == reused.as_str());
    }

    #[test]
    fn refreshes_at_one_minute_remaining_and_uses_the_new_expiry() {
        let signer = test_signer();
        let first = signer.jwt(at(1000)).unwrap();
        let refreshed = signer.jwt(at(4540)).unwrap();
        assert!(first.as_str() != refreshed.as_str());
        let payload = refreshed.split('.').nth(1).unwrap();
        let claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        assert_eq!(claims["iat"], 4540);
        assert_eq!(claims["exp"], 8140);
        assert!(refreshed.as_str() == signer.jwt(at(8079)).unwrap().as_str());
    }

    #[test]
    fn clock_rollback_does_not_reuse_a_future_dated_jwt() {
        let signer = test_signer();
        let future = signer.jwt(at(1000)).unwrap();
        let current = signer.jwt(at(990)).unwrap();
        assert!(current.as_str() != future.as_str());
        assert!(current.as_str() == signer.jwt(at(991)).unwrap().as_str());
    }

    #[test]
    fn concurrent_requests_share_one_refreshed_jwt() {
        let signer = test_signer();
        let first = signer.jwt(at(1000)).unwrap();
        let start = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        signer.jwt(at(4540)).unwrap()
                    })
                })
                .collect();
            let tokens: Vec<_> = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect();
            assert!(tokens[0].as_str() != first.as_str());
            assert!(tokens
                .iter()
                .all(|token| token.as_str() == tokens[0].as_str()));
        });
    }

    #[test]
    fn pem_armor_is_stripped_and_body_decoded() {
        // "MIIB" is not a full key, but the base64 body must decode after armor removal.
        let pem = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n";
        assert_eq!(pem_to_der(pem), Some(vec![0x30, 0x82, 0x01]));
    }

    #[test]
    fn empty_or_invalid_pem_is_rejected() {
        assert!(pem_to_der("").is_none());
        assert!(pem_to_der("-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----").is_none());
        assert!(
            pem_to_der("-----BEGIN PRIVATE KEY-----\n!!!\n-----END PRIVATE KEY-----").is_none()
        );
    }
}

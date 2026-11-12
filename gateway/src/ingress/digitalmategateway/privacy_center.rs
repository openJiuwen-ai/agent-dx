//! DigitalMate privacy operations, authenticated independently of mobile sessions.

use super::privacy_cipher::{EncryptedSecret, LegacyEnvelope, PrivacyCipher, SecretContext};
use super::privacy_grants::TokenSigner;
use crate::ingress::accounts::{
    AccountStore, Error, ExpectedRevision, PrivacyOwner, StoredPrivacyGrant,
};
use adx_agent_api::{managed::ManagedService, request::RequestContext};
use adx_agent_core::{AgentBinding, Scope};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use zeroize::Zeroizing;

#[async_trait]
pub(crate) trait BindingReader: Send + Sync {
    async fn binding(&self, scope: &Scope) -> Result<AgentBinding, Error>;
}

pub(crate) struct ManagedBindingReader(pub Arc<ManagedService>);

#[async_trait]
impl BindingReader for ManagedBindingReader {
    async fn binding(&self, scope: &Scope) -> Result<AgentBinding, Error> {
        let timeout = Duration::from_secs(5);
        let context = RequestContext::new(timeout);
        match tokio::time::timeout(timeout, self.0.binding(&context, scope)).await {
            Ok(Ok(binding)) => Ok(binding),
            Ok(Err(adx_agent_api::Error::NotFound)) => Err(Error::Unauthorized),
            _ => Err(Error::Unavailable),
        }
    }
}

pub(crate) struct SecretValue {
    pub revision: String,
    /// A missing value with a revision is a tombstone, required for safe recreation.
    pub plaintext: Option<Zeroizing<Vec<u8>>>,
}

pub(crate) struct PrivacyCenter {
    store: AccountStore,
    cipher: PrivacyCipher,
    signer: Arc<TokenSigner>,
    legacy: Option<PrivacyCipher>,
}

impl PrivacyCenter {
    pub fn new(store: AccountStore, cipher: PrivacyCipher, signer: Arc<TokenSigner>) -> Self {
        Self {
            store,
            cipher,
            signer,
            legacy: None,
        }
    }

    pub fn with_legacy(mut self, legacy: PrivacyCipher) -> Self {
        self.legacy = Some(legacy);
        self
    }

    /// Import the former workspace envelope once; never overwrite newer or revoked data.
    pub async fn import_legacy(
        &self,
        token: &str,
        envelope: &LegacyEnvelope,
    ) -> Result<String, Error> {
        const NAME: &str = "DIGITALMATE_CONFIG";
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        let legacy = self.legacy.as_ref().ok_or(Error::Unavailable)?;
        let plaintext = legacy
            .open_legacy(owner.tenant, owner.workspace, envelope)
            .map_err(|_| Error::Invalid)?;
        let values: std::collections::BTreeMap<String, String> =
            serde_json::from_slice(&plaintext).map_err(|_| Error::Invalid)?;
        let context = SecretContext {
            tenant: owner.tenant,
            workspace: owner.workspace,
            user: owner.user,
            name: NAME,
        };
        let existing = self.store.privacy_get(&owner, NAME).await?;
        if existing.is_none() {
            let sealed = self
                .cipher
                .seal(&context, &plaintext)
                .map_err(|_| Error::Unavailable)?;
            match self
                .store
                .privacy_put(
                    &owner,
                    NAME,
                    &sealed.key_id,
                    &sealed.ciphertext,
                    ExpectedRevision::New,
                )
                .await
            {
                Ok(_) => (),
                Err(Error::Conflict) => (),
                Err(error) => return Err(error),
            }
        }
        let existing = self
            .store
            .privacy_get(&owner, NAME)
            .await?
            .ok_or(Error::Conflict)?;
        let (Some(key_id), Some(ciphertext)) = (existing.key_id, existing.ciphertext) else {
            return Err(Error::Conflict);
        };
        let opened = Zeroizing::new(
            self.cipher
                .open(&context, &EncryptedSecret { key_id, ciphertext })
                .map_err(|_| Error::Unavailable)?,
        );
        let current: std::collections::BTreeMap<String, String> =
            serde_json::from_slice(&opened).map_err(|_| Error::Conflict)?;
        if current != values {
            return Err(Error::Conflict);
        }
        Ok(existing.revision)
    }

    /// The caller must stop the workspace writer until the returned envelope is installed.
    pub async fn export_legacy(
        &self,
        token: &str,
        expected: &str,
    ) -> Result<LegacyEnvelope, Error> {
        const NAME: &str = "DIGITALMATE_CONFIG";
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        let secret = self
            .store
            .privacy_exportable(&owner, NAME)
            .await?
            .ok_or(Error::Conflict)?;
        if secret.revision != expected {
            return Err(Error::Conflict);
        }
        let plaintext = match (secret.key_id, secret.ciphertext) {
            (Some(key_id), Some(ciphertext)) => {
                let context = SecretContext {
                    tenant: owner.tenant,
                    workspace: owner.workspace,
                    user: owner.user,
                    name: NAME,
                };
                Zeroizing::new(
                    self.cipher
                        .open(&context, &EncryptedSecret { key_id, ciphertext })
                        .map_err(|_| Error::Unavailable)?,
                )
            }
            (None, None) => Zeroizing::new(b"{}".to_vec()),
            _ => return Err(Error::Unavailable),
        };
        let legacy = self.legacy.as_ref().ok_or(Error::Unavailable)?;
        let envelope = legacy
            .seal_legacy(owner.tenant, owner.workspace, &plaintext)
            .map_err(|_| Error::Unavailable)?;
        if legacy
            .open_legacy(owner.tenant, owner.workspace, &envelope)
            .map_err(|_| Error::Unavailable)?
            .as_slice()
            != plaintext.as_slice()
        {
            return Err(Error::Unavailable);
        }
        let current = self
            .store
            .privacy_exportable(&owner, NAME)
            .await?
            .ok_or(Error::Conflict)?;
        if current.revision != expected {
            return Err(Error::Conflict);
        }
        Ok(envelope)
    }

    async fn authorize(&self, token: &str) -> Result<StoredPrivacyGrant, Error> {
        let claims = self.signer.verify(token, false)?;
        let grant = self
            .store
            .privacy_get_grant(&claims.id)
            .await?
            .ok_or(Error::Unauthorized)?;
        if !TokenSigner::matches(&claims, &grant) {
            return Err(Error::Unauthorized);
        }
        Ok(grant)
    }

    pub async fn get(&self, token: &str, name: &str) -> Result<Option<SecretValue>, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        let secret = self.store.privacy_get(&owner, name).await?;
        let result = secret
            .map(|secret| {
                let plaintext = match (secret.key_id, secret.ciphertext) {
                    (Some(key_id), Some(ciphertext)) => {
                        let context = SecretContext {
                            tenant: owner.tenant,
                            workspace: owner.workspace,
                            user: owner.user,
                            name,
                        };
                        Some(Zeroizing::new(
                            self.cipher
                                .open(&context, &EncryptedSecret { key_id, ciphertext })
                                .map_err(|_| Error::Unavailable)?,
                        ))
                    }
                    (None, None) => None,
                    _ => return Err(Error::Unavailable),
                };
                Ok(SecretValue {
                    revision: secret.revision,
                    plaintext,
                })
            })
            .transpose()?;
        Ok(result)
    }

    #[cfg(test)]
    pub async fn put(
        &self,
        token: &str,
        name: &str,
        value: &[u8],
        expected: ExpectedRevision<'_>,
    ) -> Result<String, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        let context = SecretContext {
            tenant: owner.tenant,
            workspace: owner.workspace,
            user: owner.user,
            name,
        };
        let sealed = self
            .cipher
            .seal(&context, value)
            .map_err(|_| Error::Invalid)?;
        self.store
            .privacy_put(&owner, name, &sealed.key_id, &sealed.ciphertext, expected)
            .await
    }

    #[cfg(test)]
    pub async fn delete(&self, token: &str, name: &str, expected: &str) -> Result<String, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        self.store.privacy_delete(&owner, name, expected).await
    }

    pub async fn stage(
        &self,
        token: &str,
        transaction_id: &str,
        name: &str,
        value: Option<&[u8]>,
        expected: ExpectedRevision<'_>,
    ) -> Result<String, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        let context = SecretContext {
            tenant: owner.tenant,
            workspace: owner.workspace,
            user: owner.user,
            name,
        };
        let sealed = value
            .map(|value| {
                self.cipher
                    .seal(&context, value)
                    .map_err(|_| Error::Invalid)
            })
            .transpose()?;
        self.store
            .privacy_stage(
                transaction_id,
                &owner,
                name,
                sealed
                    .as_ref()
                    .map(|value| (value.key_id.as_str(), value.ciphertext.as_slice())),
                expected,
            )
            .await
    }

    pub async fn promote(
        &self,
        token: &str,
        transaction_id: &str,
        name: &str,
    ) -> Result<String, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        self.store
            .privacy_promote(transaction_id, &owner, name)
            .await
    }

    pub async fn abort(&self, token: &str, transaction_id: &str, name: &str) -> Result<(), Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        self.store.privacy_abort(transaction_id, &owner, name).await
    }

    pub async fn transaction_status(
        &self,
        token: &str,
        transaction_id: &str,
        name: &str,
    ) -> Result<Option<(String, String)>, Error> {
        let grant = self.authorize(token).await?;
        let owner = PrivacyOwner {
            tenant: &grant.scope.tenant,
            workspace: &grant.workspace,
            user: &grant.user,
        };
        self.store
            .privacy_transaction_status(transaction_id, &owner, name)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{BindingReader, PrivacyCenter};
    use crate::ingress::accounts::{AccountStore, Error, ExpectedRevision, PrivacyOwner};
    use crate::ingress::digitalmategateway::privacy_cipher::PrivacyCipher;
    use crate::ingress::digitalmategateway::privacy_grants::{PrivacyIdentity, TokenSigner};
    use crate::ingress::digitalmategateway::privacy_http::PrivacyHttp;
    use adx_agent_core::{AgentBinding, BindingPhase, EnvironmentRef, Scope, TemplateRef};
    use async_trait::async_trait;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::sync::Mutex;

    type AdmissionPause = (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );
    struct Bindings(
        Mutex<AgentBinding>,
        std::sync::atomic::AtomicBool,
        Mutex<Option<AdmissionPause>>,
    );

    #[async_trait]
    impl BindingReader for Bindings {
        async fn binding(&self, scope: &Scope) -> Result<AgentBinding, Error> {
            if self.1.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::Unavailable);
            }
            let binding = self.0.lock().await.clone();
            let pause = self.2.lock().await.take();
            if let Some((entered, resume)) = pause {
                entered.send(()).unwrap();
                resume.await.unwrap();
            }
            if &binding.scope != scope {
                return Err(Error::Unauthorized);
            }
            Ok(binding)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a dedicated disposable PostgreSQL database"]
    async fn offline_background_access_remains_scoped_and_revocable() {
        let url = std::env::var("DIGITALMATE_PRIVACY_TEST_DATABASE_URL")
            .expect("dedicated privacy test database required");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        client
            .query_one("SELECT pg_advisory_lock(741650271132)", &[])
            .await
            .unwrap();
        client
            .batch_execute(include_str!("../accounts/schema.sql"))
            .await
            .unwrap();
        client
            .query_one("SELECT pg_advisory_unlock(741650271132)", &[])
            .await
            .unwrap();
        let user = format!("privacy-center-{}", uuid::Uuid::new_v4());
        client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','privacy-test','test',$1,'1')", &[&user]).await.unwrap();
        let store = AccountStore::from_url(&url, true, None).await.unwrap();
        let scope = Scope {
            tenant: "tenant-a".into(),
            template: "digitalmate".into(),
            template_ref: TemplateRef::Version("1".into()),
            binding_id: "binding-a".into(),
        };
        let binding = AgentBinding {
            scope,
            generation: "generation-a".into(),
            current: EnvironmentRef::new("1".into(), "instance-a".into()),
            secondary: None,
            phase: BindingPhase::Active,
            parameters: BTreeMap::from([("workspace".into(), "workspace-a".into())]),
            volumes: BTreeMap::new(),
            creation_attempts: BTreeMap::new(),
        };
        let bindings = Arc::new(Bindings(
            Mutex::new(binding),
            std::sync::atomic::AtomicBool::new(false),
            Mutex::new(None),
        ));
        let signer = Arc::new(TokenSigner::new(&[7; 32], "tenant-a"));
        let identity = Arc::new(PrivacyIdentity::new(
            "digitalmate".into(),
            store.clone(),
            bindings.clone(),
            signer.clone(),
        ));
        let admission_scope = bindings.0.lock().await.scope.clone();
        let token_secret = identity
            .admit(&admission_scope, "workspace-a", &user)
            .await
            .unwrap();
        let token = token_secret.as_str();
        let digest = signer.verify(token, false).unwrap().id;
        // Many reconnects reuse one business grant, rather than exhausting the quota.
        for _ in 0..70 {
            let repeated = identity
                .admit(&admission_scope, "workspace-a", &user)
                .await
                .unwrap();
            assert_eq!(signer.verify(&repeated, false).unwrap().id, digest);
        }
        let count: i64 = client
            .query_one(
                "SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 1);

        let cipher =
            PrivacyCipher::new("key-a".into(), BTreeMap::from([("key-a".into(), [7; 32])]))
                .unwrap();
        let legacy = PrivacyCipher::new(
            "legacy-v1".into(),
            BTreeMap::from([("legacy-v1".into(), [9; 32])]),
        )
        .unwrap();
        let center =
            Arc::new(PrivacyCenter::new(store.clone(), cipher, signer.clone()).with_legacy(legacy));

        // No login session exists: a valid workspace grant is sufficient for background work.
        let revision = center
            .put(
                token,
                "FEISHU_TOKEN",
                b"synthetic-privacy-value",
                ExpectedRevision::New,
            )
            .await
            .unwrap();
        // Reads are independent of ADX availability; renewals still fail closed while it is unavailable.
        bindings.1.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            identity.renew(token).await,
            Err(Error::Unavailable)
        ));
        let opened = center.get(token, "FEISHU_TOKEN").await.unwrap().unwrap();
        bindings.1.store(false, std::sync::atomic::Ordering::SeqCst);
        let renewed = identity.renew(token).await.unwrap();
        assert!(center
            .get(renewed["token"].as_str().unwrap(), "FEISHU_TOKEN")
            .await
            .is_ok());
        client
            .execute(
                "UPDATE adx_accounts.users SET status='disabled' WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        assert!(matches!(
            center.get(token, "FEISHU_TOKEN").await,
            Err(Error::Unauthorized)
        ));
        assert!(matches!(
            identity.renew(token).await,
            Err(Error::Unauthorized)
        ));
        client
            .execute(
                "UPDATE adx_accounts.users SET status='active' WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();

        assert_eq!(opened.revision, revision);
        assert_eq!(
            opened.plaintext.unwrap().as_slice(),
            b"synthetic-privacy-value"
        );
        let http = PrivacyHttp::new(center.clone(), identity.clone());
        let request = http::Request::builder()
            .method("POST")
            .uri("/v1/secret/get")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from_static(
                br#"{"name":"FEISHU_TOKEN"}"#,
            )))
            .unwrap();
        let response = http.handle(request).await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        use http_body_util::BodyExt;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["revision"], revision);
        assert!(json["value"].as_str().is_some());
        let transaction = uuid::Uuid::new_v4().to_string();
        let stage = http::Request::builder().method("POST").uri("/v1/secret/stage")
            .header("authorization", format!("Bearer {token}")).header("content-type", "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(serde_json::json!({"transaction":transaction,"name":"FEISHU_TOKEN","value":STANDARD.encode(b"new-synthetic-value"),"delete":false,"revision":revision}).to_string()))).unwrap();
        let staged = http.handle(stage).await;
        assert_eq!(staged.status(), http::StatusCode::OK);
        assert_eq!(
            center
                .get(token, "FEISHU_TOKEN")
                .await
                .unwrap()
                .unwrap()
                .plaintext
                .unwrap()
                .as_slice(),
            b"synthetic-privacy-value"
        );
        let commit = http::Request::builder()
            .method("POST")
            .uri("/v1/secret/commit")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                serde_json::json!({"transaction":transaction,"name":"FEISHU_TOKEN"}).to_string(),
            )))
            .unwrap();
        assert_eq!(http.handle(commit).await.status(), http::StatusCode::OK);
        assert_eq!(
            center
                .get(token, "FEISHU_TOKEN")
                .await
                .unwrap()
                .unwrap()
                .plaintext
                .unwrap()
                .as_slice(),
            b"new-synthetic-value"
        );
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let cert = fixture.join("ingress-cert.pem");
        let key = fixture.join("ingress-key.pem");
        let tls =
            adx_transport::tls::http_server_acceptor(&cert, &key, None, vec![b"http/1.1".to_vec()])
                .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(Arc::new(http).serve(listener, tls, shutdown_rx));
        let http_client = reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap(),
            )
            .build()
            .unwrap();
        let reply = http_client
            .post(format!(
                "https://localhost:{}/v1/secret/get",
                address.port()
            ))
            .bearer_auth(token)
            .json(&serde_json::json!({"name":"FEISHU_TOKEN"}))
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), reqwest::StatusCode::OK);
        assert_eq!(reply.headers()["cache-control"], "no-store");
        #[cfg(unix)]
        if let (Ok(python), Ok(source)) = (
            std::env::var("DIGITALMATE_PRIVACY_CLIENT_PYTHON"),
            std::env::var("DIGITALMATE_PRIVACY_CLIENT_SOURCE"),
        ) {
            use std::io::Write;
            let delivered = token.to_owned();
            let ca_path = cert.clone();
            let port = address.port();
            let output = tokio::task::spawn_blocking(move || {
                let mut child = std::process::Command::new(python).arg("-c").arg(r#"
import os, uuid, sys
from server_engine.credential_crypto import PrivacyClient
client = PrivacyClient(os.environ)
client.accept_identity(sys.stdin.readline().strip())
name = 'PYTHON_CASE'
transaction = str(uuid.uuid4())
revision = client.stage(transaction, name, {'DIGITALMATE_OPEN_METEO_API_KEY':'synthetic-python-token'}, None)
assert client.get(name) == (None, None)
assert client.status(transaction, name) == ('pending', revision)
assert client.commit(transaction, name) == revision
assert client.commit(transaction, name) == revision
assert client.get(name) == (revision, {'DIGITALMATE_OPEN_METEO_API_KEY':'synthetic-python-token'})
aborted = str(uuid.uuid4())
client.abort(aborted, name)
assert client.status(aborted, name)[0] == 'aborted'
import asyncio, tempfile
from pathlib import Path
from server_engine.engine_config import EngineConfiguration
from server_engine.engine_connectors.base import AiohttpTransport
from server_engine.engine_connectors.registry import ConnectionRegistry
from server_engine.engine_workspace import EngineStore
async def exercise_backend(workspace):
    env = {**os.environ, 'DIGITALMATE_SECRET_STORE':'privacy'}
    config = EngineConfiguration(workspace, env, privacy_client=client)
    store = EngineStore(workspace / 'digitalmate' / 'digitalmate.db')
    registry = ConnectionRegistry(store, config, AiohttpTransport())
    try:
        connection = await registry.asave('feishu', 'fixture', 'user', {'app_id':'cli_fixture','app_secret':'synthetic-app-secret','user_access_token':'u-old','user_refresh_token':'old-refresh','user_token_expires_at':'1'}, '')
        expected = dict(registry.fields(connection.id))
        await registry._asave_tokens(connection.id, expected, {'user_access_token':'u-new','user_refresh_token':'new-refresh','user_token_expires_at':'4000000000'})
        assert registry.fields(connection.id)['user_refresh_token'] == 'new-refresh'
        assert 'synthetic-app-secret' not in config.path.read_text()
        assert EngineConfiguration(workspace, env, privacy_client=client).values['DIGITALMATE_CONNECTIONS'] == config.values['DIGITALMATE_CONNECTIONS']
        await registry.aremove(connection.id)
        assert registry.fields(connection.id) == {}
    finally:
        store.close()
with tempfile.TemporaryDirectory() as directory:
    asyncio.run(exercise_backend(Path(directory)))
print('digitalmate-backend-https-ok')
print('python-client-https-ok')
"#).env("PYTHONPATH", source)
                    .env("DIGITALMATE_CRYPTO_URL", format!("https://localhost:{port}"))
                    .env("DIGITALMATE_CRYPTO_CA_FILE", ca_path)
                    .env("DIGITALMATE_CRYPTO_TENANT", "tenant-a")
                    .env("DIGITALMATE_CRYPTO_WORKSPACE", "workspace-a")
                    .env("DIGITALMATE_CRYPTO_PURPOSE", "orbit-config")
                    .env("DIGITALMATE_PRIVACY_BOOTSTRAP_TIMEOUT_SECONDS", "0")
                    .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
                child.stdin.take().unwrap().write_all(format!("{delivered}\n").as_bytes()).unwrap();
                child.wait_with_output().unwrap()
            }).await.unwrap();
            assert!(
                output.status.success(),
                "Python client failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("python-client-https-ok"));
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("digitalmate-backend-https-ok")
            );
            eprintln!("python_client_https_integration=passed");
        }
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut plain = tokio::net::TcpStream::connect(address).await.unwrap();
        plain.write_all(b"POST /v1/secret/get HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-Proto: https\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        let mut received = [0u8; 128];
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(6), plain.read(&mut received))
                .await;
        assert!(result.is_ok());
        assert!(!String::from_utf8_lossy(&received).contains("200 OK"));
        shutdown.send(true).unwrap();
        task.await.unwrap().unwrap();
        let encrypted: Vec<u8> = client
            .query_one(
                "SELECT encrypted_value FROM adx_accounts.user_secrets WHERE user_id=$1 AND secret_key='FEISHU_TOKEN'",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        assert!(!encrypted
            .windows(b"synthetic-privacy-value".len())
            .any(|window| window == b"synthetic-privacy-value"));
        // The Python subtest used only synthetic records; reset this isolated key for the legacy-import case.
        client.execute("DELETE FROM adx_accounts.privacy_transactions WHERE user_id=$1 AND secret_key='DIGITALMATE_CONFIG'", &[&user]).await.unwrap();
        client.execute("DELETE FROM adx_accounts.user_secrets WHERE user_id=$1 AND secret_key='DIGITALMATE_CONFIG'", &[&user]).await.unwrap();
        let wrap_aad = serde_json::to_vec(&[
            "adx.credentials.dek",
            "1",
            "tenant-a",
            "workspace-a",
            "orbit-config",
            "legacy-v1",
        ])
        .unwrap();
        let data_aad = serde_json::to_vec(&[
            "adx.credentials.data",
            "1",
            "tenant-a",
            "workspace-a",
            "orbit-config",
        ])
        .unwrap();
        let wrap = crate::ingress::accounts::CredentialCipher::new([9; 32], "test").unwrap();
        let payload = crate::ingress::accounts::CredentialCipher::new([4; 32], "test").unwrap();
        let envelope = serde_json::from_value(serde_json::json!({"version":1,"algorithm":"AES-256-GCM","key_backend":"local","key_id":"legacy-v1","context":{"tenant":"tenant-a","workspace":"workspace-a","purpose":"orbit-config"},"wrapped_key":STANDARD.encode(wrap.seal(&wrap_aad, &[4;32]).unwrap()),"ciphertext":STANDARD.encode(payload.seal(&data_aad, br#"{"DIGITALMATE_CONNECTIONS":"synthetic"}"#).unwrap())})).unwrap();
        let imported = center.import_legacy(token, &envelope).await.unwrap();
        assert_eq!(
            center.import_legacy(token, &envelope).await.unwrap(),
            imported
        );
        let refreshed = center
            .put(
                token,
                "DIGITALMATE_CONFIG",
                br#"{"DIGITALMATE_CONNECTIONS":"newer"}"#,
                ExpectedRevision::Existing(&imported),
            )
            .await
            .unwrap();
        let rollback = center.export_legacy(token, &refreshed).await.unwrap();
        let old = PrivacyCipher::new(
            "legacy-v1".into(),
            BTreeMap::from([("legacy-v1".into(), [9; 32])]),
        )
        .unwrap();
        assert_eq!(
            old.open_legacy("tenant-a", "workspace-a", &rollback)
                .unwrap()
                .as_slice(),
            br#"{"DIGITALMATE_CONNECTIONS":"newer"}"#
        );
        assert!(matches!(
            center.export_legacy(token, &imported).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            center.import_legacy(token, &envelope).await,
            Err(Error::Conflict)
        ));
        center
            .delete(token, "DIGITALMATE_CONFIG", &refreshed)
            .await
            .unwrap();
        assert!(matches!(
            center.import_legacy(token, &envelope).await,
            Err(Error::Conflict)
        ));
        bindings.0.lock().await.generation = "generation-b".into();
        assert!(matches!(
            identity.renew(token).await,
            Err(Error::Unauthorized)
        ));
        // Reverting a captured Binding snapshot cannot revive its revoked authorization.
        bindings.0.lock().await.generation = "generation-a".into();
        assert!(matches!(
            identity.renew(token).await,
            Err(Error::Unauthorized)
        ));
        let fresh = identity
            .admit(&admission_scope, "workspace-a", &user)
            .await
            .unwrap();
        assert_ne!(signer.verify(&fresh, false).unwrap().id, digest);
        assert!(matches!(
            center.get(token, "FEISHU_TOKEN").await,
            Err(Error::Unauthorized)
        ));
        bindings.0.lock().await.phase = BindingPhase::Deleting;
        assert!(matches!(
            identity.renew(&fresh).await,
            Err(Error::Unauthorized)
        ));

        store.privacy_revoke_grant(&digest).await.unwrap();
        assert!(matches!(
            center.get(token, "FEISHU_TOKEN").await,
            Err(Error::Unauthorized)
        ));
        let mut unrelated_scope = admission_scope.clone();
        unrelated_scope.binding_id = "unrelated-binding".into();
        let unrelated_id = "f".repeat(64);
        store
            .privacy_issue_grant(
                &unrelated_id,
                &PrivacyOwner {
                    tenant: &admission_scope.tenant,
                    workspace: "workspace-a",
                    user: &user,
                },
                &unrelated_scope,
                "unrelated-generation",
                super::super::privacy_grants::now().unwrap() + 3600,
                || async { Ok(()) },
            )
            .await
            .unwrap();
        // Rebuilding the same scope more than the quota must retire old grants immediately.
        bindings.0.lock().await.phase = BindingPhase::Active;
        let mut previous = fresh;
        for generation in 0..70 {
            bindings.0.lock().await.generation = format!("rebuilt-{generation}");
            let current = identity
                .admit(&admission_scope, "workspace-a", &user)
                .await
                .unwrap();
            assert!(matches!(
                center.get(&previous, "FEISHU_TOKEN").await,
                Err(Error::Unauthorized)
            ));
            previous = current;
        }
        let count: i64 = client
            .query_one(
                "SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 2);
        assert!(
            !store
                .privacy_get_grant(&unrelated_id)
                .await
                .unwrap()
                .unwrap()
                .revoked
        );

        // A delayed admission for an obsolete generation must not delete the new generation's grant.
        bindings.0.lock().await.generation = "race-old".into();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (resume, continue_old) = tokio::sync::oneshot::channel();
        *bindings.2.lock().await = Some((entered, continue_old));
        let old_identity = identity.clone();
        let old_scope = admission_scope.clone();
        let old_user = user.clone();
        let old_request = tokio::spawn(async move {
            old_identity
                .admit(&old_scope, "workspace-a", &old_user)
                .await
        });
        waiting.await.unwrap();
        bindings.0.lock().await.generation = "race-new".into();
        let winner = identity
            .admit(&admission_scope, "workspace-a", &user)
            .await
            .unwrap();
        resume.send(()).unwrap();
        assert!(matches!(
            old_request.await.unwrap(),
            Err(Error::Unauthorized)
        ));
        assert!(center.get(&winner, "FEISHU_TOKEN").await.is_ok());
        client
            .execute(
                "DELETE FROM adx_accounts.privacy_transactions WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM adx_accounts.user_secrets WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM adx_accounts.privacy_grants WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap();
        client
            .execute("DELETE FROM adx_accounts.users WHERE user_id=$1", &[&user])
            .await
            .unwrap();
    }
}

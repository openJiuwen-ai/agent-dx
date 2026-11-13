//! Optional HTTPS service lifecycle; identity renewal is requested by the workload.
#[cfg(unix)]
mod implementation {
    use super::super::privacy_center::{BindingReader, PrivacyCenter};
    use super::super::privacy_cipher::PrivacyCipher;
    use super::super::privacy_grants::{PrivacyIdentity, TokenSigner};
    use super::super::privacy_http::PrivacyHttp;
    use crate::ingress::accounts::{AccountStore, Error};
    use serde::Deserialize;
    use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc};
    use tokio::{sync::watch, task::JoinHandle};
    #[derive(Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct PrivacyRuntimeConfig {
        pub listen: SocketAddr,
        pub tls_certificate: PathBuf,
        pub tls_private_key: PathBuf,
        pub template: String,
        #[serde(default)]
        pub legacy_keyring_file: Option<PathBuf>,
    }
    pub(crate) struct PrivacyRuntime {
        pub identity: Arc<PrivacyIdentity>,
        pub center: Arc<PrivacyCenter>,
        pub delivery: Option<Arc<super::super::privacy_grants::DeliveryIdentity>>,
        shutdown: watch::Sender<bool>,
        listener: JoinHandle<()>,
    }
    impl PrivacyRuntime {
        pub fn start(
            config: PrivacyRuntimeConfig,
            store: AccountStore,
            bindings: Arc<dyn BindingReader>,
            root: &[u8; 32],
            tenant: &str,
            inbox: Option<super::super::ingestion::Inbox>,
        ) -> Result<Self, Error> {
            tokio::runtime::Handle::try_current().map_err(|_| Error::Configuration)?;
            let cipher = PrivacyCipher::new(
                "gateway-v1".into(),
                BTreeMap::from([("gateway-v1".into(), *root)]),
            )
            .map_err(|_| Error::Configuration)?;
            let tls = adx_transport::tls::http_server_acceptor(
                &config.tls_certificate,
                &config.tls_private_key,
                None,
                vec![b"http/1.1".to_vec()],
            )
            .map_err(|_| Error::Configuration)?;
            let listener =
                std::net::TcpListener::bind(config.listen).map_err(|_| Error::Unavailable)?;
            listener
                .set_nonblocking(true)
                .map_err(|_| Error::Unavailable)?;
            let listener =
                tokio::net::TcpListener::from_std(listener).map_err(|_| Error::Unavailable)?;
            let signer = Arc::new(TokenSigner::new(root, tenant));
            let identity = Arc::new(PrivacyIdentity::new(
                config.template,
                store.clone(),
                bindings,
                signer.clone(),
            ));
            let mut center = PrivacyCenter::new(store, cipher, signer);
            if let Some(path) = &config.legacy_keyring_file {
                center = center
                    .with_legacy(PrivacyCipher::load(path).map_err(|_| Error::Configuration)?);
            }
            let center = Arc::new(center);
            let delivery = inbox.map(|inbox| {
                Arc::new(super::super::privacy_grants::DeliveryIdentity::new(
                    identity.clone(),
                    inbox,
                ))
            });
            let mut http = PrivacyHttp::new(center.clone(), identity.clone());
            if let Some(delivery) = &delivery {
                http = http.with_ingestion(delivery.clone());
            }
            let http = Arc::new(http);
            let (shutdown, receiver) = watch::channel(false);
            let listener = tokio::spawn(async move {
                if http.serve(listener, tls, receiver).await.is_err() {
                    tracing::error!("DigitalMate privacy HTTPS listener stopped");
                }
            });
            Ok(Self {
                identity,
                center,
                delivery,
                shutdown,
                listener,
            })
        }
    }
    impl Drop for PrivacyRuntime {
        fn drop(&mut self) {
            let _ = self.shutdown.send(true);
            self.listener.abort();
        }
    }
}
#[cfg(unix)]
pub(crate) use implementation::{PrivacyRuntime, PrivacyRuntimeConfig};

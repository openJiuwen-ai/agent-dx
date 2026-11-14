//! Unix configuration-center and connector runtime assembly.
use super::*;

pub(super) struct Runtime {
    prepared_composio: Option<super::super::composio::PreparedComposio>,
    pub(super) privacy: Option<super::super::privacy_runtime::PrivacyRuntime>,
    ingestion: Option<PreparedIngestion>,
}

struct PreparedIngestion {
    settings: super::super::ingestion::IngestionConfig,
    inbox: super::super::ingestion::Inbox,
    verifier: Arc<super::super::composio::WebhookVerifier>,
}

impl DigitalMateGatewayConfig {
    pub(super) fn validate_connectors(&self) -> Result<(), Error> {
        if let Some(composio) = &self.composio {
            composio.validate().map_err(|_| invalid())?;
        }
        if let Some(ingestion) = &self.ingestion {
            ingestion.validate().map_err(|_| invalid())?;
        }
        if (self.ingestion.is_some() || self.composio.is_some())
            && self
                .privacy
                .as_ref()
                .zip(self.digitalmate.as_ref())
                .is_none_or(|(privacy, app)| privacy.template != app.template)
        {
            return Err(invalid());
        }
        if let Some(privacy) = &self.privacy {
            if ![&self.orbit, &self.digitalmate].iter().any(|app| {
                app.as_ref()
                    .is_some_and(|app| app.template == privacy.template)
            }) {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

impl DigitalMateGatewayApi {
    pub(super) fn has_prepared_composio(&self) -> bool {
        self.runtime.prepared_composio.is_some()
    }

    pub(super) fn connector_gateway(
        &self,
        ingress: Arc<Ingress>,
    ) -> Option<Arc<super::super::composio::Composio>> {
        self.composio.clone().or_else(|| {
            self.runtime.prepared_composio.as_ref().map(|prepared| {
                Arc::new(
                    prepared.bind(
                        ingress,
                        self.runtime
                            .ingestion
                            .as_ref()
                            .map(|value| value.inbox.clone()),
                    ),
                )
            })
        })
    }
    /// Narrow owner-call issuer; available only with the configured existing
    /// DigitalMate identity HTTPS service. No signing key leaves that service.
    pub fn authorization_owner(
        &self,
    ) -> accounts::Result<Arc<super::super::connector_owner::OwnerAuthority>> {
        self.runtime
            .privacy
            .as_ref()
            .and_then(|runtime| runtime.authorization_owner.clone())
            .ok_or(accounts::Error::Unavailable)
    }

    /// Authenticate the existing connector workload purpose, independently of
    /// mobile sessions. Connection intent/read permission is not implied.
    pub async fn connector_workload_identity(
        &self,
        headers: &http::HeaderMap,
    ) -> accounts::Result<super::super::connector_owner::ConnectorWorkloadIdentity> {
        let identity = self
            .runtime
            .privacy
            .as_ref()
            .ok_or(accounts::Error::Unavailable)?
            .identity
            .clone();
        super::super::connector_owner::ConnectorWorkloadIdentity::authenticate(identity, headers)
            .await
    }

    pub fn new(
        config: DigitalMateGatewayConfig,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
    ) -> Result<Self, Error> {
        if config.ingestion.is_some() || config.composio.is_some() {
            return Err(invalid());
        }
        Self::new_inner(config, managed, auth, None)
    }

    /// Existing process startup: validate every dependency before mounting the
    /// webhook or starting workers. No volatile queue or anonymous auth fallback.
    pub async fn build(
        config: DigitalMateGatewayConfig,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let inbox = match &config.ingestion {
            Some(settings) => Some(settings.open().await.map_err(|_| {
                Error::Unavailable("DigitalMate ingestion database initialization failed".into())
            })?),
            None => None,
        };
        let mut api = Self::new_inner(config, managed, auth, inbox.clone())?;
        if let Some(settings) = &api.config.composio {
            let privacy = api.runtime.privacy.as_ref().ok_or_else(invalid)?;
            let bytes = privacy
                .center
                .service_secret(
                    &api.config.tenant,
                    accounts::ServiceSecretName::ComposioProjectKey,
                )
                .await
                .map_err(|_| invalid())?
                .and_then(|value| value.plaintext)
                .ok_or_else(|| Error::Invalid("Composio project key is required".into()))?;
            let key = std::str::from_utf8(&bytes)
                .map_err(|_| invalid())?
                .to_owned();
            api.runtime.prepared_composio = Some(
                settings
                    .prepare(
                        key,
                        api.authorization_owner().map_err(|_| invalid())?,
                        api.managed.clone(),
                    )
                    .await
                    .map_err(|_| {
                        Error::Unavailable(
                            "Composio client or shared accounting initialization failed".into(),
                        )
                    })?,
            );
        }
        if let Some(settings) = api.config.ingestion.clone() {
            let privacy = api.runtime.privacy.as_ref().ok_or_else(invalid)?;
            let bytes = privacy
                .center
                .service_secret(
                    &api.config.tenant,
                    accounts::ServiceSecretName::ComposioWebhookSecret,
                )
                .await
                .map_err(|_| invalid())?
                .and_then(|value| value.plaintext)
                .ok_or_else(|| {
                    Error::Invalid("Composio webhook signing configuration is required".into())
                })?;
            let secrets = zeroize::Zeroizing::new(
                serde_json::from_slice::<Vec<String>>(&bytes).map_err(|_| invalid())?,
            );
            let keys: Vec<_> = secrets.iter().map(|secret| secret.as_bytes()).collect();
            let verifier = Arc::new(
                super::super::composio::WebhookVerifier::new(&keys).map_err(|_| invalid())?,
            );
            api.runtime.ingestion = Some(PreparedIngestion {
                settings,
                inbox: inbox.ok_or_else(invalid)?,
                verifier,
            });
        }
        if let Some(webhook) = api.ingestion_webhook() {
            api = api.with_ingestion_webhook(webhook);
        }
        Ok(api)
    }

    fn new_inner(
        config: DigitalMateGatewayConfig,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
        inbox: Option<super::super::ingestion::Inbox>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let privacy = if let Some(settings) = config.privacy.clone() {
            use zeroize::Zeroizing;
            let path = std::env::var_os("ADX_CREDENTIAL_KEY_FILE").ok_or_else(invalid)?;
            let text = Zeroizing::new(std::fs::read_to_string(path).map_err(|_| invalid())?);
            let bytes = Zeroizing::new(hex::decode(text.trim()).map_err(|_| invalid())?);
            let key =
                Zeroizing::new(<[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| invalid())?);
            let store = auth.privacy_store().ok_or_else(invalid)?;
            let reader = Arc::new(super::super::privacy_center::ManagedBindingReader(
                managed.clone(),
            ));
            let owner = config
                .digitalmate
                .clone()
                .filter(|application| application.template == settings.template)
                .map(
                    |application| super::super::connector_owner::OwnerRuntimeConfig {
                        application,
                        auth: auth.clone(),
                    },
                );
            Some(
                super::super::privacy_runtime::PrivacyRuntime::start(
                    settings,
                    store,
                    reader,
                    &key,
                    &config.tenant,
                    inbox,
                    owner,
                )
                .map_err(|_| {
                    Error::Unavailable("DigitalMate privacy service initialization failed".into())
                })?,
            )
        } else {
            None
        };
        Ok(Self {
            auth,
            config,
            managed,
            composio: None,
            webhook: None,
            runtime: Runtime {
                prepared_composio: None,
                privacy,
                ingestion: None,
            },
        })
    }

    pub(super) fn ingestion_webhook(&self) -> Option<Arc<super::super::ingestion::WebhookHttp>> {
        self.runtime.ingestion.as_ref().map(|prepared| {
            Arc::new(super::super::ingestion::WebhookHttp::new(
                prepared.verifier.clone(),
                prepared.inbox.clone(),
            ))
        })
    }

    pub(crate) fn background_service(
        &self,
        ingress: Arc<Ingress>,
    ) -> Result<Option<super::super::ingestion::BackgroundService>, Error> {
        let Some(prepared) = &self.runtime.ingestion else {
            return Ok(None);
        };
        let identity = self
            .runtime
            .privacy
            .as_ref()
            .and_then(|privacy| privacy.delivery.clone())
            .ok_or_else(invalid)?;
        let delivery = super::super::ingestion::RelayDelivery::new(
            self.managed.clone(),
            ingress,
            identity,
            Duration::from_secs(prepared.settings.delivery_timeout_seconds),
            self.config
                .digitalmate
                .as_ref()
                .ok_or_else(invalid)?
                .service_port,
        )
        .map_err(|_| invalid())?;
        let runtime = super::super::ingestion::IngestionRuntime::start(
            prepared.inbox.clone(),
            prepared.verifier.clone(),
            Arc::new(delivery),
            prepared.settings.workers,
            Duration::from_secs(prepared.settings.lease_seconds),
        )
        .map_err(|_| invalid())?;
        Ok(Some(super::super::ingestion::BackgroundService::new(
            runtime,
        )))
    }
}

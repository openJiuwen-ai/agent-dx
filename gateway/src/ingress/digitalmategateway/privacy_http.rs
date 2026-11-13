//! HTTPS-only transport for DigitalMate privacy operations.

use super::privacy_center::PrivacyCenter;
use super::privacy_cipher::LegacyEnvelope;
use crate::ingress::accounts::{self, Error, ExpectedRevision};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use http::{header, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Body, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

const MAX_REQUEST_BYTES: usize = 180_000;
const MAX_VALUE_BYTES: usize = 131_072;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetRequest {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StageRequest {
    transaction: String,
    name: String,
    value: Option<String>,
    delete: bool,
    revision: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionRequest {
    transaction: String,
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportRequest {
    envelope: LegacyEnvelope,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportRequest {
    revision: String,
}

pub(crate) struct PrivacyHttp {
    center: Arc<PrivacyCenter>,
    identity: Arc<super::privacy_grants::PrivacyIdentity>,
    ingestion: Option<Arc<super::privacy_grants::DeliveryIdentity>>,
}
impl PrivacyHttp {
    pub fn new(
        center: Arc<PrivacyCenter>,
        identity: Arc<super::privacy_grants::PrivacyIdentity>,
    ) -> Self {
        Self {
            center,
            identity,
            ingestion: None,
        }
    }

    pub(crate) fn with_ingestion(
        mut self,
        ingestion: Arc<super::privacy_grants::DeliveryIdentity>,
    ) -> Self {
        self.ingestion = Some(ingestion);
        self
    }

    pub async fn handle<B>(&self, request: Request<B>) -> Response<Full<Bytes>>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let operation = operation_label(request.uri().path());
        let started = Instant::now();
        let result = match tokio::time::timeout(Duration::from_secs(20), self.handle_inner(request))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => response(error.status(), serde_json::json!({"error": error.code()})),
            Err(_) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"error":"UNAVAILABLE"}),
            ),
        };
        tracing::info!(target: "digitalmate_privacy", operation, status = result.status().as_u16(),
            duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            "privacy request completed");
        result
    }

    async fn handle_inner<B>(&self, request: Request<B>) -> Result<Response<Full<Bytes>>, Error>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if request.method() != Method::POST || request.uri().query().is_some() {
            return Err(Error::Invalid);
        }
        if !request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
            })
        {
            return Err(Error::Invalid);
        }
        let token = Zeroizing::new(accounts::bearer(request.headers())?.to_owned());
        let path = request.uri().path().to_owned();
        if !matches!(
            path.as_str(),
            "/v1/secret/get"
                | "/v1/identity/renew"
                | "/v1/connector/identity"
                | "/v1/connector/renew"
                | "/v1/ingestion/admit"
                | "/v1/ingestion/bootstrap"
                | "/v1/secret/stage"
                | "/v1/secret/commit"
                | "/v1/secret/abort"
                | "/v1/secret/status"
                | "/v1/secret/import-legacy"
                | "/v1/secret/export-legacy"
        ) {
            return Ok(response(
                StatusCode::NOT_FOUND,
                serde_json::json!({"error":"NOT_FOUND"}),
            ));
        }
        let bytes = Limited::new(request.into_body(), MAX_REQUEST_BYTES)
            .collect()
            .await
            .map_err(|_| Error::Invalid)?
            .to_bytes();
        match path.as_str() {
            "/v1/ingestion/admit" | "/v1/ingestion/bootstrap" => {
                if bytes.as_ref() != b"{}" {
                    return Err(Error::Invalid);
                }
                let ingestion = self.ingestion.as_ref().ok_or(Error::Unavailable)?;
                let admission = ingestion.verify(&token).await?;
                if path == "/v1/ingestion/admit" {
                    return Ok(response(StatusCode::OK, admission.json()));
                }
                // Delivery grants staging only. Runtime must first publish a
                // purpose-limited bootstrap contract that exposes no secrets.
                // Never decrypt the user's configuration with this bearer.
                Err(Error::Unavailable)
            }
            "/v1/connector/identity" | "/v1/connector/renew" => {
                if bytes.as_ref() != b"{}" {
                    return Err(Error::Invalid);
                }
                let value = if path == "/v1/connector/identity" {
                    self.identity.connector_identity(&token).await?
                } else {
                    self.identity.renew_connector(&token).await?
                };
                Ok(response(StatusCode::OK, value))
            }
            "/v1/identity/renew" => {
                if bytes.as_ref() != b"{}" {
                    return Err(Error::Invalid);
                }
                Ok(response(StatusCode::OK, self.identity.renew(&token).await?))
            }
            "/v1/secret/get" => {
                let input: GetRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                let result = self.center.get(&token, &input.name).await?;
                let body = match result {
                    Some(secret) => {
                        serde_json::json!({"revision":secret.revision,"value":secret.plaintext.as_ref().map(|value| STANDARD.encode(value.as_slice()))})
                    }
                    None => serde_json::json!({"revision":null,"value":null}),
                };
                Ok(response(StatusCode::OK, body))
            }
            "/v1/secret/stage" => {
                let mut input: StageRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                if input.delete == input.value.is_some() {
                    return Err(Error::Invalid);
                }
                let decoded = input
                    .value
                    .as_ref()
                    .map(|value| {
                        STANDARD
                            .decode(value.as_bytes())
                            .map(Zeroizing::new)
                            .map_err(|_| Error::Invalid)
                    })
                    .transpose()?;
                use zeroize::Zeroize;
                if let Some(value) = &mut input.value {
                    value.zeroize();
                }
                if decoded
                    .as_ref()
                    .is_some_and(|value| value.len() > MAX_VALUE_BYTES)
                {
                    return Err(Error::Invalid);
                }
                let expected = match &input.revision {
                    Some(revision) => ExpectedRevision::Existing(revision),
                    None => ExpectedRevision::New,
                };
                let revision = self
                    .center
                    .stage(
                        &token,
                        &input.transaction,
                        &input.name,
                        decoded.as_ref().map(|value| value.as_slice()),
                        expected,
                    )
                    .await?;
                Ok(response(
                    StatusCode::OK,
                    serde_json::json!({"revision":revision}),
                ))
            }
            "/v1/secret/commit" => {
                let input: TransactionRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                let revision = self
                    .center
                    .promote(&token, &input.transaction, &input.name)
                    .await?;
                Ok(response(
                    StatusCode::OK,
                    serde_json::json!({"revision":revision}),
                ))
            }
            "/v1/secret/abort" => {
                let input: TransactionRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                self.center
                    .abort(&token, &input.transaction, &input.name)
                    .await?;
                Ok(response(StatusCode::OK, serde_json::json!({"ok":true})))
            }
            "/v1/secret/status" => {
                let input: TransactionRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                let status = self
                    .center
                    .transaction_status(&token, &input.transaction, &input.name)
                    .await?;
                Ok(response(
                    StatusCode::OK,
                    match status {
                        Some((state, revision)) => {
                            serde_json::json!({"state":state,"revision":revision})
                        }
                        None => serde_json::json!({"state":null,"revision":null}),
                    },
                ))
            }
            "/v1/secret/import-legacy" => {
                let input: ImportRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                let revision = self.center.import_legacy(&token, &input.envelope).await?;
                Ok(response(
                    StatusCode::OK,
                    serde_json::json!({"revision":revision}),
                ))
            }
            "/v1/secret/export-legacy" => {
                let input: ExportRequest =
                    serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
                let envelope = self.center.export_legacy(&token, &input.revision).await?;
                Ok(response(
                    StatusCode::OK,
                    serde_json::json!({"revision":input.revision,"envelope":envelope}),
                ))
            }
            _ => unreachable!(),
        }
    }

    /// The request handler is reachable only after a successful TLS handshake.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        tls: TlsAcceptor,
        mut shutdown: watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let permits = Arc::new(Semaphore::new(64));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let tls = tls.clone();
                    let service = self.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(5), tls.accept(socket)).await else { return; };
                        let handler = service_fn(move |request| {
                            let service = service.clone();
                            async move { Ok::<_, Infallible>(service.handle(request).await) }
                        });
                        let _ = tokio::time::timeout(Duration::from_secs(30), http1::Builder::new().keep_alive(false).serve_connection(TokioIo::new(stream), handler)).await;
                    });
                }
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

fn operation_label(path: &str) -> &'static str {
    match path {
        "/v1/identity/renew" => "renew",
        "/v1/connector/identity" => "connector_identity",
        "/v1/connector/renew" => "connector_renew",
        "/v1/ingestion/admit" => "ingestion_admit",
        "/v1/ingestion/bootstrap" => "ingestion_bootstrap",
        "/v1/secret/get" => "get",
        "/v1/secret/stage" => "stage",
        "/v1/secret/commit" => "commit",
        "/v1/secret/abort" => "abort",
        "/v1/secret/status" => "status",
        "/v1/secret/import-legacy" => "import_legacy",
        "/v1/secret/export-legacy" => "export_legacy",
        _ => "other",
    }
}

fn response(status: StatusCode, body: serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("static response headers are valid")
}

#[cfg(test)]
mod tests {
    use super::operation_label;

    #[test]
    fn telemetry_labels_are_bounded_and_never_include_user_input() {
        assert_eq!(operation_label("/v1/secret/get"), "get");
        assert_eq!(operation_label("/v1/secret/stage"), "stage");
        assert_eq!(operation_label("/private-token-in-path"), "other");
    }
}

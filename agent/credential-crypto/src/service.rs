//! Internal HTTPS API. ADX resolves authorization from existing Binding metadata for each operation.
use crate::{
    access::{AccessClient, AccessConfig},
    storage::{read_private, StorageError},
    Context, CryptoError, Envelope, Keyring, MAX_PLAINTEXT,
};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

pub struct ServiceState {
    pub keys: Keyring,
    pub authorizer: AccessClient,
}

struct ApiError(StatusCode, &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"code": self.1}))).into_response()
    }
}
impl From<CryptoError> for ApiError {
    fn from(error: CryptoError) -> Self {
        match error {
            CryptoError::InvalidInput => Self(StatusCode::BAD_REQUEST, "INVALID_REQUEST"),
            CryptoError::InvalidEnvelope => Self(StatusCode::BAD_REQUEST, "INVALID_ENVELOPE"),
            CryptoError::Unavailable => Self(StatusCode::SERVICE_UNAVAILABLE, "CRYPTO_UNAVAILABLE"),
        }
    }
}

async fn authorize(
    state: &ServiceState,
    headers: &HeaderMap,
    context: &Context,
) -> Result<(), ApiError> {
    context
        .validate()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"))?;
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "UNAUTHORIZED"))?;
    if !state
        .authorizer
        .allows(token, context)
        .await
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "AUTHORITY_UNAVAILABLE"))?
    {
        return Err(ApiError(StatusCode::FORBIDDEN, "FORBIDDEN"));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SealRequest {
    context: Context,
    plaintext: String,
}
#[derive(Serialize)]
struct SealResponse {
    envelope: Envelope,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenRequest {
    context: Context,
    envelope: Envelope,
}
#[derive(Serialize)]
struct OpenResponse {
    plaintext: String,
}

async fn seal(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(request): Json<SealRequest>,
) -> Result<Json<SealResponse>, ApiError> {
    authorize(&state, &headers, &request.context).await?;
    tokio::task::spawn_blocking(move || {
        let encoded = Zeroizing::new(request.plaintext);
        if encoded.len() > MAX_PLAINTEXT.div_ceil(3) * 4 {
            return Err(ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"));
        }
        let plaintext = Zeroizing::new(
            STANDARD
                .decode(encoded.as_bytes())
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"))?,
        );
        Ok(Json(SealResponse {
            envelope: state.keys.seal(&request.context, &plaintext)?,
        }))
    })
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "CRYPTO_UNAVAILABLE"))?
}
async fn open(
    State(state): State<Arc<ServiceState>>,
    headers: HeaderMap,
    Json(request): Json<OpenRequest>,
) -> Result<Json<OpenResponse>, ApiError> {
    authorize(&state, &headers, &request.context).await?;
    tokio::task::spawn_blocking(move || {
        let plaintext = state.keys.open(&request.context, &request.envelope)?;
        Ok(Json(OpenResponse {
            plaintext: STANDARD.encode(plaintext.as_slice()),
        }))
    })
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "CRYPTO_UNAVAILABLE"))?
}

pub fn router(state: ServiceState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/v1/seal", post(seal))
        .route("/v1/open", post(open))
        .layer(DefaultBodyLimit::max(524_288))
        .with_state(Arc::new(state))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub tls_certificate_file: PathBuf,
    pub tls_private_key_file: PathBuf,
    pub keyring_file: PathBuf,
    pub authorization: AccessConfig,
}

/// Only serves TLS; no plaintext listener or client-accessible administrative endpoint exists.
pub async fn serve(config: ServerConfig) -> Result<(), StorageError> {
    let acceptor = adx_transport::tls::http_server_acceptor_pem(
        &read_private(&config.tls_certificate_file, 65_536)?,
        &read_private(&config.tls_private_key_file, 65_536)?,
        None,
        vec![b"http/1.1".to_vec()],
    )
    .map_err(|_| StorageError::Invalid)?;
    let app = router(ServiceState {
        keys: crate::storage::KeyFile::load(&config.keyring_file)?,
        authorizer: AccessClient::new(&config.authorization)?,
    });
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|_| StorageError::File)?;
    let slots = Arc::new(Semaphore::new(128));
    loop {
        let (socket, _) = tokio::select! {
            result = listener.accept() => result.map_err(|_| StorageError::File)?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            continue;
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Ok(Ok(tls)) =
                tokio::time::timeout(Duration::from_secs(10), acceptor.accept(socket)).await
            {
                let io = hyper_util::rt::TokioIo::new(tls);
                let service = hyper_util::service::TowerToHyperService::new(app);
                let mut builder = hyper::server::conn::http1::Builder::new();
                builder.keep_alive(false).max_buf_size(524_288);
                // A bounded connection lifetime also bounds slow or stalled request bodies.
                let _ = tokio::time::timeout(
                    Duration::from_secs(30),
                    builder.serve_connection(io, service),
                )
                .await;
            }
        });
    }
}

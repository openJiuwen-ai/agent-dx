//! Jiuwen attachment response adaptation. Public identity/routing is owned by the entrypoint.
use super::{
    download_config::DownloadConfig,
    download_runtime::{AdmissionError, ReadError, Reader},
    download_token::{CheckedDownload, TokenError, MAX_TOKEN_BYTES},
};
use crate::ingress::server::{Ingress, ProxyBody};
use adx_agent_api::request::RequestContext;
use base64::{
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    Engine,
};
use bytes::Bytes;
use futures_util::stream;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::sync::Arc;

const CHUNK_BYTES: u64 = 64 * 1024;
const FILENAME_ESCAPES: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Internally assembled download: tenant, Sandbox and config come from trusted
/// binding resolution; the token remains untrusted and is verified here. The
/// entrypoint authenticates the user. Deliberately no Debug/Serialize support.
/// One context bounds admission, per-chunk authorization and the whole transfer.
pub struct Download {
    pub ingress: Arc<Ingress>,
    pub context: Arc<RequestContext>,
    pub tenant: String,
    pub sandbox_id: String,
    pub config: DownloadConfig,
    pub token: String,
    pub expected_session: Option<String>,
}
impl Download {
    fn reader(&self) -> Result<Reader, ReadError> {
        Reader::new(&self.ingress, &self.context, &self.tenant, &self.sandbox_id)
    }

    /// Build GET/HEAD responses with the existing ordinary/verified distinction.
    /// Before headers, failures become bounded {error,code} responses; after headers,
    /// failures abort the body and never produce an apparently successful truncated
    /// file. No retry, background prefetch, E2A request or public credentials forwarding.
    pub async fn respond(
        self,
        method: Method,
        headers: &HeaderMap,
        inline: bool,
    ) -> Response<ProxyBody> {
        let head = method == Method::HEAD;
        let mut response = match self.prepare(&method, headers, inline).await {
            Ok(response) => response,
            Err(error) => error.response(),
        };
        if head {
            *response.body_mut() = body(Bytes::new());
        }
        response
    }

    async fn prepare(
        mut self,
        method: &Method,
        headers: &HeaderMap,
        inline: bool,
    ) -> Result<Response<ProxyBody>, HttpError> {
        if method != Method::GET && method != Method::HEAD {
            return Err(HttpError::Method);
        }
        self.token = self.token.trim().to_owned();
        token_shape(&self.token).map_err(AdmissionError::from)?;
        let reader = self.reader().map_err(AdmissionError::from)?;
        let file = reader
            .authorize(&self.config, &self.token, self.expected_session.as_deref())
            .await?;
        let plan = ResponsePlan::new(&file, method, headers, inline)?;
        if plan.empty_body {
            return Ok(plan.response(body(Bytes::new())));
        }
        let first_length = plan.length.min(CHUNK_BYTES);
        let first = self.chunk(&reader, &file, plan.start, first_length).await?;
        let transfer = Transfer {
            download: self,
            reader,
            file,
            first: Some(first),
            next: plan.start + first_length,
            end: plan.start + plan.length,
        };
        let frames = stream::try_unfold(transfer, |mut state| async move {
            let bytes = if let Some(first) = state.first.take() {
                // Headers can be ready long before the consumer polls the body.
                // Recheck buffered bytes against current authorization/deadline.
                state
                    .download
                    .admit_chunk(&state.reader, &state.file)
                    .await?;
                first
            } else {
                if state.next == state.end {
                    return Ok(None);
                }
                let length = (state.end - state.next).min(CHUNK_BYTES);
                let bytes = state
                    .download
                    .chunk(&state.reader, &state.file, state.next, length)
                    .await?;
                state.next += length;
                bytes
            };
            Ok::<_, HttpError>(Some((Frame::data(bytes), state)))
        });
        let body = BodyExt::map_err(
            StreamBody::new(frames),
            |error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) },
        )
        .boxed_unsync();
        Ok(plan.response(body))
    }

    async fn chunk(
        &self,
        reader: &Reader,
        baseline: &CheckedDownload,
        start: u64,
        length: u64,
    ) -> Result<Bytes, HttpError> {
        let current = self.admit_chunk(reader, baseline).await?;
        Ok(reader.read_range(&current, start, length).await?)
    }

    async fn admit_chunk(
        &self,
        reader: &Reader,
        baseline: &CheckedDownload,
    ) -> Result<CheckedDownload, HttpError> {
        let current = reader
            .authorize(&self.config, &self.token, self.expected_session.as_deref())
            .await?;
        if current.path() != baseline.path()
            || current.name() != baseline.name()
            || current.size() != baseline.size()
        {
            return Err(HttpError::Admission(AdmissionError::Token(
                TokenError::FileMismatch,
            )));
        }
        Ok(current)
    }
}
struct Transfer {
    download: Download,
    reader: Reader,
    file: CheckedDownload,
    first: Option<Bytes>,
    next: u64,
    end: u64,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum HttpError {
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error("unsupported download method")]
    Method,
    #[error("invalid or unsatisfiable byte range")]
    Range(u64),
    #[error("invalid download response headers")]
    Headers,
}
impl HttpError {
    fn response(&self) -> Response<ProxyBody> {
        if let Self::Range(size) = self {
            let mut response = Response::new(body(Bytes::new()));
            *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            let headers = response.headers_mut();
            // Formatting a u64 and ASCII literals always produces a legal header.
            if let Ok(value) = HeaderValue::from_str(&format!("bytes */{size}")) {
                headers.insert("content-range", value);
            }
            headers.insert("content-length", HeaderValue::from_static("0"));
            headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
            headers.insert("cache-control", HeaderValue::from_static("no-store"));
            return response;
        }
        let (status, code) = match self {
            Self::Method => (StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED"),
            Self::Admission(AdmissionError::Token(TokenError::InvalidSecret)) => {
                (StatusCode::BAD_GATEWAY, "INTERNAL_ERROR")
            }
            Self::Admission(AdmissionError::Token(TokenError::TooLarge)) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE")
            }
            Self::Admission(AdmissionError::Token(TokenError::InvalidToken(_))) => {
                (StatusCode::BAD_REQUEST, "BAD_REQUEST")
            }
            Self::Admission(AdmissionError::Token(_)) => (StatusCode::FORBIDDEN, "FORBIDDEN"),
            Self::Admission(AdmissionError::Read(ReadError::NotFound)) => {
                (StatusCode::NOT_FOUND, "NOT_FOUND")
            }
            Self::Admission(AdmissionError::Read(ReadError::Identity)) => {
                (StatusCode::FORBIDDEN, "FORBIDDEN")
            }
            Self::Admission(AdmissionError::Read(ReadError::TooLarge)) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE")
            }
            _ => (StatusCode::BAD_GATEWAY, "INTERNAL_ERROR"),
        };
        let bytes =
            Bytes::from(serde_json::json!({"error":self.to_string(),"code":code}).to_string());
        let mut response = Response::new(body(bytes.clone()));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert("cache-control", HeaderValue::from_static("no-store"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        if let Ok(value) = HeaderValue::from_str(&bytes.len().to_string()) {
            headers.insert("content-length", value);
        }
        if status == StatusCode::METHOD_NOT_ALLOWED {
            headers.insert("allow", HeaderValue::from_static("GET, HEAD"));
        }
        response
    }
}
fn body(bytes: Bytes) -> ProxyBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}

#[derive(Debug)]
pub(super) struct ResponsePlan {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub start: u64,
    pub length: u64,
    pub empty_body: bool,
}
impl ResponsePlan {
    pub fn new(
        file: &CheckedDownload,
        method: &Method,
        input: &HeaderMap,
        inline: bool,
    ) -> Result<Self, HttpError> {
        if method != Method::GET && method != Method::HEAD {
            return Err(HttpError::Method);
        }
        // The original ordinary branch ignores Range/inline; only verified supports them.
        let selected = if file.verified() {
            let mut values = input.get_all(http::header::RANGE).iter();
            let first = values.next();
            if values.next().is_some() {
                return Err(HttpError::Range(file.size()));
            }
            match first {
                Some(value) => {
                    let value = value
                        .to_str()
                        .map_err(|_| HttpError::Range(file.size()))?
                        .trim();
                    if value.is_empty() {
                        None
                    } else {
                        Some(byte_range(value, file.size()).ok_or(HttpError::Range(file.size()))?)
                    }
                }
                None => None,
            }
        } else {
            None
        };
        let (start, length) = selected
            .map(|(start, end)| (start, end - start + 1))
            .unwrap_or((0, file.size()));
        let mut headers = HeaderMap::new();
        let mut insert = |key: &'static str, value: String| -> Result<(), HttpError> {
            headers.insert(
                key,
                HeaderValue::from_str(&value).map_err(|_| HttpError::Headers)?,
            );
            Ok(())
        };
        insert("content-length", length.to_string())?;
        insert("cache-control", "no-store".into())?;
        let mime = mime_guess::from_path(file.name())
            .first_or_octet_stream()
            .to_string();
        insert(
            "content-type",
            if mime.starts_with("text/") {
                format!("{mime}; charset=utf-8")
            } else {
                mime
            },
        )?;
        let disposition = if file.verified() && inline {
            "inline"
        } else {
            "attachment"
        };
        insert(
            "content-disposition",
            format!(
                "{disposition}; filename*=UTF-8''{}",
                utf8_percent_encode(file.name(), FILENAME_ESCAPES)
            ),
        )?;
        if file.verified() {
            insert("accept-ranges", "bytes".into())?;
        }
        if let Some((start, end)) = selected {
            insert(
                "content-range",
                format!("bytes {start}-{end}/{}", file.size()),
            )?;
        }
        Ok(Self {
            status: if selected.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            },
            headers,
            start,
            length,
            empty_body: *method == Method::HEAD || length == 0,
        })
    }
    fn response(self, body: ProxyBody) -> Response<ProxyBody> {
        let mut response = Response::new(body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }
}
fn byte_range(header: &str, size: u64) -> Option<(u64, u64)> {
    if size == 0 {
        return None;
    }
    let (start, end) = header.strip_prefix("bytes=")?.split_once('-')?;
    let decimal = |value: &str| {
        if value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        // Large suffix/end values clamp to the file; large starts are unsatisfiable.
        Some(value.bytes().fold(0u64, |value, digit| {
            value
                .saturating_mul(10)
                .saturating_add(u64::from(digit - b'0'))
        }))
    };
    if start.is_empty() {
        let suffix = decimal(end)?;
        return (suffix > 0).then_some((size.saturating_sub(suffix), size - 1));
    }
    let start = decimal(start)?;
    let end = if end.is_empty() {
        size - 1
    } else {
        decimal(end)?.min(size - 1)
    };
    (start < size && end >= start).then_some((start, end))
}

// Preserve the old HTTP parser's 400 for malformed payloads. These unverified
// fields are discarded and never used as paths, destinations or authorization.
// Reader::authorize still verifies the original encoded text before using claims.
fn token_shape(token: &str) -> Result<(), TokenError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(TokenError::TooLarge);
    }
    let encoded = token.split('.').next().unwrap_or_default();
    let bytes = URL_SAFE
        .decode(encoded)
        .or_else(|_| URL_SAFE_NO_PAD.decode(encoded))
        .map_err(|_| TokenError::InvalidToken("encoding"))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| TokenError::InvalidToken("JSON"))?;
    let object = value
        .as_object()
        .ok_or(TokenError::InvalidToken("object required"))?;
    if object
        .get("sid")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(TokenError::InvalidToken("session required"));
    }
    if object.get("kind").and_then(serde_json::Value::as_str) != Some("verified_asset_v1")
        && object
            .get("path")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|path| path.trim().is_empty())
    {
        return Err(TokenError::InvalidToken("path required"));
    }
    Ok(())
}

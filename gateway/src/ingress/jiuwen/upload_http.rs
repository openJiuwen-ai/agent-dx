//! Authenticated Jiuwen multipart uploads into the selected Sandbox workspace.
use crate::ingress::sandbox_files::{Files, ReadError};
use crate::ingress::server::{Ingress, ProxyBody};
use adx_agent_api::request::RequestContext;
use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use std::sync::Arc;

pub const MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_FILES: usize = 20;
const MAX_PARTS: usize = 32;
const MAX_PART_HEADERS: usize = 8192;
const MAX_FIELD_BYTES: usize = 4096;

pub struct Upload {
    pub ingress: Arc<Ingress>,
    pub context: Arc<RequestContext>,
    pub tenant: String,
    pub sandbox_id: String,
    pub workspace: String,
    pub user_id: String,
    pub agent_type: String,
}

impl Upload {
    pub async fn respond(self, body: Incoming, content_type: &str) -> Response<ProxyBody> {
        let Some(boundary) = boundary(content_type) else {
            return error(StatusCode::BAD_REQUEST, "INVALID_MULTIPART");
        };
        let bytes = match tokio::time::timeout_at(
            self.context.deadline(),
            Limited::new(body, MAX_UPLOAD_BYTES).collect(),
        )
        .await
        {
            Ok(Ok(body)) => body.to_bytes(),
            Ok(Err(body_error)) if body_error.is::<http_body_util::LengthLimitError>() => {
                return error(StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE");
            }
            _ => return error(StatusCode::BAD_REQUEST, "INVALID_MULTIPART"),
        };
        let form = match parse_form(bytes, &boundary) {
            Ok(form) => form,
            Err(FormError::MissingFile) => return error(StatusCode::BAD_REQUEST, "MISSING_FILE"),
            Err(FormError::TooManyFiles) => {
                return error(StatusCode::BAD_REQUEST, "TOO_MANY_FILES")
            }
            Err(FormError::Invalid) => return error(StatusCode::BAD_REQUEST, "INVALID_MULTIPART"),
        };
        if form
            .user_id
            .as_deref()
            .is_some_and(|id| !id.is_empty() && id != self.user_id)
        {
            return error(StatusCode::FORBIDDEN, "USER_MISMATCH");
        }
        if form
            .agent_type
            .as_deref()
            .is_some_and(|kind| !kind.is_empty() && kind != self.agent_type)
        {
            return error(StatusCode::BAD_REQUEST, "AGENT_TYPE_MISMATCH");
        }
        let directory = match relative_directory(form.directory.as_deref().unwrap_or("")) {
            Some(value) => value,
            None => return error(StatusCode::BAD_REQUEST, "INVALID_PATH"),
        };
        let reader = match Files::new(
            &self.ingress,
            self.context.deadline(),
            &self.tenant,
            &self.sandbox_id,
        ) {
            Ok(value) => value,
            Err(_) => return error(StatusCode::FORBIDDEN, "FORBIDDEN"),
        };
        let mut files = Vec::with_capacity(form.files.len());
        let mut errors = Vec::new();
        for file in form.files {
            let Some(filename) = safe_filename(&file.filename) else {
                errors.push(serde_json::json!({"filename":file.filename,"error":"INVALID_PATH"}));
                continue;
            };
            let relative = if directory.is_empty() {
                filename.clone()
            } else {
                format!("{directory}/{filename}")
            };
            let path = format!("{}/{}", self.workspace.trim_end_matches('/'), relative);
            let size = file.data.len();
            match reader.write_file(&path, file.data).await {
                Ok(written) if written == size => files.push(serde_json::json!({
                    "filename":filename,
                    "path":path,
                    "mime_type":mime_guess::from_path(&filename).first_raw().unwrap_or("application/octet-stream"),
                    "size_bytes":written,
                })),
                Err(ReadError::NotFound | ReadError::Revoked) => {
                    errors.push(serde_json::json!({"filename":filename,"error":"NOT_FOUND"}));
                }
                _ => errors.push(serde_json::json!({"filename":filename,"error":"UPLOAD_FAILED"})),
            }
        }
        json_response(
            StatusCode::OK,
            serde_json::json!({"ok":true,"files":files,"errors":errors}),
        )
    }
}

struct UploadedFile {
    filename: String,
    data: Bytes,
}
#[derive(Default)]
struct UploadForm {
    files: Vec<UploadedFile>,
    directory: Option<String>,
    user_id: Option<String>,
    agent_type: Option<String>,
}
enum FormError {
    Invalid,
    MissingFile,
    TooManyFiles,
}

fn boundary(content_type: &str) -> Option<String> {
    let mut parts = content_type.split(';');
    if !parts
        .next()?
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return None;
    }
    let value = parts.find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        key.trim()
            .eq_ignore_ascii_case("boundary")
            .then_some(value.trim())
    })?;
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    if value.is_empty()
        || value.len() > 70
        || !value
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'"' && b != b';')
    {
        return None;
    }
    Some(value.to_owned())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_form(body: Bytes, boundary: &str) -> Result<UploadForm, FormError> {
    let opening = format!("--{boundary}\r\n");
    let delimiter = format!("\r\n--{boundary}");
    if !body.starts_with(opening.as_bytes()) {
        return Err(FormError::Invalid);
    }
    let mut cursor = opening.len();
    let mut form = UploadForm::default();
    for _ in 0..MAX_PARTS {
        let header_end = find_bytes(&body[cursor..], b"\r\n\r\n")
            .filter(|size| *size <= MAX_PART_HEADERS)
            .ok_or(FormError::Invalid)?
            + cursor;
        let header =
            std::str::from_utf8(&body[cursor..header_end]).map_err(|_| FormError::Invalid)?;
        let (name, filename) = disposition(header)?;
        let data_start = header_end + 4;
        let mut search = data_start;
        let (data_end, next, finished) = loop {
            let at = find_bytes(&body[search..], delimiter.as_bytes()).ok_or(FormError::Invalid)?
                + search;
            let suffix = at + delimiter.len();
            if body.get(suffix..suffix + 2) == Some(&b"--"[..]) {
                break (at, suffix + 2, true);
            }
            if body.get(suffix..suffix + 2) == Some(&b"\r\n"[..]) {
                break (at, suffix + 2, false);
            }
            search = at + 2;
        };
        let data = body.slice(data_start..data_end);
        if name == "file" {
            if form.files.len() == MAX_FILES {
                return Err(FormError::TooManyFiles);
            }
            form.files.push(UploadedFile {
                filename: filename.unwrap_or_else(|| "upload.bin".into()),
                data,
            });
        } else if matches!(
            name.as_str(),
            "dir" | "session_id" | "user_id" | "agent_type"
        ) {
            if data.len() > MAX_FIELD_BYTES {
                return Err(FormError::Invalid);
            }
            let value = std::str::from_utf8(&data)
                .map_err(|_| FormError::Invalid)?
                .trim()
                .to_owned();
            let target = match name.as_str() {
                "dir" => Some(&mut form.directory),
                "user_id" => Some(&mut form.user_id),
                "agent_type" => Some(&mut form.agent_type),
                _ => None,
            };
            if let Some(target) = target {
                if target.replace(value).is_some() {
                    return Err(FormError::Invalid);
                }
            }
        }
        cursor = next;
        if finished {
            if body.get(cursor..) != Some(&b"\r\n"[..]) && cursor != body.len() {
                return Err(FormError::Invalid);
            }
            return if form.files.is_empty() {
                Err(FormError::MissingFile)
            } else {
                Ok(form)
            };
        }
    }
    Err(FormError::Invalid)
}

fn disposition(headers: &str) -> Result<(String, Option<String>), FormError> {
    let value = headers
        .split("\r\n")
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case("content-disposition")
                .then_some(value.trim())
        })
        .ok_or(FormError::Invalid)?;
    let mut parts = value.split(';');
    if !parts
        .next()
        .is_some_and(|part| part.trim().eq_ignore_ascii_case("form-data"))
    {
        return Err(FormError::Invalid);
    }
    let mut name = None;
    let mut filename = None;
    for part in parts {
        let Some((key, value)) = part.trim().split_once('=') else {
            continue;
        };
        let value = value
            .trim()
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value.trim());
        match key.trim().to_ascii_lowercase().as_str() {
            "name" => name = Some(value.to_owned()),
            "filename" => filename = Some(value.to_owned()),
            _ => {}
        }
    }
    Ok((name.ok_or(FormError::Invalid)?, filename))
}

fn relative_directory(value: &str) -> Option<String> {
    if value.len() > 1024
        || value.starts_with('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        return None;
    }
    if value.is_empty() {
        return Some(String::new());
    }
    if value
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == ".." || part.len() > 180)
    {
        return None;
    }
    Some(value.to_owned())
}

fn safe_filename(value: &str) -> Option<String> {
    let name = value
        .rsplit(['/', '\\'])
        .next()?
        .trim()
        .trim_end_matches(['.', ' ']);
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.len() > 180
        || name.chars().any(|ch| {
            ch.is_control()
                || matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*')
                || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
    {
        return None;
    }
    let extension = name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase());
    if extension.as_deref().is_some_and(|ext| {
        matches!(
            ext,
            "exe"
                | "dll"
                | "msi"
                | "scr"
                | "bat"
                | "cmd"
                | "ps1"
                | "vbs"
                | "wsf"
                | "hta"
                | "jar"
                | "lnk"
                | "bin"
                | "so"
                | "dylib"
                | "app"
                | "dmg"
                | "pkg"
                | "command"
                | "scpt"
                | "scptd"
                | "workflow"
                | "xpc"
                | "bundle"
                | "framework"
                | "kext"
                | "prefpane"
                | "saver"
                | "component"
        )
    }) {
        return None;
    }
    Some(name.to_owned())
}

fn full(bytes: Bytes) -> ProxyBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}
fn json_response(status: StatusCode, value: serde_json::Value) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(full(Bytes::from(value.to_string())))
        .unwrap_or_else(|_| Response::new(full(Bytes::new())))
}
fn error(status: StatusCode, code: &str) -> Response<ProxyBody> {
    json_response(status, serde_json::json!({"error":code,"code":code}))
}

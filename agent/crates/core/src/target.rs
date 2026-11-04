//! Public target identities shared by HTTP/WS, SSH and the CLI.
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Instance(String),
    Template {
        name: String,
        template_ref: crate::TemplateRef,
    },
}
impl FromStr for Target {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = value.split(':').collect();
        match parts.as_slice() {
            ["urn", "adx", "instance", id] => Ok(Self::Instance(decode(id)?)),
            ["urn", "adx", "template", name, version] => Ok(Self::Template {
                name: decode(name)?,
                template_ref: decode(version)?.parse()?,
            }),
            _ => Err("target must be an ADX instance or template URN".into()),
        }
    }
}
impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instance(id) => write!(f, "urn:adx:instance:{}", encode(id)),
            Self::Template { name, template_ref } => {
                write!(
                    f,
                    "urn:adx:template:{}:{}",
                    encode(name),
                    encode(template_ref.name())
                )
            }
        }
    }
}
impl From<&crate::Scope> for Target {
    fn from(scope: &crate::Scope) -> Self {
        Self::Template {
            name: scope.template.clone(),
            template_ref: scope.template_ref.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshRoute {
    pub target: Target,
    pub binding_id: Option<String>,
    pub port: Option<u16>,
    pub trace: Option<String>,
}
impl FromStr for SshRoute {
    type Err = String;
    fn from_str(username: &str) -> Result<Self, Self::Err> {
        if username.len() > 4096 {
            return Err("SSH route username is too long".into());
        }
        let parts: Vec<_> = username.split(':').collect();
        let (target, options) = match parts.as_slice() {
            ["yr", "instance", id, options @ ..] if username.len() <= 1024 => {
                (Target::Instance(legacy_field(id)?), options)
            }
            ["adx", "target", target, options @ ..] => (decode_bytes(target)?.parse()?, options),
            _ => return Err("SSH username must select instance or target".into()),
        };
        let mut result = Self {
            target,
            binding_id: None,
            port: None,
            trace: None,
        };
        for option in options {
            if let Some(value) = option.strip_prefix("binding_id=") {
                if result.binding_id.is_some() || matches!(result.target, Target::Instance(_)) {
                    return Err("binding_id requires a template target and must appear once".into());
                }
                result.binding_id = Some(decode_binding_id(value)?);
                continue;
            }
            let option = legacy_field(option)?;
            match option.split_once('=') {
                Some(("port", port)) if result.port.is_none() => {
                    result.port = Some(
                        port.parse::<u16>()
                            .ok()
                            .filter(|p| *p > 0)
                            .ok_or("invalid SSH port")?,
                    );
                }
                Some(("trace", trace)) if result.trace.is_none() => {
                    crate::identifier(trace, "SSH trace")?;
                    result.trace = Some(trace.into());
                }
                _ => return Err("invalid or duplicate SSH route option".into()),
            }
        }
        Ok(result)
    }
}
impl fmt::Display for SshRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.target {
            Target::Instance(id) => write!(f, "yr:instance:{}", encode(id))?,
            target => write!(f, "adx:target:{}", encode(&target.to_string()))?,
        }
        if let Some(id) = &self.binding_id {
            write!(f, ":binding_id={}", encode(id))?;
        }
        if let Some(port) = self.port {
            write!(f, ":port={port}")?;
        }
        if let Some(trace) = &self.trace {
            write!(f, ":trace={}", encode(trace))?;
        }
        Ok(())
    }
}
/// HTTP/WS instance selector; independent of the template target URN.
pub const BINDING_ID_HEADER: &str = "x-adx-binding-id";

/// Encodes an ID for HTTP headers and SSH route options.
/// Returns an error for an empty, oversized or control-character-containing ID.
pub fn encode_binding_id(id: &str) -> Result<String, String> {
    crate::identifier(id, "binding_id")?;
    Ok(encode(id))
}

/// Decodes one percent-encoded UTF-8 ID, preserving literal '+' and '%' identities.
/// Rejects malformed escapes, invalid IDs and unescaped whitespace or commas.
pub fn decode_binding_id(value: &str) -> Result<String, String> {
    if value.len() > crate::limits::IDENTIFIER_BYTES * 3
        || !value.is_ascii()
        || value.bytes().any(|b| b.is_ascii_whitespace() || b == b',')
    {
        return Err("binding_id must be a single percent-encoded identifier".into());
    }
    let id = decode_bytes(value)?;
    crate::identifier(&id, "binding_id")?;
    Ok(id)
}

fn legacy_field(value: &str) -> Result<String, String> {
    let decoded = decode(value)?;
    if decoded.len() > 256 {
        return Err("SSH route field exceeds 256 bytes".into());
    }
    Ok(decoded)
}
fn encode(value: &str) -> String {
    // Form encoding is available through the shared URL dependency; URNs use %20, not '+'.
    url::form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}
fn decode(value: &str) -> Result<String, String> {
    let decoded = decode_bytes(value)?;
    crate::identifier(&decoded, "target segment")?;
    Ok(decoded)
}
fn decode_bytes(value: &str) -> Result<String, String> {
    let mut bytes = Vec::new();
    let mut iter = value.bytes();
    while let Some(byte) = iter.next() {
        if byte == b'%' {
            let hi = iter
                .next()
                .and_then(|b| (b as char).to_digit(16))
                .ok_or("invalid percent escape")?;
            let lo = iter
                .next()
                .and_then(|b| (b as char).to_digit(16))
                .ok_or("invalid percent escape")?;
            bytes.push(u8::try_from(hi * 16 + lo).map_err(|_| "invalid percent escape")?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).map_err(|_| "target must be valid UTF-8".into())
}

//! Shared HTTP/WS entrypoint selection; compatibility stays behind the selected target.
use adx_agent_api::Error;
use adx_agent_core::{target::Target, Protocol, Scope};
use http::{HeaderMap, Request};

#[derive(Debug)]
pub(super) struct AccessRequest {
    pub target: Target,
    pub protocol: Protocol,
    pub port: Option<u16>,
    pub backend_uri: http::Uri,
}
#[derive(Clone)]
pub(super) struct InlineForward {
    pub backend_uri: http::Uri,
}
#[derive(Debug, Clone)]
pub(super) struct EnvironmentNotice {
    id: http::HeaderValue,
    urn: http::HeaderValue,
}
impl EnvironmentNotice {
    pub fn new(scope: &Scope) -> Result<Self, Error> {
        let urn = Target::Environment {
            name: scope.template.clone(),
            version: scope.version.clone(),
            id: scope.environment_id.clone(),
        }
        .to_string();
        Ok(Self {
            id: scope.environment_id.parse().map_err(|_| {
                Error::Invalid("Environment ID is not a valid response header".into())
            })?,
            urn: urn
                .parse()
                .map_err(|_| Error::Invalid("invalid Environment URN".into()))?,
        })
    }
    pub fn apply(&self, headers: &mut HeaderMap) {
        headers.insert("x-adx-environment-id", self.id.clone());
        headers.insert("x-adx-environment-urn", self.urn.clone());
    }
}
impl AccessRequest {
    pub fn parse<B>(request: &Request<B>) -> Result<Option<Self>, Error> {
        let path = request.uri().path();
        let (protocol, tail) = if path == "/agent/http" || path.starts_with("/agent/http/") {
            (
                Protocol::Http,
                path.strip_prefix("/agent/http").unwrap_or(""),
            )
        } else if path == "/agent/ws" || path.starts_with("/agent/ws/") {
            (Protocol::Ws, path.strip_prefix("/agent/ws").unwrap_or(""))
        } else {
            return Ok(None);
        };
        let websocket = request
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        let mut instance = None;
        let mut target = None;
        let mut port = None;
        let mut seen = std::collections::BTreeSet::new();
        let pairs: Vec<_> =
            url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes()).collect();
        for (name, value) in &pairs {
            if matches!(name.as_ref(), "instance" | "target" | "port")
                && !seen.insert(name.as_ref())
            {
                return Err(Error::Invalid("duplicate target or port parameter".into()));
            }
            match name.as_ref() {
                "instance" => instance = Some(value.trim().to_owned()),
                "target" => target = Some(value.parse::<Target>().map_err(Error::Invalid)?),
                "port" if !value.trim().is_empty() => {
                    port = Some(
                        value
                            .trim()
                            .parse::<u16>()
                            .ok()
                            .filter(|p| *p > 0)
                            .ok_or_else(|| Error::Invalid("invalid port".into()))?,
                    )
                }
                _ => {}
            }
        }
        let target = match (instance, target) {
            (Some(id), None) => Target::Instance(id),
            (None, Some(target)) => target,
            _ => {
                return Err(Error::Invalid(
                    "specify exactly one of instance or target".into(),
                ))
            }
        };
        let inline = matches!(target, Target::Instance(_));
        if websocket != (protocol == Protocol::Ws)
            || protocol == Protocol::Ws && request.method() != http::Method::GET
        {
            return Err(Error::Invalid(
                "HTTP/WS route does not match the request".into(),
            ));
        }
        if let Target::Instance(id) = &target {
            adx_agent_core::identifier(id, "instance").map_err(Error::Invalid)?;
            if id.contains(['/', '?', '#', '%']) || matches!(id.as_str(), "." | "..") {
                return Err(Error::Invalid("invalid instance identifier".into()));
            }
        }
        let backend_uri = if inline && protocol == Protocol::Ws {
            if !tail.is_empty() {
                return Err(Error::Invalid("inline WS uses /agent/ws".into()));
            }
            // Only the public entrypoint was renamed. Preserve the legacy backend handshake path.
            match request.uri().query() {
                Some(query) => format!("/serverless/v1/ws?{query}"),
                None => "/serverless/v1/ws".into(),
            }
        } else {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            for (name, value) in pairs {
                if matches!(name.as_ref(), "instance" | "target" | "port")
                    || inline && matches!(name.as_ref(), "tenant_id" | "token")
                {
                    continue;
                }
                query.append_pair(&name, &value);
            }
            let query = query.finish();
            let tail = if tail.is_empty() { "/" } else { tail };
            if query.is_empty() {
                tail.to_owned()
            } else {
                format!("{tail}?{query}")
            }
        }
        .parse()
        .map_err(|_| Error::Invalid("invalid backend URI".into()))?;
        Ok(Some(Self {
            target,
            protocol,
            port,
            backend_uri,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_and_ws_entries_enforce_the_selected_protocol_for_all_targets() {
        for selector in [
            "instance=inline-id",
            "target=urn:adx:instance:inline-id",
            "target=urn:adx:environment:demo:1:env",
        ] {
            for (method, path, upgrade) in [
                ("GET", "/agent/http", true),
                ("GET", "/agent/ws", false),
                ("POST", "/agent/ws", true),
            ] {
                let mut builder = Request::builder()
                    .method(method)
                    .uri(format!("{path}?{selector}"));
                if upgrade {
                    builder = builder.header("upgrade", "websocket");
                }
                assert!(
                    matches!(
                        AccessRequest::parse(&builder.body(()).unwrap()),
                        Err(Error::Invalid(_))
                    ),
                    "{method} {path}?{selector}"
                );
            }
            let http = Request::builder()
                .uri(format!("/agent/http/chat?{selector}&q=1"))
                .body(())
                .unwrap();
            assert_eq!(
                AccessRequest::parse(&http).unwrap().unwrap().backend_uri,
                "/chat?q=1"
            );
            let ws = Request::builder()
                .uri(format!("/agent/ws?{selector}&q=1"))
                .header("upgrade", "WebSocket")
                .body(())
                .unwrap();
            let access = AccessRequest::parse(&ws).unwrap().unwrap();
            let expected = if matches!(access.target, Target::Instance(_)) {
                format!("/serverless/v1/ws?{selector}&q=1")
            } else {
                "/?q=1".into()
            };
            assert_eq!(access.backend_uri.to_string(), expected);
        }
    }
}

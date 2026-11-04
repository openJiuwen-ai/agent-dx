//! Agent HTTP/WS target selection and backend path adaptation.
use adx_agent_api::Error;
use adx_agent_core::{
    target::{decode_binding_id, encode_binding_id, Target, BINDING_ID_HEADER},
    Protocol, Scope,
};
use http::{HeaderMap, Request};

#[derive(Debug, Clone)]
pub(super) struct InstanceAccess(pub super::path::ParsedDirectPath);

#[derive(Debug)]
pub(super) struct AccessRequest {
    pub target: Target,
    pub binding_id: Option<String>,
    pub protocol: Protocol,
    pub port: Option<u16>,
    pub backend_uri: http::Uri,
}
#[derive(Debug, Clone)]
pub(super) struct BindingNotice {
    id: http::HeaderValue,
}
impl BindingNotice {
    pub fn new(scope: &Scope) -> Result<Self, Error> {
        Ok(Self {
            id: encode_binding_id(&scope.binding_id)
                .map_err(Error::Invalid)?
                .parse()
                .map_err(|_| {
                    Error::Invalid("AgentBinding ID is not a valid response header".into())
                })?,
        })
    }
    pub fn apply(&self, headers: &mut HeaderMap) {
        headers.insert(BINDING_ID_HEADER, self.id.clone());
        headers.remove("x-adx-binding-urn");
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
                "instance" => {
                    return Err(Error::Invalid(
                        "managed access requires a template target".into(),
                    ))
                }
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
        let target = target.ok_or_else(|| Error::Invalid("managed target required".into()))?;
        let mut values = request.headers().get_all(BINDING_ID_HEADER).iter();
        let binding_id = values
            .next()
            .map(|value| {
                let value = value
                    .to_str()
                    .map_err(|_| Error::Invalid("invalid Binding ID header".into()))?;
                decode_binding_id(value).map_err(Error::Invalid)
            })
            .transpose()?;
        if values.next().is_some() {
            return Err(Error::Invalid("duplicate Binding ID header".into()));
        }
        if matches!(target, Target::Instance(_)) && binding_id.is_some() {
            return Err(Error::Invalid(
                "Binding ID requires a template target".into(),
            ));
        }
        if matches!(target, Target::Instance(_)) && port.is_none() {
            return Err(Error::Invalid(
                "instance access requires an explicit port".into(),
            ));
        }
        if websocket != (protocol == Protocol::Ws)
            || protocol == Protocol::Ws && request.method() != http::Method::GET
        {
            return Err(Error::Invalid(
                "HTTP/WS route does not match the request".into(),
            ));
        }
        let backend_uri = {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            for (name, value) in pairs {
                if matches!(name.as_ref(), "instance" | "target" | "port") {
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
            binding_id,
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
    fn binding_header_rejects_ambiguous_or_invalid_identity() {
        for values in [
            vec!["one", "two"],
            vec![""],
            vec!["%GG"],
            vec!["%0A"],
            vec!["%FF"],
            vec!["one,two"],
            vec!["%20"],
        ] {
            let mut builder =
                Request::builder().uri("/agent/http/chat?target=urn:adx:template:demo:1");
            for value in &values {
                builder = builder.header("x-adx-binding-id", *value);
            }
            assert!(
                matches!(
                    AccessRequest::parse(&builder.body(()).unwrap()),
                    Err(Error::Invalid(_))
                ),
                "{values:?}"
            );
        }
        let request = Request::builder()
            .uri("/agent/ws?target=urn:adx:instance:env&port=8080")
            .header("upgrade", "websocket")
            .header("x-adx-binding-id", "another")
            .body(())
            .unwrap();
        assert!(matches!(
            AccessRequest::parse(&request),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn http_and_ws_entries_enforce_the_selected_protocol_for_all_targets() {
        for binding_id in [None, Some("env")] {
            let selector = "target=urn:adx:template:demo:1";
            let request = || {
                let mut builder = Request::builder();
                if let Some(id) = binding_id {
                    builder = builder.header(BINDING_ID_HEADER, id);
                }
                builder
            };
            for (method, path, upgrade) in [
                ("GET", "/agent/http", true),
                ("GET", "/agent/ws", false),
                ("POST", "/agent/ws", true),
            ] {
                let mut builder = request().method(method).uri(format!("{path}?{selector}"));
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
            let http = request()
                .uri(format!("/agent/http/chat?{selector}&q=1"))
                .body(())
                .unwrap();
            assert_eq!(
                AccessRequest::parse(&http).unwrap().unwrap().backend_uri,
                "/chat?q=1"
            );
            let ws = request()
                .uri(format!("/agent/ws?{selector}&q=1"))
                .header("upgrade", "WebSocket")
                .body(())
                .unwrap();
            let access = AccessRequest::parse(&ws).unwrap().unwrap();
            assert_eq!(access.binding_id.as_deref(), binding_id);
            let expected = "/?q=1";
            assert_eq!(access.backend_uri.to_string(), expected);
        }
    }

    #[test]
    fn instance_access_requires_an_explicit_port_and_preserves_business_path() {
        for path in ["/agent/http/events", "/agent/ws/events"] {
            let ws = path.starts_with("/agent/ws");
            let make = |suffix: &str| {
                let builder = Request::builder().uri(format!(
                    "{path}?target=urn:adx:instance:default-worker{suffix}"
                ));
                let builder = if ws {
                    builder.header("upgrade", "websocket")
                } else {
                    builder
                };
                builder.body(()).unwrap()
            };
            assert!(AccessRequest::parse(&make("")).is_err());
            let access = AccessRequest::parse(&make("&port=18092&q=a&q=b"))
                .unwrap()
                .unwrap();
            assert_eq!(access.target, Target::Instance("default-worker".into()));
            assert_eq!(access.port, Some(18092));
            assert_eq!(access.backend_uri, "/events?q=a&q=b");
        }
    }
}

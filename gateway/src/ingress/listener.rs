//! Explicit public listeners. Omitting this configuration keeps legacy listeners.
use super::StaticRoute;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    pub listen: SocketAddr,
    #[serde(default)]
    pub tls: bool,
    /// Explicit opt-in for authenticated native APIs over this HTTP listener.
    #[serde(default)]
    pub allow_http_credentials: bool,
    /// None retains the existing routes; an empty list denies every route.
    #[serde(default)]
    pub allowed_routes: Option<Vec<ListenerRoute>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerRoute {
    #[serde(default)]
    pub method: Option<String>,
    /// Uses the existing exact:/path or prefix:/path boundary matching contract.
    pub path: String,
}

impl ListenerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.tls && self.allow_http_credentials {
            return Err("allow_http_credentials is only valid on HTTP listeners".into());
        }
        if let Some(routes) = &self.allowed_routes {
            if routes.len() > 256 {
                return Err("at most 256 listener routes are allowed".into());
            }
            for route in routes {
                route.path.parse::<StaticRoute>()?;
                if let Some(method) = &route.method {
                    method
                        .parse::<http::Method>()
                        .map_err(|_| "invalid listener HTTP method")?;
                }
            }
        }
        Ok(())
    }

    pub fn allows(&self, method: &http::Method, path: &str) -> bool {
        self.allowed_routes.as_ref().is_none_or(|routes| {
            routes.iter().any(|route| {
                route
                    .method
                    .as_ref()
                    .is_none_or(|allowed| allowed == method.as_str())
                    && route
                        .path
                        .parse::<StaticRoute>()
                        .is_ok_and(|route| route.matches(path))
            })
        })
    }
}

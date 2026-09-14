use super::protocol::ConnectTarget;
use serde::{de, Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct InstanceStatus {
    #[serde(rename = "code", default)]
    pub code: i32,
    #[serde(rename = "exitCode", default)]
    pub exit_code: i32,
    #[serde(rename = "msg", default)]
    pub msg: String,
    #[serde(rename = "type", default)]
    pub kind: i32,
    #[serde(rename = "errCode", default)]
    pub err_code: i32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RouteInfo {
    #[serde(rename = "instanceID", default)]
    pub instance_id: String,
    #[serde(rename = "instanceStatus", default)]
    pub instance_status: InstanceStatus,
    #[serde(rename = "tenantID", default)]
    pub tenant_id: String,
    #[serde(rename = "sandboxID", default)]
    pub sandbox_id: String,
    #[serde(rename = "nodeProxyAddress", default)]
    pub node_proxy_address: String,
    #[serde(rename = "sandboxIP", default)]
    pub sandbox_ip: String,
    #[serde(
        rename = "tunnelSecurityMode",
        default,
        deserialize_with = "deserialize_security_mode"
    )]
    pub tunnel_security_mode: DataPlaneSecurityMode,
    #[serde(
        rename = "portForwardSecurityMode",
        default,
        deserialize_with = "deserialize_security_mode"
    )]
    pub port_forward_security_mode: DataPlaneSecurityMode,
    #[serde(rename = "portForwardRoutes", default)]
    pub port_forward_routes: Vec<PortForwardRoute>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PortForwardRoute {
    #[serde(rename = "targetPort")]
    pub target_port: u16,
    #[serde(rename = "authMode", default)]
    pub auth_mode: DataPlaneAuthMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DataPlaneAuthMode {
    #[default]
    None,
    Token,
}

impl<'de> Deserialize<'de> for DataPlaneAuthMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum NumberOrString {
            Number(i32),
            String(String),
        }

        match NumberOrString::deserialize(deserializer)? {
            NumberOrString::Number(0) => Ok(Self::None),
            NumberOrString::Number(1) => Ok(Self::Token),
            NumberOrString::String(value) => match value.trim().to_ascii_uppercase().as_str() {
                "NONE" | "DATA_PLANE_AUTH_NONE" => Ok(Self::None),
                "TOKEN" | "DATA_PLANE_AUTH_TOKEN" => Ok(Self::Token),
                _ => Err(de::Error::custom(format!(
                    "invalid data-plane auth mode {value}"
                ))),
            },
            NumberOrString::Number(value) => Err(de::Error::custom(format!(
                "invalid data-plane auth mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DataPlaneSecurityMode {
    #[default]
    Inherit,
    Tls,
    TlsToken,
}

impl DataPlaneSecurityMode {
    pub fn token_required(self, inherited: bool) -> bool {
        match self {
            Self::Inherit => inherited,
            Self::Tls => false,
            Self::TlsToken => true,
        }
    }
}

fn deserialize_security_mode<'de, D>(deserializer: D) -> Result<DataPlaneSecurityMode, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString {
        Number(i32),
        String(String),
    }

    match NumberOrString::deserialize(deserializer)? {
        NumberOrString::Number(0) => Ok(DataPlaneSecurityMode::Inherit),
        NumberOrString::Number(1) => Ok(DataPlaneSecurityMode::Tls),
        NumberOrString::Number(2) => Ok(DataPlaneSecurityMode::TlsToken),
        NumberOrString::String(value) => match value.as_str() {
            "DATA_PLANE_SECURITY_INHERIT" | "INHERIT" => Ok(DataPlaneSecurityMode::Inherit),
            "DATA_PLANE_SECURITY_TLS" | "TLS" => Ok(DataPlaneSecurityMode::Tls),
            "DATA_PLANE_SECURITY_TLS_TOKEN" | "TLS_TOKEN" => Ok(DataPlaneSecurityMode::TlsToken),
            _ => Err(de::Error::custom(format!(
                "invalid data-plane security mode {value}"
            ))),
        },
        NumberOrString::Number(value) => Err(de::Error::custom(format!(
            "invalid data-plane security mode {value}"
        ))),
    }
}

impl RouteInfo {
    pub fn port_forward_auth_mode(&self, target_port: u16) -> DataPlaneAuthMode {
        self.port_forward_routes
            .iter()
            .find(|route| route.target_port == target_port)
            .map(|route| route.auth_mode)
            .unwrap_or_default()
    }

    /// Build the node CONNECT metadata for any TCP port selected by the Edge
    /// policy. The route itself supplies the sandbox identity and sandbox IP;
    /// sandboxd does not need to know the application protocol or port list.
    pub fn connect_target(
        &self,
        target_port: u16,
        request_id: impl Into<String>,
    ) -> Option<ConnectTarget> {
        if self.sandbox_id.is_empty() || self.sandbox_ip.is_empty() || target_port == 0 {
            return None;
        }
        Some(ConnectTarget {
            instance_id: self.instance_id.clone(),
            workload_id: self.sandbox_id.clone(),
            target_ip: self.sandbox_ip.parse::<IpAddr>().ok()?,
            target_port,
            request_id: request_id.into(),
        })
    }
}

#[derive(Clone, Default)]
pub struct RouteCache {
    inner: Arc<RwLock<RouteCacheInner>>,
}

#[derive(Default)]
struct RouteCacheInner {
    routes: HashMap<String, RouteCacheEntry>,
    by_instance: HashMap<String, HashSet<String>>,
}

#[derive(Clone)]
struct RouteCacheEntry {
    owner: String,
    route: RouteInfo,
}

impl RouteCache {
    pub fn put(&self, route: RouteInfo) {
        let owner = route.instance_id.clone();
        let aliases = HashSet::from([owner.clone(), sanitize_instance_id(&owner)]);
        let mut inner = self.inner.write().unwrap();
        remove_instance(&mut inner, &owner);
        for alias in &aliases {
            inner.routes.insert(
                alias.clone(),
                RouteCacheEntry {
                    owner: owner.clone(),
                    route: route.clone(),
                },
            );
        }
        inner.by_instance.insert(owner, aliases);
    }
    pub fn delete(&self, instance_id: &str) {
        remove_instance(&mut self.inner.write().unwrap(), instance_id);
    }
    pub fn get(&self, instance_id: &str) -> Option<RouteInfo> {
        self.inner
            .read()
            .unwrap()
            .routes
            .get(instance_id)
            .map(|entry| entry.route.clone())
    }
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().by_instance.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn entries(&self) -> HashMap<String, RouteInfo> {
        let inner = self.inner.read().unwrap();
        inner
            .by_instance
            .keys()
            .filter_map(|instance_id| {
                inner
                    .routes
                    .get(instance_id)
                    .map(|entry| (instance_id.clone(), entry.route.clone()))
            })
            .collect()
    }
}

fn remove_instance(inner: &mut RouteCacheInner, instance_id: &str) {
    let owner = if inner.by_instance.contains_key(instance_id) {
        instance_id.to_owned()
    } else {
        inner
            .routes
            .get(instance_id)
            .map(|entry| entry.owner.clone())
            .unwrap_or_else(|| instance_id.to_owned())
    };
    if let Some(aliases) = inner.by_instance.remove(&owner) {
        for alias in aliases {
            if inner.routes.get(&alias).map(|entry| entry.owner.as_str()) == Some(owner.as_str()) {
                inner.routes.remove(&alias);
            }
        }
    }
}

pub fn sanitize_instance_id(instance_id: &str) -> String {
    let mapped = instance_id
        .replace('@', "-at-")
        .chars()
        .map(|character| match character {
            '/' | '.' | '_' => '-',
            other => other,
        })
        .collect::<String>();
    if mapped.len() <= 200 {
        return mapped;
    }
    let mut end = 200;
    while end > 0 && !mapped.is_char_boundary(end) {
        end -= 1;
    }
    mapped[..end].to_owned()
}

pub fn route_key_id(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

pub const ROUTE_PREFIX: &str = "/adx/route/business/adxk";

pub fn is_full_route_key(key: &str) -> bool {
    key.strip_prefix(ROUTE_PREFIX).is_some_and(|suffix| {
        suffix.starts_with('/') && !suffix[1..].contains('/') && suffix.len() > 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn route_keeps_instance_status_and_endpoint() {
        let route: RouteInfo = serde_json::from_str(r#"{"instanceID":"i","instanceStatus":{"code":3,"msg":"ok"},"sandboxID":"s","nodeProxyAddress":"gw:8443","sandboxIP":"10.0.0.2"}"#).unwrap();
        let cache = RouteCache::default();
        cache.put(route.clone());
        assert_eq!(cache.get("i").unwrap().sandbox_ip, "10.0.0.2");
        assert_eq!(cache.get("i").unwrap().instance_status.code, 3);
    }

    #[test]
    fn route_accepts_protobuf_security_modes() {
        let route: RouteInfo = serde_json::from_str(
            r#"{"instanceID":"i","tunnelSecurityMode":"DATA_PLANE_SECURITY_TLS_TOKEN","portForwardSecurityMode":1}"#,
        )
        .unwrap();
        assert_eq!(route.tunnel_security_mode, DataPlaneSecurityMode::TlsToken);
        assert_eq!(route.port_forward_security_mode, DataPlaneSecurityMode::Tls);
    }

    #[test]
    fn route_resolves_port_forward_auth_per_target_port() {
        let route: RouteInfo = serde_json::from_str(
            r#"{"instanceID":"i","portForwardRoutes":[{"targetPort":22,"authMode":"TOKEN"},{"targetPort":5432,"authMode":0}]}"#,
        )
        .unwrap();
        assert_eq!(route.port_forward_auth_mode(22), DataPlaneAuthMode::Token);
        assert_eq!(route.port_forward_auth_mode(5432), DataPlaneAuthMode::None);
        assert_eq!(route.port_forward_auth_mode(8080), DataPlaneAuthMode::None);
    }

    #[test]
    fn cache_resolves_raw_and_safe_instance_ids() {
        let mut route: RouteInfo =
            serde_json::from_str(r#"{"instanceID":"user@host/f.v_1","sandboxID":"s"}"#).unwrap();
        route.instance_status.code = 3;
        let cache = RouteCache::default();
        cache.put(route);
        assert_eq!(
            cache.get("user-at-host-f-v-1").unwrap().instance_id,
            "user@host/f.v_1"
        );
        cache.delete("user@host/f.v_1");
        assert!(cache.get("user-at-host-f-v-1").is_none());

        let route: RouteInfo =
            serde_json::from_str(r#"{"instanceID":"user@host/f.v_1","sandboxID":"s"}"#).unwrap();
        cache.put(route);
        cache.delete("user-at-host-f-v-1");
        assert!(cache.get("user@host/f.v_1").is_none());
    }
}

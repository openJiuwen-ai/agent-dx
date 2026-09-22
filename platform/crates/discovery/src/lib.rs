//! Redis holds a renewable Master endpoint, never capsule ownership decisions.
use adx_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MasterEndpoint {
    pub schema: u32,
    pub epoch: u64,
    pub address: String,
}
impl MasterEndpoint {
    pub fn validate(&self) -> Result<()> {
        let uri: http::Uri = self
            .address
            .parse()
            .map_err(|_| Error::Invalid("invalid Master endpoint".into()))?;
        if self.schema != 1
            || self.epoch == 0
            || !matches!(uri.scheme_str(), Some("https" | "http"))
            || uri
                .authority()
                .is_none_or(|a| a.port_u16().is_none_or(|p| p == 0))
            || uri.authority().is_some_and(|a| a.as_str().contains('@'))
            || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
        {
            return Err(Error::Invalid(
                "versioned HTTP(S) Master endpoint required".into(),
            ));
        }
        Ok(())
    }
}
pub fn keys(namespace: &str) -> Result<(String, String)> {
    if namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(Error::Invalid("invalid Redis namespace".into()));
    }
    Ok((
        format!("adx:{{{namespace}}}:master:v1"),
        format!("adx:{{{namespace}}}:control:v1"),
    ))
}
#[derive(Clone)]
pub struct RedisDiscovery {
    client: redis::Client,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    namespace: String,
    timeout: Duration,
}
impl RedisDiscovery {
    pub fn new(url: &str, namespace: &str, timeout: Duration) -> Result<Self> {
        keys(namespace)?;
        if timeout.is_zero() {
            return Err(Error::Invalid("discovery timeout must be positive".into()));
        }
        Ok(Self {
            client: redis::Client::open(url)
                .map_err(|_| Error::Invalid("invalid discovery Redis endpoint".into()))?,
            connection: Arc::default(),
            namespace: namespace.into(),
            timeout,
        })
    }
    pub async fn lookup(&self) -> Result<MasterEndpoint> {
        let (key, control) = keys(&self.namespace)?;
        let operation = async {
            let mut guard = self.connection.lock().await;
            if guard.is_none() {
                *guard = Some(self.client.get_multiplexed_async_connection().await?);
            }
            let mut connection = guard
                .as_ref()
                .expect("connection is initialized above")
                .clone();
            drop(guard);
            redis::cmd("EVAL")
                .arg("return {redis.call('GET', KEYS[1]), redis.call('HGET', KEYS[2], 'header')}")
                .arg(2)
                .arg(key)
                .arg(control)
                .query_async::<Vec<Option<String>>>(&mut connection)
                .await
        };
        let values = match tokio::time::timeout(self.timeout, operation).await {
            Ok(Ok(v)) => v,
            _ => {
                *self.connection.lock().await = None;
                return Err(Error::Unavailable("Master discovery unavailable".into()));
            }
        };
        if values.len() != 2 {
            return Err(Error::Unavailable("invalid discovery response".into()));
        }
        let value = values[0].as_deref().ok_or(Error::NotFound)?;
        let endpoint: MasterEndpoint = serde_json::from_str(value)
            .map_err(|_| Error::Unavailable("invalid Master discovery record".into()))?;
        endpoint.validate()?;
        #[derive(Deserialize)]
        struct Header {
            epoch: u64,
        }
        let header: Header = serde_json::from_str(values[1].as_deref().ok_or(Error::NotFound)?)
            .map_err(|_| Error::Unavailable("invalid discovery epoch".into()))?;
        if endpoint.epoch != header.epoch {
            return Err(Error::NotFound);
        }
        Ok(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_versioned_rpc_endpoint_and_namespace() {
        for address in [
            "ftp://host:1",
            "https://user@host:1",
            "https://host:1/path",
            "https://host:1/?x=1",
            "bad",
        ] {
            assert!(MasterEndpoint {
                schema: 1,
                epoch: 1,
                address: address.into()
            }
            .validate()
            .is_err());
        }
        assert!(MasterEndpoint {
            schema: 1,
            epoch: 1,
            address: "https://host:123".into()
        }
        .validate()
        .is_ok());
        assert!(MasterEndpoint {
            schema: 1,
            epoch: 1,
            address: "http://host:123".into()
        }
        .validate()
        .is_ok());
        for ns in ["", "a}b", "a/b", "a:b"] {
            assert!(keys(ns).is_err());
        }
    }
}

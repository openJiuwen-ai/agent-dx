//! Internal RPC transport. Network mode trusts deployment-isolated callers;
//! it never authenticates the declared component identity cryptographically.
use adx_protocol::auth::Principal;
use tonic::{
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

pub use adx_protocol::auth::SecurityMode;

#[derive(Clone, Default)]
pub struct Caller(Option<Principal>);
impl Interceptor for Caller {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(principal) = &self.0 {
            let role = match principal {
                Principal::Master => "master",
                Principal::ApiServer => "api-server",
                Principal::Edge => "edge",
                Principal::Node(id) => {
                    request.metadata_mut().insert_bin(
                        "adx-node-id-bin",
                        tonic::metadata::MetadataValue::from_bytes(id.as_bytes()),
                    );
                    "node"
                }
            };
            request.metadata_mut().insert(
                "adx-component",
                role.parse()
                    .map_err(|_| Status::internal("invalid component identity"))?,
            );
        }
        Ok(request)
    }
}
pub type RpcChannel = InterceptedService<Channel, Caller>;

#[derive(Clone)]
pub struct RpcClient {
    tls: Option<ClientTlsConfig>,
    caller: Caller,
}
impl From<ClientTlsConfig> for RpcClient {
    fn from(tls: ClientTlsConfig) -> Self {
        Self {
            tls: Some(tls),
            caller: Caller::default(),
        }
    }
}
impl RpcClient {
    pub fn network(principal: Principal) -> Self {
        Self {
            tls: None,
            caller: Caller(Some(principal)),
        }
    }
    pub fn endpoint(&self, address: &str) -> Result<Endpoint, std::io::Error> {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let address = if address.contains("://") {
            address.to_owned()
        } else {
            format!("{scheme}://{address}")
        };
        let uri: http::Uri = address.parse().map_err(std::io::Error::other)?;
        if uri.scheme_str() != Some(scheme)
            || uri.authority().is_none_or(|a| a.as_str().contains('@'))
            || uri
                .path_and_query()
                .is_some_and(|p| !matches!(p.as_str(), "" | "/"))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "internal RPC endpoint does not match configured security mode",
            ));
        }
        let endpoint = Endpoint::from_shared(address).map_err(std::io::Error::other)?;
        Ok(match &self.tls {
            Some(tls) => endpoint
                .tls_config(tls.clone())
                .map_err(std::io::Error::other)?,
            None => endpoint,
        })
    }
    pub fn wrap(&self, channel: Channel) -> RpcChannel {
        InterceptedService::new(channel, self.caller.clone())
    }
}

/// Wrap a channel whose caller identity is already supplied by mTLS.
pub fn authenticated_channel(channel: Channel) -> RpcChannel {
    InterceptedService::new(channel, Caller::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn network_mode_requires_http_without_silent_downgrade() {
        let client = RpcClient::network(Principal::ApiServer);
        assert!(client.endpoint("127.0.0.1:19000").is_ok());
        assert!(client.endpoint("http://localhost:19000").is_ok());
        for address in [
            "https://localhost:19000",
            "http://user@localhost:19000",
            "http://localhost/path",
        ] {
            assert!(client.endpoint(address).is_err());
        }
    }
    #[test]
    fn secure_mode_rejects_plaintext_endpoint() {
        let client = RpcClient::from(ClientTlsConfig::new());
        assert!(client.endpoint("http://localhost:19000").is_err());
    }
    #[test]
    fn network_caller_keeps_node_identity() {
        let request = Caller(Some(Principal::Node("worker-1".into())))
            .call(Request::new(()))
            .unwrap();
        assert_eq!(
            adx_protocol::auth::Peers::network()
                .authenticate(&request)
                .unwrap(),
            Principal::Node("worker-1".into())
        );
    }
}

//! AFS 所有 gRPC 链路共用的明文与双向 TLS 策略。
//!
//! `SecurityManager` 仍然只修改 Tonic builder，不负责连接生命周期。这样业务代码
//! 只选择安全策略，不需要重复读取证书和拼装 `ClientTlsConfig/ServerTlsConfig`。

// 花括号 import 表示从同一个模块一次导入多个名字。
use std::{fs, path::PathBuf};

// Certificate 表示受信 CA；Identity 是“证书 + 私钥”的本端身份。
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

use crate::GrpcError;

/// Client 和 Server 共用的 TLS 配置枚举。
///
/// 枚举保证只能选择一种合法模式；不会出现 `tls_enabled=true` 但证书字段缺失的
/// 半配置状态。`#[default]` 指定 `Disabled` 是 derive(Default) 的默认分支。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TlsConfig {
    #[default]
    Disabled,
    /// 双向 TLS：双方都要出示证书，并由同一个 CA 信任链校验。
    MutualTls {
        /// 用于校验对端证书的 CA PEM 文件。
        ca_certificate: PathBuf,
        /// 本端证书 PEM；Client/Server 都使用同一数据结构。
        identity_certificate: PathBuf,
        /// 与本端证书配套的私钥 PEM。
        identity_private_key: PathBuf,
        /// Client 校验 Server 证书时使用的 DNS 名称。
        server_name: String,
    },
}

/// 保存一份已经完成静态校验的安全策略。
#[derive(Clone, Debug)]
pub struct SecurityManager {
    // 字段私有，外部只能通过 `new` 创建，防止绕过校验。
    config: TlsConfig,
}

impl SecurityManager {
    /// 校验并创建安全策略；这里不读取文件，也不发生网络 I/O。
    pub fn new(config: TlsConfig) -> Result<Self, GrpcError> {
        // `if let` 只关心 MutualTls 分支；`&config` 是借用，避免把 config 移走。
        // `&&` 后的 let-chain 让“匹配枚举”和“名称为空”写在一个条件里。
        if let TlsConfig::MutualTls { server_name, .. } = &config
            && server_name.trim().is_empty()
        {
            return Err(GrpcError::InvalidTlsConfiguration(
                "server_name must not be empty when mutual TLS is enabled".to_string(),
            ));
        }
        // 校验成功后把 config 的所有权移动进 SecurityManager。
        Ok(Self { config })
    }

    /// 把客户端安全策略应用到 Endpoint。
    pub fn configure_client(&self, endpoint: Endpoint) -> Result<Endpoint, GrpcError> {
        // `let PATTERN = value else { ... }` 表示：如果不是 MutualTls，就走 else。
        // 这里 Disabled 直接返回原 Endpoint，不做多余包装。
        let TlsConfig::MutualTls {
            ca_certificate,
            identity_certificate,
            identity_private_key,
            server_name,
        } = &self.config
        else {
            return Ok(endpoint);
        };

        // MutualTls 下，Client 同时需要：信任 Server 的 CA，以及自己的证书身份。
        // 每个 `?` 在失败时立即把 GrpcError 返回给调用者。
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(read_file(ca_certificate)?))
            .identity(Identity::from_pem(
                read_file(identity_certificate)?,
                read_file(identity_private_key)?,
            ))
            .domain_name(server_name);
        // Tonic 还会检查 TLS 配置是否合法；成功后返回带 TLS 的新 Endpoint。
        Ok(endpoint.tls_config(tls)?)
    }

    /// 把服务端安全策略应用到 Server builder。
    pub fn configure_server(&self, server: Server) -> Result<Server, GrpcError> {
        let TlsConfig::MutualTls {
            ca_certificate,
            identity_certificate,
            identity_private_key,
            ..
        } = &self.config
        else {
            return Ok(server);
        };

        // Server 的 `client_ca_root` 强制校验 Client 证书，因此这是 mTLS 而非单向 TLS。
        let tls = ServerTlsConfig::new()
            .client_ca_root(Certificate::from_pem(read_file(ca_certificate)?))
            .identity(Identity::from_pem(
                read_file(identity_certificate)?,
                read_file(identity_private_key)?,
            ));
        Ok(server.tls_config(tls)?)
    }
}

fn read_file(path: &PathBuf) -> Result<Vec<u8>, GrpcError> {
    // `map_err` 保留原始 io::Error，同时补上哪个证书路径读取失败的上下文。
    fs::read(path).map_err(|source| GrpcError::ReadTlsFile {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    // `#[cfg(test)]` 使整个模块只在 `cargo test` 时编译，不进入正式二进制。
    use super::*;

    #[test]
    fn mutual_tls_requires_server_name() {
        // 本测试不需要真实证书，因为 `new` 应在读文件之前拒绝空 server_name。
        let error = SecurityManager::new(TlsConfig::MutualTls {
            ca_certificate: "ca.pem".into(),
            identity_certificate: "client.pem".into(),
            identity_private_key: "client.key".into(),
            server_name: String::new(),
        })
        .expect_err("empty server name must be rejected before connecting");
        // `matches!` 只检查错误枚举分支，不绑定其中的字符串内容。
        assert!(matches!(error, GrpcError::InvalidTlsConfiguration(_)));
    }
}

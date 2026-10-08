//! Interactive SSH uses the user's OpenSSH configuration and terminal directly.
use crate::{Cli, ConfigFile, Error, Result, TemplateScope};
use adx_agent_core::target::{SshRoute, Target};
use clap::Args;
use std::{ffi::OsString, path::PathBuf};

#[derive(Debug, Args)]
pub struct SshArgs {
    #[command(flatten)]
    pub scope: TemplateScope,
    #[arg(long)]
    pub env: Option<String>,
    /// Template 中声明的 SSH service 端口。
    #[arg(long)]
    pub port: Option<u16>,
    /// Gateway SSH 监听地址，例如 gateway.example:2222。
    #[arg(long)]
    pub gateway: Option<String>,
    #[arg(short = 'i', long)]
    pub identity: Option<PathBuf>,
}
/// Builds argv without shell interpolation. No HTTP token is needed for SSH authentication.
pub fn arguments(
    cli: &Cli,
    ssh: &SshArgs,
    env: impl Fn(&str) -> Option<String>,
) -> Result<Vec<OsString>> {
    let path = cli
        .config
        .clone()
        .or_else(|| env("ADX_CONFIG").map(PathBuf::from));
    let file: ConfigFile = match path {
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|_| Error::Invalid("无效的 CLI JSON 配置".into()))?,
        None => ConfigFile::default(),
    };
    let address = ssh
        .gateway
        .clone()
        .or_else(|| env("ADX_SSH_ADDRESS"))
        .or(file.ssh_address)
        .ok_or_else(|| Error::Invalid("请设置 --gateway 或 ADX_SSH_ADDRESS（host:port）".into()))?;
    let url = url::Url::parse(&format!("ssh://{address}"))
        .map_err(|_| Error::Invalid("无效的 Gateway SSH 地址".into()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || !url.path().is_empty() && url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Invalid("SSH 地址只接受 host:port".into()));
    }
    let host = url
        .host_str()
        .ok_or_else(|| Error::Invalid("SSH 地址缺少 host".into()))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port = url.port().unwrap_or(22);
    if port == 0 || ssh.port == Some(0) || host.starts_with('-') {
        return Err(Error::Invalid("无效 SSH 地址或端口".into()));
    }
    let target = match &ssh.env {
        Some(id) => Target::Environment {
            name: ssh.scope.template.clone(),
            version: ssh.scope.version.clone(),
            id: id.clone(),
        },
        None => Target::Template {
            name: ssh.scope.template.clone(),
            version: ssh.scope.version.clone(),
        },
    };
    let route = SshRoute {
        target,
        port: ssh.port,
        trace: None,
    }
    .to_string();
    route.parse::<SshRoute>().map_err(Error::Invalid)?;
    let mut args: Vec<OsString> = vec![
        "-t".into(),
        "-o".into(),
        "StrictHostKeyChecking=yes".into(),
        "-o".into(),
        "PreferredAuthentications=publickey".into(),
        "-o".into(),
        "ControlMaster=no".into(),
        "-o".into(),
        "ControlPath=none".into(),
        "-o".into(),
        "RemoteCommand=none".into(),
        "-o".into(),
        "ClearAllForwardings=yes".into(),
        "-o".into(),
        "ForwardAgent=no".into(),
        "-o".into(),
        "ForwardX11=no".into(),
        "-p".into(),
        port.to_string().into(),
        "-l".into(),
        route.into(),
    ];
    if let Some(identity) = ssh
        .identity
        .clone()
        .or_else(|| env("ADX_SSH_IDENTITY").map(PathBuf::from))
        .or(file.ssh_identity)
    {
        args.push("-i".into());
        args.push(identity.into_os_string());
    }
    args.push("--".into());
    args.push(host.into());
    Ok(args)
}
pub async fn run(cli: &Cli, ssh: &SshArgs) -> Result<std::process::ExitCode> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(Error::Invalid("adx ssh 需要交互终端".into()));
    }
    let status = tokio::process::Command::new("ssh")
        .args(arguments(cli, ssh, |name| std::env::var(name).ok())?)
        .kill_on_drop(true)
        .status()
        .await?;
    Ok(std::process::ExitCode::from(
        status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .unwrap_or(255),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[test]
    fn ssh_builds_a_routable_target_and_preserves_identity_path() {
        let cli = Cli::try_parse_from([
            "adx",
            "ssh",
            "--template",
            "demo agent",
            "--version",
            "1",
            "--gateway",
            "localhost:2222",
            "--env",
            "my env",
            "--port",
            "2223",
            "-i",
            "/tmp/my key",
        ])
        .unwrap();
        let crate::Command::Ssh(ssh) = &cli.command else {
            panic!("SSH command expected")
        };
        let args = arguments(&cli, ssh, |_| None).unwrap();
        let username = args.windows(2).find(|v| v[0] == "-l").unwrap()[1]
            .to_str()
            .unwrap();
        let route: SshRoute = username.parse().unwrap();
        assert_eq!(
            route.target,
            Target::Environment {
                name: "demo agent".into(),
                version: "1".into(),
                id: "my env".into()
            }
        );
        assert_eq!(route.port, Some(2223));
        assert!(args
            .windows(2)
            .any(|v| v[0] == "-i" && v[1] == "/tmp/my key"));
        assert!(args
            .windows(2)
            .any(|v| v[0] == "-o" && v[1] == "ControlMaster=no"));
        assert_eq!(args.last().unwrap(), "localhost");
    }
    #[test]
    fn ssh_can_select_server_generated_environment_and_ipv6_endpoint() {
        let cli =
            Cli::try_parse_from(["adx", "ssh", "--template", "demo", "--version", "1"]).unwrap();
        let crate::Command::Ssh(ssh) = &cli.command else {
            panic!("SSH command expected")
        };
        let args = arguments(&cli, ssh, |name| {
            (name == "ADX_SSH_ADDRESS").then(|| "[::1]:2222".into())
        })
        .unwrap();
        let username = args.windows(2).find(|v| v[0] == "-l").unwrap()[1]
            .to_str()
            .unwrap();
        assert!(matches!(
            username.parse::<SshRoute>().unwrap().target,
            Target::Template { .. }
        ));
        assert_eq!(args.last().unwrap(), "::1");
    }
}

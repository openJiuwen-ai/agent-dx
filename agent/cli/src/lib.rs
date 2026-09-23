//! ADX user CLI: public Gateway management APIs, configuration and command output.
pub mod http;
pub mod ssh;

use adx_agent_core::{activator::default_page_size, limits, TemplateVersion};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use serde_json::Value;
use std::{path::PathBuf, time::Duration};
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("参数或配置错误：{0}")]
    Invalid(String),
    #[error("文件访问失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("连接失败，请检查 Gateway 地址、证书和网络")]
    Connect,
    #[error("请求失败或响应中断；写操作可能已提交，请回查原资源")]
    OutcomeUnknown,
    #[error("读取失败或响应超时")]
    Unavailable,
    #[error("服务端返回 HTTP {status}：{message}")]
    Server { status: u16, message: String },
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum Output {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "adx", version, about = "Agent-DX 用户命令行")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub endpoint: Option<String>,
    #[arg(long, global = true)]
    pub token_file: Option<PathBuf>,
    #[arg(long, global = true)]
    pub ca: Option<PathBuf>,
    #[arg(long, global = true)]
    pub timeout_seconds: Option<u64>,
    #[arg(long, global = true, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub allow_http: Option<bool>,
    #[arg(long, global = true, value_enum, default_value = "text")]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// 通过 Gateway 调用 Harness HTTP 服务，响应正文按块输出。
    Http(http::HttpArgs),
    /// 通过 Gateway 进入交互式 SSH 终端。
    Ssh(ssh::SshArgs),
    /// 发布或查询不可变模板版本。
    Template {
        #[command(subcommand)]
        command: TemplateCommand,
    },
    /// 查询或删除 Environment；创建由访问流量触发。
    Env {
        #[command(subcommand)]
        command: EnvironmentCommand,
    },
}
#[derive(Debug, Subcommand)]
pub enum TemplateCommand {
    Publish {
        #[arg(short, long)]
        file: PathBuf,
    },
    Get {
        name: String,
        #[arg(long)]
        version: String,
    },
}
#[derive(Debug, Args)]
pub struct TemplateScope {
    #[arg(long)]
    pub template: String,
    #[arg(long)]
    pub version: String,
}
#[derive(Debug, Subcommand)]
pub enum EnvironmentCommand {
    List {
        #[command(flatten)]
        scope: TemplateScope,
        #[arg(long, default_value_t = default_page_size(), value_parser = parse_page_size)]
        page_size: usize,
        #[arg(long)]
        page_token: Option<String>,
    },
    Get {
        id: String,
        #[command(flatten)]
        scope: TemplateScope,
    },
    Delete {
        id: String,
        #[command(flatten)]
        scope: TemplateScope,
    },
}
fn parse_page_size(value: &str) -> std::result::Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|v| (1..=limits::ENVIRONMENT_PAGE_SIZE).contains(v))
        .ok_or_else(|| {
            format!(
                "page-size 必须在 1..={} 之间",
                limits::ENVIRONMENT_PAGE_SIZE
            )
        })
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    ssh_address: Option<String>,
    ssh_identity: Option<PathBuf>,
    endpoint: Option<String>,
    token_file: Option<PathBuf>,
    ca: Option<PathBuf>,
    timeout_seconds: Option<u64>,
    allow_http: Option<bool>,
}

/// Resolved connection settings. Credentials are deliberately excluded from Debug output.
pub struct Configuration {
    pub endpoint: Url,
    token: String,
    ca: Option<PathBuf>,
    pub timeout: Duration,
}
impl Configuration {
    /// Validates an origin, opaque credential, explicit HTTP opt-in and request budget.
    pub fn new(
        endpoint: &str,
        token: String,
        ca: Option<PathBuf>,
        timeout: Duration,
        allow_http: bool,
    ) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Invalid("无效的 Gateway 地址".into()))?;
        if !(endpoint.scheme() == "https" || allow_http && endpoint.scheme() == "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Invalid(
                "endpoint 必须是 HTTPS origin；明文 HTTP 必须显式启用 allow-http".into(),
            ));
        }
        if token.trim().is_empty() || token.chars().any(char::is_control) || timeout.is_zero() {
            return Err(Error::Invalid("需要有效凭据和正数请求超时".into()));
        }
        Ok(Self {
            endpoint,
            token,
            ca,
            timeout,
        })
    }
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn load(cli: &Cli) -> Result<Self> {
        Self::load_with_env(cli, |name| std::env::var(name).ok())
    }
    /// Resolves command line > environment > explicit JSON file > defaults.
    pub fn load_with_env(cli: &Cli, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let path = cli
            .config
            .clone()
            .or_else(|| env("ADX_CONFIG").map(PathBuf::from));
        let file: ConfigFile = match path {
            Some(path) => serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| Error::Invalid("无效的 CLI JSON 配置".into()))?,
            None => ConfigFile::default(),
        };
        let endpoint = cli
            .endpoint
            .clone()
            .or_else(|| env("ADX_SERVER_ADDRESS"))
            .or(file.endpoint)
            .unwrap_or_else(|| "https://localhost:8443".into());
        let token = if let Some(path) = &cli.token_file {
            std::fs::read_to_string(path)?
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        } else if let Some(token) = env("ADX_TOKEN") {
            token
        } else if let Some(path) = file.token_file {
            std::fs::read_to_string(path)?
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        } else {
            return Err(Error::Invalid("请设置 ADX_TOKEN 或 --token-file".into()));
        };
        let timeout_seconds = match cli.timeout_seconds {
            Some(value) => value,
            None => match env("ADX_TIMEOUT_SECONDS") {
                Some(value) => value
                    .parse()
                    .map_err(|_| Error::Invalid("无效的 ADX_TIMEOUT_SECONDS".into()))?,
                None => file.timeout_seconds.unwrap_or(60),
            },
        };
        let allow_http = match cli.allow_http {
            Some(value) => value,
            None => match env("ADX_ALLOW_HTTP") {
                Some(value) => value
                    .parse()
                    .map_err(|_| Error::Invalid("ADX_ALLOW_HTTP 必须为 true 或 false".into()))?,
                None => file.allow_http.unwrap_or(false),
            },
        };
        let ca = cli
            .ca
            .clone()
            .or_else(|| env("ADX_CA_CERT").map(PathBuf::from))
            .or(file.ca);
        Self::new(
            &endpoint,
            token,
            ca,
            Duration::from_secs(timeout_seconds),
            allow_http,
        )
    }
    fn client(&self) -> Result<reqwest::Client> {
        self.client_with_timeout(true)
    }
    fn client_with_timeout(&self, total_timeout: bool) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(limits::CONNECT_TIMEOUT.min(self.timeout));
        if total_timeout {
            builder = builder.timeout(self.timeout);
        }
        if let Some(path) = &self.ca {
            let certificate = reqwest::Certificate::from_pem(&std::fs::read(path)?)
                .map_err(|_| Error::Invalid("无效的 CA PEM 文件".into()))?;
            builder = builder.add_root_certificate(certificate);
        }
        builder
            .build()
            .map_err(|_| Error::Invalid("HTTP 客户端初始化失败".into()))
    }
}

#[derive(Debug)]
pub struct CommandResult(Value);
impl CommandResult {
    pub fn render(&self, format: Output) -> Result<String> {
        match format {
            Output::Json => serde_json::to_string(&self.0),
            Output::Text => serde_json::to_string_pretty(&self.0),
        }
        .map_err(|_| Error::Unavailable)
    }
}
fn route(endpoint: &Url, segments: &[&str]) -> Result<Url> {
    for segment in segments {
        adx_agent_core::identifier(segment, "路径参数").map_err(Error::Invalid)?;
        if matches!(*segment, "." | "..") {
            return Err(Error::Invalid("路径参数不能为 . 或 ..".into()));
        }
    }
    let mut url = endpoint.clone();
    url.path_segments_mut()
        .map_err(|_| Error::Invalid("无效 endpoint".into()))?
        .clear()
        .extend(segments);
    Ok(url)
}

/// Executes one public management request. Writes and business calls are never replayed.
pub async fn execute(command: &Command, config: &Configuration) -> Result<CommandResult> {
    let mut body = None;
    let (method, url) = match command {
        Command::Http(_) => return Err(Error::Invalid("HTTP 使用流式输出入口".into())),
        Command::Ssh(_) => return Err(Error::Invalid("SSH 使用交互终端入口".into())),
        Command::Template {
            command: TemplateCommand::Publish { file },
        } => {
            if std::fs::metadata(file)?.len() > limits::HTTP_JSON_BYTES as u64 {
                return Err(Error::Invalid("模板文件超过请求大小限制".into()));
            }
            let template: TemplateVersion = serde_json::from_slice(&std::fs::read(file)?)
                .map_err(|_| Error::Invalid("无效的模板 JSON".into()))?;
            template.validate().map_err(Error::Invalid)?;
            body = Some(serde_json::to_value(template).map_err(|_| Error::Unavailable)?);
            (
                reqwest::Method::POST,
                route(&config.endpoint, &["api", "agent", "v2", "templates"])?,
            )
        }
        Command::Template {
            command: TemplateCommand::Get { name, version },
        } => (
            reqwest::Method::GET,
            route(
                &config.endpoint,
                &["api", "agent", "v2", "templates", name, "versions", version],
            )?,
        ),
        Command::Env { command } => {
            let (scope, id, method) = match command {
                EnvironmentCommand::List { scope, .. } => (scope, None, reqwest::Method::GET),
                EnvironmentCommand::Get { id, scope } => (scope, Some(id), reqwest::Method::GET),
                EnvironmentCommand::Delete { id, scope } => {
                    (scope, Some(id), reqwest::Method::DELETE)
                }
            };
            let mut segments = vec![
                "api",
                "agent",
                "v2",
                "templates",
                &scope.template,
                "versions",
                &scope.version,
                "environments",
            ];
            if let Some(id) = id {
                segments.push(id);
            }
            let mut url = route(&config.endpoint, &segments)?;
            if let EnvironmentCommand::List {
                page_size,
                page_token,
                ..
            } = command
            {
                parse_page_size(&page_size.to_string()).map_err(Error::Invalid)?;
                url.query_pairs_mut()
                    .append_pair("page_size", &page_size.to_string());
                if let Some(token) = page_token {
                    url.query_pairs_mut().append_pair("page_token", token);
                }
            }
            (method, url)
        }
    };
    let writes = method != reqwest::Method::GET;
    let mut request = config
        .client()?
        .request(method, url)
        .bearer_auth(config.token());
    if let Some(body) = body {
        request = request.json(&body);
    }
    let mut response = request.send().await.map_err(|e| {
        if e.is_connect() {
            Error::Connect
        } else if writes {
            Error::OutcomeUnknown
        } else {
            Error::Unavailable
        }
    })?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        if writes {
            Error::OutcomeUnknown
        } else {
            Error::Unavailable
        }
    })? {
        if bytes.len() + chunk.len() > limits::HTTP_JSON_BYTES {
            return Err(if writes {
                Error::OutcomeUnknown
            } else {
                Error::Unavailable
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let message = serde_json::from_slice::<Value>(&bytes)
            .map(|value| value.to_string().replace(config.token(), "[redacted]"))
            .unwrap_or_else(|_| "服务端未返回 JSON 错误详情".into());
        return Err(Error::Server {
            status: status.as_u16(),
            message,
        });
    }
    let value = serde_json::from_slice(&bytes).map_err(|_| {
        if writes {
            Error::OutcomeUnknown
        } else {
            Error::Unavailable
        }
    })?;
    Ok(CommandResult(value))
}

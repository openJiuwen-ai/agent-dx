//! Harness HTTP access. The header deadline never bounds the subsequent response stream.
use crate::{Configuration, Error, Result, TemplateScope};
use adx_agent_core::target::Target;
use clap::Args;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::{path::PathBuf, str::FromStr};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use url::Url;

#[derive(Debug, Args)]
pub struct HttpArgs {
    #[command(flatten)]
    pub scope: TemplateScope,
    #[arg(long = "env")]
    pub environment: Option<String>,
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    pub port: Option<u16>,
    #[arg(long, default_value = "/")]
    pub path: String,
    #[arg(short = 'X', long, default_value = "GET")]
    pub method: String,
    #[arg(short = 'H', long = "header")]
    pub headers: Vec<String>,
    #[arg(long, conflicts_with = "data_file")]
    pub data: Option<String>,
    /// 流式读取文件；- 表示标准输入。
    #[arg(long, conflicts_with = "data")]
    pub data_file: Option<PathBuf>,
}
impl HttpArgs {
    /// Builds a Gateway URL; rejects paths or query selectors that can escape this route.
    pub fn url(&self, endpoint: &Url) -> Result<Url> {
        let target = match &self.environment {
            Some(id) => Target::Environment {
                name: self.scope.template.clone(),
                version: self.scope.version.clone(),
                id: id.clone(),
            },
            None => Target::Template {
                name: self.scope.template.clone(),
                version: self.scope.version.clone(),
            },
        };
        target
            .to_string()
            .parse::<Target>()
            .map_err(Error::Invalid)?;
        let (path, query) = self.path.split_once('?').unwrap_or((&self.path, ""));
        if !path.starts_with('/')
            || path.starts_with("//")
            || self.path.contains(['#', '\\'])
            || self.path.chars().any(char::is_control)
            || path.split('/').any(|segment| {
                let segment = segment.to_ascii_lowercase().replace("%2e", ".");
                matches!(segment.as_str(), "." | "..")
            })
        {
            return Err(Error::Invalid(
                "path 必须是 Harness 的绝对路径，可带业务 query".into(),
            ));
        }
        if url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| {
            matches!(
                key.as_ref(),
                "instance" | "target" | "port" | "token" | "tenant_id"
            )
        }) {
            return Err(Error::Invalid("业务 query 不能覆盖路由或认证参数".into()));
        }
        let mut url = endpoint.clone();
        url.set_path(&format!("/agent/http{path}"));
        url.set_query((!query.is_empty()).then_some(query));
        url.query_pairs_mut()
            .append_pair("target", &target.to_string());
        if let Some(port) = self.port {
            url.query_pairs_mut().append_pair("port", &port.to_string());
        }
        Ok(url)
    }
    fn header_map(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        for header in &self.headers {
            let (key, value) = header
                .split_once(':')
                .ok_or_else(|| Error::Invalid("header 格式应为 Name: Value".into()))?;
            let key = HeaderName::from_str(key.trim())
                .map_err(|_| Error::Invalid("无效 HTTP header 名".into()))?;
            if matches!(
                key.as_str(),
                "authorization"
                    | "x-auth"
                    | "host"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "upgrade"
                    | "proxy-authorization"
            ) {
                return Err(Error::Invalid(format!("header {key} 由 CLI 管理")));
            }
            headers.append(
                key,
                HeaderValue::from_str(value.trim())
                    .map_err(|_| Error::Invalid("无效 HTTP header 值".into()))?,
            );
        }
        Ok(headers)
    }
}

/// Makes one business request and streams stdout with backpressure. Errors never replay it.
/// The request/header timeout may leave the business operation running in the Harness.
pub async fn execute(
    args: &HttpArgs,
    config: &Configuration,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let method = reqwest::Method::from_bytes(args.method.as_bytes())
        .map_err(|_| Error::Invalid("无效 HTTP method".into()))?;
    if method == reqwest::Method::CONNECT {
        return Err(Error::Invalid("HTTP 调用需要普通业务 method".into()));
    }
    let mut request = config
        .client_with_timeout(false)?
        .request(method, args.url(&config.endpoint)?)
        .headers(args.header_map()?)
        .bearer_auth(config.token());
    if let Some(data) = &args.data {
        request = request.body(data.clone());
    }
    if let Some(path) = &args.data_file {
        request = if path.as_os_str() == "-" {
            request.body(reqwest::Body::wrap_stream(ReaderStream::new(
                tokio::io::stdin(),
            )))
        } else {
            let file = tokio::fs::File::open(path).await?;
            request
                .header(
                    reqwest::header::CONTENT_LENGTH,
                    file.metadata().await?.len(),
                )
                .body(reqwest::Body::wrap_stream(ReaderStream::new(file)))
        };
    }
    let mut response = tokio::time::timeout(config.timeout, request.send())
        .await
        .map_err(|_| Error::OutcomeUnknown)?
        .map_err(|error| {
            if error.is_connect() {
                Error::Connect
            } else {
                Error::OutcomeUnknown
            }
        })?;
    let status = response.status();
    for (header, label) in [
        ("x-adx-environment-id", "Environment"),
        ("x-adx-environment-urn", "Target"),
    ] {
        if let Some(value) = response.headers().get(header).and_then(|v| v.to_str().ok()) {
            if value.chars().any(char::is_control) {
                return Err(Error::Unavailable);
            }
            output
                .write_all(format!("{label}: {value}\n").as_bytes())
                .await?;
        }
    }
    output.flush().await?;
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::OutcomeUnknown)? {
        output.write_all(&chunk).await?;
        output.flush().await?;
    }
    if !status.is_success() {
        return Err(Error::Server {
            status: status.as_u16(),
            message: "响应正文已输出；业务请求未自动重试".into(),
        });
    }
    Ok(())
}

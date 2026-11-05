//! Read-only resource accounting through the public Environment API.
use crate::{
    config::{Deployment, Role},
    Result,
};
use clap::{Args, ValueEnum};
use reqwest::{redirect::Policy, Certificate, Client, Url};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

type Quantities = BTreeMap<String, u64>;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
}

#[derive(Debug, Args, PartialEq, Eq)]
pub struct Options {
    /// Public HTTPS origin; defaults to the configured local loopback API Server.
    #[arg(long, env = "ADX_ENDPOINT")]
    pub endpoint: Option<String>,
    /// API Key file; alternatively set ADX_TOKEN.
    #[arg(long, env = "ADX_ADMIN_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
    /// PEM CA bundle for a private HTTPS endpoint.
    #[arg(long, env = "ADX_CA_FILE")]
    pub ca: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

#[derive(Deserialize, Serialize)]
struct Node {
    id: String,
    status: u32,
    capacity: Quantities,
    allocated: Quantities,
    #[serde(rename(deserialize = "allocatable", serialize = "remaining"))]
    remaining: Quantities,
}

#[derive(Deserialize)]
struct Response {
    items: Vec<Node>,
}

#[derive(Serialize)]
struct Summary {
    node_count: usize,
    accepting_count: usize,
    capacity: Quantities,
    allocated: Quantities,
    remaining: Quantities,
}

#[derive(Serialize)]
struct Report {
    units: BTreeMap<&'static str, &'static str>,
    summary: Summary,
    nodes: Vec<Node>,
}

impl Options {
    pub async fn execute(self, config: &Path) -> Result<()> {
        let deployment = if self.endpoint.is_none() {
            Some(Deployment::load(config)?)
        } else {
            None
        };
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => local_endpoint(deployment.as_ref().ok_or("deployment unavailable")?)?,
        };
        let url = resource_url(&endpoint)?;
        let token = self.token(deployment.as_ref())?;
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(self.timeout_seconds));
        if let Some(path) = &self.ca {
            let certificates = Certificate::from_pem_bundle(&std::fs::read(path)?)?;
            if certificates.is_empty() {
                return Err("CA bundle contains no certificates".into());
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build()?;
        let response = client.get(url).bearer_auth(token).send().await?;
        let status = response.status();
        let body = bounded_body(response).await?;
        if !status.is_success() {
            return Err(format!(
                "ADX resources returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            )
            .into());
        }
        let response: Response = serde_json::from_slice(&body)
            .map_err(|error| format!("invalid ADX accounting response: {error}"))?;
        let report = Report::new(response.items)?;
        match self.output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
            OutputFormat::Table => print!("{}", report.table()),
        }
        Ok(())
    }

    fn token(&self, deployment: Option<&Deployment>) -> Result<String> {
        let token = if let Some(path) = &self.token_file {
            std::fs::read_to_string(path)?
        } else if let Ok(token) = std::env::var("ADX_TOKEN") {
            token
        } else {
            let path = deployment
                .and_then(bootstrap_key)
                .ok_or("set --token-file or ADX_TOKEN to authenticate the resource query")?;
            std::fs::read_to_string(path)?
        };
        let token = token.trim();
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err("resource API Key must be non-empty ASCII without whitespace".into());
        }
        Ok(token.to_owned())
    }
}

fn local_endpoint(deployment: &Deployment) -> Result<String> {
    for service in &deployment.services {
        if service.role == Role::ApiServer
            && service
                .config
                .get("loopback_http")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            if let Some(listen) = service
                .config
                .get("listen")
                .and_then(serde_json::Value::as_str)
            {
                let address: SocketAddr = listen.parse()?;
                if address.ip().is_loopback() {
                    return Ok(format!("http://{address}"));
                }
            }
        }
    }
    Err("no local loopback API Server; set --endpoint or ADX_ENDPOINT to the public ADX HTTPS origin".into())
}

fn bootstrap_key(deployment: &Deployment) -> Option<&str> {
    deployment
        .services
        .iter()
        .filter(|service| service.role == Role::Coordinator)
        .filter_map(|service| service.config.get("bootstrap_credentials")?.as_array())
        .flatten()
        .find(|key| {
            key.get("administrator")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })?
        .get("key_file")?
        .as_str()
}

fn resource_url(endpoint: &str) -> Result<Url> {
    let mut url = Url::parse(endpoint)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(
            "resource endpoint must be an origin without credentials, path, query or fragment"
                .into(),
        );
    }
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err("resource endpoint requires HTTPS; HTTP is allowed only on loopback".into());
    }
    url.set_path("/api/environment/v1/resources");
    url.set_query(Some("view=accounting"));
    Ok(url)
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err("resource response exceeds 4 MiB".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn zeros() -> Quantities {
    ["CPU", "Memory", "Disk"]
        .into_iter()
        .map(|name| (name.to_owned(), 0))
        .collect()
}

impl Report {
    fn new(mut nodes: Vec<Node>) -> Result<Self> {
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        let mut summary = Summary {
            node_count: nodes.len(),
            accepting_count: nodes.iter().filter(|node| node.status == 0).count(),
            capacity: zeros(),
            allocated: zeros(),
            remaining: zeros(),
        };
        let mut ids = BTreeSet::new();
        for node in &nodes {
            if node.id.is_empty() || !ids.insert(&node.id) {
                return Err("resource response contains an empty or duplicate node ID".into());
            }
            for (total, values) in [
                (&mut summary.capacity, &node.capacity),
                (&mut summary.allocated, &node.allocated),
                (&mut summary.remaining, &node.remaining),
            ] {
                for (name, value) in values {
                    let count = total.entry(name.clone()).or_default();
                    *count = count
                        .checked_add(*value)
                        .ok_or("cluster resource total overflow")?;
                }
            }
        }
        Ok(Self {
            units: [
                ("CPU", "millicores"),
                ("Memory", "MiB"),
                ("Disk", "MiB"),
                ("GPU/NPU", "cards"),
            ]
            .into(),
            summary,
            nodes,
        })
    }

    fn table(&self) -> String {
        let mut rows = vec![vec![
            "RESOURCE".into(),
            "CAPACITY".into(),
            "ALLOCATED".into(),
            "REMAINING".into(),
        ]];
        rows.extend(resource_rows(
            &self.summary.capacity,
            &self.summary.allocated,
            &self.summary.remaining,
        ));
        let mut text = format!(
            "Cluster resources ({} nodes, {} accepting allocations)\n",
            self.summary.node_count, self.summary.accepting_count
        );
        text.push_str(&table(&rows));
        let mut rows = vec![vec![
            "NODE".into(),
            "STATUS".into(),
            "RESOURCE".into(),
            "CAPACITY".into(),
            "ALLOCATED".into(),
            "REMAINING".into(),
        ]];
        for node in &self.nodes {
            let status = if node.status == 0 {
                "accepting".into()
            } else {
                format!("blocked({})", node.status)
            };
            for values in resource_rows(&node.capacity, &node.allocated, &node.remaining) {
                let mut row = vec![node.id.escape_debug().to_string(), status.clone()];
                row.extend(values);
                rows.push(row);
            }
        }
        text.push('\n');
        text.push_str(&table(&rows));
        text.push_str("Allocated means scheduler reservations, including pending starts; it is not actual utilization.\n");
        text
    }
}

fn resource_rows(
    capacity: &Quantities,
    allocated: &Quantities,
    remaining: &Quantities,
) -> Vec<Vec<String>> {
    let names: BTreeSet<_> = capacity
        .keys()
        .chain(allocated.keys())
        .chain(remaining.keys())
        .collect();
    names
        .into_iter()
        .map(|name| {
            vec![
                name.escape_debug().to_string(),
                quantity(name, *capacity.get(name).unwrap_or(&0)),
                quantity(name, *allocated.get(name).unwrap_or(&0)),
                quantity(name, *remaining.get(name).unwrap_or(&0)),
            ]
        })
        .collect()
}

fn quantity(name: &str, value: u64) -> String {
    match name {
        "CPU" => format!("{}.{:03} cores", value / 1000, value % 1000),
        "Memory" | "Disk" if value >= 1024 => format!("{:.2} GiB", value as f64 / 1024.0),
        "Memory" | "Disk" => format!("{value} MiB"),
        _ => value.to_string(),
    }
}

fn table(rows: &[Vec<String>]) -> String {
    let mut widths = vec![0; rows[0].len()];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut output = String::new();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if index > 0 {
                output.push_str("  ");
            }
            output.push_str(cell);
            output.extend(std::iter::repeat_n(
                ' ',
                widths[index] - cell.chars().count(),
            ));
        }
        output.push('\n');
    }
    output
}

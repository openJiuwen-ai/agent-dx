//! Read-only inspection of ADX's committed Redis catalog.
use adx_deployment::config::Deployment;
use clap::{Parser, Subcommand};
use redis::{aio::MultiplexedConnection, FromRedisValue};
use serde_json::{json, Value};
use std::{error::Error, path::PathBuf, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(
    name = "adx-inspect",
    version,
    about = "Inspect committed ADX state without modifying Redis"
)]
struct Cli {
    /// The same deployment YAML used by adxctl on this host.
    #[arg(
        short,
        long,
        env = "ADX_DEPLOYMENT_CONFIG",
        default_value = "/opt/adx/config/deployment.yaml"
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show catalog generation, revision and record counts.
    Summary,
    /// Inspect registered nodes.
    Node {
        #[command(subcommand)]
        command: RecordCommand,
    },
    /// Inspect persisted Environment assignments and lifecycle results.
    Environment {
        #[command(subcommand)]
        command: RecordCommand,
    },
    /// Inspect reusable snapshot metadata.
    Snapshot {
        #[command(subcommand)]
        command: RecordCommand,
    },
}

#[derive(Subcommand)]
enum RecordCommand {
    /// List record IDs with a Redis cursor; pass next_cursor to continue.
    List {
        #[arg(long, default_value_t = 0)]
        cursor: u64,
        /// Redis SCAN work hint, not a strict page size.
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=100))]
        count: u32,
    },
    /// Show one record without environment variables or artifact locations.
    Get { id: String },
}

#[derive(Clone, Copy)]
enum Kind {
    Node,
    Environment,
    Snapshot,
}
impl Kind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Node => "node:",
            Self::Environment => "environment:",
            Self::Snapshot => "",
        }
    }
    fn key(self, control: &str) -> String {
        match self {
            Self::Snapshot => format!("{control}:snapshots"),
            _ => control.to_owned(),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("adx-inspect: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let deployment =
        Deployment::load(&cli.config).map_err(|_| "cannot load ADX deployment configuration")?;
    let client = redis::Client::open(deployment.redis_url)
        .map_err(|_| "invalid configured Redis endpoint")?;
    let mut connection = match tokio::time::timeout(
        QUERY_TIMEOUT,
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        _ => return Err("cannot connect to configured Redis".into()),
    };
    let control = format!("adx:{{{}}}:control:v1", deployment.namespace);
    let output = match cli.command {
        Command::Summary => summary(&mut connection, &control).await?,
        Command::Node { command } => {
            inspect(&mut connection, &control, Kind::Node, command).await?
        }
        Command::Environment { command } => {
            inspect(&mut connection, &control, Kind::Environment, command).await?
        }
        Command::Snapshot { command } => {
            inspect(&mut connection, &control, Kind::Snapshot, command).await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn read<T: FromRedisValue>(
    connection: &mut MultiplexedConnection,
    command: redis::Cmd,
) -> Result<T> {
    match tokio::time::timeout(QUERY_TIMEOUT, command.query_async(connection)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err("Redis read failed".into()),
        Err(_) => Err("Redis read timed out".into()),
    }
}

async fn summary(connection: &mut MultiplexedConnection, control: &str) -> Result<Value> {
    let mut header_command = redis::cmd("HGET");
    header_command.arg(control).arg("header");
    let header: Option<String> = read(connection, header_command).await?;
    let header = header.ok_or("ADX control header is missing")?;
    let header: Value = serde_json::from_str(&header).map_err(|_| "invalid ADX control header")?;
    let mut fields_command = redis::cmd("HLEN");
    fields_command.arg(control);
    let control_fields: u64 = read(connection, fields_command).await?;
    let mut snapshots_command = redis::cmd("HLEN");
    snapshots_command.arg(format!("{control}:snapshots"));
    let snapshot_records: u64 = read(connection, snapshots_command).await?;
    Ok(json!({
        "schema": at(&header, "/schema"),
        "shards": at(&header, "/shards"),
        "epoch": at(&header, "/epoch"),
        "generation": at(&header, "/generation"),
        "revision": at(&header, "/revision"),
        "control_fields": control_fields,
        "snapshot_records": snapshot_records,
    }))
}

async fn inspect(
    connection: &mut MultiplexedConnection,
    control: &str,
    kind: Kind,
    command: RecordCommand,
) -> Result<Value> {
    let key = kind.key(control);
    match command {
        RecordCommand::List { cursor, count } => {
            let mut scan = redis::cmd("HSCAN");
            scan.arg(&key)
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}*", kind.prefix()))
                .arg("COUNT")
                .arg(count);
            let (next_cursor, fields): (u64, Vec<(String, String)>) =
                read(connection, scan).await?;
            let ids: Vec<_> = fields
                .into_iter()
                .filter_map(|(field, _)| field.strip_prefix(kind.prefix()).map(str::to_owned))
                .collect();
            Ok(json!({"ids": ids, "next_cursor": next_cursor}))
        }
        RecordCommand::Get { id } => {
            valid_id(&id)?;
            let mut get = redis::cmd("HGET");
            get.arg(&key).arg(format!("{}{}", kind.prefix(), id));
            let raw: Option<String> = read(connection, get).await?;
            let raw = raw.ok_or("ADX record not found")?;
            let record: Value = serde_json::from_str(&raw).map_err(|_| "invalid ADX record")?;
            Ok(project(kind, &record))
        }
    }
}

fn valid_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
        return Err("invalid ADX record ID".into());
    }
    Ok(())
}

fn at(value: &Value, path: &str) -> Value {
    value.pointer(path).cloned().unwrap_or(Value::Null)
}

fn project(kind: Kind, record: &Value) -> Value {
    match kind {
        Kind::Node => json!({
            "id": at(record, "/node/id"),
            "shard_id": at(record, "/shard_id"),
            "address": at(record, "/address"),
            "proxy_address": at(record, "/proxy_address"),
            "capacity": at(record, "/node/capacity"),
            "available": at(record, "/node/available"),
            "device_count": record.pointer("/node/devices").and_then(Value::as_array).map_or(0, Vec::len),
            "scheduling_paused": at(record, "/scheduling_paused"),
            "session": at(record, "/session"),
        }),
        Kind::Environment => json!({
            "id": at(record, "/spec/id"),
            "tenant_id": at(record, "/spec/tenant_id"),
            "node_id": at(record, "/assignment/node_id"),
            "shard_id": at(record, "/assignment/shard_id"),
            "generation": at(record, "/assignment/generation"),
            "state": if record.get("result").is_some_and(Value::is_object) {
                at(record, "/result/state")
            } else {
                json!("Pending")
            },
            "revision": at(record, "/result/revision"),
            "runtime_id": at(record, "/result/runtime/id"),
            "resources_held": if record.get("result").is_some_and(Value::is_object) {
                at(record, "/result/resources_held")
            } else {
                json!(!record.pointer("/invalidated").and_then(Value::as_bool).unwrap_or(false))
            },
            "invalidated": at(record, "/invalidated"),
            "recovery_pending": at(record, "/recovery/pending"),
        }),
        Kind::Snapshot => json!({
            "id": at(record, "/id"),
            "state": at(record, "/state"),
            "revision": at(record, "/revision"),
            "tenant_id": at(record, "/template/tenant_id"),
            "source_node_id": at(record, "/source_node_id"),
            "storage": at(record, "/artifact/storage"),
            "reference_count": record.pointer("/references").and_then(Value::as_array).map_or(0, Vec::len),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_view_omits_user_configuration_and_checkpoint_location() {
        let record = json!({
            "spec": {"id": "env-1", "tenant_id": "team-a", "env": {"TOKEN": "secret"}},
            "assignment": {"node_id": "node-1", "shard_id": 0, "generation": 3},
            "result": {"state": "Running", "revision": 7, "runtime": {"id": "runtime-1"},
                "resources_held": true, "checkpoint": {"artifact": {"location": "private-path"}}},
            "invalidated": false,
        });
        let view = project(Kind::Environment, &record);
        assert_eq!(view["generation"], 3);
        assert_eq!(view["state"], "Running");
        assert!(!view.to_string().contains("secret"));
        assert!(!view.to_string().contains("private-path"));
    }

    #[test]
    fn ids_cannot_contain_control_characters() {
        assert!(valid_id("env-1").is_ok());
        assert!(valid_id("env-1\nother").is_err());
    }

    #[test]
    fn pending_environment_has_an_explicit_state_and_resource_ownership() {
        let record = json!({
            "spec": {"id": "env-2", "tenant_id": "team-a"},
            "assignment": {"node_id": "node-1", "shard_id": 0, "generation": 4},
            "result": null,
            "invalidated": false,
        });
        let view = project(Kind::Environment, &record);
        assert_eq!(view["state"], "Pending");
        assert_eq!(view["resources_held"], true);
    }

    #[test]
    fn snapshot_view_omits_artifact_location_and_template_configuration() {
        let record = json!({
            "id": "snap-1",
            "state": "Ready",
            "revision": 2,
            "template": {"tenant_id": "team-a", "env": {"TOKEN": "secret"}},
            "source_node_id": "node-1",
            "artifact": {"storage": "s3", "location": "private-location"},
            "references": [],
        });
        let view = project(Kind::Snapshot, &record);
        assert_eq!(view["storage"], "s3");
        assert!(!view.to_string().contains("secret"));
        assert!(!view.to_string().contains("private-location"));
    }
}

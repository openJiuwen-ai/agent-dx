//! Test-only OTLP gRPC trace receiver for local AFS white-box runs.
//!
//! The product runtime does not depend on this helper. Acceptance scripts build it
//! explicitly when they need a compact JSONL span stream for E2E diagnosis.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
    common::v1::{AnyValue, KeyValue, any_value},
    trace::v1::{ResourceSpans, ScopeSpans, Span, span, status},
};
use serde_json::{Map, Value, json};
use tonic::{Request, Response, Status, transport::Server};

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Test-only OTLP trace receiver that writes one JSON object per span")]
struct Args {
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = DEFAULT_MAX_OUTPUT_BYTES)]
    max_output_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)?;
    let state = Arc::new(ReceiverState::new(output, args.max_output_bytes));
    let service = TraceServiceServer::new(JsonlTraceReceiver {
        state: state.clone(),
    });

    println!(
        "afs-trace-receiver listening={} output={} max_output_bytes={}",
        args.listen,
        args.output.display(),
        args.max_output_bytes
    );

    Server::builder()
        .add_service(service)
        .serve_with_shutdown(args.listen, shutdown_signal())
        .await?;

    let summary = state.snapshot()?;
    eprintln!(
        "afs-trace-receiver stopped accepted_spans={} rejected_spans={} bytes={}",
        summary.accepted_spans, summary.rejected_spans, summary.bytes
    );
    Ok(())
}

struct JsonlTraceReceiver {
    state: Arc<ReceiverState>,
}

#[tonic::async_trait]
impl TraceService for JsonlTraceReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let lines = export_request_lines(request.into_inner()).map_err(|err| {
            Status::invalid_argument(format!("failed to convert OTLP traces to JSONL: {err}"))
        })?;
        self.state.write_batch(&lines).map_err(|err| match err {
            WriteBatchError::LimitExceeded { accepted, rejected } => Status::resource_exhausted(
                format!(
                    "trace receiver output limit exceeded; accepted_spans={accepted} rejected_spans={rejected}"
                ),
            ),
            WriteBatchError::Io(err) => {
                Status::internal(format!("failed to persist OTLP traces: {err}"))
            }
        })?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

struct ReceiverState {
    inner: Mutex<ReceiverInner>,
    max_output_bytes: u64,
}

struct ReceiverInner {
    writer: BufWriter<File>,
    bytes: u64,
    accepted_spans: u64,
    rejected_spans: u64,
}

#[derive(Debug, Clone, Copy)]
struct ReceiverSnapshot {
    bytes: u64,
    accepted_spans: u64,
    rejected_spans: u64,
}

#[derive(Debug)]
enum WriteBatchError {
    LimitExceeded { accepted: u64, rejected: u64 },
    Io(io::Error),
}

impl ReceiverState {
    fn new(output: File, max_output_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(ReceiverInner {
                writer: BufWriter::new(output),
                bytes: 0,
                accepted_spans: 0,
                rejected_spans: 0,
            }),
            max_output_bytes,
        }
    }

    fn write_batch(&self, lines: &[String]) -> Result<(), WriteBatchError> {
        let batch_bytes = lines.iter().try_fold(0_u64, |total, line| {
            let line_len = u64::try_from(line.len()).unwrap_or(u64::MAX);
            total.checked_add(line_len)?.checked_add(1)
        });
        let Some(batch_bytes) = batch_bytes else {
            return Err(WriteBatchError::LimitExceeded {
                accepted: 0,
                rejected: u64::try_from(lines.len()).unwrap_or(u64::MAX),
            });
        };
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| WriteBatchError::Io(io::Error::other("trace receiver lock poisoned")))?;
        if inner.bytes.saturating_add(batch_bytes) > self.max_output_bytes {
            let rejected = u64::try_from(lines.len()).unwrap_or(u64::MAX);
            inner.rejected_spans = inner.rejected_spans.saturating_add(rejected);
            inner.writer.flush().map_err(WriteBatchError::Io)?;
            return Err(WriteBatchError::LimitExceeded {
                accepted: inner.accepted_spans,
                rejected: inner.rejected_spans,
            });
        }
        for line in lines {
            inner
                .writer
                .write_all(line.as_bytes())
                .map_err(WriteBatchError::Io)?;
            inner.writer.write_all(b"\n").map_err(WriteBatchError::Io)?;
        }
        inner.writer.flush().map_err(WriteBatchError::Io)?;
        inner.bytes += batch_bytes;
        inner.accepted_spans = inner
            .accepted_spans
            .saturating_add(u64::try_from(lines.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn snapshot(&self) -> io::Result<ReceiverSnapshot> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("trace receiver lock poisoned"))?;
        inner.writer.flush()?;
        Ok(ReceiverSnapshot {
            bytes: inner.bytes,
            accepted_spans: inner.accepted_spans,
            rejected_spans: inner.rejected_spans,
        })
    }
}

fn export_request_lines(request: ExportTraceServiceRequest) -> serde_json::Result<Vec<String>> {
    let mut lines = Vec::new();
    for resource_spans in request.resource_spans {
        append_resource_spans(&mut lines, &resource_spans)?;
    }
    Ok(lines)
}

fn append_resource_spans(
    lines: &mut Vec<String>,
    resource_spans: &ResourceSpans,
) -> serde_json::Result<()> {
    let resource = resource_spans
        .resource
        .as_ref()
        .map(|resource| attributes_to_map(&resource.attributes))
        .unwrap_or_default();
    for scope_spans in &resource_spans.scope_spans {
        append_scope_spans(lines, &resource, scope_spans)?;
    }
    Ok(())
}

fn append_scope_spans(
    lines: &mut Vec<String>,
    resource: &BTreeMap<String, Value>,
    scope_spans: &ScopeSpans,
) -> serde_json::Result<()> {
    let scope = scope_spans.scope.as_ref().map_or_else(
        || json!({"name": "", "version": "", "attributes": {}}),
        |scope| {
            json!({
                "name": scope.name.as_str(),
                "version": scope.version.as_str(),
                "attributes": attributes_to_map(&scope.attributes),
            })
        },
    );
    for span in &scope_spans.spans {
        lines.push(serde_json::to_string(&span_record(resource, &scope, span))?);
    }
    Ok(())
}

fn span_record(resource: &BTreeMap<String, Value>, scope: &Value, span: &Span) -> Value {
    json!({
        "resource": resource,
        "service_instance_id": resource.get("service.instance.id").cloned().unwrap_or(Value::Null),
        "scope": scope,
        "name": span.name.as_str(),
        "trace_id": hex_id(&span.trace_id),
        "span_id": hex_id(&span.span_id),
        "parent_span_id": optional_hex_id(&span.parent_span_id),
        "start_time_unix_nano": span.start_time_unix_nano,
        "end_time_unix_nano": span.end_time_unix_nano,
        "attributes": attributes_to_map(&span.attributes),
        "events": span.events.iter().map(event_to_json).collect::<Vec<_>>(),
        "links": span.links.iter().map(link_to_json).collect::<Vec<_>>(),
        "dropped": {
            "attributes": span.dropped_attributes_count,
            "events": span.dropped_events_count,
            "links": span.dropped_links_count,
        },
        "kind": span::SpanKind::try_from(span.kind)
            .map(|kind| kind.as_str_name())
            .unwrap_or("SPAN_KIND_UNSPECIFIED"),
        "status": span.status.as_ref().map(status_to_json),
    })
}

fn event_to_json(event: &span::Event) -> Value {
    json!({
        "time_unix_nano": event.time_unix_nano,
        "name": event.name.as_str(),
        "attributes": attributes_to_map(&event.attributes),
        "dropped_attributes": event.dropped_attributes_count,
    })
}

fn link_to_json(link: &span::Link) -> Value {
    json!({
        "trace_id": hex_id(&link.trace_id),
        "span_id": hex_id(&link.span_id),
        "trace_state": link.trace_state.as_str(),
        "attributes": attributes_to_map(&link.attributes),
        "dropped_attributes": link.dropped_attributes_count,
        "flags": link.flags,
    })
}

fn status_to_json(status: &opentelemetry_proto::tonic::trace::v1::Status) -> Value {
    json!({
        "code": status::StatusCode::try_from(status.code)
            .map(|code| code.as_str_name())
            .unwrap_or("STATUS_CODE_UNSET"),
        "message": status.message.as_str(),
    })
}

fn attributes_to_map(attributes: &[KeyValue]) -> BTreeMap<String, Value> {
    attributes
        .iter()
        .filter_map(|attribute| {
            Some((
                attribute.key.clone(),
                any_value_to_json(attribute.value.as_ref()?),
            ))
        })
        .collect()
}

fn any_value_to_json(value: &AnyValue) -> Value {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(any_value::Value::IntValue(value)) => json!(value),
        Some(any_value::Value::DoubleValue(value)) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        Some(any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let object = value
                .values
                .iter()
                .filter_map(|entry| {
                    Some((entry.key.clone(), any_value_to_json(entry.value.as_ref()?)))
                })
                .collect::<Map<_, _>>();
            Value::Object(object)
        }
        Some(any_value::Value::BytesValue(value)) => Value::String(hex_id(value)),
        Some(any_value::Value::StringValueStrindex(value)) => json!(value),
        None => Value::Null,
    }
}

fn hex_id(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn optional_hex_id(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| hex_id(bytes))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let Ok(mut interrupt) = signal(SignalKind::interrupt()) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        let Ok(mut terminate) = signal(SignalKind::terminate()) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value;
    use tempfile::NamedTempFile;

    #[test]
    fn converts_ids_and_attributes_to_stable_json() {
        let span = Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            parent_span_id: vec![3; 8],
            name: "afs.meta.v1.DfsMeta.GetInode".to_owned(),
            start_time_unix_nano: 10,
            end_time_unix_nano: 20,
            attributes: vec![kv("rpc.method", "GetInode")],
            links: vec![span::Link {
                trace_id: vec![4; 16],
                span_id: vec![5; 8],
                ..Default::default()
            }],
            ..Default::default()
        };

        let record = span_record(
            &BTreeMap::from([(
                "service.instance.id".to_owned(),
                Value::String("node-a".to_owned()),
            )]),
            &json!({"name": "scope", "version": "", "attributes": {}}),
            &span,
        );

        assert_eq!(record["service_instance_id"], "node-a");
        assert_eq!(record["trace_id"], "01010101010101010101010101010101");
        assert_eq!(record["span_id"], "0202020202020202");
        assert_eq!(record["parent_span_id"], "0303030303030303");
        assert_eq!(record["attributes"]["rpc.method"], "GetInode");
        assert_eq!(
            record["links"][0]["trace_id"],
            "04040404040404040404040404040404"
        );
    }

    #[test]
    fn rejects_batches_that_exceed_output_limit_without_partial_write() {
        let file = NamedTempFile::new().expect("temp file");
        let state = ReceiverState::new(file.reopen().expect("reopen"), 4);

        let err = state
            .write_batch(&["too-large".to_owned()])
            .expect_err("batch should exceed output limit");
        assert!(matches!(err, WriteBatchError::LimitExceeded { .. }));
        let snapshot = state.snapshot().expect("snapshot");
        assert_eq!(snapshot.accepted_spans, 0);
        assert_eq!(snapshot.rejected_spans, 1);
        assert_eq!(snapshot.bytes, 0);
    }

    #[test]
    fn writes_complete_batches_and_counts_bytes() {
        let file = NamedTempFile::new().expect("temp file");
        let state = ReceiverState::new(file.reopen().expect("reopen"), 64);

        state
            .write_batch(&["{}".to_owned(), "{\"a\":1}".to_owned()])
            .expect("write batch");
        let snapshot = state.snapshot().expect("snapshot");
        assert_eq!(snapshot.accepted_spans, 2);
        assert_eq!(snapshot.rejected_spans, 0);
        assert_eq!(snapshot.bytes, 11);
        assert_eq!(
            std::fs::read(file.path()).expect("persisted complete JSONL batch"),
            b"{}\n{\"a\":1}\n"
        );
    }

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_owned())),
            }),
            key_strindex: 0,
        }
    }
}

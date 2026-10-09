//! Test-only OTLP receiver; proves cross-process spans without an external collector.
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use std::{fs::OpenOptions, io::Write, path::PathBuf, sync::Mutex};
use tonic::{Request, Response, Status};
struct Collector {
    file: Mutex<std::fs::File>,
}
#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| Status::internal("collector poisoned"))?;
        for resource in request.into_inner().resource_spans {
            let service = resource
                .resource
                .as_ref()
                .and_then(|r| r.attributes.iter().find(|a| a.key == "service.name"))
                .map(|v| format!("{:?}", v.value))
                .unwrap_or_default();
            for scope in resource.scope_spans {
                for span in scope.spans {
                    let hex = |bytes: Vec<u8>| {
                        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    };
                    writeln!(file,"{}",serde_json::json!({"service":service,"name":span.name,"trace_id":hex(span.trace_id),"span_id":hex(span.span_id),"parent_span_id":hex(span.parent_span_id)})).map_err(|e|Status::internal(e.to_string()))?;
                }
            }
        }
        file.flush().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().ok_or("address required")?.parse()?;
    let path = PathBuf::from(args.next().ok_or("JSONL output required")?);
    let collector = Collector {
        file: Mutex::new(OpenOptions::new().create(true).append(true).open(path)?),
    };
    tonic::transport::Server::builder()
        .add_service(TraceServiceServer::new(collector))
        .serve(addr)
        .await?;
    Ok(())
}

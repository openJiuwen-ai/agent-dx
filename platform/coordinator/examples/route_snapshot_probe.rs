//! Test-only reader for the Coordinator's published route snapshot.
use adx_protocol::{auth::Principal, control as pb};
use adx_transport::rpc::RpcClient;
use std::{error::Error, fs, io, time::Duration};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("route_snapshot_probe: {error}");
        std::process::exit(1);
    }
}

async fn run() -> ProbeResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !matches!(args.len(), 1 | 5) {
        return Err(
            invalid("usage: route_snapshot_probe ADDRESS [CA CERT KEY SERVER_NAME]").into(),
        );
    }
    let client = if args.len() == 5 {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(fs::read(&args[1])?))
            .identity(Identity::from_pem(fs::read(&args[2])?, fs::read(&args[3])?))
            .domain_name(&args[4]);
        RpcClient::from(tls)
    } else {
        RpcClient::network(Principal::Ingress)
    };
    let endpoint = client.endpoint(&args[0])?.timeout(Duration::from_secs(10));
    let channel = tokio::time::timeout(Duration::from_secs(10), endpoint.connect()).await??;
    let mut rpc = pb::route_service_client::RouteServiceClient::new(client.wrap(channel));
    let mut stream = rpc
        .watch_routes(pb::WatchRoutesRequest {})
        .await?
        .into_inner();
    let frame = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await??
        .ok_or_else(|| invalid("Coordinator closed the route stream before the full snapshot"))?;
    if !frame.reset {
        return Err(invalid("first route frame was not a full snapshot").into());
    }
    println!(
        "{}",
        serde_json::json!({
            "status": "passed",
            "reset": true,
            "epoch": frame.epoch,
            "revision": frame.revision,
            "published_routes": frame.upserts.len(),
        })
    );
    Ok(())
}

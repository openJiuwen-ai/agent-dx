//! Test-only RPC probe for a retired Adxlet session after quick process replacement.
use adx_protocol::{auth::Principal, control as pb};
use adx_transport::rpc::RpcClient;
use std::{error::Error, fs, io, time::Duration};
use tonic::{
    transport::{Certificate, ClientTlsConfig, Identity},
    Code,
};

type ProbeResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("stale_session_probe: {error}");
        std::process::exit(1);
    }
}

async fn run() -> ProbeResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !matches!(args.len(), 5 | 9) {
        return Err(invalid(
            "usage: stale_session_probe ADDRESS NODE_ID ENV_ID OLD_SESSION NEW_SESSION [CA CERT KEY SERVER_NAME]",
        )
        .into());
    }
    let [address, node_id, instance_id, old_session, new_session, ..] = args.as_slice() else {
        return Err(invalid("missing probe arguments").into());
    };
    if old_session == new_session || node_id.is_empty() || instance_id.is_empty() {
        return Err(invalid("probe needs distinct sessions and nonempty identities").into());
    }
    let client = if args.len() == 9 {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(fs::read(&args[5])?))
            .identity(Identity::from_pem(fs::read(&args[6])?, fs::read(&args[7])?))
            .domain_name(&args[8]);
        RpcClient::from(tls)
    } else {
        RpcClient::network(Principal::Node(node_id.clone()))
    };
    let endpoint = client.endpoint(address)?.timeout(Duration::from_secs(10));
    let channel = tokio::time::timeout(Duration::from_secs(10), endpoint.connect()).await??;
    let mut rpc =
        pb::coordinator_service_client::CoordinatorServiceClient::new(client.wrap(channel));
    let query = pb::GetEnvironmentRequest {
        environment_id: instance_id.clone(),
        caller: None,
    };
    let before = rpc
        .get_environment(query.clone())
        .await?
        .into_inner()
        .record
        .ok_or_else(|| invalid("Environment record is missing"))?;
    if before
        .assignment
        .as_ref()
        .is_none_or(|assignment| assignment.node_id != *node_id)
    {
        return Err(invalid("Environment is not assigned to the expected node").into());
    }
    let rejected = rpc
        .commit_environment(pb::CommitEnvironmentRequest {
            record: Some(before.clone()),
            node_session_id: old_session.clone(),
        })
        .await
        .err()
        .ok_or_else(|| invalid("retired session commit was accepted"))?;
    if rejected.code() != Code::FailedPrecondition {
        return Err(invalid("retired session returned the wrong gRPC status").into());
    }
    let after = rpc.get_environment(query).await?.into_inner().record;
    if after.as_ref() != Some(&before) {
        return Err(invalid("retired session changed the Environment record").into());
    }
    println!(
        "{}",
        serde_json::json!({
            "status": "passed",
            "grpc_code": "FailedPrecondition",
            "record_unchanged": true,
            "node_id": node_id,
            "instance_id": instance_id,
            "old_session": old_session,
            "new_session": new_session,
        })
    );
    Ok(())
}

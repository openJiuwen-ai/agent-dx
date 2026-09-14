use rrt_daemon::pb::health_client::HealthClient;
use rrt_daemon::pb::HealthCheckRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50088".into());
    let mut c = HealthClient::connect(addr).await?;
    let r = c.check(HealthCheckRequest {}).await?.into_inner();
    println!("healthy={} version={}", r.healthy, r.version);
    if !r.healthy {
        std::process::exit(1);
    }
    Ok(())
}

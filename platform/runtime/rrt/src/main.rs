#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("RRT_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:50088".to_string())
        .parse()?;
    println!("rrt listening on {addr}");
    rrt_daemon::serve(addr).await
}

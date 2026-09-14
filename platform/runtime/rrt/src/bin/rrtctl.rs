use rrt_daemon::pb::process_client::ProcessClient;
use rrt_daemon::pb::ExecRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    // Usage: rrtctl exec <addr> <cmd> [args...]
    if args.len() < 4 || args[1] != "exec" {
        eprintln!("usage: rrtctl exec <addr> <cmd> [args...]");
        std::process::exit(2);
    }
    let addr = args[2].clone();
    let cmd = args[3].clone();
    let cmd_args: Vec<String> = args[4..].to_vec();

    let mut client = ProcessClient::connect(addr).await?;
    let r = client
        .exec(ExecRequest {
            cmd,
            args: cmd_args,
            cwd: String::new(),
            env: Default::default(),
            timeout_ms: 0,
        })
        .await?
        .into_inner();

    // Output JSON shaped like the legacy SandboxInstance.execute response.
    let out = serde_json::json!({
        "returncode": r.exit_code,
        "stdout": String::from_utf8_lossy(&r.stdout),
        "stderr": String::from_utf8_lossy(&r.stderr),
    });
    println!("{out}");
    Ok(())
}

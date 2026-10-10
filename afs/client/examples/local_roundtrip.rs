use std::path::PathBuf;

use afs_client::{DiagnosticLocalClient, DiagnosticLocalClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let Some(socket_path) = args.next().map(PathBuf::from) else {
        eprintln!("usage: local_roundtrip <uds-path> [name]");
        std::process::exit(2);
    };
    let name = args
        .next()
        .unwrap_or_else(|| "local-roundtrip.bin".to_owned());
    let client =
        DiagnosticLocalClient::connect(DiagnosticLocalClientConfig::new(socket_path)).await?;
    let written = client.write(&name, 0, b"AFShello".to_vec()).await?;
    let read = client.read(&name, 0, 8).await?;
    println!(
        "{{\"ok\":{},\"written\":{},\"read\":\"{}\"}}",
        read == b"AFShello",
        written,
        String::from_utf8_lossy(&read)
    );
    Ok(())
}

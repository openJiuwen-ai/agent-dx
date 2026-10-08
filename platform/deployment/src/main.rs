use adx_deployment::{cli::Cli, Result};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    Cli::parse().execute().await
}

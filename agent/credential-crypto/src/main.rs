use adx_credential_crypto::{
    storage::{read_private, write_private, KeyFile, StorageError},
    Envelope,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Internal credential encryption service and offline administration")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    InitKeyring {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "connection-v1")]
        key_id: String,
    },
    RotateKey {
        #[arg(long)]
        keyring: PathBuf,
        #[arg(long)]
        key_id: String,
    },
    Rewrap {
        #[arg(long)]
        keyring: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

async fn run(arguments: Arguments) -> Result<(), StorageError> {
    match arguments.command {
        Command::Serve { config } => {
            let config = serde_json::from_slice(&read_private(&config, 65_536)?)
                .map_err(|_| StorageError::Invalid)?;
            adx_credential_crypto::service::serve(config).await
        }
        Command::InitKeyring { output, key_id } => KeyFile::initialize(&output, key_id),
        Command::RotateKey { keyring, key_id } => KeyFile::rotate(&keyring, key_id),
        Command::Rewrap {
            keyring,
            input,
            output,
        } => {
            let envelope: Envelope = serde_json::from_slice(&read_private(&input, 524_288)?)
                .map_err(|_| StorageError::Invalid)?;
            let updated = KeyFile::load(&keyring)?.rewrap(&envelope.context, &envelope)?;
            write_private(
                &output,
                &serde_json::to_vec_pretty(&updated).map_err(|_| StorageError::Invalid)?,
                false,
            )
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run(Arguments::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // Structured errors deliberately omit payloads, bearer identities and key material.
            eprintln!("adx-credential-crypto: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

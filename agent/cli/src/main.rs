use adx_cli::{execute, Cli, Configuration};
use clap::Parser;
use std::io::Write;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let adx_cli::Command::Ssh(ssh) = &cli.command {
        return match adx_cli::ssh::run(&cli, ssh).await {
            Ok(code) => code,
            Err(error) => {
                eprintln!("{error}");
                std::process::ExitCode::FAILURE
            }
        };
    }
    let result = async {
        let config = Configuration::load(&cli)?;
        let value = execute(&cli.command, &config).await?;
        let rendered = value.render(cli.output)?;
        writeln!(std::io::stdout().lock(), "{rendered}")?;
        Ok::<(), adx_cli::Error>(())
    };
    tokio::select! {
        result = result => match result {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                std::process::ExitCode::FAILURE
            }
        },
        signal = tokio::signal::ctrl_c() => {
            if let Err(error) = signal { eprintln!("无法监听中断信号：{error}"); }
            std::process::ExitCode::from(130)
        }
    }
}

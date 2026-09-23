use adx_cli::{execute, Cli, Configuration};
use clap::Parser;
use std::io::Write;

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("无法启动异步运行时：{error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(run(cli));
    // A pending stdin read cannot be cancelled; it must not hold up Ctrl-C or an early response.
    runtime.shutdown_background();
    code
}

async fn run(cli: Cli) -> std::process::ExitCode {
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
        if let adx_cli::Command::Http(args) = &cli.command {
            return adx_cli::http::execute(args, &config, &mut tokio::io::stdout()).await;
        }
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

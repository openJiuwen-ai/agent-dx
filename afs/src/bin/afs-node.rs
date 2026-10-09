//! afs-node 的进程入口：配置解析 → 观测初始化 → 服务装配与运行。
//! 入口只做组装，不写文件业务。`--print-config` 在创建监听/挂载之前返回，
//! 方便检查 TOML 与 CLI 合并结果；其成功不表示服务已经启动。

use afs::{
    config::{Cli, Config, Role},
    runtime::{self, BoxError, Observability},
};
use clap::Parser;
fn main() -> Result<(), BoxError> {
    let cfg = Config::resolve(Role::Node, Cli::parse())?;
    if cfg.print_config {
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }
    // processctl's default stop wait is 20s. Leave a margin for the caller
    // to observe failure. Register the independent signal observer before the
    // business executor so its polling cannot delay arming the process budget.
    let deadline = runtime::ShutdownDeadline::for_process(std::time::Duration::from_secs(15))?;
    let trigger = deadline.trigger();
    let executor = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let obs = Observability::new()?;
    // Guard 必须留到 run 返回之后，退出时才能刷新日志与导出尚未发送的 Trace。
    let guards = runtime::initialize(&cfg, "afs-node", &obs)?;
    let result = executor.block_on(afs::node::run_with_shutdown(cfg, obs, trigger.clone()));
    // Includes failure-path cleanup and runtime blocking workers. A running
    // worker is still owned; returning from run does not mean it was cancelled.
    trigger.arm();
    drop(executor);
    drop(guards);
    deadline.complete();
    result
}

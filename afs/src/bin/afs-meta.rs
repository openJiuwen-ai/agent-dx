//! afs-meta 的进程入口：配置解析 → 观测初始化 → 服务装配与运行。
//! 入口只做组装，不写文件业务。`--print-config` 在创建监听/挂载之前返回，
//! 方便检查 TOML 与 CLI 合并结果；其成功不表示服务已经启动。

use afs::{
    config::{Cli, Config, Role},
    runtime::{self, BoxError, Observability},
};
use clap::Parser;
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = Config::resolve(Role::Meta, Cli::parse())?;
    if cfg.print_config {
        println!("{}", serde_json::to_string_pretty(&cfg)?);
        return Ok(());
    }
    let obs = Observability::new()?;
    // Guard 必须留到 run 返回之后，退出时才能刷新日志与导出尚未发送的 Trace。
    let _guards = runtime::initialize(&cfg, "afs-meta", &obs)?;
    afs::meta::run(cfg, obs).await
}

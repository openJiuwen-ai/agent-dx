# Runtime stdout/stderr 日志

Adxlet 在 sandboxd `StartRequest` 中设置宿主机重定向路径。Jiuwenbox Supervisor 使用同一节点日志策略，通过创建请求顶层的 `stdout` / `stderr` 传递宿主路径。Jiuwenbox 在降权前打开文件，daemon、后台 Execd 和 workload 继承输出描述符，无需挂载日志目录。每个执行代次使用独立的 `<runtime-id>.out` 和 `<runtime-id>.err`；`runtime-id` 是 ADX 的执行 ID，不使用 sandboxd 生成的后端 ID。SDK 命令调用返回的 stdout/stderr 是另一条通道。

执行 ID 通常为 `<environment-id>-<generation>`，重启或恢复时追加 `-rN`。完整 ID（包括代次后缀）超过 240 字节时，主体使用 `rt_<environment-id的SHA256>_<generation>`，重启或恢复时仍追加 `-rN`，使执行 ID 保持在日志文件名上限内。公开的 Environment ID、name 和 namespace 保留原值。创建、资源预留、恢复及身份校验使用同一规则；已有记录和检查点中的原始执行 ID 仍可校验。日志文件名、回收清单和 Collector 的 `runtime_id` 都使用记录中的真实执行 ID。

节点部署配置中的 `services[].config.runtime_logs` 控制此目录和已终止日志的回收：

```yaml
runtime_logs:
  directory: /opt/adx/run/node/logs/runtime
  max_terminated: 50
  max_age_seconds: 86400
  max_total_bytes: 1073741824
  compress_after_seconds: 300
  interval_seconds: 60
```

直接启动 `adxlet` 而未配置该字段时，目录默认为 `/opt/adx/logs/runtime`。ADX 创建专用目录并设置权限为 `0700`；部署时应让 sandboxd 和日志采集器能以适当的身份访问它。Collector 样例读取未压缩的 `.out/.err`，保留 `runtime_id`、`stream` 和文件路径属性。终止后先保留原文件 300 秒供采集，再压缩为 `.gz`。外部采集不可用超过该窗口时，需从压缩文件补采，不能依赖实时采集自动补全。

每轮清理使用节点管理器保留的运行代次清单。节点恢复尚未完成时不执行回收；仍活动或尚未确认清理完毕的 runtime 日志不会清理。首次确认不在清单时将终止时间持久化到同目录的 `.terminated.json`。已终止日志最多保留 24 小时、50 对、压缩后合计 1 GiB；任一限制触发时从最早终止的 runtime 开始删除。运行中的日志不计入这些限额，目录空间需要单独监控。

**当前没有安全的运行中轮转。** sandboxd 的 runsc 路径以 `O_TRUNC` 打开日志并保持文件描述符；从外部 `copytruncate` 后继续写入会从旧偏移产生稀疏空洞。Firecracker 与 runsc 的写入方式也不同。要给运行中日志设置硬上限，需要 sandboxd 支持重新打开文件或提供独立的流式日志输出；在此之前不要对这些文件使用外部 logrotate。终止后的压缩与回收不依赖该能力。

standalone 的 `sdk` 验收使用真实 sandboxd 创建两个实例，随后在两个节点检查每个实例对应的 `<runtime-id>.out` 和 `<runtime-id>.err` 成对存在，结果记为 `runtime.host-stdout-stderr` 子用例。此项验证重定向链路；终止后压缩、24 小时和数量上限由 `platform/adxlet/tests/runtime_logs.rs` 验证。


## Jiuwenbox Supervisor

Jiuwenbox 服务需要支持创建请求顶层的 `stdout` / `stderr` 字段。
两字段是宿主绝对路径，由服务在进入命名空间和降权前打开；父目录由节点日志策略准备，
管理服务身份需有写入权限。新文件权限为 `0600`，已有文件追加写入。
重启到新的运行代次时使用新的 `<runtime-id>.out/.err`。

后台输出写入宿主文件，同步命令的输出仍由命令接口返回。未配置日志策略时不传递重定向字段。
Conch 没有对应的宿主 stdio 接口，该接线仅适用于 Supervisor。
Jiuwenbox 对运行中日志不做轮转，终止后的压缩与回收由上述 ADX 节点策略处理。

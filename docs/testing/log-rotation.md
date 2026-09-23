# 组件日志滚动与压缩

统一部署配置的 `logging` 控制 Supervisor 管理的各组件 stdout/stderr 文件日志。进程部署和 Pod 内运行使用同一套实现；由部署环境管理的 sandboxd，以及 guest 内实例日志，仍由实际日志生产方负责。结构化日志、Trace 和外部采集已接通，见 [日志采集](log-collection.md) 与 [Trace](distributed-traces.md)。

## 配置

```yaml
logging:
  enabled: true
  max_file_bytes: 104857600
  rotate_seconds: 86400
  compress: true
  max_files: 5
  max_age_seconds: 604800
  max_total_bytes: 1073741824
```

省略 `logging` 时默认关闭滚动并保持文件追加；统一部署示例显式启用。除 `enabled` 外，上例均为默认值。大小、数量和时间必须大于零；`rotate_seconds` 和 `max_age_seconds` 可设置为 `null`，分别关闭时间滚动和年龄清理。修改配置后重启 Supervisor 生效。

所有限制均按**每个组件**计算。`max_file_bytes` 限制当前文件；`max_files`、`max_age_seconds`、`max_total_bytes` 限制已关闭历史文件，按历史序号从旧到新清理；历史年龄依据源文件最后写入时间，压缩保留该时间。总容量使用压缩后的实际文件大小，不含当前文件和压缩临时文件。它是后台保留目标，不能作为磁盘配额：滚动、压缩追赶与故障期间可能暂时超过目标，需要预留工作空间。

## 文件和写入契约

日志目录为 `state_dir/logs`，当前文件 `<service-id>.log`，历史文件 `<service-id>.log.<20位序号>`，压缩后增加 `.gz`。读取时按序号读取历史文件，再读取当前文件。默认 `line_records=false` 按字节滚动，一行可能跨文件边界；还原时先拼接字节。启用 `line_records=true` 后只在完整行间滚动，单条记录可超过文件阈值，超长记录按 `max_record_bytes` 限制，详见 [采集契约](log-collection.md)。

Supervisor 接管子进程 stdout/stderr，通过专用读取线程写文件；达到大小或时间阈值时关闭当前文件、重命名并打开新的当前文件。没有新输出时也检查非空文件的时间阈值。stdout/stderr 共用通道，保留读取到的字节顺序，不为多个并发写入者另定义业务事件顺序。

每组件一个后台维护线程负责压缩与清理，通知队列容量为1。压缩只操作已关闭文件：生成 `.gz.tmp`、完成 gzip 和刷盘、重命名为 `.gz`，最后删除原文件。重启识别已有序号，遇到中断压缩从仍保留的原文件重试。清理只匹配该组件的历史文件，保护当前文件、临时文件及其他组件文件。

正常停止或子进程重启会先收尾输出和后台压缩，再交出最终日志状态。由组件自行写入的其他文件不在这个通道内；部署环境应采集这里的文件，避免再对相同文件执行外部 rename/logrotate。

Ingress/Relay 已有组件文件日志开关。统一 Supervisor 部署建议保持 `ADX_DATA_PLANE_LOG_STDOUT=true`（组件默认值），不设置 `ADX_DATA_PLANE_LOG_DIR`，由 Supervisor 管理文件。默认共进程时，Ingress 输出归入 API Server 的服务日志，Proxy 输出归入 adxlet 的服务日志；显式分进程时才分别生成 Ingress 或 Relay 的 Supervisor 日志。如果另行开启组件文件日志，必须使用不同目录，并单独配置其保留策略；不要让两个写入方操作同一个日志文件。

## 故障与可观测边界

`adxctl status` 的每个服务包含 `logging.error` 和 `logging.failed_bytes`；关闭本功能时该字段为 `null`。停止响应也返回最终服务日志状态。错误同时写入 Supervisor stderr，便于由进程托管环境收集。

- 压缩失败保留原文件，后续维护再次尝试；失败轮次不继续保留清理，防止删除唯一副本。
- 文件写入失败记录错误、继续读取输出，并在后续批次尝试重新打开文件。`failed_bytes` 是失败批次的字节数上界，可能包含已部分写入的前缀；它不承诺精确丢失字节数，也不会重放失败批次。
- 正常文件 I/O 仍可能反压输出；该实现没有无限内存队列，也不承诺磁盘卡死时业务完全不受影响。
- 状态错误保留到该次捕获结束，组件新启动建立新的捕获状态。需要部署环境持续收集错误，不能用重启后正常状态否定之前发生过 I/O 故障。
- 不保证断电、Supervisor 强杀或组件用户态缓冲尚未输出时的日志完整性。保留策略按配置主动淘汰历史日志；若需长期审计，应由外部系统采集和存储。

## 验证

组件测试验证 stdout/stderr 连续输出在多次大小滚动、压缩及重新启动后的逐字节一致性、空闲时间滚动、历史数量/年龄/容量清理及压缩发布失败恢复。Linux 使用 `/dev/full` 验证写入层返回 ENOSPC；这不等同于整盘耗尽或 I/O 挂起故障注入。

共享 E2E 驱动启用小阈值滚动，真实创建、执行、重启和删除后停止 Supervisor，解压两节点归档并检查最终日志健康，输出 `logging-node1.json` / `logging-node2.json` 以及 `[LOGGING PASS]`。本地与 K8s 使用相同检查。本地macOS组件16项、Linux组件17项、Clippy及53项驱动测试通过；ARM64双节点真实runc公共SDK七组全通过，两个节点分别40/6个gzip归档可读，无报告I/O错误或临时文件残留，清理无残留。Buildkite #18基础K8s七组及两节点40/6个gzip归档检查也通过，详见[验收记录](2026-09-16-logging-acceptance.md)。

用于结构化文件采集时，可以开启完整行滚动与延迟压缩。新增配置、超长记录及采集窗口的磁盘预算语义见[组件日志采集](log-collection.md)。

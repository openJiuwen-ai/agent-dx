# Node Manager 生命周期与降级

Node Manager 拥有实例串行状态机。Master 提供归属校验、Redis 持久化和路由发布；现存实例的生命周期请求由 API Server 缓存命中后直达节点。

## 运行策略

`InstanceSpec.lifecycle` 包含 `idle_timeout_seconds` 和可选 `restart`。HTTP 保留 `idleTimeoutSeconds`，新增 `restartPolicy`；Python SDK 支持 `RestartPolicy(max_attempts=3, initial_backoff_seconds=1, max_backoff_seconds=30)`。

- 空闲超时为 0 时不回收。启用后，运行实例的 RRT 请求／命令计数与 Node Proxy 流计数必须同时为 0，且活动版本在整个空闲窗口内未改变。采样失败或活动版本改变会重置窗口。Node Manager 执行删除。
- RuntimeBackend 确认执行退出后，节点先退役路由、确认旧执行清理，再标记 Failed。启用重启时，按照指数退避和累计重试上限重新申请本机资源，并分配新的执行 ID，Instance ID 与归属代次保持。
- `restart_attempts` 和 `restart_pending` 随实例结果写入 Redis／降级日志。进程重启不重置次数。显式删除取消重启。清理未确认时保留资源，禁止启动替代执行。
- 可配置 `rrt_health_failure_threshold`；省略时不以健康探测失败触发重启。连续失败达到阈值后，仍须先确认旧执行清理。健康检查只覆盖 RRT。

## 资源与指标

`resource_source` 选择以下之一，也可保留既有 `capacity_file`。两者不能同时配置。

```json
{"kind":"sandboxd","socket":"/run/sandboxd/resource.sock","valid_for_seconds":30}
```

sandboxd 的 `/resource` 通过 Unix Socket 提供 CPU 核数、内存／存储字节数和整卡设备 ID；节点先验证 sandboxd 健康 RPC，再转换为调度器毫核单位。健康检查覆盖 sandboxd 内部资源刷新有效期。

```json
{"kind":"auto","disk_path":"/var/lib/adx","valid_for_seconds":30}
```

自动模式读取当前 Linux 进程的 CPU 可用并行度、cgroup v1/v2 层级限制、cpuset、内存上限与目标磁盘可用空间。它不虚构 GPU/NPU 清单；需要设备清单的部署选择 sandboxd 源或带设备信息的观测文件。

采集只更新容量，不重置已有资源预留。首次无有效样本时等待；后续失败沿用旧样本至其过期，过期后关闭新准入。`pressure` 提供内存和磁盘高／低阈值，任一达到高阈值关闭准入，两者都降到低阈值以下才恢复；压力采集失败也关闭准入。

配置 `metrics_listen` 后提供 `/metrics`。实例 CPU／内存经 RuntimeBackend Stats 定时采样，并携带采样年龄；资源预留和节点准入也作为指标导出。日志由部署环境收集。

## SQLite 降级契约

`degradation_journal` 为可选本地 SQLite 路径。

1. 正常操作完成后，Node Manager 经 Master 提交结果。没有待补写日志时不创建本地数据库。
2. 只有 Master 不可用才尝试 SQLite 落盘，WAL 使用 FULL 同步。明确的权限、归属和版本拒绝不会降级。落盘失败不会报告 Journaled。
3. SQLite 保存有序、去重的待补写结果，不保存完整实例目录。暂停后恢复等连续操作必须按序补写，不能只保留最后状态。
4. 本地操作已完成不等于集群可见。内部 RPC 返回 Journaled；公共暂停／恢复 API 返回暂不可用并说明集群发布待完成。Edge 仍使用缓存路由，由 Node Proxy 复核本机绑定。
5. 当前节点进程在已对账的状态下可继续处理生命周期操作，心跳恢复后补写日志。补写成功后清理旧制品。
6. 节点进程重启后，先等 Master 完整目录。日志记录仅在归属／请求规格匹配且版本更新时补写；已提交结果和退役归属被清除。补写后再次读取目录，再与真实 backend 对账，最后开放生命周期和新准入。
7. Master 不可用时不能以 SQLite 缺记录为由清理或重建运行实例。SQLite 损坏也不能解释为空目录。

本地目录是降级可用性的条件；删除日志意味着丢失其中尚未进入 Redis 的结果。日志不是 Edge 的发现或路由来源。

## 验证

Rust 用例位于 Node Manager 的 `journal.rs`、`resources.rs`、`pause_resume.rs` 与 Master 的 `storage.rs` 测试。真实 Redis 用例覆盖 Redis 进程中断、节点持久化组件重建、有序暂停／恢复补写与重启次数保护。

`build/e2e/firecracker/sdk_node_lifecycle.py` 在显式指定的 Linux 测试部署中通过公共 SDK 验证后端异常退出重启、Master 失联空闲删除、SQLite 日志、节点进程重启等待、恢复补写和资源源断开。脚本中的 Redis／backend 查询是验收证据，生命周期正常操作仍使用公共 API。

后端首次启动使用独立的 `runtime_ready_timeout_seconds`（默认 120 秒）；日常 `rpc_timeout_seconds` 控制单次 RPC，避免后端资源初始化时间挤占日常故障降级预算。

## 本地验收（2026-09-16）

独立 Lima `adx-fc`，运行目录 `/var/lib/adx-pause-r7`，ARM64 原生 KVM + Firecracker，`package-v6`，源码包含未提交修改。继承同节点 checkpoint 5 项用例全部通过，新增 5 项真实运行时故障用例全部通过：

- 后端执行实例被删除后，按策略自动重启并使用新的执行身份。
- Master 被暂停时，节点空闲删除写入 SQLite，Redis 保留旧状态，未冒充全局提交成功。
- 此时重启 Node Manager，等待 Master 对账，保留已有运行实例。
- Master 恢复后，重放删除日志并恢复已有实例路由，SQLite 待同步记录归零。
- 资源采集 socket 暂时不可访问，超过有效期关闭准入；采集恢复后重新开放，已有实例继续运行。

同一轮最终目录中实例全部 Deleted，资源已释放，sandboxd 清单与 checkpoint 目录为空，服务正常停止。
证据位于 `out/ci/pause-resume/fc-r7/evidence/`，包括 `sdk/result.json`、`lifecycle/result.json`、`catalog-final.json`、`inventory-final.txt` 和 `result.json`；构建/部署完整日志位于 `out/ci/stage-2/{linux-2,package-2,fc-r7}.log`。回归日志为 `rust-5.log`（141 项测试，Clippy 通过）及 `clients-4.log`（Go 检查及 138 项 SDK 测试）。这些是本地证据，Kubernetes 验收尚未包含新增生命周期场景。

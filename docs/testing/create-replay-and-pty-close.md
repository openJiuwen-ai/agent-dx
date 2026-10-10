# 创建重放、删除目录与 PTY 关闭

2026-10-09 核对 `community-refactor-sync` 工作树。以下记录已实现修复，并区分历史诊断、组件测试和本轮真实端到端回归。

## 已删除名称立即复用

API Server 原来把所有生命周期 RPC 成功结果都写回实例目录，包含资源已释放的 Deleted。随后的同名创建命中旧规格，先比较参数再检查状态，导致改规格重建报 AlreadyExists。删除订阅增量先到、RPC 结果后到时，还会重新插入旧记录。

`EnvironmentDirectory::put` 现在先比较 `(generation, revision)`：确认 Deleted 且 `resources_held=false` 时移除旧项，不缓存完整 Deleted 记录。如果目录中已有更高版本，旧删除结果不能影响它。目录未首次同步时，删除旧项并不把整个目录变成可查询状态；其他缺失项仍返回 Unavailable。

验证边界：新增目录测试覆盖删除后立即缺失、订阅已移除后迟到的删除应答、旧删除不能移除新 generation；真实 TCP/gRPC 删除测试覆盖 Coordinator 查询不可用时仍按已有归属执行并清除旧缓存。API Server 组件测试通过；cn-north-4 的新 API Server 测试入口连续完成 3 次同名删除后立即改 CPU／内存规格重建，均成功。

## PTY 固定 10 秒的根因

Execd 的 `handle_conn` 是 HTTP keep-alive 循环。`handle_one_request` 在 WebSocket Upgrade 后借用同一 TCP socket，PTY 发出 exited/Close 后返回，外层仍等待下一条 HTTP 请求，没有释放 TCP。command watch 也使用相同结构。

旧版本 cn-north-4 的定向诊断记录表明：

- 两次业务均正常退出，客户端在进入关闭后约 33–36 ms 收到 WebSocket Close 帧。
- 后台任务停在 `websockets.Connection.close` 等待 `connection_lost_waiter`，尚未收到 TCP 断开。
- 即使不立即调用 SDK `close()`，传输仍约 10 秒后才退出。因此 SDK 的线程 join 是等待位置，不能把它当成根因。
- 仅作为诊断，将库的 `close_timeout` 改为 0.25 秒后，等待变成约 252 ms。这证明原 10 秒来自 WebSocket 的关闭超时；缩短超时不是本次修复。
- 集群明文入口返回 HTTP 426，因此该路径没有形成有效对照。

两个真实 Socket 回归直接绕过 SDK 等待逻辑，确认旧 Execd 在 WebSocket 已结束后仍不返回 TCP EOF；修复前两项超时失败，其余 13 项 HTTP 测试通过。

修复是在 `/pty` 与 `/commands/watch` 升级分支设置 `close_after_response`，使升级会话结束后退出整个连接任务，不再进入 HTTP keep-alive。未修改 SDK 的关闭超时。修复后 Execd 90 项库测试通过。公开 SDK 直连真实修复版 Execd 的 3 次自然退出关闭耗时为 0.273、0.219、0.222 ms，主动关闭运行中 PTY 为 0.331 ms，4 次后台线程均已结束。此验证未经过 sandboxd 或 Gateway。Workspace Clippy（全部 targets/features、`-D warnings`）、格式和文档检查通过。

证据目录：`out/ci/create-cache-pty-20261009/`。`pty-diagnosis{,-followup}.json` 是旧集群诊断，`execd-red-tcp.log` 与 `execd-green.log` 是本地真实 Socket 的红绿测试。本轮使用新 Execd 镜像更新 cn-north-4 的 4 个节点，并通过真实 runsc Sandbox、Gateway 和公开 ADX SDK 验证 30 次 PTY 自然退出关闭：最大 10.40 ms；未调整 SDK 的关闭超时。

## 创建重放缓存

创建记录分成两个集合，使用同一个短时索引锁协调查找与迁移；每个请求自身仍通过 Mutex 串行执行：

| 集合 | 保留与退出规则 | 容量行为 |
|---|---|---|
| 正在执行／结果未知 | 执行期间固定租户、Request ID、Environment ID 和规格摘要；明确结果移入完成缓存，未知结果超过保留窗口回收 | 不使用 `cache_entries` 拒绝新创建，也不因完成缓存淘汰而丢失身份 |
| 已完成响应 | 从完成时开始保留 600 秒；读取不延长 TTL | 最多 `cache_entries` 条，满时淘汰最近最少使用的完成项，新创建继续接收 |

新请求不再扫描整个创建表，也不再返回 `create replay budget exhausted`。`cache_entries` 对创建只控制已完成响应缓存大小，不是创建并发或资源准入上限。实际资源不足和节点准入失败仍使用原资源错误契约。

同 Request ID 的并发请求共享同一操作锁与执行结果；不同参数重用该 ID 报冲突。结果未知的重试先查 Coordinator 最新归属：已运行则复用原结果；查不到则继续以原 Request ID、Environment ID 和规格进入原子归属链路，不能换 ID。单次 NotFound 不证明先前在途写入不可能完成。未知结果不会被 API Server 当作 Environment Failed，也不会由 API Server 释放节点资源；adxlet 的 Start 失败收敛契约保持。

未知请求采用 `create_unknown_retention_seconds`，默认 600 秒，必须大于零。窗口从首次未知结果开始，后续仍无法确认的重试不延长窗口。API Server 每隔 `min(配置保留秒数, 60)` 秒回收一次到期上下文，因此空闲记录最长在窗口结束后的一个扫描周期内回收。已取消的调用如果没有到达结果处理，也按创建上下文产生时间回收；有执行持锁或已取得共享身份等待执行的请求不被 GC。

GC 只删除 API Server 的请求上下文及相匹配的残留名称锁，不访问 Coordinator，不改变 Environment 状态、不释放资源或删除后端。它不依赖实例订阅是否到达，也不把“过期”解释为“实例一定不存在”。未新增后台对账或并发拒绝预算；未知请求窗口内的数量不受完成缓存容量限制。

完成响应过期／淘汰、未知请求 GC 或进程重启后，不再承诺请求级历史去重或参数绑定。重试按当前 Environment 归属与规格处理：实例仍存在时通过本地目录或原子创建链路收敛到已有归属；已删除的实例名称可以重新创建。调用方不能把过期旧请求当作无限期历史回执。

部署配置示例：

```yaml
services:
  - id: apiserver
    role: apiserver
    config:
      # 合并到现有 API Server 配置；这里只展示重放相关字段。
      cache_entries: 10000
      create_unknown_retention_seconds: 600
```

### 定向回归

`gateway/apiserver/tests/create_replay.rs` 启动真实 TCP/gRPC 服务、生产 Clients 与 SandboxService，完成缓存容量固定为 1：

- 连续 12 个不同创建均成功；完成项被淘汰后重试仍复用已有归属，不产生第二个后端执行。
- 多个创建返回未知结果时仍接收新名称；重试先查权威结果，不额外调用 CreateEnvironment；同 Request ID 改参数报冲突。
- 首个创建跨过 GC 窗口仍未结束时，其身份不被回收；第二个独立创建仍进入执行，同请求并发重试只共享一次创建结果。
- 未知请求到期后无需客户端重试即可回收，GC 不查询权威状态，原已运行实例保持不变；窗口结束后原 Request ID 不再绑定旧参数。
- 单元回归覆盖已取得共享身份的等待者、取消后遗留上下文以及从首次未知结果开始的保留时间。

服务夹具模拟应答错误，不是真实网络丢包或 sandboxd 执行。运行入口：`cargo test --locked -p adx-apiserver --test create_replay`。证据保存在上述目录的 `replay-*.log`。本轮 cn-north-4 的独立 API Server 测试入口配置 `cache_entries: 1`，连续创建并保留 12 个 Sandbox，全部成功执行命令；8 个同名并发创建收敛到一个 Environment。随后原规模压力回归以容量 10,000 完成 19,700 次生命周期创建，未再触发缓存容量拒绝，详见[压力回归报告](2026-10-09-cn4-pressure-regression.md)。未知上下文自动 GC 的精确时间与保护条件仍由组件测试看护。

## 生命周期操作表容量

`Operations` 原来把尚未确认结果的生命周期请求也计入 `cache_entries`。容量设为 1 时，无恢复点的 Reload 返回 Unavailable 并保留重试身份，之后删除会被错误地返回 429。现在移除该条数准入限制，保留原操作 ID、Assignment、revision 与串行操作锁。成功或明确失败仍释放操作上下文，未知结果按原身份重试；本项不改变 Environment 状态或节点资源账本。

`create_unknown_retention_seconds` 只覆盖创建上下文。生命周期操作表本轮仍沿用原保留规则，尚无自动过期回收；不能将创建 GC 的验证结果推广到该表。

新增真实 TCP/gRPC 回归将缓存容量固定为 1，连续 12 个不同 Reload 请求返回 Unavailable 后仍能删除；首次运行实际出现 ResourceExhausted，证据为 `operation-capacity-red.log`。修复后 API Server 67 项测试与 Workspace Clippy 全部通过。cn-north-4 在容量为 1 时连续 12 次无 checkpoint 的 Reload 返回 false，随后仍可删除、新建并执行命令；与此前 4 项专项合计 5 项全部通过。

## 本轮端到端证据

2026-10-09 从当前未提交源码构建 Linux ARM64 与 x86_64 测试产物，证据目录为 `out/ci/replay-e2e-20261009/`。原有无关工作树改动保持不变。

- 本地 Lima Linux ARM64、真实 sandboxd/runc 双节点 standalone：标准 11 组完成，并通过 `create-response-cut`、`create-unknown-query`，合计 13 组、42 个具名子场景；所有结果均无清理错误。
- 首轮本地停机组因为驱动仍匹配旧 HTTP Span 名称失败。当前驱动按接口 Span 名称、Execd 服务、父 Span 和两层入口父子关系校验；其余 10 组无需重跑，停机组修正后通过。生产 Trace 未被关闭，断言未被删除。
- cn-north-4/akernel：新 API Server 独立测试入口与四个新 Execd 节点，创建缓存、同名改规格重建、并发同名收敛、PTY 关闭 4 项专项通过；专项前后 Redis 实例记录和 held 预留均为零。
- 测试镜像 digest：`sha256:4f98767a85fc9607373397d7c588d30749dc6453824887fe943f585fb0a22073`。镜像在既有部署镜像上替换本轮 API Server、Execd 和内置 runtime rootfs；Coordinator、adxlet、sandboxd 沿用已部署版本。API Server 二进制 SHA256 已与构建机产物逐字核对。
- 生命周期容量修复后重新构建 API Server，本地标准 11 组再次全部通过，结果位于 `local-results/standalone-v4/`。cn-north-4 使用 API Server 镜像 `sha256:764138bb124494c28c00afc4802fbd8fd70edffedc637f3fa8fab3565fcffc83`，与已有修复版 Execd 配合；部署二进制 SHA256 已与构建机产物核对，配置仍为 `cache_entries=1`。
- 本地结果位于 `local-results/`，首轮集群专项位于 `cluster-focused.json`，修复后的 5 项专项位于 `cluster-focused-v3.json`。SDK full 首轮客户端缺少发行包元数据；第二轮正确安装发行包后暴露生命周期表错误的容量拒绝，该失败证据保留。第三轮使用修复版 API Server、正式 wheel 与容量 1 配置完成 31 组：24 组通过、2 组部分跳过、5 组全部跳过，0 失败。unittest 计数为 63 项通过、10 项跳过；另外包括示例脚本组。耗时 356.45 秒，结果位于 `cluster-sdk-full-v3/summary.json`。
- SDK full 的跳过项为异构 runtime 节点 1 项、S3 2 项、GPU 1 项、维护注入 1 项、进程故障 2 项、节点故障 1 项、checkpoint 故障 2 项。未提供对应环境或故障注入钩子，不记为通过。SDK full 前后 Redis 实例记录和 held 均为零；没有新增 Failed 预留或残留执行归属。
- cn-north-4 追加 ADX 原生 SDK 数据面验证，18 个具名子场景全部通过，覆盖 command、shell、files、PTY、Host/TLS 端口路由与 reverse tunnel；结果为 `cluster-native-functional.json`。该组不含独立 idle／动态网络策略／亲和用例，不能将其结果扩展到这些能力。
- 最终 `sbox list` 确认四个节点无残留后端，Redis 实例目录和预留均为空，证据为 `backend-final.json`、`redis-final.json`。本轮独立测试 API Server、ConfigMap、Service、客户端 Pod 已清理，节点 DaemonSet 更新策略恢复原值。正式 Coordinator 和 API Server 入口未更新；四个节点保留本轮已验证的 Execd 测试镜像。源码改动仍未提交或推送。

本轮未执行 Firecracker、Kata 或真实 GPU/NPU 验收。集群故障注入需要独立测试配置与恢复钩子，不能将 SDK 中跳过的用例记为通过。

## cn-north-4 复跑

2026-10-09 在 `akernel` 命名空间复用上轮修复镜像，重新建立独立 API Server 与客户端测试入口，串行复跑相同三套用例。没有重新构建或上传镜像，正式 Coordinator、API Server 与节点部署保持本轮开始时的版本。

API Server 镜像 digest 为 `sha256:764138bb124494c28c00afc4802fbd8fd70edffedc637f3fa8fab3565fcffc83`，部署二进制 SHA256 与构建产物一致，配置仍为 `cache_entries=1`。四个节点均就绪，测试前后重启次数均为零。

| 用例集 | 本次结果 |
|---|---|
| 小缓存、删除重建、并发创建、PTY 专项 | 5/5 通过；30 次 PTY 关闭最大 10.09 ms |
| AKernel SDK full | 31 组完成：24 组通过、2 组部分跳过、5 组全部跳过；unittest 计数 63 通过、10 跳过、0 失败；耗时 372.08 秒 |
| ADX 原生 SDK 数据面 | 18/18 具名子场景通过，0 跳过、0 失败 |

10 项跳过的环境依赖与上一轮相同：异构 runtime 1 项、S3 2 项、GPU 1 项、维护注入 1 项、进程故障 2 项、节点故障 1 项、checkpoint 故障 2 项；这些场景尚无本次执行证据。

基线、SDK 前后和最终 Redis 实例记录、held 均为空。最终逐节点执行 `sbox list`，四个节点的后端数均为零。独立测试 Deployment、Service、ConfigMap 与客户端 Pod 清理完成。

证据目录为 `out/ci/cn4-regression-20261009-r2/`，与上一轮分开保存：`regression.log`、`campaign-identity.json`、`cluster-focused-v3.json`、`cluster-sdk-full-v3/summary.json`、`cluster-native-functional.json`、`redis-final.json`、`backend-final.json`。

另在 `out/ci/cn4-pressure-regression-20261009/` 复跑原规模生命周期、常驻加流转及三种七类复合负载。C64 完成 12,913 次事务、零失败；PTY 在三种 profile 下共 300 次全部成功且无客户端拒绝。checkpoint 单路在途仍导致 288 次未提交，目标速率尚未全部达成。逐项对照与最终清理证据见[压力回归报告](2026-10-09-cn4-pressure-regression.md)。

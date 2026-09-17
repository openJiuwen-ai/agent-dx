# 节点本地优先创建与原子归属确认

核对日期：2026-09-17。创建链路已接入 API Server、Node Manager 和 Master。
当前改动位于 `fix/atomic-instance-claim`，基于 `6b2d30b`；正式 K8s 验收结果不沿用该基线的成功记录。

## 开启方式与调度语义

API Server 配置 `create_mode: "local_first"` 开启；默认 `"central"` 保持中心创建路径。
统一部署配置中放在 `role: "api-server"` 对应的 `config` 下；组件更新后重启生效。
所有控制面组件需要使用同一版本的 Instance RPC。

| 路径 | 放置与排队 |
|---|---|
| `central` | Global 轮转进入 ShardScheduler，按租户轮转、租户内优先级/FIFO 排队；Filter/Score 执行 Pack/Spread 和放置规则 |
| `local_first` 本地成功 | API Server 轮转一个可用入口；节点暂留资源，Master 对该节点检查全部硬约束并原子确认归属；不做跨节点 Score、不进入中心队列 |
| 本地不足或硬约束不满足 | 释放可确认属于本次申请的暂留，使用原 Instance ID/规格进入 ShardScheduler；中心排队和评分在这里生效 |

因此本地优先不提供集群级 Pack/Spread、软偏好最优选择或全局队列公平性承诺。
已进入中心队列的同 ID 请求继续使用原队列，不通过本地重试插队。

## 创建链路

```text
API Server：WatchNodes 全量目录 → 轮转入口
  → Node Manager：CreateLocalInstance，检查租户、节点会话与生命周期准入门
  → 共用 Mutex<Admission>：一次性暂留标量资源及 GPU/NPU 卡
  → Master：ClaimInstance
      mTLS 节点身份 + CallerContext 租户 + 实时心跳/会话 + 硬约束
      Redis CAS 写入唯一归属与 generation
      同一协调锁内导入内存资源/设备账本、调度快照与 generation
  → Node Manager：暂留 token 转交 Assignment，复用该 Instance 串行控制器
  → sandboxd → RRT 就绪 → 本机路由绑定 → Master 提交 Running → 返回成功
```

快照创建先通过 `PrepareCreate` 解析模板资源和环境，再按相同链路申请。
Master 持有恢复引用；CAS 同时比较快照原始记录，防止引用检查和归属登记之间的竞态。

Master 的 `ForwardCreate` 仅接受受信节点调用；原有中心 `CreateInstance` 仍接受 API Server。
Node Manager 的未调度入口仅接受受信 API Server，已分配执行入口仅接受 Master。
租户由已认证的调用上下文传递，节点执行及已存在实例的复用均检查归属。

## 唯一归属与资源规则

`Session::claim(spec, LocalClaim {node_id, node_session_id, devices})` 与中心 `reserve`
共用 Redis 控制 header 和 Instance 字段；generation 在 Rust 中按 `u64` 计算。

| 结果 | 含义和动作 |
|---|---|
| `Owned(record)` | 当前节点拥有尚未提交结果的首次创建，包括重复申请；使用同一 Assignment/控制器启动，不重复预留或启动后端 |
| `Existing(record)` | 其他节点拥有，或已有结果、失效、恢复流程；不作为启动许可，释放本次暂留并复用已有结果/交给中心查询处理 |
| 规格/租户冲突 | Master RPC 返回 `AlreadyExists`，不会覆盖旧记录；节点只释放匹配当前 token 的暂留 |
| 会话/设备选择冲突 | 拒绝启动；不能把此错误当作未知写入从未成功的证明，等待同身份重试或权威对账 |
| `Unavailable`/超时 | 结果未知，保留 Instance ID、规格、设备选择和暂留；不得换 ID 重建 |

同节点入口按 Instance ID 串行化；不同调用共用同一暂留 token。标量资源和卡全部成功才更新 Admission。
暂留转为 `instance_id-generation` 执行记录后，迟到的 token 释放不能影响执行占用。
新增加的暂留表只保存规格、token 和设备选择；资源计量仍只在现有 Admission 内。

本地暂留尚未登记时，中心可能已把另一个请求分配给该节点。已确认的 Assignment 保持不变，
中心重试本机准入，等待暂留收敛；本地申请发现中心账本已不足则释放 token 并回退。
不会为同一已确认实例改 ID 或重复扣除本机资源。

## 结果未知与恢复

1. Node Manager 先使用同一 claim 重试；调用结束仍未知的暂留，由后台任务按现有报告间隔继续处理。
2. Master 对写入超时标记协调状态需要恢复，停止使用可能漏项的内存账本。
3. 在前一个写入生产者已经返回后，CAS 推进控制 header，再读取权威快照。先前迟到的 CAS
   要么发生在这个屏障前，要么因旧 header 不匹配失败；一次 `NotFound` 查询本身不提供该保证。
4. 重建全部分配和 generation，并恢复本进程尚未持久化的等待请求。中心 reserve 遇到竞争时也重建整个预计算轮次。
   重复 claim 读到已释放资源的终态记录时，也会移除内存旧分配，处理提交已落盘但先前应答未知的情况。
5. 心跳/周期节点检查、后续创建和提交可触发该恢复，不依赖原 HTTP 请求再次到达。
6. Node Manager 重启或重新对账时，以 Master 完整目录重建 Admission，再清除旧暂留元数据；主端不可用时不开启生命周期操作。

后台任务跳过仍在处理中的入口；权威恢复和暂留重试使用现有生命周期门及每实例串行控制器。
未确认暂留未解决时，节点停机清理不能报告成功。存储 claim 不转移既有归属，也不会复活终态实例。

## API 节点目录与失败处理

`WatchNodes` 为 API Server 提供带 Master epoch、节点进程会话和有效期的全量可用目录。
只包含已完成对账、心跳未超时且允许分配的节点；每秒刷新，目录有独立的短有效期。
断流/异常后清空并重订阅；目录过期或为空时使用中心创建。节点会话始终在实际入口再次校验。

入口连接失败、超时或会话前置条件失效时，API Server 使用剩余预算向 Master 提交**同一 ID 和规格**。
Master 将它与仍在途的 claim 串行协调。API 不把结果未知缓存成最终失败；保留的创建操作可继续使用原规格重试。
这里保证同 Instance ID 的收敛，不提供跨 API Server 的匿名 HTTP 请求 ID 全局持久化服务。

## 验证分层

- `master/tests/storage/claims.rs`：9 项新增存储用例，真实 Redis/AOF；跨节点和中心 reserve 竞争、会话、设备、快照引用及超过 `2^53` 的 generation。
- `master/tests/scheduling.rs`、`node-manager/tests/lifecycle.rs`：账本导入、中心队列去重、generation、共享暂留、迟到释放和标量/整卡原子性。
- `master/tests/rpc/local_first.rs`：真实 Redis/mTLS，双节点/同节点/中心并发、心跳与调用身份、延迟写入后的屏障重建、无人重试时的后台收敛、两侧资源账本。
- `build/ci/local_first_http.py`：实际 Rust API HTTPS → Node/Master RPC，目录轮转、4 个并发请求、不同 HTTP 请求 ID 复用同 Instance、规格/租户冲突、删除后资源归零。
- `build/e2e/local_first.py`：独立 `local-first` 基础验收组；切换 API 配置后通过安装的 SDK 创建、实际 RRT 命令和删除，检查本地 claim 日志；随后恢复中心模式。Docker/K8s 共用该组。

真实 Redis 的 `CLIENT PAUSE ... WRITE` 验证延迟写入/未知结果；RPC 截止时间用例验证调用被取消后仍只执行一次。
迟到终态用例在实际后端清理后通过 Session 注入未被协调器观察的提交，不能把它当作真实网络丢包。存储层原“丢失应答”测试只是丢弃返回值。
RPC/HTTPS fixture 中的 RuntimeBackend 是测试实现；另行完成真实 sandboxd/runc/RRT 新制品本地双节点8组验收，见 [端到端报告](2026-09-17-local-first-e2e.md)。正式K8s验收待执行。
日志位于当前工作树 `out/ci/local-first/`。最终计数见同目录各套件 `result.json` 和测试日志。

本轮最终通过计数、二进制身份与未运行范围见 [接线验证记录](2026-09-17-local-first-create.md)。

# 【建议方案】ADX 发布管理入口统一

日期：2026-11-16。实施前基线：`8fa64c1c`；当前行为以本次修订的源码为准。

状态：已实现并完成本地聚焦验证；adxlet 进程级及独立 Relay publisher 专项尚未完成集群验收。同日 Node0 空 Pod 验证的独立边界见第 9 节。提交、推送状态以 Git 记录为准。

当前实现：**发布脚本只负责进入维护、执行本地升级、确认完成、结束维护。取消发布方 replace-session 动作，新 adxlet 自行注册，Coordinator 在注册处理内部核实并完成会话接替。** 以下为本轮已实现的职责边界；验证证据和部署限制见第 9 节。

## 1. Coordinator 当前怎样管理节点

Coordinator 按 node_id 维护节点地址、当前 node_session、心跳序号与时间、容量、调度状态及 Environment 归属。Node 认证约束请求所属节点，注册/Inspect/对账确定是否真正可用；Coordinator 不直接启动或停止 adxlet 的 OS 进程。

node_id 是既有节点身份，不是本轮升级新增：本地历史最早可追溯到 2026-09-15 的 `4ecef1ffef`，旧 `platform/api/proto/control.proto` 已有 `RegisterNodeRequest.node_id`，调度模型已有 `Node.id`；master 和 PR178 基线均包含它。运行时由 adxlet 配置提供并在注册时上报，与每次进程启动的 session、容器注入的 Pod UID 分开。证据见 `out/node-id-audit-20261116/RESULT.md`。

普通重启已有新会话注册和真实进程探测：`rpc.rs::register_node/replacement_predecessor` 使用 Node 身份，并可经原 RPC 地址调用 `GetSession` 核对真实进程。改造前，维护分支的 `NodeMaintenance::registration_session` 要求 Yield 后先保存 replacement，再允许新 session 注册，因而需要 publisher 批准接替。当前由注册处理内部核实并调用原接替 CAS。

**本次已将接替登记移入已有注册处理，保留维护和会话校验。** 原 adxlet 已有的 GetSession 在初始化对账期间即可回应 Coordinator，无需新增注册 RPC 或协议字段。

## 2. 发布流程与入口

**publisher 对 Coordinator 的发布操作统一使用现有管理 HTTP 监听和运维网络边界，按升级对象分前缀：Pod 沿用 `/v1/node-upgrades`；adxlet 进程升级使用 `/v1/adxlet-process-upgrades`。不新增监听端口，也不再单独要求 publisher 携带逐节点 Supervisor 证书。**

使用一个写入口、一个查询入口；写入口只保留进入/结束维护，删除 publisher 专用 gRPC 链路和独立 session 接替阶段。接替的存储校验复用，但触发位置改到真实注册处理内部，不重建升级系统。

改造前，同一轮 adxlet 升级混用了 HTTP 维护与 Supervisor mTLS 接替，Pod 升级则走管理 HTTP。本次已删除发布方专用接替链路，统一使用上述管理入口。

### 两个 HTTP 入口，两个写动作

| 方法与完整路径 | 用途 |
| --- | --- |
| `POST /v1/adxlet-process-upgrades` | 执行一个明确的管理动作 |
| `POST /v1/adxlet-process-upgrades/query` | 查询本轮交接状态及解除回执 |

写入口使用固定的 `action`，仅允许两个值，直接转给已有维护方法，不建设通用动作执行器：

- `prepare`：输入 `owner, node_session`，进入本轮维护，暂停新调度并启用原实例保护。
- `release`：输入 `owner, node_session`，确认恢复后结束本轮维护，恢复管理员应有的调度状态。

不提供 `replace-session` action 或子接口；publisher 不再向 Coordinator 提交 expected/next session 预批准请求。

`owner` 沿用 `{node_id, rollout_id, local_task_id, epoch}`。各动作只接受自己的必要字段，未知动作或无效参数返回 400；响应沿用该动作原来的交接信息或解除回执。

查询只输入 `owner`。合法查询返回 200，结果中的 `status` 根据已有记录计算，不新增存储状态：

| status | 返回内容 | 含义 |
| --- | --- | --- |
| `absent` | 相同 owner | 本轮没有维护记录，也没有解除回执 |
| `active` | owner、origin_session、current_session、yielded | 本轮仍有交接记录 |
| `released` | owner、node_session | 已保存本轮解除回执 |

查询在同一次存储读取中取得维护记录与本轮回执，避免解除时两次读取看到不同状态。优先识别本 owner 的历史回执：即便节点后来进入另一任务，本轮仍返回 `released`；这不表示节点当前无其他维护。没有本轮回执但维护属于其他 owner/epoch，返回 409，不能当 `absent`。publisher 继续精确核对原 owner 与已保存的 session。

初始 `/query` 成功且返回结构与 owner 正确，就是新 HTTP 链路的只读预检；旧 Coordinator 的 404、错误页、断连和超时均拒绝继续，不作“不存在”解释。**不再新增 `handoff_http` 能力字段或特殊 404 处理**，既有 Pod 能力接口与协议版本不变。

旧三条 `/v1/node-upgrades/process-maintenance` 相关 HTTP 路由删除，不保留别名，也不实施上一版五条路径。实施后 Pod/通用 11 项加本组 2 项，合计 13 项。

## 3. publisher 怎样改

继续使用计划中已有的 `origin`，复用 `http_json`。进入/结束维护走同一写入口；交权状态、注册后的会话和解除回执都从 `/query` 核对。文件准备仍由 adxctl 完成，尽量放在进入维护之前。

HTTP helper 保留非 200 拒绝行为，并在原大小与期限边界内附加 Coordinator 的结构化 `error.code/message`，不回显非结构化正文；记录不存在由查询的 200 响应明确表达。写请求回复丢失时，先查询同一 owner 与保存的 session，不刷新身份或期限，不把任意 200 当成功。publisher 的接替意图、replace_expected 及单独批准重试分支随外部动作删除；保留本机启动所需的原有 task/attempt/session 传递和结果核对，不重写 adxctl 步骤协议。

Start 前只核对本轮交权和真实退出，不再要求 Coordinator 先登记待启动 session；新进程注册并恢复后，才核对实际会话和恢复结果。

删除 `--handoff-config`、`--handoff-stubs`、`--handoff-timeout`；复用现有 HTTP 超时。不增加替代证书参数、逐节点连接配置或 Token。

## 4. 认证边界：需要明确的取舍

**本方案采用现有 Pod 管理 HTTP 的受信运维网络边界，取消 publisher 接替操作原有的逐节点 mTLS 身份校验。两者不是同等的调用者认证保证。**

管理 HTTP 当前无调用者认证，不能把它写成“已有受认证入口”；owner 是任务归属数据，也不是凭据。部署必须限制为可信发布 Job/运维来源，不能经公共 API/Ingress 暴露；Kubernetes ServiceAccount/RBAC 不能替代 Coordinator HTTP 的访问限制。

实现不扩建通用认证，也不直接操作集群网络。若环境尚无有效管理网隔离，这仍是部署前置缺口，统一 HTTP 不会自动补齐。adxlet 自身的 Node mTLS、Yield、注册和 QueryRelease 认证继续保留。

## 5. 复用、删除和文件范围

- Coordinator：在 `platform/coordinator/src/autoscaling.rs` 识别新根路径及 `/query`，在 `rpc/upgrade.rs` 接维护写分发和查询，删除旧三条进程路径；共用请求限制、错误映射和执行锁。`rpc/process_upgrade.rs` 复用进入/结束维护，`rpc.rs::register_node` 增加维护下的自动接替处理，复用 `replace_node_session` 既有 CAS 与状态恢复/通知。存储层只需补同次读取维护和回执的窄查询，不新增存储格式；Node 的 QueryRelease 不受影响。
- publisher：修改 `build/release/node_rollout.py` 与对应测试。删除仅供旧链路使用的 `node_rollout_rpc.py`、`build_rollout_rpc.py` 和 gRPC/TLS 专用用例；有效业务断言迁到既有 `test_process_rollout.py`，不靠删除故障用例减行数。
- 旧 RPC：删除不再有生产调用者的 `NodeHandoffService.Query/ReplaceSession` 及其专用协议消息。保留 Node 使用的 `Yield/QueryRelease` 和共享 owner/state/receipt；保留注册内部仍需使用的存储接替方法及 SessionReplacement 数据，不按名称误删底层校验。
- 旧权限：引用核对后删除仅服务旧 publisher 链路的 `Principal::Supervisor`、`Peers::supervisor()`、TLS 的 `supervisor:` 映射和相关网络模式分支；同步清理对应样例/部署配置。共享 Node/Coordinator TLS、传输库及其他组件需要的 Python gRPC 依赖不能整体删除。
- 本项入口统一沿用 adxctl、adxlet、Relay 的本机进程操作；Coordinator 不增加 PID、制品、回退阶段或执行预算记录。socket、OnDelete、后续 successor 收尾和 Pod abort 修复单独核对，不混入本项增量统计。

## 6. 自动注册怎样接替

对外流程按用户提出的方式收敛：

```text
准备文件 → 进入维护（不可调度） → adxctl 升级本机进程
        → 新 adxlet 自行注册、对账、恢复管理
        → publisher 核实本机结果与 Coordinator 恢复状态
        → 结束维护（恢复应有的调度状态）
```

注册内部只适配维护分支：

1. 同一已注册实际 session 的心跳沿原路径处理，不能把维护记录中的预留值当成已完成注册。新 session 仅在已有本轮维护、旧进程已确认 Yield、Node 身份及原 RPC/Relay 地址匹配、首次注册处于 reconciling 且不接新分配时，进入接替核实。
2. 复用现有 Coordinator→Node 的 GetSession，核对原地址上真实进程返回的 node_id/session 与本次注册一致。探测在调度状态锁外执行；维护可超过普通心跳期限，因此不能沿用“旧心跳已过期就跳过核实”的处理，也不能触发普通失效清理。
3. 探测后重核同一维护 owner 和原 current_session，再复用现有接替 CAS 内部登记，继续原来的注册、Inspect 和恢复处理。不得只将 registration_session 改为放行任意 session；相同注册重试幂等，晚到请求不能覆盖随后已接纳的会话。
4. 未维护节点继续原普通注册流程，不改变 PR178 Pod 升级路径，也不引入发布方审批或新注册字段。

“不可调度”不能只落成普通 scheduling_paused 布尔值：原实例与路由的维护保护仍保留，避免长时间无心跳后被判失效。新进程注册也不自动解除本轮维护；最后 release 继续核对本轮 session、真实 Inspect 和 reconciling=false 的新鲜上报。管理员原先或维护期间设置的暂停仍有效。

回退仍由 publisher 决策；先确认失败进程退出或原有 NeverCreated 证明，用原版文件启动另一个新进程，由它自行注册，不恢复旧进程或旧 session。旧进程异常消失但未确认 Yield 时保持维护，不绕过交权检查。丢回复先查询同一任务，不重复创建，不停止可能已成功的新版。脚本中断保持维护，900/900/600 秒原期限不刷新。

## 7. 落地顺序与验证

本轮按“聚焦失败用例 → 两个入口及查询结果 → publisher 切换 → 核对并删除旧专用实现 → 同步文档与检查”实施。相对接手时磁盘快照，生产源码净减少 281 行，测试源码净减少 123 行；包含注释与空行，不计此前 socket/OnDelete 修改及文档。

验证至少覆盖：

1. publisher 不提供 Supervisor 凭据、无需 Python gRPC/stubs，即可通过受限 HTTP 管理入口完成本地模拟的正常/回退流程；旧接口或不合法的查询响应在任何副作用前拒绝。
2. 自动注册接替覆盖超过普通心跳期限的维护、Coordinator 重启、真实 GetSession 不符、owner 变化和晚到旧会话；保留旧 session 封闭，重复注册不重置身份。未 Yield 时拒绝接替，不靠超时推断旧进程已退出。
3. 脚本只发送进入/结束维护，不发送 replace-session；注册回复丢失、脚本中断、解除回执丢失及错误/超时不当作未执行；查询覆盖历史回执与后来其他任务并存，保留管理员暂停。
4. 仅提供两个新入口，旧三条进程路由不再可用。owning Node 的 Yield/QueryRelease、注册恢复，以及 Pod/独立 Relay 发布既有断言不回归。

已运行相关 Python/Rust 聚焦检查及仓库要求的格式、静态和文档检查并保留日志。进程级及独立 Relay publisher 专项集群验收仍未完成，本地通过不表示环境隔离或集群滚动升级通过。

已同步 `platform/api/http/node-upgrades.md`、`build/release/NODE-ROLLOUT.md`、`docs/deployment/adxctl.md` 及北京环境职责调整建议，替换旧 publisher Supervisor mTLS 条款。此前验证作为历史记录保留，本轮证据单列。

## 8. 已发现的冲突

| 冲突 | 处理 | 是否阻塞 |
| --- | --- | --- |
| 旧职责调整建议明确保留 Supervisor mTLS | 已统一到现有管理 HTTP 边界，删除旧专用 RPC，替换旧约定 | 已按批准方案实施；管理网隔离仍是部署前提 |
| 交接记录与解除回执原先分开查询 | 合并成一个只读查询结果；同次读存储，历史回执仍须精确匹配本轮 owner/session | 不阻塞，不新增存储状态机 |
| 管理网络实际隔离情况未验证 | 记录为部署验收前置项，不声称代码或 RBAC 已提供调用者认证 | 不阻塞本地开发，阻塞缺少隔离的环境验收 |
| 普通重启自动注册，计划升级却另需 publisher 登记 session | 取消发布方动作，在维护下注册处理内复用 Node 认证、GetSession 和既有接替 CAS；保留维护保护 | 本地 RPC/存储用例已验证过期心跳、重启、重放与晚到会话；未做集群并发验收 |
| 工作区另有未跟踪 `node_rollout_debug.py`，仍引用旧客户端 | 该未跟踪副本字节保持不变；正式工具已切换，旧专用模块删除，不为副本添加兼容层 | 旧 debug 副本仍依赖已删除模块，不能用作本轮发布入口 |

## 9. 本轮实现与验证记录（2026-11-16）

新增的维护查询在一次存储读取中核对 Header、marker 与本 owner 的解除回执；正常注册在锁外探测真实 Node，提交前重核 owner/current/实际已注册 session，并复用原接替 CAS。保留 Node Yield/QueryRelease、维护路由保护、管理员暂停、本机进程监督、原身份重放保护和 900/900/600 秒预算。Pod、独立 Relay 与此前 socket 修改未重写。

真实 RPC 用例发现既有维护准备只读取 raw Redis 的旧 routable 位，而正常心跳已将新状态发布到内存 overlay，导致真实注册完成后 prepare 返回 409。已在维护准备中仅合并同 session 已确认的 routable 位，保留 raw session/token/pause/retirement 和 CAS 检查，避免 Prepare 提前关闭原路由；用例检查了 Prepare、Yield、过期等待和 Coordinator 重启期间的原实例路由。

入口统一阶段的本地证据：publisher 68 项、Pod 发布工具 101 项、交接/存储 5 项、新自动注册 RPC 2 项和原普通重启注册 RPC 1 项均通过；三份 Python 文件 Ruff/Pylint、全仓格式检查及 workspace all-targets/all-features Clippy（`-D warnings`）通过。完整命令、RED 与 GREEN 日志见 [交付记录](out/publisher-http-20261116/RESULT.md)。这是隔离 Redis、测试 TLS、真实 NodeRpc/Coordinator RPC 与注入后端的本地验证，不是实际进程/集群滚动升级验收。

同日北京环境已完成 Node0 空 Pod 的 Active 正常 `verify-node`，随后按仅 batch0 的范围收口为 `Aborted/partial`；Node1 保留原 Pod、session 和镜像，未推进 batch1。该次验证同时确认新 Node 镜像 PATH 与新 immutable ConfigMap 中的 runsc 恢复资格配置生效，复用原 Node 业务二进制。它不证明两节点全量 Completed、Environment 跨 Worker 恢复或本方案进程级升级已验收；已 Aborted 的任务不再续做。

管理 HTTP 无调用者认证，实际运维网隔离仍待部署侧验收。未跟踪 debug 副本仍引用已删除的旧客户端，保持原样且不纳入正式工具；正式使用 `build/release/node_rollout.py`。

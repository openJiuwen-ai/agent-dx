# 【待精简】ADX 升级代码优化清单

记录日期：2026-11-14；实施状态核对：2026-11-16。下表历史代码量依据：`8fa64c1c` 相对父提交 `665080fa`，不代表后续修订的增量。提交、推送状态以 Git 记录为准。

本文件记录读代码时发现的精简点，实施状态按各条目标记。优化以减少重复实现和维护成本为目标，保留已经约定的故障处理边界，不通过删除必要保护或测试断言凑删行数。

## 历史基线中三个文件的增加位置

| 文件 | 生产部分净增 | 测试部分净增 | 合计净增 |
| --- | ---: | ---: | ---: |
| [rollout.rs](platform/deployment/src/rollout.rs) | 424 行 | 2,045 行 | 2,469 行 |
| [rollout/adxlet.rs](platform/deployment/src/rollout/adxlet.rs) | 494 行 | 42 行 | 536 行 |
| [supervisor.rs](platform/deployment/src/supervisor.rs) | 10 行 | 228 行 | 238 行 |
| 合计 | 928 行 | 2,315 行 | 3,243 行 |

三个文件的 Git 差异合计新增 3,900 行、删除 657 行。生产/测试部分按 `#[cfg(test)]` 模块边界划分，包含注释和空行，不是纯有效代码行数。测试部分约占净增的 71%。这张表只统计上述三个文件，不是整个提交。

## 优先优化：生产路径的重复处理

- [x] **【已实现并完成本地验证】统一 publisher 的 Coordinator 发布管理入口。**
  - 方案：[【建议方案】ADX发布管理入口统一.md](【建议方案】ADX发布管理入口统一.md)，2026-11-16。复用现有受限管理 HTTP，采用 `/v1/adxlet-process-upgrades`，收敛为两个入口：写入口仅进入/结束维护（prepare/release），query 查询状态或解除回执。取消 publisher replace-session，由新 adxlet 自行注册，Coordinator 在注册内部核实并完成接替。旧三条路径删除，不加能力字段或特殊 404 逻辑；Pod 原入口不动。
  - 明确取舍：管理 HTTP 依赖部署网络限制，未具备调用者认证；这改变原来的逐节点 mTLS 边界。Node 自身的 Yield/注册/QueryRelease 认证、任务与存储校验继续保留；已删除旧 publisher 接替链路及专用认证分支。
  - 代码核对结论：已复用 GetSession 并在维护注册处理内部调用原接替 CAS；本地用例覆盖维护超过心跳期限、Coordinator 重启、回复丢失和晚到会话。保留维护保护、旧 session 封闭和管理员暂停。进程级及独立 Relay publisher 专项集群验收尚未完成。

- [x] **【本地验证通过】缩短进程升级 Unix socket 路径，保留完整任务身份。**
  - 方案：[【已定方案】ADX进程升级Socket路径修复.md](【已定方案】ADX进程升级Socket路径修复.md)。2026-11-14 用户选择最小修改：Relay 正向/回退使用 `state_dir/s` 下的短摘要同级目录；adxlet 保留两种用途，启动 socket 改为固定 `<service_id>-start.sock`，管理地址不变。不新增 RPC、不合并监听、不修改启动流程。
  - 确认日期：2026-11-14；修复前代码依据：`8fa64c1c`，以及用户提供的短/长 task ID 对照结果。
  - 修复前，publisher 在 `build/release/node_rollout.py:734` 生成 64 位 SHA-256 本地 task ID；`rollout/relay.rs:355` 直接将它拼入 `relay-<task_id>/route.sock`，第 396 行拒绝超过 100 字节的完整路径。这里的行号与长路径仅描述修复前代码。
  - 修复前，以 `/work/supervisor-s7ojqg9k` 为父目录：Relay 正向路径为 107 字节，回退追加 `rollback/` 后为 116 字节。正向检查发生在创建 execution 和触发旧进程退出之前，符合 `phase=failed`、`execution=null`、原 Relay 未重启的现象。
  - 修复前 adxlet 也有同类缺陷：`rollout/adxlet.rs:760` 使用 `<state_dir>/rollout/<64位task_id>/bootstrap-<64位attempt>.sock`，同一父目录下为 178 字节。
  - 完整 task ID/attempt 继续保留在 submission、task、journal 和协议身份中。代码修改只涉及原路径拼接、长度检查及固定启动地址保护，保留现有启动信息交付、父子 PID、单次启动和真实就绪校验。
  - [x] adxlet 回归覆盖 64 位身份、正向/回退、回复丢失、固定启动地址及占用保护；原版文件、配置与期限不变。
  - [x] Relay 覆盖 64 位 publisher task ID 正向/回退路径的实际 bind/connect、稳定派生、较长父目录及字节边界；准备阶段检查两条路径，回退不再追加长目录后缀。代次端点的归属和拒绝覆盖逻辑沿用，未进行集群验收。
  - 用户已报告两个节点以短 task ID 直接 submit 成功，但使用的是追加标记的测试二进制；这是进程升级机制对照证据，不是新业务版本或 publisher 整体验收。
  - CSI Flash 专员执行：90 项 adx-deployment 本地测试、cargo fmt 全工作区检查、全工作区 all-targets/all-features Clippy 均通过。生产 Rust 净增 59 行，测试净增 84 行，包含注释和空行；不含先前 OnDelete 清理及文档。完整记录见 `out/adx-socket-fix-20261114/RESULT.md`。本项专项集群验收尚未完成。

- [x] **【代码已清理】移除发布脚本强制要求 OnDelete 的校验。**
  - 确认日期：2026-11-14；用户认定该限制属于过度实现，已删除。
  - 位置：`build/release/node_rollout.py` 的 `plan_pod()`，提交 `8fa64c1c` 第 269 行。
  - 对应代码：`require(obj["spec"]["updateStrategy"]["type"] == "OnDelete", kind + " must use OnDelete")`。
  - 已核对并清理 apply/cleanup 等路径中的同类硬性策略校验，不扩建另一套更新策略控制系统。
  - 保留 PR178 的迁移、验空、删除许可和 UID/RV 条件删除要求；不把工作负载策略检查与这些协议检查一并删除。
  - 已清理 plan/apply/终态重入/cleanup 的策略值判断，以及正向和恢复模板 JSONPatch 中的策略条件；对应的旧拒绝用例已更新，Pod 发布本地测试 101 项通过。
  - 部署方仍须保证迁移和验空前控制器不会提前替换 Pod。脚本不校验、不修改、不恢复更新策略；不据此宣称未受控 RollingUpdate 已完成安全发布验证。
  - 本项 OnDelete 清理未修改 PR178 的 Coordinator、adxlet、迁移和存储逻辑；生产脚本净减少 6 行，测试和文档增量另计。后续交接与 Pod abort 故障修复单独审核和统计。

- [ ] **合并 Submit 与 Step worker 的公共收尾。**
  - 位置：`rollout.rs` 的 `Tasks::handle` 与 `Tasks::begin_step`。
  - 两处都创建 worker、保存结果、记录 finished、更新 Reservation，以及在终态成功落盘后释放任务。
  - 优化方向：收敛真正相同的结果持久化和任务释放处理；保留准备、退出、启动各自的错误语义和接收条件。不要为此再造一套执行器。

- [ ] **收敛 Query 与 Start 的终态确认和落盘处理。**
  - 位置：`rollout.rs` 的 `Tasks::handle`；`rollout/adxlet.rs` 的 `commit`、`record_commit`。
  - Query 处理 commit-unknown 回执后，单独重写 phase/stage、清理 error、记录 finished、落盘并释放任务；Start 中也有对应的终态转换和落盘处理。
  - 优化方向：统一“回执已经确认之后”的状态转换和持久化规则。查询方式、是否还能发 Complete、期限后的只读观察等差异继续显式保留。

- [ ] **集中阶段判断和目标/原版终态映射。**
  - 位置：`mark_start_unknown`、`record_step_failure`、`Tasks::begin_step`、`Execution::startable`、`start`、`record_commit`。
  - 合法阶段、unknown 阶段以及目标成功/原版恢复的结果映射，分散在多处字符串判断中。
  - 优化方向：先集中重复规则，必要时使用窄类型表达；不引入通用状态机框架，不合并语义不同的 unknown 状态。

- [ ] **合并相同的固定期限转换。**
  - 位置：`rollout/adxlet.rs` 的 `exit`、`rollback_stop`、`start`。
  - 多处重复将已保存的绝对期限转成剩余时间及本机 `Instant`。
  - 优化方向：复用相同转换，保持各阶段首次计时位置和期限不变。准备 900 秒、升级 900 秒、回退 600 秒不能混成一套会刷新期限的逻辑。

- [ ] **删除没有独立职责的转发包装。**
  - 已确认例子：`Tasks::dispatch_step` 只转调 `begin_step`。
  - 优化方向：核对调用者后直接调用实际实现；类似包装逐个核对，不为了形式保留额外层级。

## 优先优化：测试搭建过长

- [ ] **收敛重复的临时目录、运行时、Task 和固定文件搭建。**
  - 主要位置：`rollout.rs` 的测试模块。
  - 当前已有 `prepared_task`、`paired_adxlet`、`pin_snapshots`、`wait_worker`、`rollback_fixture` 等辅助函数，但部分用例仍各自铺开相似准备过程。
  - 优化方向：沿用已有辅助函数，只抽取重复搭建，避免为一次性用例建立新的大测试框架。

- [ ] **减少重复的模拟 actor/回执处理代码。**
  - 主要位置：Exit、Start、回退和写盘不确定用例中的 channel 接收、Operation 匹配及回复搭建。
  - 优化方向：对重复协议交互使用小型辅助函数或固定脚本；让每个用例突出自身注入的故障和断言，保留调用顺序、次数与身份检查。

- [ ] **将仅输入和预期结果不同的测试改为表驱动。**
  - 已有目标/原版共用的 `start_leg_replays_same_attempt_after_lost_replies` 与 `unpersisted_selection_falls_back_to_receipt` 可作为继续收敛的起点。
  - 优化方向：合并相同流程的搭建，保留每个场景独立断言与失败定位。不能把不同故障场景简单删成一条 happy path。

## 次优先：注释和长函数

- [ ] **压缩重复解释同一不变量的长注释。**
  - 多处反复解释：rename/fsync 出错不等于未写入、已保存身份不得重生成、未知结果不得推断已退出、成功落盘后才释放任务。
  - 优化方向：在规则归属处集中说明，分支只保留该分支特殊的原因；删除随旧实现失效的注释。对迟到 Start 封闭、真实 reap、所有权检查等关键不变量保留本地说明。

- [ ] **精简后再评估长函数和文件边界。**
  - 重点函数：`Tasks::handle`、`Tasks::begin_step`、adxlet 的 `exit`/`start`，以及 supervisor 的 `adxlet_rollout_command`。
  - 优化方向：先减少重复，再按职责拆出必要的小函数。仅把测试或函数搬到另一个文件，不计为实际代码精简。

- [ ] **supervisor 只核对相同语义的回执匹配。**
  - `Receipt`、`StopFailed`、`Complete` 都匹配 task、attempt、PID 和进程身份，但各操作允许的状态不完全相同。
  - 优化方向：相同匹配可以局部复用，操作特有检查保留。该文件本次生产部分仅净增 10 行，不借机重写通用 supervisor、Relay 或初始化恢复。

## 精简时必须保留

- 同一次启动最多创建一个进程；丢回复先查原 Task、原操作和原身份。
- 旧进程真实退出与 Coordinator 确认 Yield 两个条件都满足后才能接替。
- 回退前封闭失败 attempt，确认未创建或真实 reap；迟到 Start 不得复活它。
- 已成功选中的新进程不能因最终回复丢失而被误停。
- 目标与原版的实际二进制、配置文件内容和摘要固定；原版恢复启动另一个新进程。
- 期限不因重试或脚本恢复刷新；回退 600 秒包含停止失败进程的耗时。
- 持久化失败保留 Unknown 和原身份；任务只有在终态可靠落盘后才释放。
- publisher 统一跨组件决策，adxctl 不重新获得 Coordinator 编排职责。
- Node 的认证、Yield、注册、恢复与 QueryRelease，以及管理员额外暂停继续保留。
- 独立 Relay 和 PR178 Pod 升级流程不扩大改动。

## 补充记录：Pod 升级管理入口的访问边界

记录日期：2026-11-14。依据：PR178 本地整合基线、已推送提交 `8fa64c1c` 及 akernel POC 原文。

**发布脚本到 Coordinator 的 Pod 升级管理 HTTP 通路，仍要靠管理入口部署方式和网络策略限制访问。当前没有应用层调用者认证，不能仅凭接口能连通就认为访问保护已满足。**

- 当前使用 Coordinator 的可选 `autoscaling_listen` 普通 HTTP 监听，路径为 `/v1/node-upgrades/...`；不经公共 Gateway/API/Ingress 暴露。
- [现有接口文档](platform/api/http/node-upgrades.md)明确该入口没有 Bearer 鉴权，必须限制到受信运维 Pod。POC 原文描述了集群内升级 Job，以及 Helm 中的管理监听和网络策略；未宣称管理 HTTP 已增加 mTLS 或 Token 认证。
- Kubernetes ServiceAccount/RBAC 负责 Job 操作 Kubernetes API 的权限，不能自动保护 Coordinator 的 HTTP 接口。
- `NodeHandoffService` 现仅保留 Node 使用的 Yield/QueryRelease，实际进程接替在 Node 注册处理内部完成。这些 Node mTLS 通路不能证明发布方管理 HTTP 已具备调用者认证。
- [ ] 部署材料需落实并核验：管理监听与 Service 的访问范围、NetworkPolicy 对指定运维 Job/Pod 的允许规则，以及对其他来源的限制；不暴露到公共入口。
- [ ] 记录集群实际执行该网络策略的证据和访问验证结果。配置文件存在或本地测试通过，不等于集群访问限制已经生效。
- 管理网络隔离的部署与访问验证证据尚未补齐；Node0 空 Pod 升级验证不能替代这项验收。本记录不新增通用 HTTP 鉴权改造，也不将现有 HTTP 路径改称已完成认证。

## 实施顺序与记录方式

建议依次处理：公共收尾与终态转换 → 测试重复搭建 → 阶段规则和期限转换 → 无效包装与注释。每项完成后记录生产/测试各自的实际增删行数，核对原场景及关键断言仍然存在，并运行对应的聚焦检查。

当前状态：已勾选条目的实现和验证范围见各项记录；其余重复处理、测试搭建和注释精简仍是未实施建议，不据此宣称已经减少代码量。

## 补充记录：StatefulSet OnDelete

记录日期：2026-11-14。

- [x] 北京环境已选择 `adx-node` StatefulSet 的 `updateStrategy: OnDelete`，并完成 Node0 空 Pod 替换验证。
  - 2026-11-16 模板更新未提前替换 Node0/Node1；发布脚本随后执行 Node0 的 batch0，Active 正常 verify 成功，再按范围收口为 `Aborted/partial`，Node1 未升级。
  - 这是该环境的部署选择，publisher 不强制校验、修改或恢复更新策略；其他策略的受控升级不属于本次验证范围。
  - 已验证的路径仍使用迁移、验空许可及 Pod UID/resourceVersion 条件删除；未创建 Environment，不能把空 Pod 替换写成跨 Worker 业务恢复验收。

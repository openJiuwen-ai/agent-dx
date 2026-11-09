# Sandbox 可观测本地验证记录

初版验证日期：2026-10-26。实现分支：`feat/sandbox-observability-20261026`。
以下按验证轮次列出实现版本及结果；本记录描述本地验证，
不表示测试或生产集群已部署。
指标口径见 [Sandbox 指标合同](sandbox-observability.md)。

## 统计覆盖起点与创建收尾取消回归

2026-11-04，基于 `master@d6a8f7bd` 的 `fix/observability-history-p95` 工作区，
在 Linux x86_64 ECS 的隔离构建容器中验证；未更新集群或运行真实 Sandbox 生命周期端到端测试。

- 先复现四项行为失败，再修复：旧起点保持为零、损坏数据修复后历史仍未知、
  等待收尾时提前出现成功耗时样本、非成功调用的候选耗时被立即提交。
- 修复后四项定向回归通过；创建测试遍历 `names`/`creates` 两处收尾锁的取消与正常完成，
  并验证后续重放、同名复用不回填或重复计入成功样本。
- 真实 Redis 存储回归：124 passed、0 failed；新统计起点持久化、不回溯、不重复累计，
  损坏统计仍不阻断业务，修复后由后台刷新恢复。
- API Server 与 Coordinator 默认回归：313 passed、0 failed、260 ignored。
  ignored 中的上述 124 项存储测试已另行执行；其余 ignored 项及原生 Prometheus 测试本轮未运行。
- 新增测试、辅助代码和私有同步点按保守口径计 178 行；已有测试全部保留。
  约 690 行已有 RPC 夹具搬迁复用，单独统计，不作为新增测试或删减测试抵扣。
- Linux 完整 workspace all-target/all-feature Clippy（包含 AFS），以及九个策略包的 unwrap 检查通过。
  首轮缺 RDMA 开发头文件，仅在本次专用构建容器层补齐依赖后重跑通过；宿主服务和集群未变更。
- 全仓格式检查存在十个未修改主干文件的既有差异；本次修改的 Rust 模块已格式化。

核心回归未新增确定性的同 epoch CAS 争用、仅写失败或提交后丢响应故障注入；
不将现有存储回归通过表述为这些故障均已专项验证。
执行日志、失败与修复后的源码快照及行数核算保存在执行环境的独立测试目录，不随源码提交。

## PR #125 第二轮复核修复验证

实现提交：`5ca837d7868a1f62a44e5cfba1cea69c5bf24f09`，2026-10-26 验证。
本轮修复替代执行首次 Running 未发布时的终态提交、正常停止后的迟到退出归因、
以及默认名称生成前的参数类型校验。三个问题均先通过失败断言复现。

- 真实 Redis 存储：63 passed，新增替代执行终态、重放、恢复及错误 worker/generation/runtime 证据拒绝测试。
- adxlet：140 passed（journal 6、lifecycle 39、pause_resume 55、reconciliation 40），
  包括正常停止迟到 exit 137 不计中断及独立 OOM 仍计中断。
- API create_replay：18 passed、1 ignored；ignored 项需原生 Prometheus，
  本轮实际采集通过下述 Docker E2E 验证。
- `make rust-check JOBS=2 ADX_WITH_AFS=0`、文档和 diff 检查通过；macOS 检查未包含 Linux AFS。
- 从干净实现提交构建 Linux ARM64 安装包，manifest commit 与上述提交一致、dirty=false，
  package verify 通过；SDK、二进制使用同一提交。
- 本地双节点 Docker/runc、真实 sandboxd/Redis/SDK/Prometheus：18 组完整执行，
  首轮 **16 passed、2 failed**；15 个通过场景完成实际指标采集，stop 场景验证停止清理。
- auth 首轮重复撤销密钥返回 HTTP 409；同一 bundle、未修改源码或断言的独立复测通过，
  实际采集也通过。保留首轮失败，尚未确认该间歇性失败的根因。
- sandboxd-restart 仍失败：强杀后原 runtime 丢失，after=[]；后端记录 exit 137、oom=false。
  固定 backend 提交 `efc201531d7e2e9d69505da151eb66084b61eebf` 未修改，完整 E2E 尚未全绿。
- 首轮与复测 cleanup_errors 均为空，所属容器和网络均已清理。

组件证据：`out/observability/review-fixes-round2/`。安装包、18 组首轮汇总、独立鉴权复测及
原始指标：`out/observability/e2e-round2/`。没有部署测试或生产集群。

## PR #125 第一轮复核修复验证

基于 `c8f5091` 加本次复核修复，2026-10-26 在独立 worktree 验证：

- 真实 Redis 存储：62 passed，覆盖未发布 Running 后终态归并、同进程重连后第二次事故、
  reconciliation 期间回放去重，以及 CAS 返回完整摘要和删除回放。
- adxlet：139 passed（journal 6、lifecycle 39、pause_resume 54、reconciliation 40），
  覆盖 reload 清理结果不确定时保留意图、后续确认缺失、重连归并和独立 OOM 不被屏蔽。
- mTLS RPC：串行 33 passed，另有 ignored lib 2、background_traces 1 passed。
  默认并行首轮有 Redis 写暂停故障注入超时、Coordinator 启动等待超时各 1 项；
  配置相同证书串行复测全部通过，保留首轮失败日志。
- `make rust-check JOBS=2 ADX_WITH_AFS=0`、文档检查和 diff 检查通过。
- native Prometheus：Coordinator/API Server 各 1 项实际采集及查询通过。

三项原始行为缺陷先通过失败断言复现，再加入正式回归测试。共享 Cargo 缓存曾命中其他
worktree 的旧类型产物；最终验证使用独立缓存，未清理共享缓存。日志与采集证据位于
`out/observability/review-fixes/`。本次未重跑完整 Docker E2E，未部署测试或生产集群；
下文端到端结果及外部 sandboxd 已知失败仍来自初版验证。

## 组件与采集

| 检查 | 结果 |
| --- | --- |
| core/protocol/adxlet/apiserver/coordinator 常规测试 | 565 passed，124 ignored |
| 真实 Redis 存储测试 | 59 passed |
| Coordinator ignored lib/background trace | 2 + 1 passed |
| 真实 Redis + mTLS RPC | 33 passed |
| native Prometheus 实际采集与查询 | Coordinator/API 各 1 项通过 |
| Linux ARM64 managed_mounts | 5 passed |
| Rust fmt/Clippy | `make rust-check JOBS=2 ADX_WITH_AFS=0` 通过 |
| Prometheus 配置与 Grafana 表达式 | 配置与 18 条表达式语法通过 |
| 调度超时 E2E 脚本定向单测 | 1 passed；旧合同断言先失败，修订后通过 |

上述测试类别存在重叠，不相加为一个总测试数。macOS 上完整 all-features 检查受既有
Linux AFS eventfd 依赖限制；未将排除 AFS 的检查描述为完整 all-features 通过。
`promtool check metrics` 报缺少 HELP 文本并退出 3，该 lint 检查未通过。
实际 Prometheus 抓取与 PromQL 查询通过；Grafana UI 未验证。

## 双节点端到端

本地 Docker Desktop Linux ARM64，两个隔离节点，真实 Redis 7.2.5、sandboxd/runc、
安装后的 Python SDK 和从实现工作树统一构建的 ADX debug 二进制。
sandboxd 固定版本为 `efc201531d7e2e9d69505da151eb66084b61eebf`。
Prometheus v3.2.1 每秒采集 API、Coordinator 和两个 adxlet，共 4 个 target。

**18 类场景中 17 类通过，sandboxd 强杀重启失败，两次复现；完整 E2E 未全绿。**

通过：sdk、data-plane、lifecycle、auth、capacity、placement、local-first、node-failure、
runtime-exit、workload-probes、create-response-cut、create-unknown-query、schedule-deadline、
coordinator-restart、redis-restart、restart、stop。
除 stop 关闭服务后不再采集外，通过场景均执行实际指标采集。
所有场景 cleanup_errors 为空，最终检查无本轮残留容器或网络。

| 业务事实 | 实测结果 |
| --- | --- |
| 调度排队 | queued=1，最老等待约 2 秒；释放后归零 |
| 确认调度超时 | 调用 failure=1，stage=scheduling/reason=timeout；队列排空、无 assignment，owned creation=0 |
| 无成功耗时样本 | histogram count=0，P95 为 NaN |
| worker 失联 | 1 次执行中断、1 次 worker 事故；当前影响 1 env / 1 worker |
| 运行期异常与恢复 | runtime_exit=4，recovered=2，unrecoverable=2；当前影响最多 2 env / 1 worker |
| readiness 失败 | 创建 failure=1，stage=readiness/reason=probe_failed；未混入运行中断 |
| 健康探针失败 | probe_failed 中断=2，recovered=1，unrecoverable=1 |
| 响应丢失/unknown 后查询 | 两组各 2 次成功调用、各 1 个成功新建耗时样本，重放未重复采样 |
| Coordinator/Redis 重启 | 统计覆盖起点不变 |
| 正常删除与空闲回收 | 常规场景未增加异常中断累计数 |

响应丢失用例的 unknown 属于客户端视角，API 已完成成功；不能据此宣称验证了 API
unknown 计数。API unknown 分类与成功样本排除由上表 native 采集测试另行验证。
调度超时脚本已对齐确认取消排队后的 `never / not_started` 合同，并在真实链路复验通过。

## 未通过项与边界

`sandboxd-restart` 在新建后不到 1 秒强杀 daemon，重启后原 runtime 消失、退出码 137。
日志与锁定版本源码指向 cgroup 使用关系的持久化窗口：使用集合每 5 秒保存，恢复时磁盘
使用记录为空但实际 cgroup 仍存在，未登记的 cgroup 进入清理路径并被杀掉。
独立复测再次出现相同结果。不能仅凭 137 归为 OOM；本改动不包含外部 sandboxd 修复。

本轮未验证真实内核 OOM、runsc/Firecracker、KVM checkpoint、生产网络与生产规模。
真实 worker 失联覆盖一个运行 env，多对象批量失联仍需专项验证。
直接恢复 Running 保留旧异常、正常操作意图竞态与重复提交去重有组件测试，不能视为
这些路径已全部完成真实故障注入。当前影响摘要不提供任意历史窗口完整去重。

本地完整日志、原始 scrape、PromQL、影响范围和包身份保存在执行者工作树的
`out/observability/`，端到端机器可读汇总为 `e2e/regression-summary.json`。
这些输出不随仓库发布；本页提供可审阅的验证范围和结果摘要。

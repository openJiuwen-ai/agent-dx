# 管控面后续实施阶段

逐阶段以行为测试驱动实现，以真实部署用例验收。组件测试、本地端到端和 Kubernetes Buildkite 三类证据分别记录。

| 阶段 | 实施范围 | 验收重点 |
| --- | --- | --- |
| 1. 同节点暂停／恢复 | adxlet 串行状态机、Execd HTTP 协作、sandboxd checkpoint/restore、本地存储、Redis 提交、API Server、路由 | 真实 Firecracker 创建→执行→暂停→adxlet 重启→恢复→删除；内存计数器、PID、文件、执行身份、资源和路由 |
| 2. 节点生命周期与降级 | 空闲删除、可配置自动重启及退避、资源源选择与过期保护、压力准入、SQLite 降级日志与补写 | Coordinator 失联、节点进程重启、采集失败、清理失败；遵守节点生命周期所有权和权威对账规则 |
| 3. 快照与存储 | 对象存储适配、可复用快照目录、引用与延迟删除、预算缓存、未登记制品回收 | 上传失败回滚、恢复点过期、引用期间删除、缓存淘汰、从快照创建新实例 |
| 4. 跨节点恢复与接管 | 同 Environment ID 归属转移、旧归属撤销与返回清理、心跳故障处理、恢复与路由切换 | 源节点恢复、旧请求重放、接管中途失败；缺少 checkpoint 或 local-only 制品明确失败 |
| 5. 调度能力与性能 | Global 轮转、Shard 调度、Local 准入；优先级、GPU/NPU 整卡、亲和／反亲和；增量状态与候选复用 | 不超分、卡号正确、排队公平、规则正确；同一 Linux 主机与既有分支统一负载比较 QPS/P99/更新及冲突成本 |
| 6. 接口与部署收口 | 最小 API Key 管理、启动配置和证书加载、adxlet/Relay 两种进程模式、统一 CLI 与发布包 | HTTP/SDK 兼容、身份隔离、进程重启、停止清理、同包角色组合、安装文档可复现 |
| 7. 正式基础端到端流水线 | 独立 K8s 部署和用例步骤、基础功能/节点故障用例、日志及制品汇总；FC 本轮保留本地验收 | 部署过程可见、逐用例结果可见、失败日志可定位、包/镜像/源码身份一致 |
| 8. 可观测与日志采集 | 优先补齐实例数量与资源分配 Metrics；结构化日志采集、Trace 上下文与导出 | 指标与权威状态/资源账本一致；真实采集、跨组件关联与采集端故障验收 |
| 9. 日志滚动与压缩 | 按大小/时间滚动、历史文件压缩、保留数量/时长/总容量、部署配置 | 持续写入及重启下日志完整；压缩/磁盘异常可诊断；进程与Pod部署行为清晰 |
| 10. Rust API Server 与调度命名 | Rust HTTP 服务直连 Environment RPC、删除 Go/legacy 消息链、调度 Shard 命名与配置迁移 | HTTP/SDK 契约、SSE、认证/缓存/重试、真实 RPC、发布包及基础 K8s 七组 |
| 11. 节点本地优先创建 | API 节点轮转、共用 Admission 暂留、Coordinator 原子 claim 与中心账本同步、同 ID 收敛 | 本地 Redis/mTLS/HTTPS、真实 sandboxd/runc/Execd 和正式 K8s 八组 |
| 12. 内置运行环境 | 本地 EROFS 与不可变 OCI 双路径、静态 Execd、PID 1 回收、自定义镜像 bootstrap | standalone EROFS、OCI K8s 三模式；Firecracker 新入口单列验证 |

阶段 1 已完成本地验收：2026-09-15，独立 Lima ARM64 KVM 节点使用真实 OCI 镜像，通过公共 SDK 完成创建、执行、暂停、adxlet 重启、恢复和删除。恢复后内存计数器继续增加、PID 不变、二进制文件一致；Redis 记录为 Deleted 且资源释放，sandboxd 清单与 checkpoint 目录为空。122 项定向 Rust/Redis 测试、Clippy、真实 RPC 与 API Server HTTP 集成通过。详细证据见 [本地验收记录](2026-09-15-checkpoint-acceptance.md)。

阶段 2 已完成本地验收：Lima r7 的 checkpoint 与节点生命周期共 10 项真实 Firecracker 用例通过，141 项 Rust/Redis/HTTP 定向测试、138 项 SDK 单测和 Go 检查通过。契约及证据见 [节点生命周期与降级](node-lifecycle.md)。阶段 3 已完成本地快照与存储验收；阶段4已完成本地故障恢复验收，阶段6已完成本地部署收口验收，阶段7按本轮基础K8s范围完成正式验收，阶段5尚未全部验收完成。本地验收与 Kubernetes Buildkite 分开记录。

阶段 5、6 包含既有实现的补齐与验收，不意味着这些组件需要从头重写。平台控制面不提供函数／Actor／FaaS、抢占、成组调度、租户配额或 Coordinator 弹性池。拓扑分布不作为本期扩展目标；当前内部类型/规则仍存在，公开 HTTP 未暴露。Agent 的 Template/Environment 与 Activator 属于产品层，当前能力与验证限制单列于 [实现边界](control-plane-implementation.md)。


当前推进记录（2026-09-16）：

- 阶段3：本地验收完成。统一 package-v16 / Lima r20 的17项真实 MinIO/FC 用例通过，包含双克隆、源快照物理回收后的暂停恢复，以及节点重启后清理未登记上传残留。节点85项、真实Redis/mTLS 8项、Clippy和驱动6项通过，见 [存储说明](snapshot-storage.md)。
- 阶段4：已实现心跳超时失效与持久化、路由撤销、adxlet恢复对账清理和迟到状态拒收；补齐超时前正常进程接续、Coordinator重启后报到期限。真实Redis/mTLS22项、库5项及Clippy通过。package-v19故障组通过，但正常重启因强制等待超时失败；修复后package-v20本地双节点七组全部通过，清理无残留。共享checkpoint的新代次分配、恢复RPC、重复请求处理与制品引用保护已接通；177项组件回归、Clippy及package-v21本地双节点七组通过，见 [跨节点恢复](2026-09-16-cross-node-recovery.md)；Lima双命名空间真实FC六项及恢复计划落盘后Coordinator重启已通过（[记录](firecracker-cross-node.md)）；r5目标adxlet执行中重启已通过，旧backend清理后同代次恢复；52项驱动回归通过，本地阶段4完成，见 [验收记录](2026-09-16-node-failure-acceptance.md) 和 [契约](node-failure-takeover.md)。
- 阶段5：当前源码与历史制品的6场景×7轮同机复测通过，见 [新报告](2026-09-16-scheduling-recheck.md)；HTTP/SDK的节点与实例亲和、反亲和、实例标签、权重和有序偏好已接线，61项Rust、8项真实Redis/mTLS/HTTPS、Go检查及140项SDK测试通过，见 [放置约束](http-node-placement.md)。package-v17/Lima r21、r22的新放置用例通过，但完整运行均在双克隆文件写入遇到Relay CONNECT 504（10/18），见 [网络取证](2026-09-16-fc-clone-network.md)。本轮普通路径6场景×7轮复测完成，见 [条件组接线后性能](2026-09-16-placement-groups-recheck.md)。持续新请求的deferred队列饥饿和唤醒边界已修复，46项回归、Clippy及52.8万请求混合调度验收通过，见 [公平性与混合负载](2026-09-16-scheduling-fairness.md)。修复提交 `d032459` 的[Buildkite #16 基础K8s复验](2026-09-16-buildkite-16.md)已通过。真实设备与真实服务长稳仍待验收。
- 阶段6：共用 RelayService 与 adxlet 共进程接线通过157项定向测试，真实共进程 Firecracker/S3 10项验收通过，见 [进程模式](relay-process-modes.md)。API Key管理的真实HTTPS/mTLS/Redis集成已通过，见 [密钥管理](api-key-management.md)。SDK命令订阅的TLS校验连接已修复，真实TLS Socket与package-v13/Lima r16复验通过；本期证书更新后重启组件生效；热重载移入后续待办。现已修复默认管理路由及部署示例接线，补齐单机安装文档；package-v18 真实HTTPS Ingress密钥管理与双节点六组全部通过，示例通过真实CLI校验/渲染，见 [部署验收](2026-09-16-deployment-acceptance.md)。随后直接启动完整示例发现Sandbox API发现轮询默认值遗漏；修复后package-v22/Lima r2六项真实FC安装验收全部通过，7项配置测试及53项驱动回归通过，本地阶段6完成，见 [完整示例验收](2026-09-16-installed-example.md)。
- 阶段7：基础 K8s 正式验收完成。[Buildkite #15](https://buildkite.com/agent-dx/agent-dx/builds/15) 在已提交的 `85d89e8` 上完成构建、镜像发布与独立 K8s 部署；SDK、认证、容量、放置、节点失联、重启和停机七组全部通过，JUnit 8项无失败/跳过，namespace清理无残留。两个Pod位于同一宿主节点；发布包、镜像与源码身份已核对，见 [正式验收记录](2026-09-16-buildkite-k8s.md)。调度修复后的[Buildkite #16](2026-09-16-buildkite-16.md)也已通过三步骤及七组用例，清理无残留。按本轮决策，FC继续本地验收，独立FC profile暂不启用。

阶段10已完成：Rust API Server、Shard命名和协议拆分通过本地回归与 [Buildkite #24](2026-09-17-rust-apiserver-k8s.md)。阶段11及阶段12的 OCI K8s 部分随后由 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 完成，八组及日志/Trace/Metrics 检查通过；这是当前最新正式基础 K8s。

当前正式流水线范围（2026-09-18）：基础 Kubernetes 公共 SDK 八组验收，使用 OCI 运行环境；Firecracker 暂继续本地验收，不启用 `ADX_E2E_CHECKPOINT`。基础 K8s 通过不替代 FC 的暂停、快照与跨节点恢复证据。

进度页的独立未完成清单见 [事项数据](control-plane-remaining.json)，包含当前剩余验收及暂缓项。

## 仍需完成的闭环

| 阶段 | 具体剩余项 | 当前限制 |
| --- | --- | --- |
| 5 | 双克隆网络故障修复及完整FC复验；GPU/NPU真实设备、真实服务混合长稳验收 | HTTP/SDK放置约束已接线；r21/r22双克隆出现FDB错误端口学习及CONNECT超时，待修复。本机FC环境没有实际GPU/NPU卡。 |
| 7 | 完整控制面三 VM、异构 Runtime 正向放置、Redis PVC 跨 Pod 恢复及剩余组合故障 | 基础包 #90 的独立 L0 和 Full #26 的跨物理 worker 十一组已通过；Full #27–#43 的定向回归已覆盖分进程组件、网络隔离、SQLite 降级、资源过期和对账中断。三 VM 部署执行器与实机验收仍缺，当前 runc-only fixture 不能验证 runsc 正向放置，K8s 集群的动态 StorageClass 尚未确认。用例编号和证据见 [测试分层](test-inventory-and-buildkite-gates.md)。 |
| 12 | 使用新的 EROFS/OCI 运行环境入口复验 Firecracker 创建与快照恢复 | 基础 K8s OCI 三模式已通过；正式 K8s FC profile 按决定暂缓，先保留本地 KVM 验收。 |

## 已登记待办

- 正式 K8s Firecracker profile：本轮暂不执行，FC继续本地验收。后续准备目标架构 runtime kit 与 KVM worker 后独立运行；基础 K8s 的通过不算 FC 用例通过。

- x86双克隆对照验证（用户暂缓）：在47.83.171.126上使用对齐的sandboxd/FC版本，运行同一快照创建A/B、B启动后访问A的顺序，采集MAC/TAP/FDB及网络头。保留Lima r21/r22失败证据；当前不启动该验证，不据此归因于ARM/x86。

- 配置/证书热重载：后续按运维需求规划，本期保留启动时读取配置与证书，更新后重启相应组件。

- 节点模板预热：按用户决定暂不支持；后续有需求时再规划，不计入本期快照与存储验收。

## 新增特性规划（2026-09-16）

阶段8、9为新增特性，不影响已有阶段的验收记录。阶段8先完成实例数量和资源分配Metrics，再接日志采集与Trace；阶段9单独交付日志滚动、压缩和保留策略。详细范围及验收见[可观测与日志规划](observability-logging-plan.md)。

阶段8首批Metrics已完成：提交 `5f592cf`，53项Rust、53项驱动、Clippy、本地双节点SDK七组与Buildkite #17基础K8s通过；真实抓取确认满载/排队/删除后的指标与两侧资源账本一致。见[验收报告](2026-09-16-metrics-acceptance.md)。阶段8的日志采集与Trace已通过本地及Buildkite #21正式K8s验收，见[日志与Trace报告](2026-09-17-observability-k8s.md)；阶段8按本期范围完成；统一实时队列丢弃指标按用户决定后置；阶段9日志滚动压缩已完成：`3985f4b`，macOS 16项/Linux 17项Rust检查、53项驱动检查、Clippy、本地双节点SDK与Buildkite #18基础K8s七组通过；两节点40/6个gzip归档可读，清理无残留。见[日志验收记录](2026-09-16-logging-acceptance.md)。


## 阶段11：节点本地优先创建

已接入可配置 API 节点轮转、共用 Admission 暂留、原子 claim 与中心账本同步、同 ID 并发收敛、未知写入屏障恢复及后台重试。
本地 Redis/mTLS/HTTPS 验证及新制品真实 sandboxd/runc/Execd 双节点8组验收均通过，包含 `local-first`，见 [端到端报告](2026-09-17-local-first-e2e.md)；提交 `363e44f` 的 [Buildkite #30](2026-09-18-runtime-environment-k8s.md) 已使用正式发布包完成同一八组 K8s 验收。见 [契约](atomic-environment-claim.md)。

## 阶段12：内置运行环境

本地 EROFS 和不可变 OCI image 两种来源已接入统一配置、Environment 持久化和 adxlet。默认实例直接使用内置环境；自定义用户镜像只读挂载同一 bootstrap 到 `/__adx`。静态 Execd、PID 1 孤儿回收、standalone EROFS 及本地真实 runc 双节点验收已通过；Buildkite #30 使用 OCI 完成 default、runtime-only、custom 三模式及八组 K8s 验收。Firecracker 新入口与快照恢复复验仍单列，不用基础 runc K8s 结果代替。

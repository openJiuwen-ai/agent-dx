# 管控面后续实施阶段

逐阶段以行为测试驱动实现，以真实部署用例验收。组件测试、本地端到端和 Kubernetes Buildkite 三类证据分别记录。

| 阶段 | 实施范围 | 验收重点 |
| --- | --- | --- |
| 1. 同节点暂停／恢复 | Node Manager 串行状态机、RRT HTTP 协作、sandboxd checkpoint/restore、本地存储、Redis 提交、Frontend、路由 | 真实 Firecracker 创建→执行→暂停→Node Manager 重启→恢复→删除；内存计数器、PID、文件、执行身份、资源和路由 |
| 2. 节点生命周期与降级 | 空闲删除、可配置自动重启及退避、资源源选择与过期保护、压力准入、SQLite 降级日志与补写 | Master 失联、节点进程重启、采集失败、清理失败；遵守节点生命周期所有权和权威对账规则 |
| 3. 快照与存储 | 对象存储适配、可复用快照目录、引用与延迟删除、预算缓存、未登记制品回收 | 上传失败回滚、恢复点过期、引用期间删除、缓存淘汰、从快照创建新实例 |
| 4. 跨节点恢复与接管 | 同 Instance ID 归属转移、旧执行隔离、心跳故障处理、恢复与路由切换 | 源节点恢复、旧请求重放、接管中途失败；缺少 checkpoint 或 local-only 制品明确失败 |
| 5. 调度能力与性能 | Global 轮转、Domain 调度、Local 准入；优先级、GPU/NPU 整卡、亲和／反亲和；增量状态与候选复用 | 不超分、卡号正确、排队公平、规则正确；同一 Linux 主机与既有分支统一负载比较 QPS/P99/更新及冲突成本 |
| 6. 接口与部署收口 | 最小 API Key 管理、启动配置和证书加载、Node Manager/Node Proxy 两种进程模式、统一 CLI 与发布包 | HTTP/SDK 兼容、身份隔离、进程重启、停止清理、同包角色组合、安装文档可复现 |
| 7. 正式端到端流水线 | 独立 K8s 部署和用例步骤、基础功能/暂停恢复/故障用例、日志及制品汇总 | 部署过程可见、逐用例结果可见、失败日志可定位、包/镜像/源码身份一致 |

阶段 1 已完成本地验收：2026-09-15，独立 Lima ARM64 KVM 节点使用真实 OCI 镜像，通过公共 SDK 完成创建、执行、暂停、Node Manager 重启、恢复和删除。恢复后内存计数器继续增加、PID 不变、二进制文件一致；Redis 记录为 Deleted 且资源释放，sandboxd 清单与 checkpoint 目录为空。122 项定向 Rust/Redis 测试、Clippy、真实 RPC 与 Frontend HTTP 集成通过。详细证据见 [本地验收记录](2026-09-15-checkpoint-acceptance.md)。

阶段 2 已完成本地验收：Lima r7 的 checkpoint 与节点生命周期共 10 项真实 Firecracker 用例通过，141 项 Rust/Redis/HTTP 定向测试、138 项 SDK 单测和 Go 检查通过。契约及证据见 [节点生命周期与降级](node-lifecycle.md)。阶段 3 已完成本地快照与存储验收；阶段4已完成本地故障恢复验收，阶段6已完成本地部署收口验收，阶段5与7尚未全部验收完成。本地验收与 Kubernetes Buildkite 分开记录。

阶段 5、6 包含既有实现的补齐与验收，不意味着这些组件需要从头重写。已经确定删除的函数／Actor／FaaS、抢占、拓扑分布、成组调度、租户配额和 Master 弹性池不重新引入。


当前推进记录（2026-09-16）：

- 阶段3：本地验收完成。统一 package-v16 / Lima r20 的17项真实 MinIO/FC 用例通过，包含双克隆、源快照物理回收后的暂停恢复，以及节点重启后清理未登记上传残留。节点85项、真实Redis/mTLS 8项、Clippy和驱动6项通过，见 [存储说明](snapshot-storage.md)。
- 阶段4：已实现心跳超时失效与持久化、路由撤销、Node Manager恢复对账清理和迟到状态拒收；补齐超时前正常进程接续、Master重启后报到期限。真实Redis/mTLS22项、库5项及Clippy通过。package-v19故障组通过，但正常重启因强制等待超时失败；修复后package-v20本地双节点七组全部通过，清理无残留。共享checkpoint的新代次分配、恢复RPC、重复请求处理与制品引用保护已接通；177项组件回归、Clippy及package-v21本地双节点七组通过，见 [跨节点恢复](2026-09-16-cross-node-recovery.md)；Lima双命名空间真实FC六项及恢复计划落盘后Master重启已通过（[记录](firecracker-cross-node.md)）；r5目标Node Manager执行中重启已通过，旧backend清理后同代次恢复；52项驱动回归通过，本地阶段4完成，见 [验收记录](2026-09-16-node-failure-acceptance.md) 和 [契约](node-failure-takeover.md)。
- 阶段5：当前源码与历史制品的6场景×7轮同机复测通过，见 [新报告](2026-09-16-scheduling-recheck.md)；HTTP/SDK的节点与实例亲和、反亲和、实例标签、权重和有序偏好已接线，61项Rust、8项真实Redis/mTLS/HTTPS、Go检查及140项SDK测试通过，见 [放置约束](http-node-placement.md)。package-v17/Lima r21、r22的新放置用例通过，但完整运行均在双克隆文件写入遇到Node Proxy CONNECT 504（10/18），见 [网络取证](2026-09-16-fc-clone-network.md)。本轮普通路径6场景×7轮复测完成，见 [条件组接线后性能](2026-09-16-placement-groups-recheck.md)。真实设备/混合负载仍待验收。
- 阶段6：共用 NodeProxyService 与 Node Manager 共进程接线通过157项定向测试，真实共进程 Firecracker/S3 10项验收通过，见 [进程模式](node-proxy-process-modes.md)。API Key管理的真实HTTPS/mTLS/Redis集成已通过，见 [密钥管理](api-key-management.md)。SDK命令订阅的TLS校验连接已修复，真实TLS Socket与package-v13/Lima r16复验通过；本期证书更新后重启组件生效；热重载移入后续待办。现已修复默认管理路由及部署示例接线，补齐单机安装文档；package-v18 真实HTTPS Edge密钥管理与双节点六组全部通过，示例通过真实CLI校验/渲染，见 [部署验收](2026-09-16-deployment-acceptance.md)。随后直接启动完整示例发现Sandbox API发现轮询默认值遗漏；修复后package-v22/Lima r2六项真实FC安装验收全部通过，7项配置测试及53项驱动回归通过，本地阶段6完成，见 [完整示例验收](2026-09-16-installed-example.md)。
- 阶段7：基础门禁已增加双节点放置约束；package-v17 本地 ARM64/runc 的 SDK、认证、容量、放置、重启、停机六组全部通过，放置6/6，清理无残留，驱动契约48项通过，见 [双节点验收](2026-09-16-placement-e2e.md)。新增独立 `platform-fc-e2e` profile和可复用FC驱动；入库驱动最新package-v16/Lima r20共17项通过。目标K8s的原生kit供应和支持KVM的worker仍待落实，尚未完成新增功能的正式K8s验收，见 [FC驱动](../../build/e2e/firecracker/README.md)。

本轮正式流水线范围（2026-09-16）：基础 Kubernetes 公共 SDK 七组验收；Firecracker 暂继续本地验收，不启用 `ADX_E2E_CHECKPOINT`。基础 K8s 通过不替代 FC 的暂停、快照与跨节点恢复证据。

## 仍需完成的闭环

| 阶段 | 具体剩余项 | 当前限制 |
| --- | --- | --- |
| 5 | 双克隆网络故障修复及完整FC复验；GPU/NPU真实设备、混合负载及长期公平性验收 | HTTP/SDK放置约束已接线；r21/r22双克隆出现FDB错误端口学习及CONNECT超时，待修复。本机FC环境没有实际GPU/NPU卡。 |
| 7 | 目标架构FC runtime kit供应、KVM worker确认、提交版本的正式Buildkite/K8s运行 | 新增步骤和驱动已实现并通过本地测试；profile未选中或未运行不能算阶段通过。 |

## 已登记待办

- x86双克隆对照验证（用户暂缓）：在47.83.171.126上使用对齐的sandboxd/FC版本，运行同一快照创建A/B、B启动后访问A的顺序，采集MAC/TAP/FDB及网络头。保留Lima r21/r22失败证据；当前不启动该验证，不据此归因于ARM/x86。

- 配置/证书热重载：后续按运维需求规划，本期保留启动时读取配置与证书，更新后重启相应组件。

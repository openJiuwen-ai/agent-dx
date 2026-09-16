# 管控面实施状态

2026-09-16 更新：暂停/恢复、S3 存储、SQLite 降级、空闲回收及自动重启已有真实本地 FC 验收。共用 NodeProxyService、目录 RPC 与 Frontend 查询也已接通。最新能力和未完成项以 [阶段进度](control-plane-roadmap.md) 为准；本文保留早期实施与 TDD 记录。

2026-09-15。内部抽象为 Instance，外部 Sandbox API 保持兼容。已实现库、适配器和服务进程入口，创建／执行／删除及节点进程重启已通过真实两节点本地 E2E，见 [本地验收](local-e2e.md)。Buildkite 远端门禁尚未运行。当前发现与恢复边界见 [节点恢复阶段](recovery-discovery.md)，路由发布与本机代理同步见 [路由阶段](route-publication.md)。

## 已落地模块

| 目录 | 已实现职责 | 当前边界 |
|---|---|---|
| `platform/crates/core` | Instance 类型、纯状态转换、标量/设备账本、类型化调度策略 | 创建/删除状态子集，标量单位为毫核和字节，设备为整卡 |
| `platform/crates/protocol` | 新控制面 gRPC 消息、校验转换；新 Node Proxy 版本化协议 | Master/Node RPC 已实现；协议 crate 不承载业务状态机 |
| `platform/crates/scheduling` | 公共 Filter/Score 接口、静态规则组合、评分权重、确定性选节点 | 已实现整卡、节点/实例亲和反亲和及硬软拓扑分布；内部协议已承载策略 |
| `platform/control-plane/master` | Global 轮转，Domain 调用公共调度框架选节点，节点自动分域，租户轮转/优先级/FIFO，资源预留 | 内存调度内核＋Redis 存储和恢复＋RPC 协调、服务入口、发现续期及心跳管理 |
| `platform/control-plane/node-manager` | 每 Instance 串行任务、本机准入、创建/删除/提交重试、失败清理 | 本机拥有状态机；Node RPC、MasterStateSink、服务入口及权威目录对账已接通 |
| 同上 `sandboxd.rs` | UDS gRPC Start/Delete/List/Stats，PR #56 协议，IP 校验、资源单位转换 | 当前生命周期接入创建/删除；checkpoint 仅已纳入生成协议 |
| 同上 `readiness.rs` / `runtime_control.rs` | HTTP 状态/身份校验、checkpoint 准备/撤销客户端、运行状态复核、超时与重试 | 客户端及 RRT 服务已接通；完整暂停状态机与制品提交另行接线 |
| `platform/runtime/rrt` | HTTP 控制器、后台 handoff、恢复身份校验、监听器重建、旧连接退役 | 不依赖 POSIX/RuntimeRPC；本地进程测试使用 handoff fixture |
| 同上 `routes.rs` | Node Proxy UDS 完整绑定同步、本机绑定/退役、会话与版本校验、代理重启重放 | 分进程已接线；共进程通过同一个 `Routes` 接口注入，装配待实现 |
| `gateway/src/edge/master_routes.rs` | Redis 发现 Master、gRPC 全量/增量路由缓存、Master API Key 校验缓存 | 已连接生产入口；真实 Redis/mTLS/H2/TCP 服务协作验证 |
| `third_party/sandboxd` | 上游原始 proto、许可证、固定提交和文件 SHA256 | 不包含构建好的 sandboxd，不由产品 supervisor 托管 |

## 本机创建/删除的实际顺序

```text
已分配的 Instance
  → 本机准入并预留资源
  → sandboxd Start，获得实例 IP
  → 确认后端在运行、RRT 就绪
  → Node Proxy 绑定确认
  → Running
  → StateSink 提交结果
```

删除依次完成本机路由退役、后端删除确认、释放本机预留、提交 Deleted。清理失败记录 `Failed + resources_held=true`，不能因 Failed 状态就重用资源。

每个 Instance 的操作在一个任务内顺序执行。客户端断开不会取消已经接收的操作；重复创建不会再执行 Start，提交失败后的重试只重新提交结果。Deleted 不能被旧创建请求重新启动。

`StateSink` 区分 Published（Master 已提交集群存储）与 Journaled（节点持久化降级日志）。Master 现已实现 Redis 存储和调度恢复库；StateSink 的 Master RPC 已接通并通过真实 Redis 协作测试；SQLite 降级与有序补写现已实现，见 [节点生命周期](node-lifecycle.md)；节点重启对账已接通，完整平台部署仍待装配。

sandboxd 适配器把已开始的 RPC 放在独立任务中，持有同一执行身份的锁，防止清理越过未结束的 Start。后端结果不明确时保留资源等待对账。Node Proxy 使用新 gRPC 协议，在接收端按归属代次和绑定版本拒绝迟到更新；退役保留版本记录。两者的进程重启仍需权威对账，内存锁和版本记录不能替代跨重启隔离。

## sandboxd 对接基线

选用 [PR #56](https://github.com/inclusionAI/sandboxd/pull/56)，固定提交 `efc201531d7e2e9d69505da151eb66084b61eebf`。`third_party/sandboxd/source.json` 是后续构建及 E2E 制品溯源输入。

已核对这个提交的服务源码：

- Start 返回 `sandbox_ip`，protobuf 字段号 4；包含 Checkpoint RPC。
- `CPU` 为毫核；`Memory` 在实现中乘以 `1024 * 1024`，适配器从字节换算为 MiB；磁盘限制使用 `writable_layer_limit_bytes`。
- 按 ID List 不存在的实例返回 gRPC NotFound，作为删除确认；其他查询错误不能当作实例已消失。
- RRT 启动命令、环境变量、工作目录、RPC 超时均可配置。默认命令要求实例镜像包含 `/usr/local/bin/rrt-runtime`，并开启 `RRT_HTTP_ONLY=1`、端口 `50090`。运行镜像仍需在完整部署包/E2E 环境中实际构建验证。

## 下一条接线边界

1. Master Redis 存储与调度恢复库已完成，见 [存储契约与真实 Redis 验证](master-storage.md)。服务启动必须先建立 Session、恢复持久化 generation 和占用，再接受节点注册；派发分配前必须完成持久化。
2. Master/Node RPC、mTLS 组件身份验证与 StateSink 已实现，见 [RPC 接线说明](control-rpc.md)。启动配置、Go Sandbox API 的新 RPC 后端、API Key、Redis 发现及节点重启对账已经接通；现有 API 和 Agent 入口保持。
3. Master 路由全量/增量已接入 Edge，新入口使用 Redis 发现与 Master 认证；Node Manager 与 Node Proxy 的 UDS 全量同步、代理重启重放已经接通，见 [路由阶段](route-publication.md)。统一进程托管与发布包已实现，见 [部署阶段](process-deployment.md)；Activity 接收装配仍待完成。
4. 统一构建、两节点进程部署、安装公共 SDK、真实创建/命令/文件/删除 E2E 已打通。仓库已包含 `build/e2e/kubernetes` 部署驱动器及 `.buildkite/pipeline.yml`，待配置远端队列／固定镜像后启用提交级门禁。
5. 在这条链路上继续补暂停/恢复、快照存储、SQLite 降级、故障接管、空闲回收及其余已决定的调度能力。

## 验证口径

规则测试、模拟依赖的生命周期测试、使用上游生成协议的本地 gRPC/UDS 测试、RRT HTTP 契约测试均属于本地开发验证。它们没有启动真实 sandboxd 或完整新控制面，不构成端到端验收。

每个长测试由独立验证任务执行并保留完整日志；失败记录与成功记录分开。Buildkite 必须遵循 [完整平台 E2E 契约](control-plane-ci.md)，仓库已有完整平台流水线配置；尚无远端 Buildkite 通过记录。

前一批创建/删除模块验证结果（本次 RRT 修改前）：Rust workspace 全特性 240 项通过（新增控制面 41 项），新增四个控制面 crate 的 Clippy `--all-targets -D warnings` 通过；Go 107 个顶层测试、含子测试 200 项及 vet 通过。完整日志分别在本机 `/tmp/adx-control-workspace.log`、`/tmp/adx-control-clippy.log`、`/tmp/adx-control-go-protocol.log`。这些结果对应当前工作树，尚未形成提交级 CI 证据。

TDD 红灯证据：调度内核和节点控制器分别先因缺失实现失败；sandboxd UDS 契约测试先复现 3 项 NotFound 处理失败，修复后通过。对应本机日志 `/tmp/adx-control-scheduling-red.log`、`/tmp/adx-control-node-red.log`、`/tmp/adx-sandboxd-rpc-red.log`。

## 内部协议调整

仅 `frontend_proxy_service.proto` 保留为旧 gRPC 兼容服务，导入的消息类型限制在 Frontend 兼容层。已删除其生成范围内的 CoreService、RuntimeService、RuntimeRPC、InvocationRPC 服务。

Node Proxy 绑定/活跃度已改为 `adx.node.v1`，同时修改客户端和服务端；新增的 Node Manager 活跃度接收器会校验会话、序号与观测时效。共进程和分进程使用相同绑定处理契约，现由共享 NodeProxyService 统一装配，见 [进程模式](node-proxy-process-modes.md)。

RRT `rrt.v1` 的 Process/Health/Filesystem/Port 协议及服务、工具入口已经删除，命令、文件等能力通过 HTTP 验证。RRT POSIX 流、通用 signal 上报、函数调用分发和 protobuf 构建依赖已移除。Node Manager 通过新 HTTP 控制接口读取身份/活跃度、准备 checkpoint、撤销确认未启动的 checkpoint；RRT 在后端 handoff 后完成环境更新和监听器重建。完整协议清单见 [协议边界](../../platform/api/proto/README.md)。

前一批 gRPC 调整验证结果（本次 RRT 修改前）：Rust workspace 全特性 239 项通过，新增控制面 Clippy 严格检查通过，Go 107 个顶层测试（含子测试 200 项）及 vet 通过，SDK/RRT Socket 互操作 11 项通过。删除旧 gRPC 专属测试，并增加 HTTP 命令与二进制文件读写测试、新 gRPC 绑定代次/重试/迟到请求测试及活跃度快照测试。结果仍属于本地验证，完整平台 E2E 尚未执行。

日志：`/tmp/adx-grpc-final-tests.log`、`/tmp/adx-grpc-final-clippy.log`、`/tmp/adx-grpc-redesign-go.log`、`/tmp/adx-grpc-final-interop.log`。互操作证据保存在 `out/ci/interop/20260914T115453Z-57213f99/`。

## RRT HTTP 控制替换验证

RRT 的 POSIX/RuntimeRPC 调用链已移除，协议见 [HTTP 控制契约](../../platform/api/http/runtime-control.md)。Start 注入明确的 Instance/执行/归属代次；Node Manager HTTP 客户端用于状态、准备和确认未开始后的撤销。活动计数通过状态读取，RRT 不再发送通用 signal。暂停制品和持久化提交仍由 Node Manager 生命周期模块接线。

本轮 TDD 红灯：`/tmp/adx-rrt-control-red.log`，先因缺少 HTTP 控制器及共享类型失败。替换后 workspace 全特性 207 项通过；RRT 和四个新控制面 crate 的 Clippy `--all-targets -D warnings` 通过；SDK/真实 RRT Socket 互操作 11 项通过。旧协议专属测试随实现删除，新增 HTTP 控制与 handoff 测试。

`control_http.rs` 启动真实 RRT HTTP/隧道监听并调用生产 Node Manager 客户端，验证认证、完整身份、prepare 重试、abort 后复用、过期请求拒绝、恢复后 token/身份更新、旧隧道关闭及新连接建立、非法恢复身份拒绝。FIFO 模拟后端 handoff，不等同于真实 sandboxd checkpoint。

日志：`/tmp/adx-rrt-http-final-tests.log`、`/tmp/adx-rrt-http-final-clippy.log`、`/tmp/adx-rrt-http-final-interop.log`。Socket 证据目录为 `out/ci/interop/20260914T122123Z-f1b17ce4/`。尚未执行完整平台 Buildkite E2E。

## Filter / Score 插件边界

先前 Domain 内联的可用性过滤、资源适配和 Pack/Spread 评分已抽到 [公共 scheduling crate](../../platform/crates/scheduling/README.md)。Domain 保留排队和资源预留，Global 继续只做轮转，Node Manager 负责最终本机准入。默认静态注册 NodeAvailable、ResourceFit、ResourceBalance；通过 `Framework::new` / `Master::with_framework` 组合附加 Filter 和带权重的 Score。

插件只读请求与本轮节点快照。过滤不通过不进入评分；评分越高越优，同分按节点 ID 排序；插件错误保留等待请求，不消耗资源。必需准入规则不能被自定义组合移除。设备、亲和/反亲和和拓扑分布规则现已实现，详见下节。

TDD 红灯日志：`/tmp/adx-scheduling-plugins-red.log`（缺少 adx-scheduling 接口）。16 项定向测试（既有调度 9、插件接线 3、公共框架 4）及严格 Clippy 全部通过。日志分别为 `/tmp/adx-scheduling-plugins-tests.log`、`/tmp/adx-scheduling-plugins-clippy.log`。

## 整卡、亲和与拓扑分布

`adx-core::scheduling` 定义设备类型/型号/物理 ID、标签选择器、硬软策略和设备账本。公共调度框架静态注册 DeviceFit、NodeAffinity、InstanceAffinity、Topology 过滤器及对应软约束评分器；原 Pack/Spread 保持。Master 在调度前构造所有内嵌 Domain 的一致快照，计入尚未启动的分配；Global 仍然只轮转，候选节点仍属于选中的 Domain。

Domain 原子预留标量资源和具体卡；设备清单更新保留占用。Node Manager 根据独立有效期复核设备清单，进行本机原子预留，向 sandboxd PR #56 的 `xpu_allocations` 传递具体卡 ID。清理失败继续占用，确认删除后释放。内部 protobuf 已包含策略、节点标签/卡清单及具体分配，带严格转换和往返测试。

规则语义及配置示例见 [调度规则](../../platform/crates/scheduling/README.md)。本批实现的是调度库、内部契约和执行适配，公开 Sandbox API 映射、自动硬件发现、完整进程服务接线及真实 GPU/NPU 部署验证另属后续工作。

TDD 红灯：`/tmp/adx-scheduling-constraints-red.log`。Workspace 全特性 234 项通过，Go 107 个顶层测试（含子测试 200）及 vet 通过。日志 `/tmp/adx-scheduling-constraints-workspace.log`、`/tmp/adx-scheduling-constraints-go.log`，Go 证据目录 `out/ci/go/20260914T124511Z-9c22efd9/`。这不是完整平台 Buildkite E2E。

归一化评分收尾修改后，5 个相关 crate 的 70 项定向测试与 Clippy `--all-targets -D warnings` 全部通过。日志 `/tmp/adx-scheduling-constraints-final-tests.log`、`/tmp/adx-scheduling-constraints-final-clippy.log`。

## 调度热路径优化

已补增量不可变快照、节点/租户/标签查询索引、请求级查询准备、256 请求 / 10 ms 有界轮次、预留变化日志与溢出重建、受内置规则能力限制的语义计算聚合及候选排序复用。节点状态先发布再唤醒等待队列；保留租户轮转、优先级和 FIFO，插件中途报错会同时返回之前的成功分配。拓扑分布不作为本轮扩展目标。

实现映射、配置与验证入口见 [调度性能说明](scheduling-performance.md)。本轮增量对账基于 Master 内存账本和变化节点序号，不增加 Redis/SQLite 的持久化承诺。

本地 release 基准以同一 ADX 实现关闭候选复用为对照，128 节点、4,096 次同步调度请求、256 个未释放实例的保留窗口，预热后交替运行 7 轮；全部放置逐次一致。同类请求中位数 38.231 → 22.188 ms（1.72×），混合请求＋容量更新 40.331 → 27.358 ms（1.47×）。日志 `/tmp/adx-scheduler-optimization-benchmark-v2.log`。这些是调度库本地数据，不是旧分支对比或完整平台 E2E。

Workspace 全特性测试 250 项通过，严格 Clippy 通过（`/tmp/adx-scheduler-optimization-workspace-v2.log`、`/tmp/adx-scheduler-optimization-clippy-v2.log`）。收尾新增的等待队列 FIFO 测试先复现“容量恢复后新请求越过老请求”（`/tmp/adx-scheduler-fifo-red.log`），修复后 5 个相关 crate 的 87 项定向测试及严格 Clippy 均通过（`/tmp/adx-scheduler-optimization-final-tests.log`、`/tmp/adx-scheduler-optimization-final-check.log`）。

## Linux 调度基线复测

已完成历史 Unit Snapshot 基准二进制与当前 ADX Release 程序的同容器复测：1,000 节点、Pack、一次完整预热、七轮正式测量，18 组、126 条正式结果。开启聚合／候选复用的持续调度与报告确认闭环为 14,219 → 21,180 QPS，生命周期 P99 为 373.46 → 236.24 ms。关闭复用的吞吐基本持平。详见 [基线比较报告](scheduling-baseline-comparison.md)。

新增节点拒绝后重新分配的 `Master::retry`，校验精确 assignment，释放旧资源、记录当前请求拒绝过的节点并重新入队。修复重试绕过共享候选缓存的退化，保留 generation 和请求隔离检查。该入口要求节点未执行或已经完成清理，不承担运行中实例迁移。

本轮三个相关 crate 的 51 项测试通过、1 项性能用例忽略，严格 Clippy 和 Linux Release 构建通过。旧基准执行文件匹配历史报告摘要，未重新编译当前分支 HEAD。ADX 上报确认使用测试邮箱驱动，不代表生产 RPC/Redis/Node Manager 或完整 Buildkite E2E 已接通。

## Master Redis 存储与恢复

已实现节点归属、精确 Assignment、节点结果与集群发布版本的 Redis 条件提交，以及调度内核恢复。重启后保留资源占用与 generation 下限，节点重新注册前不开放新调度；旧 Master Session、旧 Assignment 和旧实例 revision 不能覆盖新结果。节点明确拒绝执行后可条件替换分配。

55 项普通回归、5 项真实 Redis 集成、严格 Clippy 和 7 项 CI harness 测试通过。真实 Redis 用例包含 AOF 写入后 SIGKILL／重启。契约、运行方法与证据见 [Master 存储说明](master-storage.md)。

## Master / Node RPC 与 StateSink

已接通节点注册、创建分配、实例查询、Frontend 直达节点删除和结果提交。Master 持久化 Assignment 后调用 Node；Node 的串行控制任务执行生命周期操作，再通过 `MasterStateSink` 向 Master 提交结果。Master 不持锁等待 Node RPC，节点回调不会与创建互相等待。

RPC 从 mTLS 对端证书识别组件身份，Frontend 传递已经验证的租户上下文。并发重复创建复用同一分配；删除完成但提交失败时，重试只补交结果。资源不足的请求保留在内存队列，客户端断线后已接收任务继续运行。

真实 Redis＋mTLS Socket 协作测试通过，覆盖上述链路、权限拒绝、等待调度和 Redis 故障恢复。当前为可装配服务和客户端，尚未完成 Go HTTP 后端、产品进程启动、Edge 路由订阅和完整平台 Buildkite E2E。实现及验证记录见 [RPC 阶段说明](control-rpc.md)。

## Go HTTP 后端与服务入口

新增 `controlbackend` 适配器与有界归属缓存，Frontend 缓存命中直接调用 Node Manager；刷新地址也不能把未确认的旧删除请求重放到新 generation。镜像创建、资源单位转换、整卡请求与环境变量通过新 Instance RPC 下发。API Key 经 Master 验证，Frontend 短时缓存摘要与身份，支持可选到期时间。

增加 Master、Node Manager 和 Go Sandbox API 的 `--config` 进程入口；Master 支持初始化密钥摘要，Node 装配 sandboxd／RRT HTTP／Node Proxy UDS／StateSink。既有实例重启对账及 Redis 地址发现见 [节点恢复阶段](recovery-discovery.md)；自动资源探测、其余生命周期和完整 E2E 尚未装配。详见 [HTTP 与进程阶段](frontend-control.md)。

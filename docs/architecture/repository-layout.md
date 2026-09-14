# agent-dx 统一仓库目录规划

创建：2026-09-13；更新：2026-09-14。状态：目标架构方案；首批现有源码已在本地 agent-dx 迁移分支归位，新管控面仍待实施。Sandbox HTTP 接口以 Go 代码为定义来源，OpenAPI 按需生成。

范围：已实现的数据面、计划重构的 Instance 控制面、Sandbox HTTP 服务和客户端 SDK，整合到 `git@gitcode.com:robbluo/agent-dx.git`。服务端与客户端 SDK 均已由用户确认纳入。

## 产品分层与依赖契约

用户明确的分层契约：原有 Agent Distributed Executor 是上层 Agent 产品，通过 Sandbox SDK 封装 Agent 接口；重构后的管理面、数据面及基础运行时是底层执行平台。两层在同一仓库维护，通过公开 Sandbox 能力衔接。

```text
Agent 产品层：agent/
  Agent CLI / Agent SDK / Agent Executor
  Agent 接口、会话与执行编排
                 ↓ 使用
公开能力边界：platform/sdk/sandbox/
  Sandbox 创建、暂停恢复、删除、命令、文件、终端与访问能力
                 ↓ 调用公开接口
执行平台层：platform/
  管控：Sandbox API → Master / Node Manager
  运行时：RRT；后端：Node Manager → sandboxd

共享接入与转发：gateway/（Agent 与平台均可使用）
  Edge → Agent 服务 / Sandbox API / Node Proxy → RRT / 实例服务
```

这里的上下层表示依赖关系，不表示新增 Agent 服务进程或每次请求必经全部组件。Agent SDK、CLI 与 Executor 之间的具体调用编排在 Agent 层内定义；仅需要用户态编程能力的模块无需依赖底层 SDK，实际使用 Sandbox 能力的模块通过 Sandbox SDK 接入。

| 边界 | 契约 |
|---|---|
| Agent 业务模型 | Agent、Agent 会话、turn/event 等语义及其与 Sandbox 的关联由 agent/ 持有；平台保留通用 Instance、资源、归属与运行状态模型 |
| Agent 使用平台 | 通过 Sandbox SDK 的公开接口；不直接操作 Master 内部 RPC、平台 Redis/SQLite、Node Proxy 路由控制或 sandboxd API |
| SDK 能力不足 | 在 Sandbox SDK 和公开服务接口中补齐通用执行能力，再由 Agent 层组合；不通过跨层 import 或访问内部存储绕过边界 |
| Agent 生命周期 | Agent 层决定何时申请/复用/释放 Sandbox，平台继续按既定契约调度并管理 Instance；基础设施恢复结果由 Agent 层映射成 Agent 业务状态 |
| Agent Executor 归属 | 属于 Agent 层；即使仍部署在 Sandbox 内，也不归入平台通用 runtime。逻辑分层与进程部署位置分别表达 |
| 身份与配额语义 | Agent 调用沿用 Sandbox SDK 的租户凭证与资源归属规则；不通过内部特权接口跳过用户鉴权 |
| 构建与测试 | 平台构建、单测和基础 E2E 可在不安装 Agent 包的情况下运行；Agent 单测可 mock Sandbox SDK，跨层测试通过公开契约执行 |

顶层按 agent/ 产品、platform/ 执行平台和 gateway/ 共享接入与转发组织。Agent 层现有三个分发包保持独立；按实际 Agent Gateway 需求补充入口模块。Agent 状态持久化的具体后端不在本次目录调整中决定，也不与平台 Instance 存储表混用。

### 顶层 gateway：共享接入与转发

将现有 data-plane-gateway 迁入顶层 `gateway/`。该目录承载连接、鉴权与转发等接入能力；名称暂用 gateway，最终命名待定。该工程供 Agent 和执行平台复用，先保持一个 Rust crate、多二进制。目录位置表达代码归属，进程组合仍由部署配置决定。

| 模块 | 归属与职责 |
|---|---|
| 公共传输 | gateway/src/common；HTTP/TLS、连接池、流式转发、路由匹配、指标与排空等机制；不包含 Agent 会话或 Instance 生命周期规则 |
| Edge 入口 | gateway/src/edge；统一监听与转发，按配置启用 Agent upstream 路由、Sandbox API 路由和 Instance 数据路由。Instance 路由订阅与认证客户端保持明确模块边界 |
| Node Proxy | gateway/src/node；节点绑定校验、连接转发及可嵌入的 NodeProxyService，供 Node Manager 以进程内接口或 UDS 控制 |
| 进程入口 | gateway/src/bin；Edge、Node Proxy 等薄入口，负责加载配置和装配所需模块 |
| Agent 业务接入 | agent/gateway；按需实现 Agent API、身份与业务上下文适配；Agent 会话、执行编排留在 agent/，使用执行资源时调用 Sandbox SDK |

建议从现有 Edge 的 Host/path 反向代理能力演进。锁定的源码快照已具有 ReverseProxy 和 ProxyRoute；Agent 路由和新认证接入仍需实现与验证。通用传输代码归入 common，平台专属订阅和绑定规则分别留在 edge/node，避免将整个 gateway 视为无业务差异的纯传输库。

```text
独立入口：
  Agent 客户端 → Edge（Agent 路由配置）→ Agent 服务
  Sandbox 客户端 → Edge（Sandbox 路由配置）→ Sandbox API / Node Proxy

共用入口：
  客户端 → Edge（同一进程启用两组路由）
               ├── Agent upstream → Agent 服务
               └── Sandbox 路由 → Sandbox API / Node Proxy

Agent 需要执行资源时：Agent 服务 → Sandbox SDK → 平台公开接口
```

这里的独立或共用入口复用同一个 gateway 工程，部署时选择启动一个还是多个 Edge 进程。Agent 业务服务自身的语言和部署方式可以独立选择；将业务处理器直接嵌入网关进程属于后续需求，不因顶层目录调整而预设。

依赖方向：Node Manager 可以依赖 gateway 的 node 库接口；gateway 不依赖 Master、Node Manager 或 Agent 业务实现。平台路由适配可依赖 platform/crates/protocol 等协议与纯类型包，这些包不能反向依赖 gateway。common 保持通用，Agent upstream 通过地址与转发配置接入；仅启用 Agent 路由时不要求连接平台 Master。平台独立部署不要求安装 Agent 包。

共用入口按 Host 或明确路径命名空间区分路由，启动时拒绝歧义配置；每条路由明确认证责任，外部身份 header 不直接作为可信身份。保留 SSE、WebSocket、长连接的流式与取消语义，并按路由控制并发与缓冲。网关进程共用时，共享崩溃与内存故障域。

RRT 继续位于 platform/runtime/rrt：它负责实例内执行与运行时协作，属于执行平台。现有 Edge 与 Node Proxy 的目录提升不改变 RRT 归属，也不改变 Node Manager 对 Instance 生命周期的管理权。

## 已核对的输入

- 2026-09-13 核对时，agent-dx 远端默认分支为 master，读取提交 `c816f8bc6a0c7ad68921f541046bb4e256f0b797`；不声明为最新 HEAD。现有 `cli/` 为 Python adx CLI，`python/` 为 Agent SDK，`executor/` 为实例内 Agent Executor；三者当前共用根 VERSION。现有 AGENTS.md 仍以早期 CLI 工程为主要描述，实施时需按新范围更新。
- 数据面结构沿用架构分析的锁定快照：gateway 当前是一个 Cargo package，包含 Edge、Node Proxy 和转发工具多个二进制；RRT 是单独 crate，挂在原 api/rust workspace 下。这里用于判断目录迁移边界，不代表已经确定最终迁移提交。
- Go Frontend 现有多层 go.mod 及对 `../api/go` 的 replace 依赖。目录迁移不能保留这个仓库外的构建假设。
- 新控制面依旧以 Instance 为核心；Global / Domain / Local 分层保留，Domain 当前内嵌 Master，普通生命周期由 Node Manager 管理。

## 推荐目录

顶层划分 Agent 产品、执行平台、共享接入服务 gateway，层内按组件职责组织。Gateway 与平台 Rust crate 加入根 Cargo workspace；通用组件不反向依赖 Agent 业务。下面是目标布局，按实际迁入的组件创建目录，不提前铺满空工程。

```text
agent-dx/
├── Cargo.toml / Cargo.lock       # Rust workspace、统一依赖与构建策略
├── rust-toolchain.toml           # 固定 Rust 工具链
├── VERSION                      # 仓库发布版本入口
├── Makefile                     # generate / build / test / package 的薄入口
├── AGENTS.md / README.md
├── LICENSE / Third_Party_Open_Source_Software_Notice.txt
│
├── agent/                       # 上层 Agent Distributed Executor
│   ├── gateway/                 # 后续 Agent 入口与路由适配，按需求建立
│   ├── cli/                     # 现有 cli/，Python adx 开发者命令
│   ├── sdk/python/              # 现有 python/，Agent 对外接口与编程模型
│   └── executor/                # 现有 executor/，Agent 执行与运行适配
│                                # 实际平台操作统一使用 Sandbox SDK
│
├── platform/                    # 通用 Instance / Sandbox 执行平台
│   ├── sdk/sandbox/             # Agent 层及其他用户使用的公开能力边界
│   │   ├── python/
│   │   └── ...                  # 其他语言按实际实现迁入
│   ├── api/proto/               # 平台内部协议定义
│   │   ├── control.proto        # Instance、调度、节点、认证、路由发布
│   │   ├── runtime.proto        # RRT 协作，提案批准后建立
│   │   └── node_proxy.proto     # 本机绑定与会话退役
│   ├── control-plane/
│   │   ├── master/              # Rust；Global、Domain、故障 InstanceManager
│   │   ├── node-manager/        # Rust；Local、本机生命周期、执行与存储适配
│   │   └── sandbox-api/         # Go；HTTP 路由、请求响应类型与平台客户端
│   │       ├── go.mod / go.sum
│   │       ├── cmd/server/
│   │       └── internal/
│   ├── runtime/
│   │   └── rrt/                 # 通用命令、文件、终端与运行时协作
│   ├── tools/
│   │   └── control-cli/         # Rust 平台运维 CLI，内部包含 supervisor
│   └── crates/                  # 平台内部共享库
│       ├── core/                # Instance / Node / 资源 / 归属等纯类型
│       ├── protocol/            # 平台 RPC 生成代码与薄转换
│       └── scheduler/           # Filter / Score 与共享规则
│
├── gateway/                     # 共享接入与转发；单 crate、多二进制
│   ├── Cargo.toml
│   └── src/
│       ├── common/              # 通用传输、连接池、路由匹配、指标
│       ├── edge/                # Agent upstream / Sandbox / Instance 路由
│       ├── node/                # NodeProxyService、绑定与本机转发
│       └── bin/                 # Edge / Node Proxy 薄入口与装配
│
├── third_party/
│   ├── sandboxd/                # 后端 API 来源、固定版本与必要协议副本
│   └── redis/                   # 固定版本、校验和、构建配方与许可证
├── build/                       # 构建脚本源码，不放临时构建产物
│   ├── codegen/                 # 固定工具版本，统一协议生成与一致性检查
│   ├── images/                  # 构建环境与实例基础镜像配方
│   └── packaging/              # 汇总二进制、runtime、SDK 和版本清单
├── deploy/
│   ├── config/                  # 统一部署配置 schema 与示例
│   ├── process/                 # 进程部署样例
│   └── kubernetes/              # 相同进程在 Pod 内启动的部署描述
├── tests/
│   ├── contracts/              # HTTP / RPC / SDK 兼容契约
│   ├── integration/            # 跨组件联调
│   └── e2e/                    # 创建、数据路径、恢复及故障场景
├── examples/                    # SDK 使用与部署示例
└── docs/
    ├── architecture/            # 前后 SVG、HTML 汇总及模块设计
    ├── decisions/               # 已决策 / 待决策 ADR
    └── migration/               # 来源版本、目录映射、接入差异和迁移进度
```

`target/`、`out/`、虚拟环境及各语言缓存为忽略目录。需要导出 OpenAPI 时输出到 `out/openapi/sandbox.yaml`，可随文档或发行包发布，不建立一套手工维护的 YAML 接口定义。运行时 SQLite、日志、checkpoint 和 Redis 数据使用部署配置的数据目录，不能放进源码树。现有根 build.sh 的 clean 逻辑会清理 build/；将 build/ 用作脚本目录前必须先调整该逻辑，防止误删脚本。

## 组件内先划模块，不逐项创建 crate

Master 使用一个 crate，建议 `src/{global,domain,instances,nodes,snapshots,auth,store,routes,rpc}`。Domain、API Key、Redis 代理等是内部模块，不是独立进程。

Node Manager 使用一个 crate，建议 `src/{instances,scheduler,resources,runtime_backend,artifacts,templates,sync,routes,rpc}`。`instances` 内承载 controller、state_machine、idle、restart；`sync` 内承载集群提交、对账和 SQLite 降级日志；`runtime_backend/sandboxd` 实现后端调用；`artifacts` 内放 SnapshotArtifactStore 及本地/对象后端。RRT 控制提案批准后增加 `runtime_control` 模块，不让通用 Instance 类型依赖 RRT 消息。

顶层 gateway 保留现有包内 Edge / Node 模块与多个二进制入口；只有出现独立依赖或发布需求再拆 crate。platform/runtime/rrt 承担通用数据面执行和运行时控制协作；agent/executor 承担上层 Agent 语义，两者按职责分层，即使部署时同处一个 Sandbox 也保持依赖边界。

`platform/crates/core` 不包含 Redis、SQLite、tonic 或 sandboxd 客户端。`platform/crates/protocol` 不做调度和状态机业务。Redis 存储实现先放 Master，SQLite 降级实现先放 Node Manager，sandboxd 和对象存储实现先放 Node Manager；不提前建立一个包揽所有基础设施的 common 库。

## Node Manager / Node Proxy 进程组合（2026-09-14 补充提案）

用户要求同时支持共进程与分进程。建议保持组件职责独立，使用同一份 Node Proxy 实现，通过启动配置选择进程组合。以下接口名与默认行为为设计建议，尚未实施。

```text
共进程 embedded：
  node-manager 进程
    ├── Node Manager：生命周期、资源、持久化同步
    └── NodeProxyService：数据监听、转发、绑定、会话
           ↑ 进程内控制句柄 / 有界事件通道 ↓

分进程 standalone：
  node-manager 进程 ← UDS 控制 / 事件 → node-proxy 进程
  两个进程由统一 supervisor 分别托管

两种模式的数据请求均为：Edge → Node Proxy 数据端口 → RRT / 实例服务
```

### 复用边界

当前 gateway 已有 public node 模块与 NodeProxy 类型；activate_route、retire_route、metrics、start_drain 和 serve_h2 都有库入口。但监听器、TLS、健康服务、活动发布和退出编排仍有相当部分在 bin/node_proxy.rs。先将其整理为可复用 NodeProxyService，独立二进制只负责配置、进程级初始化和信号处理；嵌入时由宿主负责日志、全局 TLS provider、信号和最终退出。

初期继续使用一个 gateway crate，不因支持两种模式而强行拆 Edge / Node 两个工程。Node Manager 只使用 node 的服务与控制 API；通过模块/feature 边界隔离 Edge 特有依赖。后续若出现独立发布或明显依赖隔离需要，再提取 node-proxy crate。

```text
gateway/src/
├── node/
│   ├── service.rs         # NodeProxyService 启动、就绪、排空、关闭
│   ├── control.rs         # 控制契约、统一校验与命令处理
│   ├── events.rs          # 活动全量、Proxy 启动代次、健康事件
│   ├── route_control.rs   # UDS 控制传输；复用 control 处理语义
│   ├── activity.rs        # 活动跟踪与上报
│   └── server.rs          # 数据转发与本机绑定，沿用现有实现
└── bin/node_proxy.rs      # standalone 薄入口

platform/control-plane/node-manager/src/
├── proxy/
│   ├── mod.rs             # NodeProxyControl 适配与事件接收
│   ├── embedded.rs        # 启动服务，使用进程内句柄与有界通道
│   └── standalone.rs      # UDS 客户端、连接恢复与重新同步
└── routes/                # 根据有效 Instance 状态构建路由；不直接改 Proxy 表
```

NodeProxyControl 的建议操作为路由全量同步、激活/更新、退役、查询就绪与排空。Proxy 反向提供启动代次、活动快照和健康事件。消息名称待接口设计固定；UDS 可以沿用当前 protobuf framing，不因分进程就必需增加 TCP/gRPC 服务。

两种传输进入同一个校验/应用逻辑：绑定 Instance 与后端运行身份/执行代次，确认激活已生效、退役已关闭准入并触发会话终止。不得以“命令入队”代替应用完成，也不能在 embedded 模式直接让 Node Manager 改内部 HashMap。完成响应的确切范围需与后端资源释放顺序一致。

### 生命周期、资源与故障语义

- Node Manager 管 Instance 生命周期和持久化；Node Proxy 管数据监听、连接、转发和内存绑定，两种模式职责一致。
- 数据请求始终不经过 InstanceController 队列。进程内模式使用独立任务和有界控制/事件通道；阻塞操作放到适当执行器。初期可共用异步运行时，实际压测需要时再分线程/运行时；线程隔离不等于独立进程的内存或崩溃隔离。
- 限制活动连接、缓冲与控制队列；持续活动信息用全量快照校正。控制事件滞后或失效不能被判定为空闲，也不能因事件通道拥塞阻塞业务数据读写。
- Node Proxy 新启动时默认没有有效绑定，关闭数据准入，等待 Node Manager 进行权威全量同步后再开放；通过 Proxy 启动代次和控制会话区分迟到消息。仅 TCP listener 成功不等于可服务。
- 分进程模式下，Node Manager 故障时 Proxy 可继续使用既有绑定服务，等待重连对账；新 Proxy 进程不能从磁盘缓存自行恢复路由。Node Manager 重启但 Master 不可用时，沿用既定等待归属对账契约。
- 共进程共享进程故障域，进程崩溃会断开已有转发连接；不能承诺与分进程完全相同的故障可用性。建议关键 Proxy 服务任务异常时关闭相关准入并上报，由统一监督策略处理，不静默保留一个失效的数据端口。
- 新建实例需要本机数据面就绪；Proxy 故障不直接证明 sandboxd 中的实例已退出，不应因此无条件删除或重建实例。
- 显式 stop 沿用“先清理本机受管 Instance”契约：保留 Proxy 控制能力完成退役和必要清理，完成集群结果提交后停止 Proxy 和 Manager。崩溃恢复与显式 stop 分开处理。

### 配置与交付

建议统一部署配置仅增加 `node.proxy.mode: embedded | standalone`，共用 bind、安全、连接上限、排空期限等配置；standalone 额外配置控制 UDS 地址。该 YAML 是部署配置，不是 Sandbox HTTP 接口契约。

embedded 由 Node Manager 启动 NodeProxyService；standalone 由 supervisor 启动独立 node-proxy，Node Manager 不再自行拉起子进程。发布包可以同时携带两个二进制，由模式选择启动组合；禁止两个实例同时占用同一数据端口。模式切换按重启部署处理，当前不设计存量 TCP 会话热迁移。默认模式尚未选定。

共进程主要减少托管进程和控制 IPC；当前每个业务请求本就不经过 Node Manager，因此不将共进程描述为必然消除一次数据转发跳数。

源码核对来自既有架构快照：data-plane-gateway/src/lib.rs 已导出 node，node/server.rs 提供 NodeProxy，node/route_control.rs 实现 UDS 路由控制，bin/node_proxy.rs 编排活动上报、TLS、监听器和退出。新的双模式服务入口及控制抽象尚未实现。

## Sandbox API、SDK 与底层协议分开

1. `platform/control-plane/sandbox-api` 是 Go HTTP 服务，继续兼容已有 Sandbox API；它将外部模型映射为内部 Instance 请求。
2. `platform/sdk/sandbox/<language>` 是客户使用的库，不包含服务进程和运维逻辑。Sandbox SDK 已确定使用 adx-sandbox 分发包、adx_sandbox Python import、adx-sandbox CLI 和 ADX_* 配置；外部 HTTP 字段保持兼容。
3. 对外 HTTP 接口以 Go 服务中的路由、请求/响应类型及必要接口注解为定义来源；需要文档或代码生成时导出 OpenAPI，具体生成工具后续选择。`platform/api/proto` 集中保存组件间 RPC 契约。HTTP DTO、RPC 消息与内部持久化实体分别映射，不把协议生成代码直接当作全部业务模型。
4. sandboxd API 由外部后端维护，放 `third_party/sandboxd` 并固定来源版本；不得在内部协议目录建立另一份自行演进的同名 SandboxService 定义。
5. 协议源文件只有一个维护位置。Rust 在构建时生成到 OUT_DIR，由 protocol crate 导出；Go 生成到 platform/control-plane/sandbox-api/internal/gen。生成策略与工具版本由 build/codegen 统一，CI 检查可重复生成；不让多个组件各藏一份 proto。

## 构建、交付和兼容边界

- Gateway 与平台 Rust 使用根 Cargo workspace，包名建议统一 `adx-*`，代码目录不用重复加前缀。初期只纳入实际迁入/实现的 crate，不提交无法构建的空依赖。对外 Rust SDK 如确有独立 MSRV/发布要求，可通过 workspace 排除保留独立构建边界。
- Sandbox API 目标为一个 Go module，消除现有 Frontend 的嵌套本地模块及 `../api/go` 依赖；只迁入实际需要的公共代码与生成协议，不整包引入旧 Go runtime。
- Python SDK / CLI / Executor 各自保留 pyproject/setup 等发布配置；统一仓库不等于合成一个 wheel。移动后需调整相对 VERSION 路径、pytest 的 pythonpath、build.sh 与 README 链接。
- Makefile 只协调语言原生构建入口，不接入旧 C++ 总构建链。根 VERSION 标识统一发布批次，发布清单记录每个组件与 SDK 包版本、来源提交、外部依赖校验和；协议版本不由目录迁移或发布版本自动改变。
- 构建产物可在一个统一发行目录中按 `bin/`、`runtime/`、`sdk/`、`etc/`、`third_party/`、`manifest.json` 汇总。RRT 仍需进入相应实例环境；统一出包不表示所有进程都由宿主 supervisor 启动。
- sandboxd 由部署环境独立托管，不纳入本仓后台进程管理；Redis 可用内置固定版本或外部实例，延续既定方案。
- 现有 Python CLI 已占用 `adx`。建议新 Rust 运维入口暂名 `adxctl`，先区分运维命令与 Agent 开发者命令；最终是否合并 CLI 另行决定，不能无声覆盖已有 adx。
- 当前读取的 Agent CLI 与 Agent Executor 仍有旧函数/FaaS 接入。目标明确改为通过 Sandbox SDK 封装 Agent 接口；迁移时将旧 meta_service / 函数调用依赖替换为 Agent 层的 Sandbox SDK 适配。Agent 业务接口的兼容映射在 agent/ 内完成，平台核心保持通用 Instance 模型。该替换尚未实施。
- 组件单测跟随源码目录；根 tests/ 按 platform、agent 和跨层用例区分依赖。平台基础 E2E 不启动 Agent 产品，Agent 单测 mock Sandbox SDK，跨层契约测试覆盖实际 SDK 调用。

## 迁移映射与顺序

| 来源 | 目标 | 要点 |
|---|---|---|
| adx/data-plane-gateway | gateway | 先保留 crate、二进制与模块边界；更新协议生成路径 |
| 运行时源仓 api/rust/rrt-daemon | platform/runtime/rrt | 纳入根 workspace；现有 RuntimeRPC 退出要配合新控制链，不能直接删协议导致失能 |
| adx-frontend 的 Sandbox 子集 | platform/control-plane/sandbox-api | 裁剪 Go 工程，替换内部客户端，保留外部 HTTP 兼容契约 |
| Sandbox SDK 源仓 | platform/sdk/sandbox | Python 实现迁入，保留客户端测试；包与入口采用 adx 命名 |
| 计划 Rust Master / Node Manager | platform/control-plane/master、node-manager | 按批准的模块方案新建，迁移必要规则 |
| 现有 agent-dx/python | agent/sdk/python | 归入 Agent 层，保留编程模型；执行能力通过 Sandbox SDK 组合 |
| 现有 agent-dx/executor | agent/executor | 归入 Agent 层，保留独立 wheel；平台访问改接 Sandbox SDK |
| 现有 agent-dx/cli | agent/cli | 归入 Agent 层，保留 adx 入口；平台接入经 Agent 层与 Sandbox SDK |
| 当前设计稿与图 | docs/architecture、docs/decisions | 带上来源 ref，区分已决策方向与 RRT 提案 |

建议分四步：

1. 先归位现有 Agent 三个包及文档，调整路径引用，保留既有分发物与命令行为。
2. 导入已实现 gateway / RRT / Sandbox SDK 的明确版本，建立根 workspace 与按组件构建；每次导入记录源仓 commit 和保留的许可证信息。
3. 抽出 Go Sandbox API 与统一协议源；按计划实现 Master / Node Manager，再替换旧 etcd / IAM / RuntimeRPC 接入。这一步完成前，新仓有源码不等于新系统已能独立运行。
4. 将 Agent 层的旧平台访问改接 Sandbox SDK，并验证 Agent 接口兼容、Sandbox SDK 契约及平台独立运行。统一出包时区分 Agent 与平台构建目标，再做跨层 E2E 和故障验证。

后续代码以 agent-dx 为单一维护位置；建议一次性导入并记录来源，不新增三个长期同步 submodule。本仓 Sandbox SDK 使用 adx 命名；原源仓的历史分发物不在本次更名中修改。

## 验证范围

2026-09-14：首批现有源码已迁入 agent-dx 本地分支 refactor/monorepo-layout。来源为运行时源仓 feature/distribute_env@f3d3d520、Frontend ea1de40c、Sandbox SDK d5fde037。Rust 测试 199 通过，Agent 测试 234 通过/1 跳过，Sandbox SDK 测试 233 通过，Go 最小集完整构建、107 个顶层测试及 vet 通过；保留 Sandbox 与 9 条 Agent 兼容入口，删除 runtimeapi 和旧依赖链，Python 四个包均已生成 wheel 和 sdist。详细来源、适配与边界记录在目标仓 docs/migration/2026-09-14-import.md；新 Rust 管控面、完整发布部署与集群 E2E 尚未实施。迁移变更在 refactor/monorepo-layout 分支统一维护。

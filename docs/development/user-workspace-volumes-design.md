# ADX 用户工作区持久化挂载设计草案

状态：挂载请求链路已实现，部署与接管仍待完成。Template mounts、AgentBinding parameters、Jiuwen 目录映射及公共 host_path 转换已接入；工作区独占准入、存量迁移尚未完成；目录自动创建已按请求字段实现并通过本地回归测试，真实 SFS 基础验收已在 test 执行。后文对这些部分的描述是目标设计，不能据此认定当前系统已经保证顺序接管。

部署将 SFS Turbo 挂到 adx-node；Jiuwen 根据可信身份确定稳定的相对子目录；Agent 模板通过占位符声明挂载路径；Activator 使用 Binding parameters 渲染出公共 Sandbox API 的 Local 挂载描述；Platform 经 adxlet、sandboxd 和实际隔离运行时执行读写挂载。

## 1 已确定的设计

| 项目 | 决定 |
| --- | --- |
| 存储方式 | 节点侧挂载 SFS，沙箱使用 Local 目录挂载 |
| 模板声明 | mounts 中直接使用占位符，不增加 subPathFrom |
| 参数来源 | Jiuwen 在准备 AgentBinding 时传入 parameters |
| 用户语义 | 只在 Jiuwen 选择目录时使用 user_id；Activator 不引入 owner_id 或 user_id |
| 数据关联 | 同租户、同用户、同模板跨版本使用相同目录；版本和 Sandbox ID 不参与目录身份 |
| 沙箱内目录 | 统一挂载 /home/jiuwen，应用数据、任务成果和用户工具配置收敛到此目录 |
| SQLite | 保留现有数据库及 journal 模式，包括 WAL；以工作区独占和顺序接管为前提，完成真实 SFS 验收后启用 |
| 接管约束 | 同一工作区任何时刻最多一个沙箱访问；旧实例完全停止访问后，新实例才能接管，覆盖不同 Binding 和模板版本 |
| 读写要求 | 工作区以 rw 挂载，文件修改必须写回 SFS |
| 平台资源 | 复用现有 Mount 和 StorageSource::Local，不新增独立 Volume、Workspace 或 Attachment 资源 |
| 数据生命周期 | 删除 Sandbox、Binding 或旧模板版本不自动删除 SFS 数据 |
| 当前阶段 | test 已部署并完成基础存储验收；真实业务首用目录修复待部署验证，接管协调职责待确定 |

## 2 当前实际运行时

2026-10-13 对 adx-jiuwen-dev 做只读核对：adx-node-0 至 adx-node-7 的 /work/deployment.yaml 均配置 runtime_profile.rootfs.runtime_class 为 runsc；抽查节点的 sandboxd 配置指定 /opt/adx/backend/runsc，platform 为 systrap。在节点 1、2、3、5、6、7 的 /proc 可执行文件中观察到 runsc 进程。

当前链路为：

```text
adxlet → sandboxd → runsc（gVisor）→ 用户沙箱
```

sandboxd 是接收挂载请求、管理运行时的节点组件；runsc 是本环境实际执行隔离的运行时。runc 和 runsc 是两个不同实现；本方案以当前部署的 runsc 为首个验收目标。

gVisor 通过 Gofer 等文件访问机制提供宿主机目录访问；当前设计应验证 Local bind 的写入确实到达 SFS，不能被覆盖所有挂载的临时 overlay 截留。共享目录需使用与外部修改一致的缓存策略。具体原则见 [gVisor 文件系统文档](https://gvisor.dev/docs/user_guide/filesystem/)。

sandboxd 的 Firecracker 目录挂载限制不适用于当前 runsc 链路。test 环境已验证 SFS 读写、顺序重建后的 SQLite 数据、实际 AgentServer 会话跨版本恢复，以及 OBS pause/resume；这些测试显式停止旧实例，不能证明系统已实现跨 Binding 独占。

## 3 模块职责

| 模块 | 输入与职责 |
| --- | --- |
| 部署 | 将同一个 SFS 文件系统挂到所有可调度 adx-node 的固定路径，并保证在 sandboxd 所在 mount namespace 可见 |
| Jiuwen | 从认证 Principal 获取 tenant、user_id，结合稳定模板身份生成工作区相对路径；将其放入 parameters |
| Agent 模型与存储 | TemplateVersion 保存带占位符的 mounts；AgentBinding 保存不可变 parameters |
| Activator | 校验和渲染模板，组装具体 mounts，通过 Sandbox capability 创建沙箱；不访问 SFS 或管理存储生命周期 |
| API Server | 校验 mounts 的 source 恰好一个有值，将 host_path 转换为 StorageSource::Local |
| Platform 与 adxlet | 传递具体挂载规格，选择满足部署条件的节点，完成目录准备和运行时挂载协调 |
| sandboxd 与 runsc | 将具体目录按 rw 提供给沙箱；执行挂载与隔离，不解释用户、模板或占位符 |

Agent 继续通过公共 Sandbox capability 的 HTTP 或同进程适配器访问 Platform，不直接调用 Coordinator。RuntimeProfile 仍只承担 rootfs/bootstrap 配置，不放用户工作区参数。

## 4 模板与 Binding 参数

以下字段已在模型中实现，片段省略已有必填字段。完整配置见 [Jiuwen SFS 模板](../../agent/activator/examples/jiuwen-sfs-template.json)。

模板版本声明：

```json
{
  "name": "coding",
  "version": "v2",
  "mounts": [
    {
      "type": "bind",
      "host_path": "/mnt/sfs/workspaces/${workspace}",
      "target": "/home/jiuwen",
      "options": ["rw"],
      "create_if_missing": true
    }
  ]
}
```

/mnt/sfs/workspaces 是 SFS 在 adx-node 上的本地路径，不是远端 NFS 地址。模板挂载规则由受信任的模板发布方配置。

Jiuwen 首次访问通用入口时提交初始化参数，例如：

```http
GET /agent/ws?target=urn:adx:binding:coding:v2:jiuwen-example
Authorization: Bearer <ADX platform key>
X-ADX-Parameters: workspace=w-0123456789abcdef
```

Header 使用 URL 表单编码的字符串键值对。parameters 是通用字符串映射；workspace 是模板约定的参数名，Agent 不给它附加用户含义。Binding 不存在时，共享解析/激活流程才解析参数并原子创建 Binding；不提供独立准备接口。

建议 Jiuwen 首版采用固定算法生成目录名，例如 w- 加 SHA256(JSON([tenant, template, user_id])) 的完整十六进制编码，示例值仅作缩写。使用认证后的内部 user_id，不使用用户可任意提交的身份字符串。算法编码和字段顺序固定，不包含 version、binding_id、generation、sandbox_id 或 agent_type；同一模板的不同 agent_type 因而可以选到同一目录，这一点需与业务权限一致。

当前 Jiuwen binding_id 按 tenant、user_id、agent_type 生成；Binding 的 Scope 还包含 template 和 version。工作区参数应独立计算，不能直接复用 binding_id 来代表用户模板工作区。

目录算法属于 Jiuwen 业务契约。模板改名、账号重建、身份映射或算法变化会改变目录，应显式迁移，不能在普通发布时悄悄修改。若允许删除模板后重新使用同名名称，业务需决定是否复用旧数据，不能将名称复用自动等同于原模板身份。

## 5 占位符渲染契约

Template 的 mounts[].host_path 与 env 值支持 ${parameter_name} 字符串占位符，不执行表达式、Shell、环境变量读取或递归展开；command 等其他字段不参与渲染。

- 模板发布时校验占位符语法、挂载目标和 source 互斥。host_path 保留固定的绝对根路径前缀，参数用于卷内相对路径。
- 首次激活创建 Binding 时，所有被引用的参数必须存在；挂载路径参数还必须非空，不能把占位符替换为空后挂载整个根目录。env 值允许普通字符串，不套用路径限制。
- 路径参数不得含绝对路径、空路径分量、点路径分量、反斜杠或控制字符；拒绝含 .. 的路径分量，不能靠清理路径将越界输入变为合法值。
- 渲染后校验规范化结果位于模板的固定根路径下。路径字符串校验不代替节点上的符号链接与实际挂载检查。
- parameters 在创建 Binding 时与 generation 一起持久化；已有 Binding 完全忽略新请求的初始化参数，包括缺失、不同或格式错误的参数。并发首次创建以原子写入获胜者为准，不能覆盖已有绑定的数据目录。
- 模板版本不可变。激活、重试、缓存恢复均使用保存的参数；ActivationRequest 透传原始初始化参数，仅缺失 Binding 时消费。删除 Binding 后再次访问使用新参数初始化。

这是通用模板渲染能力。Activator 不反解 workspace 参数，也不据此查用户或计算目录。

## 6 调用链路与每步数据

```text
认证用户
   │ Principal(tenant, user_id)
   ▼
Jiuwen
   │ 通用 /agent/ws + X-ADX-Parameters
   ▼
共享解析 → Activator Activate
   │ 首次原子保存 Scope、generation、environment_id、parameters
   │ 读取不可变模板 + 已保存 Binding parameters
   │ 渲染 mounts 和 env
   ▼
Sandbox capability（HTTP 或同进程）
   │ Create Sandbox JSON，mounts 已没有占位符
   ▼
API Server → Platform EnvironmentSpec
   │ Mount(kind=bind, source=Local(具体路径), target, options=[rw], create_if_missing=true)
   ▼
adxlet 按 create_if_missing 准备目录 → sandboxd
   │ Mount(type=bind, host_path=具体路径, target, options=[rw])
   ▼
runsc
   │ 沙箱内 /home/jiuwen 读写节点可见的 SFS 子目录
   ▼
SFS 持久化数据
```

Activator 发给公共 Sandbox 能力的挂载片段为：

```json
{
  "mounts": [
    {
      "type": "bind",
      "host_path": "/mnt/sfs/workspaces/w-0123456789abcdef",
      "target": "/home/jiuwen",
      "options": ["rw"],
      "create_if_missing": true
    }
  ]
}
```

Agent 的 ExecutionSpec 和公共请求转换已携带渲染后的 mounts，HTTP 和同进程适配器共用转换。

## 7 公共 host_path 接口调整

公共 API 已接受仅 host_path 有值的挂载请求，复用内部 Local 模型和 adxlet 到 sandboxd 的 HostPath 转换。

当前转换要求三者恰好一个有值：image_url 转 Image，s3_config 转 S3，host_path 转 Local；全部为空或多个同时存在均返回 InvalidArgument。保留 hostPath 作为当前已支持的输入别名。此次不放开 public local rootfs。

放开 source 转换并不等于增加路径授权：现有此分支没有租户级目录范围校验，sandboxd 的 Linux capability 也不会证明请求者有权访问某个目录。部署应将能够提交任意 host_path 的 Sandbox 管理接口限制给受信任调用方；如果普通租户可直接调用，则在上线前补充授权范围校验。该边界与三选一转换分开表达，不在转换分支继续无条件禁用 Local。

本方案不要求给用户沙箱增加 CAP_SYS_ADMIN。挂载由已有节点运行时完成。节点权限、源目录可见性、运行时支持和调用方授权分别验证。

## 8 部署与目录准备

以下路径为方案示例，不是现有部署默认值：

```text
SFS Turbo
  → adx-node 中 /mnt/sfs
    → workspaces/w-<稳定目录标识>
      → Sandbox /home/jiuwen（rw）
```

SFS 必须在 sandboxd 执行挂载的 mount namespace 中可见。本环境 node 启动脚本使用私有 mount namespace，并设置 rprivate，因此部署优先在 sandboxd 启动前提供 SFS；不能假设启动后在其他 namespace 添加挂载会自动传播进来。

所有可承载这些沙箱的节点应挂载同一个 SFS 后端且路径一致。若只有部分节点挂载 SFS，必须配置相应调度约束。Local 路径本身不会自动表达共享存储身份，也不会自动保证跨节点读到同一数据。

目录创建由每个挂载的 `create_if_missing` 布尔字段控制，默认 false。模板声明该字段，Activator 将它与渲染后的具体路径一同传入 Sandbox capability，Platform 在 EnvironmentSpec 和内部 RPC 中保留它；adxlet 在 start/restore 前执行。Jiuwen 与 Activator 不直接 mkdir，也不增加节点受管目录配置项。

为 true 时只创建最终一级源目录，父目录须由部署预先准备。节点逐级使用目录描述符和 O_NOFOLLOW 解析路径，拒绝符号链接；已有目录复用，不递归 chown 或清理。未指定创建属性时，新目录 mode 为 0755（受节点 umask 影响），属主是节点进程。非 root 应用可在挂载内声明 `create_options: {"uid":10001,"gid":10001,"mode":"0700"}`，与 `create_if_missing: true` 配合使用。三个属性均必填；UID/GID 为 0–4294967294 的整数，mode 为 0000–0777 的四位八进制字符串，不解析镜像用户名。节点直接 mkdir 最终源目录，仅本次创建成功的调用者通过目录描述符设置所有权和权限。并发 mkdir 返回已存在时复用目录，不重复设置。创建与属性设置不是一个原子操作；并发首次使用或进程中断可能看到未完成的属性。设置失败返回错误、阻止本次启动，保留目录供修正，不自动删除、重命名或修复已有目录。已有目录不变，不递归修改；不引入工作区所有者字段或迁移逻辑。共享文件系统需允许节点设置所有权和权限；不要求 renameat2 或额外目录发布协议。Orbit 示例使用 UID/GID 10001、0700；当前仅完成本地组件验证，需要升级 adxlet 后才能使用，尚未进行 SFS/Orbit 云端验收。SFS 类型、来源和读写选项由现有节点启动检查确认。

该开关采用显式“缺失即创建”语义，不保存工作区初始化登记，也不能区分首次创建和已存在数据被外部删除。需要禁止数据丢失后自动创建的场景，应在目录已准备后使用 false，或另行实现数据完整性策略。此能力不新增独立 Volume 资源 API，不解决多实例并发访问 SQLite 的互斥。

必须验证以下条件：

- SFS 缺失或来源不符时拒绝启动，不能退化为同名节点本地目录。
- 节点安全解析源路径，防止符号链接和检查后替换导致挂到根目录之外；祖先目录由受信任节点流程控制。
- 沙箱实际 UID/GID 对 SFS 子目录可读写，已有目录不被每次启动递归 chown。
- rw 修改确实落在 SFS，不能只存在于沙箱临时 overlay；完成顺序接管的新实例能观察到已持久化修改。
- 卸载或删除沙箱只移除实例挂载，不递归删除共享目录。

## 9 跨版本复用与顺序接管

v1 与 v2 的模板都声明同一个宿主机根目录，Jiuwen 为同一 tenant、template、user_id 生成同一个 workspace 参数，则两个版本挂载同一 SFS 目录。升级不复制目录，也不从旧沙箱导出文件。Binding 删除后重建仍按同一算法得到原路径。

改变模板根路径或 Jiuwen 参数生成规则会改变数据位置，应作为显式存储迁移处理。改变沙箱内 target 不改变源数据身份，但应用必须适配路径。数据格式兼容性由应用版本负责，回退镜像不会回退 SFS 文件。

Local/rw 本身不提供互斥。本方案要求**同一工作区任何时刻最多一个沙箱访问**，新旧版本通过顺序接管复用目录，不允许滚动升级期间同时访问。独占范围按实际工作区确定，必须覆盖不同版本、Binding、generation 和 agent_type；现有按 Scope 管理的生命周期不能单独证明工作区独占。

正常接管顺序：

1. Jiuwen 停止向旧实例分发新请求，进入工作区切换流程；同一工作区的其他激活请求等待或返回冲突。
2. 旧实例停止后台任务并结束写入，正常关闭数据库连接及相关进程；按应用退出流程完成必要刷盘或 checkpoint。
3. 通过 Sandbox capability 确认旧沙箱和全部子进程已停止，旧执行侧不能继续访问目录，再允许新实例创建并挂载。
4. 新实例打开原目录和数据库，按 SQLite 原有规则恢复，完成启动检查后接收请求。

异常退出时无需假定已成功 checkpoint，也不能删除 WAL 来跳过恢复。若旧节点失联、停止结果不确定或可能仍在写入，禁止仅凭心跳超时、Binding generation 变化或控制面租约过期启动后继实例；必须确认旧执行已终止，或完成能阻止旧执行继续访问存储的隔离。该策略在无法确认独占时选择等待，而不是自动并发接管。

职责上，Jiuwen 负责工作区选择和切换编排；跨请求的工作区占用状态必须持久化，并能协调所有 Jiuwen 副本。实现可复用已有业务状态存储，不要求新增独立卷服务。Activator 仍是无状态模板渲染和激活执行组件，不引入 user_id；旧实例停止及执行状态确认继续通过 Sandbox capability 完成。工作区准入必须覆盖所有能触发该工作区激活的入口，不能仅在单个请求或进程内加锁。具体状态存储与原子准入接口是实施前需落实的契约，当前代码尚不具备此保证。

顺序接管在应用生命周期上类似“旧进程退出后重新启动”。跨 adx-node 接管还涉及不同 NFS 客户端和 runsc 文件访问状态，不能因此直接视为已验证的本地文件系统重启；相关缓存、持久化和故障恢复条件见第 11 节。

外部 SFS 数据不属于 Sandbox rootfs 快照；暂停、恢复、克隆的具体支持边界需按 runsc 与应用验证，不承诺通过恢复 Sandbox 快照回滚共享工作区。每用户硬配额和自动清理也不由 Local 挂载自动提供。

## 10 实施顺序与验收

| 阶段 | 待实施事项与验证 |
| --- | --- |
| 1 契约（已实现） | TemplateVersion mounts/env、Binding parameters、首次激活原子存储；测试参数缺失、非法路径、热请求忽略输入与并发首次创建 |
| 2 渲染与传输（已实现） | Activator 渲染 mounts，扩展 ExecutionSpec，统一 HTTP/同进程请求；测试跨版本同路径、跨用户及模板不同路径 |
| 3 Local 入口（已实现） | API source 三选一转换、SDK/API 文档；host_path/hostPath 接受、rw 保留及全量 source 互斥组合已有契约测试 |
| 4 节点部署 | SFS 挂载、namespace 可见性、受管目录准备、权限、调度范围和管理入口授权；缺失挂载必须失败 |
| 5 工作区准入与接管 | 实现跨 Binding、版本和服务副本的独占协调；重复激活幂等，重叠访问拒绝，旧实例状态不明时阻止接管 |
| 6 真实 runsc 验收 | 沙箱写文件后从 SFS 验证；同节点及跨节点顺序接管、跨版本数据库兼容；保留现有 WAL 测试正常关闭和异常恢复；验证不同用户隔离 |

Rust 实现按仓库要求测试先行，完成聚焦测试、全局格式及 workspace Clippy。组件测试与真实集群 SFS 验收分别报告；当前环境确认属于只读检查，不是存储端到端验收。

## 11 Jiuwen 数据目录与重建保留范围

### 统一挂载 /home/jiuwen

为同一用户模板工作区只挂载一个 SFS 子目录，沙箱内统一使用 /home/jiuwen。以下是待应用到 Jiuwen 模板的环境配置和挂载片段；环境值均为常量，不需要扩展 env 占位符语法：

```yaml
env:
  HOME: /home/jiuwen
  JIUWENSWARM_HOME: /home/jiuwen
  JIUWENSWARM_DATA_DIR: /home/jiuwen/.jiuwenswarm
  JIUWENSWARM_WORKSPACE: /home/jiuwen/.jiuwenswarm
  JIUWENSWARM_TASKS_DIR: /home/jiuwen/Documents/JiuwenSwarm
  JIUWENSWARM_DOWNLOAD_ASSET_ROOT: /home/jiuwen/.jiuwenswarm/assets
mounts:
  - type: bind
    host_path: /mnt/sfs/workspaces/${workspace}
    target: /home/jiuwen
    options: [rw]
```

预期目录布局：

```text
/home/jiuwen/                         ← 单个 Local 挂载
├── .jiuwenswarm/
│   ├── config/                      用户配置及下载签名文件
│   ├── .personal_context/           PersonalContext
│   ├── agent/
│   │   ├── sessions/                会话历史及元数据
│   │   ├── session_messages.sqlite3 会话消息邮箱及相应 sidecar
│   │   ├── .checkpoint/             执行检查点
│   │   ├── workspace/               记忆、技能、项目与任务注册记录
│   │   └── home/                    定时与心跳任务定义
│   └── assets/                      下载副本及注册信息
├── Documents/JiuwenSwarm/           无项目任务成果
├── projects/                       可选的显式项目根目录
└── .config/ 等                     配置为使用 HOME 的工具状态
```

DATA_DIR 统一应用数据，WORKSPACE 覆盖 Gateway 上传路径，TASKS_DIR 固定无项目任务成果位置。PersonalContext 的相关代码使用 Path.home()/.jiuwenswarm/.personal_context，所以必须同时设置实际进程的 HOME，仅设置 JIUWENSWARM_HOME 不够。HOME 不改变运行用户或 UID/GID；节点目录权限、工具使用的用户目录解析方式和各子进程环境仍需验证。明确使用其他 XDG 路径、passwd home 或绝对路径的工具需要分别配置。

| 数据 | 持久化策略与恢复边界 |
| --- | --- |
| 会话历史、配置、记忆、技能、项目文件及 Git 数据 | 整体保留，不只挂载 agent/workspace |
| session_messages.sqlite3 | 保留消息内容、投递状态、幂等标识等；它不是可任意丢弃的缓存，维持现有 WAL/FULL 配置 |
| agent/.checkpoint | 保留 Agent/Workflow 已持久化状态及未完成目标；仅保留聊天历史不能替代执行检查点 |
| 定时和心跳任务定义、任务注册记录 | 保留定义与关联路径，启动后重载；不承诺恢复原进程或原定时器 |
| PersonalContext、MCP 状态及工具凭据 | 保留位于统一 home 下的状态，核对工具实际写入路径 |
| 上传文件、无项目任务成果 | 收敛至挂载内；任务注册记录中的绝对路径与实际文件一致 |
| 下载副本、sidecar 及签名文件 | 一起保留；assets 有 TTL，永久成果不能只保存在下载分发目录，旧 URL 仍受有效期和路由限制 |
| 可选功能数据库 | 启用时一并保留，包括追踪记录、发布状态、语音任务、团队及长期记忆存储；不能统一按 SQLite 扩展名排除 |
| 可重建缓存 | 例如公共 marketplace 目录缓存可以另定清理策略；首版不为减少持久化范围而批量删除数据库 |

上述数据库清单来自源码中的功能路径，不代表当前部署已启用所有可选功能。应用代码、虚拟环境和系统程序仍随镜像提供，用户需保留的文件应写入挂载范围。

### SQLite：保留现有模式，按独占顺序接管验证

本轮选择将现有 SQLite 数据直接保留在 SFS 子目录，**不迁移外部数据库，不强制切换 journal 模式，也不放弃数据库内的业务状态**。维持现有应用行为，包括 SessionMessageStore 使用 WAL；一个沙箱内的多进程、多连接访问仍需按实际调用方式验证，并非“一个沙箱”等于“一个 SQLite 连接”。

本方案的核心前提是第 9 节的独占访问。其他沙箱、宿主机工具和后台组件不得同时打开该工作区数据库。正常退出后由新实例重新打开；异常退出且确认旧执行已停止后，由 SQLite 完成其原有恢复。WAL 中可能仍有已提交事务，迁移和恢复必须保留数据库及有效 WAL/journal，不能仅复制主数据库文件、对在线文件做普通复制，或在启动时盲目删除 sidecar。正常关闭不要求每次手工删 WAL。

SQLite 官方对 WAL 给出的通用约束是不支持网络文件系统，其共享内存机制要求访问进程处于同一主机。独占顺序接管避免了两个节点同时参与同一 WAL 的场景，但不构成官方对 SFS 的支持保证。Local bind 不改变底层网络文件系统性质；SFS/NFS、runsc 的 mmap、文件锁、刷盘和跨节点重新打开行为仍需实测。本方案将它作为有明确前提的待验证实现路线，尚未认定其满足所有故障下的数据持久性。[SQLite WAL 文档](https://sqlite.org/wal.html)、[网络文件系统注意事项](https://sqlite.org/useovernet.html)

不能依赖 SQLite 文件锁替代工作区独占协调。例如 SFS Turbo 推荐挂载参数中的 nolock 不提供跨主机的 NLM 锁保护。实施时记录实际 NFS 协议、挂载选项及 runsc 文件系统配置，验证其行为，不把修改单个挂载参数视为可靠性结论。[SFS Turbo 挂载参数说明](https://support.huaweicloud.com/usermanual-sfsturbo/sfsturbo_01_0361.html)

### 存量迁移与启动

2026-10-13 只读抽查的现有镜像使用 HOME=/root，DATA_DIR 和 WORKSPACE=/var/lib/jiuwenswarm，DOWNLOAD_ASSET_ROOT=/var/lib/jiuwenswarm/assets；未设置 TASKS_DIR 或 TASK_REGISTRY_DIR，且 /root/.jiuwenswarm 是独立目录。这些是迁移来源，并非新方案的目录布局。

首次启用新挂载前停止旧工作区写入，将 /var/lib/jiuwenswarm 中的数据迁入新 home 的 .jiuwenswarm，将旧 /root/.jiuwenswarm 中的 PersonalContext 等独有状态合并迁入，将旧任务成果迁入 Documents/JiuwenSwarm。重名文件不能盲目覆盖，数据库按停止后的完整状态迁移；同步更新任务注册等记录中的绝对路径，并核对权限。先完成迁移和校验再删除旧沙箱；失败保留旧数据，不能假设挂载空目录会自动复制镜像或旧沙箱中的内容。

prepare_runtime_workspace 会补齐和迁移配置并清理旧 team 文件，平台启动器会更新 models.defaults。需验证初始化不会覆盖用户设置或误删有效状态，且平台管理配置能随新模板正确更新。跨版本 SQLite schema 和其他文件格式迁移由 Jiuwenswarm 负责，回退镜像不回退持久化数据，也不自动保证旧版可以读取新版格式。

### 验收与承诺范围

使用合成账号准备聊天、消息邮箱、检查点、附件、项目文件、记忆、技能、定时任务和 PersonalContext，在实际 SFS → adx-node → sandboxd → runsc 链路执行：

| 场景 | 验证目标 |
| --- | --- |
| 正常停止后同节点重建 | 数据库正常重新打开，已提交事务和用户文件可读写，引用完整 |
| 正常停止后跨节点接管 | 使用同一 SFS 目录，后继实例看到已持久化内容，锁、缓存和 WAL 恢复符合预期 |
| 进程异常终止后接管 | 保留未 checkpoint 的 WAL，确认旧执行停止后恢复；检查事务与数据库完整性 |
| 节点失联或停止结果不明 | 阻止接管；确认旧执行终止或隔离后才能在其他节点恢复 |
| 并发激活与跨版本切换 | 不同 Binding、版本和 Jiuwen 副本不能同时获得同一工作区；后台写入也纳入停止确认 |
| 跨版本初始化和 schema 变化 | 用户配置、历史与任务保留，迁移不会误删数据；明确回退兼容边界 |
| 断连、I/O 错误和刷盘失败 | 写入失败不得错误地报告成功；结合实际确认点检查已确认数据能否恢复 |

数据库验证既检查 integrity_check 等完整性结果，也核对已提交记录、投递状态和幂等标识；数据库结构未损坏不等于没有丢失事务。验收使用真实镜像的多进程和连接方式，不能仅用一个 SQLite 脚本替代应用测试。

持久化目标是“沙箱重建后，已持久化的用户文件、配置、历史与任务状态可恢复”。当前历史 JSONL 追加未显式 fsync，文件关闭不能直接等同于故障下持久化；若要承诺节点异常后已确认消息零丢失，还需核对确认时机、写入顺序和 SFS 持久化语义。内存中未落盘的回复、运行中 shell、浏览器连接及执行到一半的工具调用不能由文件卷恢复，检查点也不自动保证外部副作用恰好执行一次。

沙箱外账号与身份映射仍由原业务存储管理。/tmp、镜像目录及其他未挂载路径不在本卷保留范围；允许用户选择项目路径时，Jiuwen 应将需要保留的项目约束或配置到 /home/jiuwen 内。以上验收尚未执行，当前结论是设计选择与源码/环境核对，不是 SQLite WAL 在 SFS 上的可靠性认证。

## 12 当前验证记录

挂载请求链路已完成测试先行验证：初始 Local 入口测试因 public host mounts are not allowed 失败，模板测试因缺少 mounts/render_mounts 失败；实现后通过以下检查：

- Agent 四包回归：70 项通过，10 项依赖真实服务或手工测量的测试未执行；最终挂载边界专项 4 项通过。
- Gateway 库及 Jiuwen 入口：96 项通过，1 项忽略。
- API Server 两组创建契约：18 项通过，包含 host_path/hostPath、rw、source 互斥及 HTTP/嵌入式相同规格。
- 全 workspace cargo fmt 检查与 all-targets/all-features Clippy（-D warnings）通过；文档检查与 git diff --check 通过。

这些结果来自现有构建容器中的单元/组件测试。尚无真实 SFS 挂载、跨节点顺序接管、SQLite/WAL 故障恢复或新参数存储的真实 Redis 验证；本轮未变更集群配置。工作区独占协调的职责选择待确定，不能以同 Scope 的幂等创建代替跨 Binding 独占。

## 13 实现依据

- [Agent 核心模型](../../agent/crates/core/src/lib.rs)与[准备和激活契约](../../agent/crates/core/src/activator.rs)：已新增 mounts 和 parameters，保持 Scope 和执行生命周期分离。
- [Agent 存储](../../agent/crates/store/src/state.rs)：模板不可变、绑定条件创建及 generation 约束。
- [Jiuwen 入口](../../gateway/src/ingress/digitalmategateway/entrypoint.rs)：可信 Principal、binding_id 和 Scope 的现有映射。
- [Agent Environment 请求转换](../../agent/activator/src/environment_request.rs)：统一公共请求生成，已传递渲染后的 mounts。
- [公共 Environment 挂载校验](../../gateway/apiserver/src/contract.rs)、[平台 Mount 模型](../../platform/crates/core/src/execution.rs)、[adxlet 挂载转换](../../platform/adxlet/src/sandboxd.rs)：公共入口将 host_path 转 Local，经现有节点转换传给 sandboxd。
- [sandboxd 锁定版本](../../third_party/sandboxd/source.json)与[运行时协议](../../third_party/sandboxd/sandbox-api.proto)：协议已有 HostPath。
- [SFS Turbo 挂载说明](https://support.huaweicloud.com/intl/zh-cn/qs-sfsturbo/sfsturbo_01_0028.html)：实际部署的网络和挂载条件。

# `adxctl` 部署指南

Pod/process 发布、ConfigMap 输入 CAS/abort 恢复及显式 ready 提升的命令与恢复合同统一见 [Rollout Publisher](../../build/release/NODE-ROLLOUT.md)。两个 apply 入口以 `--batch-index`、`--promote-ready` 二选一，后者只能独立操作已完成发布的原 plan/journal，不自动推进批次。

当前发布包默认共进程部署：`adx-apiserver` 内嵌 Ingress，`adxlet` 内嵌 Relay；同时提供 `adx-ingress` 和 `adx-relay`，供显式分进程部署。调试用 `adx-data-plane-forward` 不随包发布。

`adxctl` 是 ADX 统一发布包的本机进程部署工具。它读取一份 YAML 部署文件，生成本机各组件的最终配置，并以前台 supervisor 方式启动这些组件。它不创建 Environment，也不调用 Environment API；集群 API Key 等远程管理操作使用独立的 [`adxadmin`](adxadmin.md)。

release 安装器默认创建 `/usr/local/bin/adxctl -> /opt/adx/current/bin/adxctl`。因此日常命令直接使用 `adxctl`；切换 `/opt/adx/current` 后，命令自动指向新版本。安装器不会把其他内部服务二进制加入系统 `PATH`。发布包还包含与 Redis Server 同为 7.2.5 的 `/opt/adx/current/bin/redis-cli`，供部署者进入 Coordinator 容器后检查 Redis；它不会自动连接或认证，使用时应从部署环境获取地址和凭证。

只读检查 ADX 持久化目录可使用发布包内的 [`adx-inspect`](adx-inspect.md)。它复用部署 YAML 的 Redis 连接信息，提供节点、Environment 和快照查询。

一份部署 YAML 只描述**当前主机**。单机部署可以在一份文件中包含所有角色；多机部署时，每台主机使用自己的文件，各文件通过相同的 `redis_url` 和 `namespace` 加入同一集群。`adxctl` 默认读取 `/opt/adx/config/deployment.yaml`，也可用全局 `-c/--config` 或 `ADX_DEPLOYMENT_CONFIG` 选择其他文件。它只接受 `.yaml` 或 `.yml`，不接受 JSON 部署文件。

## 组件名

| 部署角色 | 二进制 | 部署位置 | 作用 |
| --- | --- | --- | --- |
| `coordinator` | `adx-coordinator` | 控制节点 | 持久化状态、全局轮转、Shard 调度、节点心跳与路由发布 |
| `adxlet` | `adxlet` | 每个工作节点 | 本机资源准入、Environment 生命周期、sandboxd 与恢复；默认同时嵌入 Relay |
| `relay` | `adx-relay` | 显式选择分进程时 | 数据面绑定和到 Environment Execd 的转发 |
| `apiserver` | `adx-apiserver` | 接入节点，默认内嵌 Ingress | 用户 API Key、Environment HTTP API、归属缓存和生命周期转发 |
| `ingress` | 内嵌时无独立进程；分进程时为 `adx-ingress` | 接入节点 | 对外 TLS、控制请求转发以及到 Relay 的数据连接 |
| `redis` | `redis-server` | 可选，仅一个主机 | 由 `adxctl` 托管的 Redis 7.2.5 和 AOF |
| `afs-meta` | `afs-meta` | 显式 Agent FS（AFS） 主机 | AFS 命名空间、版本、布局和副本目录；仅 `with_afs: true` 可用 |
| `afs-node` | `afs-node` | 显式 Agent FS（AFS） 节点 | AFS OwnerFs/DFS 挂载、缓存、复制和节点间传输；仅 `with_afs: true` 可用 |

当前没有 `adx-frontend` 二进制。原控制面 Frontend 已重写并命名为 `adx-apiserver`。API Server 默认在同一进程中托管 Ingress，两个模块仍保持独立监听，默认使用各自的 TLS 身份；显式设置 `ingress_mode: standalone` 时才启动 `adx-ingress`。API Server 只监听回环地址，由 Ingress 对外提供 HTTPS。

sandboxd 不属于上述角色，始终由部署环境独立启动。Execd 位于发布包 `runtime/`，进入 Environment 环境运行，也不是宿主机服务。

Execd 主动控制连接是节点服务的可选配置，见[运行时控制流部署](runtime-control-stream.md)；它需要运行环境能够访问本节点控制地址，并配套新 Execd。

## 命令

Adxlet 和 Relay 将已确认的配置解析或纯语义校验失败标记为退出码 `78`（共享 `CONFIGURATION_EXIT_CODE`）。使用 ConfigMap版本声明初始化时，观察到本组实际 owned Child 以该状态退出后，以 Preserve 停止完整 ADX 组并等待真实 reap，然后返回配置失败；target 配置错误不尝试 ready，ready 配置错误也不再回退。普通运行初始化失败仍可按下述合同尝试一次完整 ready 组。配置文件读取 IO、权限、TLS/key 文件、目录 IO、端口绑定、网络和 OS 资源不可用不按配置错误猜测；未观察到退出或仅 proof 不可达也不能认定为 78。Relay FD 预算资源不足与 OS syscall 错误仍为运行失败，仅明确的环境解析错误归入配置错误。主动进程升级的失败恢复保持原 Process 合同，不因候选退出 78 而跳过恢复。

ConfigMap版本声明提供 `target.yaml`、制品 URL + SHA 及配套配置，通过部署 YAML 的可选绝对挂载路径 `external_declaration: /mounted/target.yaml` 读取。部署模式仍为 embedded 或 standalone，`external_declaration` 只是声明输入路径，不是新增部署模式。未设置时保留原 image 启动入口，不下载声明制品。显式配置时，supervisor 在锁、基线渲染和可执行文件预检后、任何 Child spawn 前，准备 schema 1 YAML 声明中的 target/ready 两个完整节点组；声明、摘要、下载或渲染失败直接返回错误，不回退 image 或 ready。

声明的 `components` 与 `ready.components` 均要求 `adxlet`、`relay`；每个组件包含 `version`（40 位小写十六进制 commit）、`release_id`、`artifact: {url, sha256}` 与 `config: {mount_path, sha256}`。制品 SHA 认证整个包，实际运行二进制 SHA 由本机 adxctl准备时从已认证的 manifest 派生，不需要用户手抄。声明与每份配置均限 4 MiB，显式投射路径打开后通过固定 FD 读取并检查 regular file。配置文件是 SHA 认证的原始 `ServiceOverride` JSON，Relay 仅接受 env payload；`ADX_POD_UID` 和 `ADX_SUPERVISOR_*` 不允许声明注入。embedded 两个角色必须选择同一版本、标签及完整包，只准备运行 Adxlet，并合入不冲突的 Relay env；两个角色的实际 binary SHA 均是 owned Adxlet SHA。standalone 准备两个实际 Process。配置输入摘要与渲染后的有效配置/env 摘要分别记录，运行目录和受保护的节点、控制面边界保持部署基线。两个组共享首次准备起算的 900 秒期限，复用 schema 1 完整包验证与固定二进制安全复制。

启动先尝试完整 target 组，自首次 spawn 前固定 300 秒。后台 proof 通过 supervisor 的 owned Child 观察、NodeAdmin 普通注册 ACK/catalog/ready 及实际 executable SHA 确认整组；单次观察总界限为 5 秒。初始化 pending 期间仍处理 status、Query、runtime 观察和停止，但禁止主动 rollout 与 ADX 自动重启。明确 spawn/Child 退出失败或期限届满后，仅向 owned ADX Child 发送 Preserve 停止信号并按固定停止期限升级；只有真实 reap 完整组后才尝试一次完整 ready 组，使用独立固定 300 秒期限，不混搭角色或回到 target。未确认停止不会启动下一组。成功后冻结所选 Process，普通 restart 不重读投射配置；node-info 的 `external_startup.selection` 区分 `target` 与 `ready_fallback` 并保留组件摘要事实。

以下是 ConfigMap版本声明中可解析的 embedded `target.yaml` 形状。示例 URL、摘要、commit 和挂载路径须替换为实际发布事实；配套 config 文件的内容仍是 `ServiceOverride` JSON，不是 YAML。每个组内两个角色选择同一个完整包，target 与 ready 可以选择不同包。

```yaml
schema: 1
mode: embedded
components:
  adxlet:
    version: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    release_id: "release-target"
    artifact:
      url: "https://releases.example.invalid/target.tar.gz"
      sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    config:
      mount_path: /mounted/target-adxlet.json
      sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  relay:
    version: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    release_id: "release-target"
    artifact:
      url: "https://releases.example.invalid/target.tar.gz"
      sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    config:
      mount_path: /mounted/target-relay.json
      sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
ready:
  components:
    adxlet:
      version: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      release_id: "release-ready"
      artifact:
        url: "https://releases.example.invalid/ready.tar.gz"
        sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
      config:
        mount_path: /mounted/ready-adxlet.json
        sha256: "1111111111111111111111111111111111111111111111111111111111111111"
    relay:
      version: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      release_id: "release-ready"
      artifact:
        url: "https://releases.example.invalid/ready.tar.gz"
        sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
      config:
        mount_path: /mounted/ready-relay.json
        sha256: "2222222222222222222222222222222222222222222222222222222222222222"
```

`effective_config_sha256` 表示渲染后的版本有效配置和 env，不包含随后注入的瞬态 `ADX_SUPERVISOR_*` 父控制键，也不是全部 OS 环境的摘要。`external_startup` 仅记录初始启动选择事实，不是持续的 current binary/config 证明。

NodeInfo 组件独立的可选 `sources` 按 Role 公示已启动 owned Process 的认证来源，不从当前 ConfigMap版本声明或 `external_startup` 推断。每项仅有可独立 unknown 的 `version`（已认证 manifest commit）、`archive_sha256`（下载并校验的整个包）和 `raw_config_sha256`（固定 FD 读取并校验的 ServiceOverride 原始 bytes）。成功观察的初始镜像 sources 为空 map，观察失败为 null；embedded 唯一 Adxlet Process 保留两个角色各自的配置来源，不构造 Relay Child 或未执行的 Relay binary SHA。binary-only 更新保留 actual 配置来源并替换 binary 来源，local 制品不沿用旧 archive 摘要。来源来自同一次 owned FD/PID/config/NodeAdmin ready proof，recheck 发现来源变化同样返回 unknown，不增加 RPC 或 ExpectedRuntime/Submission 字段。来源 unknown 不抹掉 actual runtime、不禁止升级，但不能作为完整 ready promotion 的证明。

当前仅支持上述窄 schema；`format`、`ref`、`binary_path`、`size` 等未使用字段不接受。

```sh
# 首次生成默认单机配置：全角色＋adxctl 托管的本地 Redis
sudo adxctl config init

# 查看模板但不写文件；profile 还支持 standalone-external-redis/coordinator/node/ingress-api
adxctl config template --profile node

# 正式部署可生成只引用内置 profile 的精简文件
sudo adxctl config init --profile node --compact
export ADX_NODE_ID=worker-a

# 展示环境变量与覆盖项合并后的完整有效配置
sudo adxctl config dump

# 编辑生成的 /opt/adx/config/deployment.yaml 后检查，不启动进程
sudo adxctl validate

# 生成最终组件配置，供上线前审查；输出目录必须不存在
sudo adxctl render \
  --output /opt/adx/run/config-review

# 前台启动 supervisor；start 是 run 的别名
sudo adxctl run

# 以下命令在另一个终端执行，读取同一份默认配置
sudo adxctl status
sudo adxctl stop

# Read-only rollout discovery from the running supervisor
sudo adxctl rollout node-info

# 本次停止保留 Environment；显式清理则使用 --mode drain
sudo adxctl stop --mode preserve

# 非默认位置可在所有子命令前后传入，也可设置 ADX_DEPLOYMENT_CONFIG
adxctl -c /srv/adx/node.yaml validate
```

`config init` 默认选择 `standalone` profile，即单机全角色并托管本地 Redis。它创建父目录并以 `0600` 写入完整模板；增加 `--compact` 时只写入 `schema_version` 和 `profile`，运行时再合并内置默认值。目标已存在时会失败，只有显式增加 `--force` 才覆盖。其他 profile 如下：

| profile | 生成的本机角色 |
| --- | --- |
| `standalone` | Redis、Coordinator、adxlet（内嵌 Relay）、API Server（内嵌 Ingress） |
| `standalone-external-redis` | 单机全角色，不托管 Redis；API Server 仍默认内嵌 Ingress |
| `coordinator` | Coordinator |
| `node` | adxlet（内嵌 Relay） |
| `ingress-api` | API Server（默认内嵌 Ingress；可显式拆分） |

`validate` 负责部署结构、路径、角色、Redis 和 socket 约束。组件证书内容、端口占用、sandboxd、Redis 连通性及服务自身字段由 `run` 和真实请求继续验证。

`render` 将 `redis_url`、`namespace`、Coordinator 发现配置、Node 管理 socket 和运行环境写入各组件最终配置。生成目录权限为 `0700`，文件权限为 `0600`。它可能包含 Redis 凭证，只用于本机审查。

`run` 不转入后台。systemd、Pod 或其他进程管理器应直接托管它。supervisor 在 `state_dir` 创建锁和 `supervisor.sock`，同一目录只能运行一个部署。日志写入 `state_dir/logs/<service-id>.log`。

### 本机进程升级

standalone 单组件升级可提供成对 `--component-config-path` / `--component-config-sha256`，指向 ConfigMap版本声明配套的 `ServiceOverride` JSON。不提供时保留当前实际配置；提供时以 supervisor 启动时固定的部署基线加该覆盖生成私有快照，不继承上个版本未声明的业务调参。输入读取、摘要或保护边界校验失败发生在退出前，不退回旧配置继续升级。另一组件不因此重启。

实际运行证据的 `configuration_sha256` 与准备结果中的同名字段表示排序规范化后的渲染 JSON 和显式 env，不是全部 OS 继承环境摘要；父控制键及父进程改写的 Relay route socket 不参与版本摘要。候选验收同时比对二进制与配置摘要，失败恢复使用原实际完整配置。

远端制品首版源码为 `adxctl rollout submit` 和 `node_rollout.py plan-process`
增加成对 `--archive-url` / `--archive-sha256`。未指定时仍使用已安装 releases；
指定时每节点 Submission 增加 `{url, archive_sha256}`，角色二进制 `--sha256` 可省略，
由本机 adxctl准备时从整包 SHA 认证且完整校验的 manifest 派生；显式提供时仍严格比对。
镜像自带版本的已安装 releases 路径无 archive 时仍要求角色 SHA。发布脚本不下载包，
只发送指令并维护发布流程。原 Submission 不回填派生值，Query/同 task 重放精确比较完整来源；
准备结果以 `task.prepared.binary_sha256` 在首次退出或封存前持久化，缺失或无效时不得推进或验收目标成功。
只接受无用户信息/fragment 的 HTTPS，
不跟随 redirect，不读取 proxy 环境，不附加认证。下载、最多三次请求和退避、双遍解压、
全文件 SHA 与原复制共同使用原 900 秒准备期限及 stopped；准备失败不退出 old Child。
HTTP connect 10 秒、无数据读取 30 秒；仅 transport/429/500/502/503/504 可重试。
可重试 HTTP 回复保留 Retry-After，支持 delta-seconds 和标准 HTTP-date；过去日期可零等待，
无效/缺失使用原 1/2 秒退避。有效等待超过本 task 剩余期限（含极大数字溢出）直接准备失败，
不提前重试或刷新期限，等待仍可 stop 取消。IO/大小/SHA/取消为终止错误，保留底层 IO/transport 原因。
Python planning 在 maintenance 前拒 host 禁止字符、控制字符和无效 percent escape，
仍接受标准 DNS、IPv4、带方括号 IPv6 和合法 archive 路径。
task 私有 archive/unpacked 排他创建，不翻 current、不全局安装、不共享 cache。
压缩上限 512 MiB，实际展开（含 header/padding）2 GiB，最多 10000 headers。
沿 schema1 manifest（4 MiB、files JSON 键唯一、全 regular file exact set/SHA），
允许 builder extra 字段；双遍标准 tar 支持真实 `./` 和不超过 4096 字节 GNU L 长名，
不支持 PAX/GNU longlink/sparse/link/device，拒路径穿越/重复/冲突与非零目录 payload。
必须完整双 zero tar 尾、gzip CRC/ISIZE、单 member 且无压缩尾垃圾，不执行包内脚本。
全文件校验使用迭代相对路径队列、常数目录 FD 和单份 hash buffer，不递归目录树；
从固定 package DirFD 逐 component NOFOLLOW 重开，每 component/file chunk 检查原期限和 stop。
深/宽目录与小私有 decoded cap 边界均有测试源码，生产两个 pass 仍固定 2 GiB。

`adxctl rollout node-info --declaration-path /mounted/target.yaml` 只读观察
ConfigMap版本声明；省略参数时使用部署的 `external_declaration`，两者都未设置时
响应的 `external_declaration` 为 null。该参数不启用声明初始化、不改变启动选版，
也不 watch 或自动提交升级。声明仍须包含完整 target/ready 双角色，新增可选
`upgrade_components` 默认为空，最多一个 `adxlet` 或 `relay`；embedded 必须为空。

只读观察使用一个固定 FD、4 MiB 输入上限和不刷新的 5 秒预算，不读取组件配置正文，
不执行本机取包、渲染或启动。成功返回 `{path,raw_sha256,desired}`，其中 desired
是官方 typed 声明，artifact 固定为 `{url,sha256}`，不是当前 running 身份。
失败仅返回 `{path,error:{code}}` 静态错误码，不返回 parser 原文；desired 序列化
超过 64 KiB 也只返回该观测错误，不抹掉 actual runtime、`external_startup` 或
顶层 `ok`。此入口只读，publisher 的输入修改与恢复使用独立的有 journal CAS 流程。

`adxctl rollout node-info`、`submit`、`query` 使用现有 supervisor 私有 UDS，
不增加公开监听。socket 为 `0600`，受 flock 保护的 `state_dir` 为 `0700`。
仅已发现的独立 `adxlet`、`relay` 可作为目标；内嵌组合不接受进程升级提交。
Relay 必须与一个实际 standalone adxlet 的 route socket 唯一配对。
service ID 不是 node ID，缺失或有歧义的配对不能用配置值猜测。

NodeInfo 的 `runtime` 包括 supervisor 启动 `boot_nonce`、可选实际 `pod_uid`、
owned Child PID、可执行文件 SHA-256、Node session 和 Relay session。
实际 Inspect 已移至同一 admin socket 的 NodeAdminService，两模式共享真实 routes/manager，
仍只授权启动时直接父进程的 UDS peer PID；Drain 原语义不变。四项身份外仅新增 live ready。
ready 需要当前 session 普通 reconciling=false 注册明确 ACK、返回 upgrade_control 成功应用，
authority initialized、lifecycle ready 且不 reconciling/draining/stopping/process sealing；
Relay 还须本地完整 catalog/owner/session/sync epoch 明确确认、无 buffering/未确认写及远端 ready。
successor 额外要求 false 注册与 Inspect epoch 一致、control 应用及 successor 发布成功后才记 ACK。
reconciling/FailedPrecondition 清 ACK；普通 Unavailable 本身不撤销仍有效 session 证据。
手动 pause/Pod isolated 的不接受调度不等于启动失败，ready 不取 accepting_allocations。
父进程仍核真实 owned Child PID/FD、UDS/Relay 绑定并二次确认 generation，整次 proof 共用 5 秒期限。
embedded Adxlet 可观察实际 runtime 且无需独立 Relay pair，但 can_upgrade 永远 false，
Process Begin/Query 与 RelayHandoff 仍只挂 standalone；ExpectedRuntime 不新增 ready 冻结字段。
初始化不在 actor loop 启动前等待自身观察。
Kubernetes 部署须通过 Downward API 提供 `ADX_POD_UID`，不能从 Pod 名推导。

在运行 supervisor/adxlet 的容器及 DaemonSet 模板中使用真实 Pod 身份：

```yaml
env:
  - name: ADX_POD_UID
    valueFrom:
      fieldRef:
        apiVersion: v1
        fieldPath: metadata.uid
```

`ADX_POD_UID` 是保留键，`services[].env` 和合并后的
`service_overrides.<role>.env` 均禁止覆盖；受管进程继承父容器的 Downward 环境。
adxlet 的 `UpgradeGate::open` 仅启动时捕获一次，不写入 authority 文件。
缺失表示未知；显式空白、非 UTF-8 或超过 128 UTF-8 字节的值返回结构化启动错误。
升级 inspect 的 `observed_pod_uid` 是这个实际启动值，不是任务绑定的 `pod_uid`。
Coordinator 沿用认证节点 RPC、fresh live 与匹配 session 的检查链输出该字段。
`node_rollout.py plan-pod` 必须提供并冻结 `--context`，检查模板和每个源 Pod
目标容器唯一的 `metadata.uid` Downward 声明，并在两轮检查中要求实际 UID 精确匹配。
该 Pod planner 自身仍仅 GET；独立的 Linux `apply-pod` 正常路径发布器使用现有
Coordinator 迁移/验空/许可 API 和 UID+RV 条件 DELETE，不在 publisher 直接 Pause/checkpoint。
`plan-pod --batch-size` 默认 1，首节点独立 canary；每次
`apply-pod --plan plan.json --journal pod-journal.json --batch-index 0` 仅推进明确的一批，
此前批必须 verified，最后一批才 complete。模板只保存原/目标指纹，image-only JSONPatch
及 DELETE 都先保存 intent；不确定回执保留同 owner/operation ID 读后续做，不换 RV 绕过许可。
`stop-pod --plan plan.json --journal pod-journal.json` 持久冻结同任务并禁止 normal resume；
`abort-pod` 同参数继续清理，不开启新迁移、不将已迁 Env 或 verified replacement 搬回旧版。
DELETE 使用 metadata UID/fresh-RV CAS 栅栏后 revoke，模板仅恢复自有 image；结果报告 partial
与 fresh 核实的实际节点版本，不承诺全量回滚。freeze 只有明确成功完整 HTTP task 回复
才能确认完成，GET Aborting 不代替 grant/cancellation 收尾；丢 ACK 同 body 重放。
无 Begin intent 可本地停止/中止，Begin 未知则同 Begin 收口再 freeze；Completed/Aborted
终态不再 freeze，重读完整 owned inventory 后报告，额外 Pod/terminating/未知身份保持 pending。
清理 replacement 仅复用当前节点，允许精确原模板或自有 target 模板；原 Pod released
恢复不比较过时 task.original_paused，尊重服务端最新管理员 pause intent。
publisher 冻结全部 owned original UID/session，Begin 固定完整 original binding，替代绑定不清空
original，因此没有可达的 late-original register_source 分支，也不需要新输入；未知或新增
original session 直接拒绝，不猜 session 或改写冻结身份。
partial abort 对 normal 已 verified 的 target Pod 保留业务，仅同身份 verify 明确成功收尾，
不重新强制 quiesce/疏散；未 verified replacement 才严格验空/new permit/verify，不开启
Aborting migration。CM 输入恢复在确认 freeze 后、放行 cleanup 前完成。
仅 `Aborting` 且 `rollback=true` 的收尾 verify 可不要求跨节点恢复指纹；真实注册、可路由且未 fenced、有效 checkpoint scope、未 verified 节点的验空与 permit、操作收敛和暂停归属仍须满足。
此收尾保留目标版本并报告 partial，不授予缺指纹节点后续迁移/恢复资格；普通 Active verify 即使携带 rollback=true 仍检查完整恢复能力。
Pod inventory 统一读取 `kubectl get --raw /api/v1/namespaces/NAMESPACE/pods`，冻结 context，
namespace 路径编码，不追加 `-o json`；严格要求完整 v1/PodList 和逐项真实 namespace/UID，
高层 kubectl 输出的 generic List 不作为证明。详见 publisher 清理边界。
raw PodList 还必须有非空 list resourceVersion；typed PodList 允许省略 item TypeMeta，
由容器类型补齐 Pod/v1，显式其它 kind/apiVersion 仍拒绝，namespace/UID/name/RV 校验不放宽。
Coordinator 对尚未绑定的新 session inspect 会屏蔽 HTTP strict_empty；isolated 替代预检
核对真实 UID/session、protocol 3、unknown=false、task/epoch 及四个零计数，不依赖该屏蔽字段。
服务端 raw replacement_binding 仍严格验空；重绑后的 Quiescing 必须 strict_empty=true
并匹配新 permit/revision。模板仅 RV 变化且完整原模板指纹/UID 未变时，先持久化
新 RV 的 image-only CAS intent 再重试；DELETE 不刷新 RV，terminating 原 UID 等待权威消失。
quiesce 若仅保存了 exact binding 的 quiescing control、尚无 permit/delete intent，
fresh inspect 确认真 UID/session、unknown=false、同 task/epoch 后可重放原固定 body，
允许 Node 仍报告旧 revision/Evacuating。这不证明旧调用未执行，最终仍由服务端串行
raw 严格验空并取得固定 permit/revision；已有 permit/delete intent 不因此重签或替换。
本地 verified 必须收到 verify-node 明确成功的完整 HTTP task 回复，核对 owner/epoch
及 verified 替代 UID/session；GET task、实际 verified control 或调度布尔值都不能证明
服务端已经完成调度收尾，特别是原本 paused 的节点。任何 ACK 丢失均保留 verify intent，
续做时重放原固定 body，成功回复后才持久化本地 verified。首次仍要求严格 Quiescing
permit/revision；服务端已 verified 时不再用旧 permit/strict_empty 阻断合法新工作。
详见 [publisher 边界与恢复说明](../../build/release/NODE-ROLLOUT.md#pod-apply)。
观察通过 pinned `/proc/<pid>/exe`、parent-authenticated admin UDS 和实际 Relay
完成；失败返回 `runtime:null`，不是配置中的静态摘要。
NodeInfo 以 `schema_version` 标识协议，`step_protocol`（当前为 `2`）标识本机分步
adxlet 升级协议；publisher 在 Submit 前须核对该值，旧版 adxctl 不返回此字段。
各组件返回 role、mode、runtime 和 `can_upgrade`。后者仅表示 standalone、本机
executor/runtime 已就绪、实际快照可获取，且 adxlet 具备唯一实际 `node_id`、绝对
`proxy_socket`，并恰有一个受管 Relay 的 `ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR`
下 `route.sock` 与之相同；不表示升级功能已验收。外置 Relay 仍可正常启动，
只是该 adxlet 不具备本机升级能力。
Node session 与 Relay route-controller owner 是不同身份。

publisher 提交必须传 `--expected-runtime`，值为选中 NodeInfo 的完整 runtime。
手动本机操作显式传 `--local-manual`；没有隐式无 fencing 默认值。
supervisor 在接受任务及文件准备后核对 runtime，actor 再核实际 owned PID，
拒绝跨 boot/Pod 或同 boot 自动重启后悄悄改投其他进程。

```sh
sudo adxctl rollout node-info
sudo adxctl rollout submit --task-id relay-1 --role relay --service-id relay \
  --commit "$RELEASE_COMMIT" --sha256 "$RELAY_SHA256" --local-manual
sudo adxctl rollout query --task-id relay-1
```

未指定远端 archive flags 时，制品必须预装到同一安装的 `<prefix>/releases/<commit>/bin/<role-binary>`。
commit 为 40 位小写十六进制，SHA-256 为该可执行文件的 64 位摘要，
不是归档或 manifest 的摘要。schema-1 manifest 须匹配 commit、Linux host target
及所选文件摘要；这不是全包校验或 publisher 签名认证。
复制时拒绝路径符号链接，复制后再次校验私有文件，不执行目标包校验程序。
不要用会切换全局 `current` 的普通安装器只准备单个组件；远端包可走上述 task 私有准备入口。
supervisor 启动时解析固定 package 路径，后续 `current` 变化不会改其重启选择。

adxlet 提交还需 `--process-identity` JSON，形状为
`{"node_id":"n","identity":{"rollout_id":"r","epoch":1,"old_node_session":"s","old_relay_session":"p","original_sha256":"<sha256>"}}`。
本轮 owner 的 local task ID 直接取 `--task-id`，目标摘要取已认证 `task.prepared`（显式 `--sha256` 仍严格比对），node ID 须匹配实际服务。
这是本机 Supervisor 的文件选择，不发送给 Coordinator 或 Relay。

adxlet Submit 只做准备，不 fence、不退出进程、不连接 Coordinator：在固定 900 秒
准备期限内准备目标可执行文件与渲染配置，核对原进程实际可执行文件摘要并复制为
task 私有 `original`，把原/目标渲染 Process 快照写入 `original-process.json`、
`target-process.json`；全部成功后才持久化 `task.prepared`。准备失败写 Failed。
成功后任务保持 Preparing 与 `prepared`，继续占用本机槽，Query 不将其标为 Interrupted。
旧配置中的 `supervisor_process` 字段已移除，出现时配置加载失败。

publisher 通过计划的管理 HTTP `origin` 预检并管理本轮维护：
`POST /v1/adxlet-process-upgrades` 只接受 `action:prepare|release`、`owner`、
`node_session`；`POST /v1/adxlet-process-upgrades/query` 只接受 `owner`。
查询返回 200 的 flat `absent`、`active` 或 `released` 状态，分别包含 owner、
owner/origin_session/current_session/yielded，或 owner/node_session。
同 owner 历史解除回执优先于后来任务 marker；无本轮回执的其他 owner/epoch 冲突返回 409，
不能当作 absent。旧 Coordinator 的 404、非法响应和传输未知均在副作用前拒绝。
publisher 不再需要 Supervisor 证书、专用 gRPC/stubs 或 handoff 三个配置参数，
也不向 Coordinator 预先批准下一 session；本机 Step 协议保持不变。

管理 HTTP 没有调用者认证，owner 也不是凭据。部署必须限制可信发布 Job/运维来源，
不能经公共 API/Ingress 暴露；ServiceAccount/RBAC 不能替代 HTTP 网络访问限制。
该边界不提供原 publisher mTLS 调用者认证；本轮未验证环境隔离或集群升级。
adxlet 的 Node 身份认证、Yield、注册与 QueryRelease 继续保留。

```sh
sudo adxctl rollout step --task-id adxlet-1 --step exit
sudo adxctl rollout step --task-id adxlet-1 --step start --attempt "$ATTEMPT" --node-session "$SESSION"
```

`rollout step` 对已准备任务按 `exit`、`start`、`rollback` 发出请求；
仅首个 `exit` 不带 attempt/session，`start` 仅接受目标 A，须与任务记录完全一致且无 rollback_from，期望 runtime 只取任务
提交时冻结的值。`exit`、`start` 与 `rollback` 共用同任务的异步 worker。
`start` 仅接受 Exit 已真实 reap 后记录的 attempt/node_session，沿用 Exit 保存的绝对执行期限
（不刷新）；使用任务目录中准备时固定的 target Process 快照、私有配置副本与 executable，
spawn 前重核配置有效摘要与 executable SHA。Start 请求回复丢失或失败时先以同 attempt 只读回执查询：
已记录的 Child 以同 PID 继续，NeverCreated 作为事实返回，仍未知时才再发同 attempt 的幂等 Start。
bootstrap 仅交付给实际 owned Child PID；ready 要求该 PID 的实际配置摘要、executable SHA
及 admin socket `Query` 报告 `ready` 且 node_session 与保存值一致，然后 Complete。
Complete 成功但 `completed` 写盘失败时记录退回 `target_commit_unknown`（PID/attempt 不变），
由回执收敛；worker 已写入终态但最终写盘失败而保留槽时，同任务 Query 重写终态，写盘成功才释放槽。

目标候选 A 失败后，publisher 用 A 的 attempt/node_session 发出 `rollback`。
worker 先查询同任务 actor 回执：目标已 Committed 时收口为 Completed/completed，不停止已选进程；
回执未知不作为未创建或已退出的证明。真正进入回退前保存一次固定 600 秒绝对期限，
阶段为 `rollback_intent`；失败不确定时保留 `rollback_unknown` 和同一期限。
actor 先封闭 A，再证明 NeverCreated 或等待该 owned Child 的真实 reap。
尚未执行的迟到 Start(A) 也由同一 spawn receipt 的 closed 记录拒绝，不会复活。
只有上述证明成立、准备时固定的 original 文件及私有配置重新验证通过，才生成并保存身份 B，
阶段为 `rollback_ready`。`adxlet_execution` 新增可选字段
`rollback_from: {"attempt":"A","node_session":"session-A"}`，只保存这一个来源身份。
若 B 的首次写盘失败，重试保留并重存同 B，不生成 C。

`rollback` 始终用来源 A 的 attempt/node_session 重放；已有 B 时重存并继续同一个 B，
已有活动 worker 时不创建第二个 worker/Child，不停止 B 或生成 C。外部 Start(B) 被拒绝。
publisher 首次提交 rollback 前核实同 owner 的 Yield 和已知 current_session；
本地 worker 保存 B 后直接复用现有 Start/bootstrap/ready/Complete 路径，使用固定的
original executable、Process 和私有配置副本，不再等待 publisher 发 Start(B)。
因此已接收的 rollback 可在脚本断连后启动 B；Coordinator 不可达时也可先启动 B 等待注册，
publisher 保持 pending，维护不会自动解除。同 owner 只部署一个 publisher 是部署约定，
journal flock 仅约束使用相同 journal/lock 路径的调用，不提供跨路径的单执行者保证。
原版阶段依次为 `original_start_intent`、`original_start_unknown`、`original_commit_unknown`，
成功写 Failed/original_selected。Complete 丢回执或 original_selected 写盘失败时保留
`original_commit_unknown`，由同一 B 的回执收口，不重新启动或停止可能已选的原版。
600 秒包括 A stop/reap 和原版 Start/bootstrap/ready，所有重试和查询不刷新期限。
publisher 即使首次观察到已完成的 B，也先校验 rollback_from 并采用 B，再核实际原版
PID、配置、executable、注册 session 和独立 Relay session，最后 release 并按升级失败停止后续批次。
原版失败或超时保留维护，不递归回退。

adxlet process publisher journal 使用 schema 2；Relay/Pod journal 和 process plan 仍为 schema 1。
旧 adxlet schema-1 journal 在任何远端效果前拒绝，无迁移器；进行中的任务须用原版本收口后再升级。
旧 `rollback_stop` 命令不保留别名。
Node admin 校验启动及当前 parent PID，
bootstrap socket 校验 parent PID/UID，candidate admin 连接校验实际 owned Child PID。
管理地址仍为 `<state_dir>/<service_id>-admin.sock`，启动信息交付使用固定的
`<state_dir>/<service_id>-start.sock`，不再将 task ID/attempt 拼入路径。
准备时检查目标和原版启动地址，绑定前再次核对完整路径不超过 100 字节。
启动地址为 `0600`，活动监听、普通文件和符号链接占用时拒绝覆盖；完整身份继续在任务与启动信息中校验。
支持 PID1 常驻 supervisor，仍精确核启动时 parent、当前 parent 与内核 peer PID；bootstrap 另核 peer UID，原父进程退出后 reparent 到 PID1 不通过精确身份检查。

准备固定 900 秒；首次退出前固定整体 900 秒；首次进入原文件恢复前固定 600 秒，
后者包括失败候选 stop/reap、fresh session、启动和恢复。重试不刷新期限。
Supervisor 独占文件、SHA、期限和 Child；Begin 之前持久 execution/期限/意图。
Node 先把已接受操作 seal 到 Published，再以 Node 身份 Yield，随后 Preserve 退出。
Coordinator 只保存 per-node owner、保护、session handoff 和 release receipt，
不保存版本、物理阶段、执行期限或批次结果；批次属于 publisher journal。

只有真实 reap 或 actor 已记录 NeverCreated 才能换下一 session。
Start 同 task/attempt/内容重放不重复 spawn；迟到 closed attempt 不得复活。
目标与原文件共用候选启动路径，每个 attempt 使用 fresh Node session。
未知创建、端口不可达、超时或丢 ACK 不证明退出，保留 Unknown 和维护保护。
精确 ready 后 Complete 丢 ACK 只重放同 attempt；期限后只读已完成回执，
不能以 `target_commit_unknown` 或 `original_commit_unknown` 停掉可能已选中的 candidate 或启动竞争候选。
worker 结束后，Query 或同参数 Submit 可用本机 actor 的精确只读完成回执收敛任务：
目标 leg 写 Completed/completed，原文件 leg 写 Failed/original_selected，写盘成功才释放槽。
活 worker 的 Query 仍纯读；回执未知、观察超时、写盘失败或重启丢失 actor 记录时保留 Unknown，
后续查询可以重试观察，不发送 Complete/Stop/Start，也不刷新执行期限。

publisher 在确认本轮 Yield 和本机真实 reap 后用保存的身份调用 Start；
启动前不要求 Coordinator 的 current_session 已等于新 session。
successor 使用普通 `RegisterNode(true) -> InspectNode -> 只读恢复 -> RegisterNode(false)`。
维护下的新 session 注册须具备 owning Node 身份、原 RPC/Relay 地址、已确认 Yield，
首次处于 reconciling 且关闭新分配。Coordinator 在状态锁外调用现有 GetSession，
核对原地址的真实 node_id/session，再重核维护 owner/current_session 并通过原接替 CAS。
心跳已过期或 Coordinator 重启均不能跳过 GetSession；相同已注册会话的重试幂等，
晚到旧请求不能覆盖后续已接纳会话。NodeHandoff 的 Query/ReplaceSession 已删除，
Node 自身的 Yield/QueryRelease 保留。
首个 false 前完成目录、执行身份、Execd/readiness 和本机恢复准备。
exact false ACK 后解开 controller authority fence，恢复正常生命周期。
successor 完成与 parent ready 查询按 `rollout_id/local_task_id/epoch` 核对任务归属，
并绑定当前启动 session；继承的旧交权记录仍须已 sealed，且不能是当前进程自己的旧 session。
旧 Begin、收尾和未完成 Query 继续完整核对原请求及固定期限，不因新进程使用任务归属字段而放宽。
此前 Pod 升级的本 session `released` 记录保留；活动 Pod 控制仍阻止进程交接。
回退进程不能继承失败候选的 ready，必须完成自身恢复后记录本 session 的 ready。
维护未显式释放时重握手仍只读，不调用 journal recover 或普通 retained cleanup。
Coordinator 重启或 epoch 变化重走握手，heartbeat sequence 保持同 session 单调递增；
首次等待也继续 discovery。首次成功前受 bootstrap 固定期限限制。

Relay 的 generic `TakeoverBindings` 冻结 actual expected owner/epoch 和 snapshot，
pending 先重放保存 payload，exact ACK 不重新安装旧快照，陈旧 epoch 拒绝。
RouteHold 仅是非 routable 期间的合法 Environment route fallback，显式 release 才移除。
publisher 核对实际注册 session 和恢复结果后，用 owner/current session 释放维护；
HTTP query 同时区分本轮 active marker、absent 和 exact release receipt，
解除回复丢失时只以同 owner 与已冻结 release session 的 released 回执收口。
不存在、冲突或超时都不能证明已解除。release 保留最新人工 pause 意图；
Node 仍用自己的 QueryRelease 核实 exact receipt 后清本轮 local maintenance。
HTTP release 回执不能替代上述本地恢复；日志中的 `successor register-reconciling/register-normal`
表示注册阶段，`successor inventory/restore-runtime/publish-ready` 表示本地恢复阶段。
排查通用 Conflict 时应结合该阶段与 node/session/任务关联字段，不直接认定是 Coordinator CAS 失败。

独立 Relay 升级不暂停 Node 调度，不借用 Coordinator maintenance。
Begin 拒新流并确认业务/health listener 释放；候选使用固定业务端口及分代控制 UDS。
旧 CONNECT 与双代撤权保留，释放时固定 120 秒收尾，重试不刷新。
自然归零、强制关闭、打断和失败分别记录；自然退出不保证每条业务请求成功。
分代 route socket 不改变活动发布目录，socket 清理须确认 owned Child 退出及 inode。
升级使用 `<state_dir>/s/r-<key>/route.sock`，回退使用同级的 `o-<key>/route.sock`；
key 为完整 task ID 派生的 24 个十六进制字符，仅用于路径，完整任务身份照常传递和记录。
两条路径在准备阶段一起校验，避免旧进程退出后才发现回退路径超长。

任务在 `state_dir/rollout/<task-id>`，JSON 状态为 `0600`，原子替换并 sync。
请求上限 4096 字节，journal 上限 64 KiB；超限更新保留旧状态。
同 task 同参数返回同任务，变参 conflict；未知结果保留槽位，新任务 busy。
缺 executor/runtime 或 adxlet 必要配置时不接受任务；query 不因客户端断开而取消 worker。
Supervisor 重启不自动恢复未完成执行，未知状态须保留，不能当成 Failed 自动释放。


`status` 返回子进程 PID、角色、重启次数、当前退出／启动失败标记和日志状态。进程退出或启动失败后，按 `restart_delay_ms` 持续重试，成功启动后清除失败标记。PID 存在只代表进程存活，不代表集群已经可以创建 Environment。AFS 服务配置了 `health_url` 时，supervisor 会查询 HTTP `/health`，只有返回 JSON 且 `status` 为 `ready` 才报告 ready；HTTP 200 但状态仍是 `starting` 或 `degraded` 不算 ready。

`shutdown_mode` 控制退出策略，可选 `preserve`（默认）或 `drain`。`stop` 未传 `--mode` 时，以及 supervisor 收到 SIGTERM/SIGINT 时，都使用 supervisor 启动时加载的策略；`stop --mode preserve|drain` 只覆盖本次停止。运行中修改 YAML 不会改变已加载策略。`status` 输出 `shutdown_mode`，成功停止响应输出本次实际使用的模式。

`drain` 先要求本机 adxlet 停止新准入并删除其管理的全部 Environment，包括 Paused 对象，清理成功后才按反向顺序退出组件。显式 Drain RPC 完成清理后，adxctl 向 adxlet 发送 SIGTERM，触发 planned unregister。清理失败时命令失败，并保留 Coordinator、Redis、代理等依赖以便重试。

`preserve` 跳过 NodeAdmin Drain，不因退出而删除 Environment 或清理其 checkpoint，按相同的反向顺序停止组件，并仅向 adxlet PID 发送 SIGUSR1 明确保留退出意图（不广播到其进程组）；无需 adxlet 管理 socket 可用。adxlet 收到停止信号后立即关闭新分配与本地创建准入，保留已有 Environment；内嵌 Relay 排水期间继续运行心跳和运行时控制服务，排水完成后退出，不主动注销节点。其余受管进程仍使用 SIGTERM，并执行停止超时及日志收尾，preserve 下 adxlet 终止超时的 SIGKILL 也只作用于其 PID，避免误杀同组后端子进程；进程终止失败仍返回错误，不自动切换为 drain。停止失败的结构化响应包含 `mode`、`phase`（`drain` 或 `terminate`）、`service_id` 和 `message`，CLI 将这些上下文输出到标准错误。直接向 adxlet 发送 SIGTERM/SIGINT 仍表示 planned departure，会主动注销；preserve 应通过 adxctl 选择或向 adxlet 发送 SIGUSR1。保留模式只约束 adxctl 的退出动作，不承诺运行时独立于 Pod 存活或重启后的恢复结果。

## 部署文件的公共字段

```yaml
schema_version: 1
package_dir: /opt/adx/current
state_dir: /opt/adx/run/control
redis_url: redis://127.0.0.1:6379/
namespace: adx
restart_delay_ms: 1000
stop_timeout_seconds: 90
shutdown_mode: preserve
services: []
```

| 字段 | 含义 |
| --- | --- |
| `package_dir` | 当前主机安装的完整发布包；组件从其 `bin/` 目录启动 |
| `state_dir` | supervisor 锁、控制 socket、生成配置和日志目录；每个本机部署必须唯一 |
| `redis_url` | 集群状态和 Coordinator 发现使用的 Redis；所有主机必须指向同一个后端 |
| `namespace` | Redis 中的 ADX 集群隔离名；同一集群必须一致，不同集群必须不同 |
| `services` | 当前主机需要启动的角色，不是整个集群的角色清单 |
| `with_afs` | 默认 `false`；必须显式为 `true` 才允许 `afs-meta` 或 `afs-node` 角色 |
| `restart_delay_ms` | 进程退出或启动失败后，再次启动前的等待时间；默认 1000 毫秒，持续重试 |
| `stop_timeout_seconds` | 单个 Drain 或进程停止阶段的超时；须覆盖完整 Relay 排水及退出收尾时间 |
| `shutdown_mode` | `preserve`（默认）跳过删除；`drain` 先删除本机 Environment。无参数 `stop` 和 SIGTERM/SIGINT 使用启动时加载的模式 |
| `environment` | adxlet 和 API Server 共用的本地 EROFS 或 OCI 运行环境定义 |
| `logging` | supervisor 接管组件输出时的滚动、压缩和保留策略 |

`stop_timeout_seconds` 应大于 Relay 的完整退出时间，并为调度和收尾留出余量。内嵌 Relay 的 `ADX_DATA_PLANE_RELAY_DRAIN_TIMEOUT_SEC` 默认 30 秒；等待活跃流结束后，后台服务收尾还可再等待同样的超时，因此不能只按单个 30 秒窗口设置 supervisor 超时，可按 90 秒起配置。e2e 部署生成器当前使用 30 秒，慢排水时可能先触发 supervisor 超时：preserve 不转换为删除，但 adxlet 会被 SIGKILL，`stop` 返回 `phase=terminate` 失败，未完成的排水和心跳随之中断。外层 systemd／Pod 终止宽限期还需覆盖全部受管组件的顺序退出。

每个 `services[]` 元素由 `id`、`role`、`config` 和可选 `env` 组成。`config` 是组件配置，`env` 用于 Ingress 和 Relay 等环境变量入口。不要手工重复公共 `redis_url` 和 `namespace`；`adxctl render` 会按角色注入。

### profile 默认值与受控覆盖

完整 YAML 继续受支持。需要减少重复配置时，文件可以引用一个内置 profile，并只写当前主机的差异：

```yaml
schema_version: 1
profile: node

state_dir: "${ADX_STATE_DIR:-/opt/adx/run/node}"
redis_url: "${ADX_REDIS_URL}"
namespace: "${ADX_NAMESPACE:-adx}"

logging:
  max_files: 9

service_overrides:
  adxlet:
    config:
      node_id: "${ADX_NODE_ID}"
      advertised_address: "${ADX_NODE_ADDRESS}"
      tls:
        certificate: "${ADX_NODE_CERT:-/opt/adx/config/tls/node-1.pem}"
    env:
      ADX_DATA_PLANE_RELAY_BIND: "${ADX_PROXY_BIND:-0.0.0.0:19002}"
```

合并规则固定如下：

- 顶层标量覆盖 profile 默认值，未出现的字段保留默认值。
- `logging` 和 `environment` 对象递归合并。
- `service_overrides` 按本机角色名定位；默认 Node profile 只有 `adxlet`，Proxy 的环境项也覆盖在该角色下。不能改变角色或增加隐藏进程。
- 服务 `config` 对象递归合并，`env` 按变量名覆盖。
- 数组整体替换，不按下标合并。
- profile 文件不能同时声明完整 `services`，避免两套来源产生歧义。
- 合并后仍执行与完整部署文件相同的类型检查和部署校验。

每台 Worker 使用自己的部署文件；`service_overrides.adxlet` 表示覆盖本机唯一的 adxlet，不是引用名为 `adxlet` 的集群节点。集群身份由该文件中的 `config.node_id` 单独指定，并且必须在集群中唯一。本机 supervisor 的服务 ID 固定使用角色名，不参与节点归属。精简 `node` profile 会生成 `node_id: "${ADX_NODE_ID}"`；未设置该变量或没有在 YAML 中改成明确值时，配置加载直接失败。

`adxctl config dump` 将当前环境变量和覆盖项解析成完整 YAML，便于上线前审查，也可以再次作为完整配置读取。输出可能包含带凭证的 Redis URL，不要写入公开日志。

### 使用环境变量设置字符串字段

部署环境不同但结构相同时，可在任意 YAML 字符串值中引用环境变量：

```yaml
package_dir: "${ADX_PACKAGE_DIR:-/opt/adx/current}"
state_dir: "${ADX_STATE_DIR:-/opt/adx/run/control}"
redis_url: "${ADX_REDIS_URL}"
namespace: "${ADX_NAMESPACE:-adx}"
services:
  - id: adxlet
    role: adxlet
    config:
      node_id: "${ADX_NODE_ID}"
      advertised_address: "${ADX_NODE_ADDRESS:-127.0.0.1:50052}"
```

- `${VAR}` 要求变量存在且非空，否则配置加载失败。
- `${VAR:-default}` 在变量未设置或为空时使用默认值。
- `$$` 生成字面量 `$`。
- 展开在 YAML 解析完成后执行，只处理值中的字符串，不处理字段名，也不会把变量内容解析成新的 YAML 对象或数组。
- 数字和布尔字段继续要求原生 YAML 类型。例如 `restart_delay_ms: 1000`；不要写成 `restart_delay_ms: "${DELAY}"`。需要按环境调整此类字段时，为不同部署文件保留明确数值。

`validate`、`render`、`run`、`status` 和 `stop` 每次读取配置时都会使用当前进程环境。运行中的 supervisor 已经生成并启动的组件不会因环境变量变化而自动更新；修改后需要按部署维护流程重新加载对应进程。

## 可选 Agent FS（AFS） 进程部署

Agent FS（AFS） 不属于默认 ADX profile。需要试用文件系统组件时，先使用 `ADX_WITH_AFS=1` 生成包含 `afs-meta`、`afs-node` 和 `etc/examples/afs/` 的发布包，再准备一份显式部署 YAML：

```yaml
schema_version: 1
with_afs: true
package_dir: /opt/adx/current
state_dir: /opt/adx/run/afs
redis_url: redis://127.0.0.1:6379/
namespace: adx-afs
restart_delay_ms: 1000
stop_timeout_seconds: 20
services:
  - id: meta
    role: afs-meta
    config:
      config_file: /opt/adx/config/afs/meta.toml
      health_url: http://127.0.0.1:7401/health
  - id: node-a
    role: afs-node
    config:
      config_file: /opt/adx/config/afs/node.toml
      health_url: http://127.0.0.1:7501/health
```

`adxctl` 不生成 AFS TOML，也不把 Redis namespace 注入 AFS 配置。`config_file` 必须是绝对路径，并指向部署者已经审查过的 AFS TOML。示例文件位于发布包 `etc/examples/afs/`，可复制到 `/opt/adx/config/afs/` 后按主机地址、挂载点、数据目录和 OwnerFs workspace bind 开关修改。默认示例显式使用 `meta_store = "local-file"`，Meta 状态写入 `/opt/adx/data/afs/meta`，Node 数据和 UDS 分别写入 `/opt/adx/data/afs/node-a` 与 `/opt/adx/run/afs/node-a.sock`。启用 workspace bind 前必须先通过 OwnerFs 创建并授权对应 Home workspace；不能直接把开关当作目录初始化步骤。启动顺序为 `afs-meta` 后 `afs-node`；停止顺序相反。 AFS 异常退出或正常停止失败后保留失败状态，不自动重启掩盖未完成的排空或恢复；其他 ADX 角色继续使用公共 supervisor 的持续重试策略。

## 单机 standalone 部署

单机部署在一台 Linux 主机上运行 Coordinator、adxlet（内嵌 Relay）和 API Server（内嵌 Ingress）。主机还必须先准备 sandboxd、证书、初始管理员密钥以及 Environment 网络。

### 使用 `adxctl` 托管 Redis

发布包提供完整示例 [deployment-standalone-managed-redis.yaml](../../build/config/examples/deployment-standalone-managed-redis.yaml)。其中包含 `redis` 角色，Redis 只监听 `127.0.0.1:6379`，数据写入 `/opt/adx/data/redis`，使用 AOF `everysec`。

```sh
sudo adxctl config init

# 修改证书路径、监听地址、Environment CIDR、sandboxd socket 和磁盘路径后执行
sudo adxctl validate
sudo adxctl run
```

supervisor 按 Redis → Coordinator → adxlet（含内嵌 Proxy）→ API Server（含内嵌 Ingress）的顺序拉起进程。启动顺序不替代业务就绪检查；应等待节点完成 Coordinator 对账和 Relay 全量绑定同步，再使用 SDK 创建实例。显式分进程时，独立 Relay 会在 adxlet 前启动，独立 Ingress 会在 API Server 后启动。

### 使用外置 Redis

`standalone-external-redis` profile 对应发布包中的 [deployment.yaml](../../build/config/examples/deployment.yaml)。它是单机全角色、外置 Redis 配置，不包含 `redis` 角色，示例地址为 `redis://127.0.0.1:6379/`。

1. 执行 `sudo adxctl config init --profile standalone-external-redis`。
2. 先启动外置 Redis，并配置持久化与访问控制。
3. 将部署文件顶层 `redis_url` 改成实际地址。
4. 保持所有组件使用同一个顶层 `namespace`。
5. 执行相同的 `validate` 和 `run` 命令。

外置 Redis 不受 `adxctl status`、重启或 `stop` 管理。ADX 停止后 Redis 应继续运行并保留状态。

## 多机按角色部署

推荐至少分为一个控制节点、一个或多个工作节点、一个接入节点。三类主机均安装相同版本的完整 `/opt/adx` 包，但各自的 `services` 不同。

### 1. Coordinator 控制节点

使用 `coordinator` profile，其中只包含 `coordinator` 角色：

```sh
sudo adxctl config init --profile coordinator
sudo vi /opt/adx/config/deployment.yaml
# 修改 redis_url、advertised_address、TLS peers 和 bootstrap key 路径
sudo adxctl validate
sudo adxctl run
```

`listen` 是 Coordinator 本机监听地址；`advertised_address` 必须是 adxlet、API Server 和 Ingress 可访问的地址（默认 mTLS 使用 `https://`，network 模式使用 `http://`）。Coordinator 会将该地址带 TTL 写入共享 Redis。mTLS 模式的 `tls.peers` 必须登记实际 API Server、Ingress 和所有 `node:<node_id>` 的叶证书 DER。

若 Redis 也由控制节点托管，可在该文件的 `services` 开头增加 `redis` 角色。多机访问时 Redis 不能只绑定回环地址；非回环监听必须配置绝对路径 `password_file`，并将带 URL 编码凭证的同一个 `redis_url` 配置到所有主机。

### 2. Worker 节点

每个 Worker 使用 `node` profile，默认只启动 `adxlet` 进程，并在其中嵌入 Relay：

```sh
sudo adxctl config init --profile node
sudo vi /opt/adx/config/deployment.yaml
# 设置唯一 node_id、可达的 advertised_address/proxy_address、Redis、证书和网络 CIDR
# 确认外部 sandboxd 已创建 /run/sandboxd/sandboxd.sock
sudo adxctl validate
sudo adxctl run
```

每个节点的 `node_id` 必须唯一，并与 Coordinator `tls.peers` 中的 `node:<node_id>` 对应。`advertised_address` 是控制 RPC 地址，`proxy_address` 是 Ingress 连接的数据面地址。内嵌 Relay 的 `ADX_DATA_PLANE_*` 配置位于 `adxlet.env`；其中 `ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS` 必须覆盖 sandboxd 实际分配的 Environment 网段。

需要独立故障域或单独限制资源时，可以使用完整部署 YAML：为 adxlet 显式设置 `proxy_mode: standalone`，把 Proxy 环境项移到独立的 `role: relay` 服务。省略 `proxy_mode` 与设置 `embedded` 等价，不能在默认内嵌模式下再启动独立 Relay。

sandboxd 仍由节点部署环境单独管理。`adxctl stop` 只停止 ADX 进程，不停止 sandboxd；Environment 是否先清理由所选模式决定。

### 3. API Server 与 Ingress 接入节点

使用 `ingress-api` profile。部署文件保留 `apiserver` 和 `ingress` 两个逻辑角色，默认只启动一个 `adx-apiserver` 进程：

```sh
sudo adxctl config init --profile ingress-api
sudo vi /opt/adx/config/deployment.yaml
# 设置共享 Redis、Ingress 公网证书、允许的客户端 CIDR 和对外监听地址
sudo adxctl validate
sudo adxctl run
```

API Frontend 对应 `role: "apiserver"` 和 `adx-apiserver`。示例将 API 监听绑定到 `127.0.0.1:8888` 并启用 `loopback_http`。`role: "ingress"` 提供 Ingress 的控制配置和 `ADX_DATA_PLANE_*` 环境；默认由 `adxctl render` 合并进 API Server 进程。Ingress 对外监听 `0.0.0.0:8443`，将 Environment 控制请求转发到同进程的 API 回环监听，并按 Coordinator 路由把实例数据请求转发到目标 Relay。

需要独立故障域、日志或资源限制时，在 `apiserver.config` 中设置 `ingress_mode: standalone`。此时渲染结果包含 `adx-apiserver` 与 `adx-ingress` 两个进程，但继续使用相同的 Ingress 服务实现、端口和证书配置。默认模式下不要手工启动 `adx-ingress`，否则会争用 Ingress 监听端口。

当前受支持的部署要求 Ingress 与 API Server 同机：Ingress 到 API Server 的控制请求使用回环 HTTP，API Server 的 `loopback_http` 也拒绝非回环监听。`deployment-ingress-api.yaml` 因此把两者放在同一份本机清单中。若后续需要分开部署，必须先为 Ingress 的 API Server 上游连接补齐 HTTPS 和服务端身份校验，不能直接将当前回环地址替换为远端 IP。

### 推荐启动顺序

1. Redis。
2. Coordinator，确认 `adxctl status` 中进程存活并已发布发现地址。
3. 各 Worker，确认 adxlet 完成对账并上报资源。
4. API Server（默认同时启动 Ingress；显式分进程时再启动独立 Ingress）。
5. 使用公开 SDK 完成创建、执行命令、删除的业务就绪检查。

组件发现允许稍后收敛，但上述顺序能提供更清晰的首次部署日志。所有主机必须使用同一构建的发布包、同一 `redis_url`、同一 `namespace` 和一致的内部通信模式；mTLS 模式还需要相互匹配的证书身份。

## 状态、故障与配置更新

典型状态输出如下：

```json
{
  "ok": true,
  "services": [
    {"id": "coordinator", "role": "coordinator", "pid": 1234, "restarts": 0, "failed": false}
  ]
}
```

- `status` 连接失败：supervisor 未运行，或命令所读部署文件的 `state_dir` 与运行实例不同。
- `deployment already supervised`：同一 `state_dir` 已有 supervisor。
- `package executable missing or not executable`：`package_dir/bin` 不完整、架构错误或权限错误。
- `stop` 失败：至少一个 adxlet Drain 或组件停止未完成；依赖会保留，检查 `state_dir/logs/` 后使用相同命令重试。
- `render` 失败且目录已存在：换一个新的输出目录；工具不会覆盖已有审查证据。

配置和证书在组件启动时读取。`adxctl` 当前没有 `restart` 或热重载子命令。仅重启 adxlet、后端执行仍存活且保持节点 ID、RPC/Relay 地址时，需要保留 Environment 的维护可在部署文件中配置 `shutdown_mode: preserve`，或单次执行 `stop --mode preserve`，再由部署环境在心跳期限内启动新进程，经 Coordinator 的 GetSession 身份校验接续会话，完成 adxlet 对账和路由同步。超时、身份复核失败或后端执行已停止时不能接续；preserve 不延长心跳期限。需要清理本机 Environment 时显式使用 `stop --mode drain`；只停止某个组件仍由部署环境管理。

完整的单机依赖、证书和 SDK 业务就绪步骤见[单机进程部署](standalone.md)，运行环境见[本地 EROFS 与 OCI](runtime-environment.md)。

### 内部通信使用 network 模式

Profile 部署可显式设置 `internal_security: network`：

```yaml
schema_version: 1
profile: standalone
internal_security: network
```

这会将 Coordinator、adxlet、API Server 和 Ingress 的内部 gRPC，以及
Ingress → Relay 数据链路改为明文；组件角色和节点 ID 由请求声明，依赖部署网络隔离，
不再通过证书认证。节点会话、实例归属、租户权限和 API Key 校验仍然执行。
对外 Ingress HTTPS 证书和监听保持不变。省略该配置仍使用 mTLS，连接失败不会自动降级。

使用完整 `services` 配置时，Coordinator、adxlet、Ingress 的 `config.tls` 设置为
`{mode: network}`，API Server 设置 `config.internal_security: network`；Coordinator
公布 `http://` 地址。Ingress 和 Relay 设置
`ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network`，并移除内部
`ADX_DATA_PLANE_INGRESS_NODE_TLS_*`、`ADX_DATA_PLANE_RELAY_TLS_*`
及 `ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA` 环境项；对外 HTTPS 配置保留。
同一条内部链路的两端必须使用一致模式。

### 管理员 Key 的生成、读取与轮换

Key 由部署者生成并写入受保护文件，Coordinator 继续通过
`bootstrap_credentials[].key_file` 读取。配置和渲染结果只包含文件路径。
例如首次部署时生成密钥（已有文件应复用，不要在每次启动时重新生成）：

```sh
install -d -m 0700 /opt/adx/config/secrets
(umask 077; set -C; openssl rand -hex 32 > /opt/adx/config/secrets/admin-key)
```

Coordinator 启动时将配置中的管理员 Key 视为完整目标集合，并在 Redis 中原子更新：
新增 Key 生效、移出的管理员 Key 记录永久撤销标记，租户 Key 保留。
至少配置一个未过期的有效管理员 Key。重复 Key、已撤销 Key、无效文件或
与租户 Key 冲突的配置会导致启动失败，不能通过恢复旧文件重新启用撤销的 Key。
这适用于升级前已登记的管理员 Key，不需要另行迁移目录。

轮换时先准备新的权限为 0600 的密钥文件，原子替换原 `key_file`，再重启
Coordinator；仅修改文件不会触发热重载。部署者从新文件读取 Key 更新客户端，
不要把内容写入服务日志。需要分阶段切换时，先同时配置两个 Key，重启后
更新客户端，再移除旧 Key 并再次重启。保留 Redis 数据。

API Server 和 Ingress 的既有认证缓存可能继续接受旧 Key 直到缓存到期；默认
示例为 10 秒，在途认证另受 RPC 超时限制。轮换不撤销已建立的长连接。

## 验证边界

2026-11-14 的记录属于此前 publisher mTLS 方案：deployment lib 86 项、adxlet 进程预检 4 项、
Coordinator 交接与暂停恢复 5 项及 Pod 回归 2 项；发布脚本 162 项，以及真实
mTLS 客户端 24 项。已通过四模块 all-targets/all-features 编译、workspace
Clippy（`-D warnings`）、格式、文档和差异检查。该版本修改的六个 Python 文件
Ruff/Pylint 均通过；面向 PR 基线的全量 Python 预检仍有 388 条既有诊断，
未将该全量检查记为通过。完整命令和日志保存在本次工作区
`out/adx-grpc-handoff-20261113/RESULT.md`。

2026-11-16 本轮通过：publisher 68 项、Pod 发布工具 101 项、交接/存储 5 项、自动注册 RPC 2 项及原普通重启注册 RPC 1 项；三份 Python 文件 Ruff/Pylint、全仓格式和 workspace all-targets/all-features Clippy（`-D warnings`）均通过。完整命令和日志见 `out/publisher-http-20261116/RESULT.md`。
这些是本地单元、组件及故障注入证据，未验证管理网络隔离、实际集群升级、跨 Worker 恢复、
生产下载或新镜像；这轮本地检查没有操作北京集群。

2026-11-16 后续独立部署会话已验证空 replacement 的中止收口：更新 Coordinator 后，原任务 `pod-retest-20261116-1700` 按原 plan/journal 收口为 `Aborted / partial`，Node0 保留目标镜像及 UID/session，Node1 保留原 Pod/镜像，模板恢复、本轮维护解除，两个节点 Ready，Environment 均为0。现场没有管理员暂停；暂停保留仍以本地矩阵测试为证。此记录不代表正常升级完成、新恢复指纹已生效、跨 Worker Environment 恢复或旧版本全量回滚。证据索引见 `out/pod-abort-cleanup-20261116/RESULT.md`。
初始管理员 Key 不由租户 Key 删除接口管理，而由上述部署配置集合管理。

2026-11-16 随后的空 Node0 现场验证已通过正常 Active verify：新 PATH 镜像和新 immutable ConfigMap 生效，恢复指纹正常生成，Pod 从 Worker .168 替换到 .210；Node1 未升级。本轮仅完成 batch0，随后收口为 Aborted/partial，未验收整轮 Completed 或业务 Environment 迁移/跨 Worker 恢复。镜像复用已有 Rust 二进制和 rootfs，不代表全部最新 Node 源码已部署。已终态任务不能继续 batch1，后续验证须新建任务。证据及表述复核见 `out/pod-normal-verify-20261116/REVIEW.md`。

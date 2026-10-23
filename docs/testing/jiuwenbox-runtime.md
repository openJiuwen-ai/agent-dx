# Jiuwenbox runtime P0

本实现基于 ADX master `fb77fc419558`，适配 Jiuwenbox develop
`9e0569f4fd047e510f4ee4ff28c26c54aaec5066` 和 Conch master
`a9a756d86e4e48305c4a19cc3a63bd53ae972659` 的现有接口。Conch dev
`a83e40fc3bf29792eeeafbd61585afad86426e36` 的 SDK 和 agent proto 与该 master 相同。
本变更仅修改 ADX，不修改外部仓库，不提供此驱动的 checkpoint/restore。

## 启动和配置

配置例子为 [node-jiuwenbox.json](../../build/config/examples/node-jiuwenbox.json)。
它是 `adxlet --config <path>` 接收的完整 JSON；也可以把字段放入部署 YAML
对应 adxlet service 的 `config`。统一部署的 runtime_profile 必须同步给 API Server。
`sandboxd_socket` 可以省略；保留它时，同一节点还可以提供原有 sandboxd runtime classes。
容量使用现有 Auto/File 源，同一宿主机容量只记一次。证书、地址、数据面允许的 CIDR
和目录须按实际部署填写。

Jiuwenbox 和 conchd 必须独立部署，Jiuwenbox 使用单 worker，且由该进程自己绑定 UDS，不能使用保留监听 socket 的进程热替换或 socket activation。Jiuwenbox 所在 Python 环境须安装匹配的
Conch SDK。该版本 SDK 固定连接 `/var/run/conch/conchd.sock`，不读取
`CONCH_SDK_CONFIG`；conchd 的 work_dir 及 ADX 的 conch_socket 必须对应这个 socket。
Jiuwenbox 的 idle_timeout 必须为 null。
ADX 通过现有 `/api/v1/timeout` 验证 idle reaper 已关闭，不修改其设置。
可选 `token_file` 包含 Jiuwenbox Bearer token，仅在启动时读取。

同一节点的 Jiuwenbox/conchd 实例及其 `adx-*` 资源必须由本 ADXlet 独占管理，不能让
其他节点或另一个 ADXlet 共用这些后端。非 ADX 前缀实例不参与孤儿回收。
节点上安装 mount、iproute2，使用支持 `cgroup.kill` 的 cgroup v2；Jiuwenbox/Conch
自身所需的防火墙工具由其部署提供。ADXlet 需要只读 EROFS 挂载、读取和清理 supervisor
cgroup/netns/veth 的权限；其宿主挂载、网络和 PID/cgroup 视图须与
Jiuwenbox/conchd 一致。容器部署还需要正确的挂载传播，不能仅在 adxlet 私有命名空间
看到 bootstrap。Conch guest 必须包含现有 virtiofs volume 支持。

bootstrap.image 是独立 Execd EROFS，sha256 必须替换为安装产物的真实摘要，不能使用
示例中的零值。启动入口必须位于 bootstrap.target 下。ADX 检查内容摘要和 EROFS magic，
只读挂载后再次确认 loop backing image；再转换为 supervisor bind 或 Conch volume。
同一驱动按摘要首次使用时校验并挂载，后续创建直接复用；环境归零及驱动退出后仍保留
EROFS 挂载，重启复用时重新校验。旧制品挂载的维护回收由运维在确认不再使用后执行。

classes 的键只允许 supervisor/conch。每个 classes.images 显式绑定公开 Environment
image 名称；它不会自动拉取 OCI image。supervisor 的程序和依赖由宿主及 Jiuwenbox
原有 policy 提供，conch 的程序和依赖来自绑定的模板。Conch image 必须且只能配置
template_name 或 template_id 之一；模板名须由部署约束为不可变版本，内容 ID 可直接固定
模板。ADX 只向 Jiuwenbox 发送选中的字段；只接受 template_id 的交付包应配置内容 ID。
process 定义继承镜像启动时的 argv/cwd/user，ADX 将生成 Execd
既有 ProcessSpec 配置并独立只读挂载。业务镜像无需预装 Execd。入口配置通过只读 `/__adx_launch` 挂载；Conch 的 `/run`、`/proc`、`/sys`、`/dev` 及其子目录不接受 volume 目标。

class.policy 是可信节点配置，使用现有 append policy 模式合并到 Jiuwenbox 基础 policy。
ADX 接管 cgroup、网络、bootstrap、声明的工作目录挂载和执行身份。
Jiuwenbox 基础 policy 的网络默认应允许所需数据面和控制流量，bind 目标不得与 ADX 保留路径冲突；基础 policy 中保留的限制仍会生效。
示例 supervisor 以 root 运行 Execd，由 Execd 按 ProcessSpec 切换业务用户；实际配置
必须保证目标用户、UID/GID、目录权限及原有隔离策略相容。

Conch CPU limit 要求整 vCPU，memory limit 要求整 MiB，不能静默舍入。设备分配、
磁盘配额、只读业务 rootfs、extra_config、非 Local bind 以及 DNS/IPv6 网络策略会在创建前拒绝。
网络策略通过 Jiuwenbox 创建参数和 `PUT /api/v1/policies/{sandbox_id}` 更新，
ADXlet 不另建宿主过滤链。创建与更新使用相同的保守映射，无法保持语义的配置返回 Invalid。

- supervisor 支持 stateful 入站 TCP 目的端口允许/拒绝列表；入站默认拒绝时显式放行
  Execd、声明端口和启用反向隧道时的 WebSocket 监听端口（`tunnel_http_port - 1`）。
  隧道监听端口只增加入站例外；其值由执行配置派生，不采用用户环境变量覆盖。
  出站默认允许时支持 IPv4/CIDR 或 TCP 目的端口拒绝列表。
- Conch 支持 stateful 出站 IPv4/CIDR 允许或拒绝列表；入站须默认允许且无额外规则，
  因为其原生 IP ACL 无法单独表达平台控制端口例外。总地址数不超过 1024。
- stateless、DNS/IPv6、UDP/ICMP 专属规则、端口范围、源端口、IP 与端口组合、
  依赖 allow/deny 优先级的规则均拒绝。supervisor 出站默认拒绝自带 DNS 例外，
  不能忠实表达 ADX 语义，因此拒绝；启用 runtime-control stream 时，受限出站策略也拒绝，
  避免丢失其控制连接例外。端口声明只影响访问路径和允许规则，不分配宿主端口。

## 生命周期和故障边界

`state_dir` 放置进程文件锁、Execd 入口配置和共享 EROFS 挂载目录，不保存额外的
所有权数据库或恢复日志。文件锁防止同一目录由两个 ADXlet 同时使用；挂载及入口文件
在使用期间不可清空。内存记录只协调当前进程的重复请求、创建与删除，并不作为恢复权威。
后端 ID 是完整 runtime ID 的固定摘要（`adx-` 加 28 位 SHA256 十六进制，总长 32）。
同一进程内相同 runtime ID 的规格变化或删除后重用返回冲突；跨进程的有效执行以
Coordinator 完整目录为准，不能从本地文件或后端 list 推断控制面归属。

调用方 deadline 不取消已发送请求。驱动独立等待响应，成功晚到时只清理、不发布为成功、
不启动业务；同进程删除仍等待已接受的创建任务。响应丢失时一次 404/空列表只证明当前
未观察到资源，后续新出现的资源仍须被持续对账发现并删除，不要求重启后端才能收敛。

ADXlet 公共层在启动及每个节点上报周期重新获取并校验完整权威目录，统一覆盖所有后端。
在关闭准入、排空已接受控制器操作后，把目录中的已发布 Running 执行与 Jiuwenbox
注册表关联，重建内存身份映射。
先完整读取 Jiuwenbox、Conch 原生列表和 supervisor cgroup/netns，再清理没有有效归属的
ADX 资源；删除失败或创建仍在进行时本轮对账失败，下轮重试。权威目录获取失败不能当空目录；
首次有效对账前不删除未知实例。周期触发、长对账心跳和成功后的 ready 发布由公共层负责，
驱动仅承担各自的资源发现、身份关联和清理。完整扫描失败或 Conch 列表达到 5000 条上限时也不作空列表处理。

Linux 通过 socket、SO_PEERCRED PID、进程 start time 和 boot ID 识别后端代次。
ADXlet 重启且 Jiuwenbox 注册表仍在时，可根据权威目录恢复现有已发布执行。
Jiuwenbox 重启后不热接管其丢失的后台 job：Conch 从原生 list/get/delete 清理后冷重建；
supervisor 从 cgroup/netns 发现孤儿，通过所属 cgroup.kill 终止进程，再清理 netns/veth/cgroup。
正常创建、Execd 启动和策略更新仍经 Jiuwenbox，原生 Conch API 仅用于资源发现及清理。

存活判断优先使用 Execd 自身控制状态并验证完整执行身份，健康检查断连不是退出。
驱动不采信 ConchRuntime 推断的 0/-1 退出码。Execd 不可达时，cgroup 非空或 Conch 原生 running 列表确认后端存活，继续由控制器 readiness 失败阈值处理；确定不存在才报告退出，后端也无法确认时返回观测失败。配置示例将 Execd 连续失败阈值设为 3。
原有 Execd readiness、业务探针及路由启用流程仍由 ADX 生命周期控制器负责。

启动对账确认非 failover Environment 的原 Running 执行不存在时，在物理清理成功后继续应用显式配置的
RestartPolicy；保持 Environment ID/assignment，使用新 runtime ID，沿用已消耗的尝试次数。
未配置重启、预算耗尽、存量无法确认或清理失败时不会据此创建新执行。
这是节点本地冷重建，不依赖快照；卷文件是否保留取决于原卷及宿主存储的持久性。

## 验证

`platform/adxlet/tests/jiuwenbox_runtime.rs` 使用真实 HTTP/UDS，替换特权 host 边界，
覆盖挂载请求、身份注入、重复创建、deadline 晚到、删除失败、后端重启清理、未知状态
和删除屏障。它不启动 bubblewrap/VM，也不证明 virtiofs、EROFS 或内核网络运行成功。

```sh
cargo test -p adxlet --test jiuwenbox_policy --test jiuwenbox_runtime --test jiuwenbox_network -j 2
cargo test -p adxlet -j 2
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
python3 build/docs/check.py
```

完整部署、公开 SDK 和故障恢复的复现入口见
[验收工具](../../build/e2e/jiuwenbox/README.md)。预编译依赖的版本和 SHA256 见
[产物锁文件](../../build/e2e/jiuwenbox/prebuilt-lock.json)。

本驱动暂未上报后端实际资源用量和精确作业退出码；节点容量仍由 Auto/File 采样，
业务故障由 Execd 探针和现有恢复策略处理。

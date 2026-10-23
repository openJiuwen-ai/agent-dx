# Firecracker checkpoint 验收

JuiceFS / AFS 的从零准备、双执行节点命令、用例和性能对比见[部署与验证手册](../../../docs/testing/shared-filesystem-runbook.md)。

这组脚本由独立 Linux VM 或 Kubernetes Pod 内的进程部署共用。运行完整 ADX 发布包、外部 sandboxd/Firecracker、真实 S3 服务与公共 Environment SDK。不会用模拟后端替代 checkpoint。运行前要求 `/dev/kvm` 支持 API 12；Linux 内核、EROFS、网络及 FC 制品还需符合所选 sandboxd 版本。

## 运行布局

默认基目录 `/opt/adx`，可设置 `ADX_FC_BASE`。目录需包含：

- `package/`：经过 `build/release/package.py verify` 验证的统一发布包。
- `client/bin/python`：已安装该包 Environment SDK 的 Python 环境。
- `e2e/`：本目录所属的 `build/e2e`，以及 `rpc_certificates.py`、发布包验证器 `package.py` 和同一发布包的 `lib/package_manifest.py`（复制到 `e2e/package_manifest.py`）。
- `tools/minio`、`tools/redis-cli`、`tools/distill_fs`；本地 OCI tar 模式另需 `tools/docker-registry`、`execd.tar` 和用于入口继承验收的 `entrypoint.tar`。

外部执行后端在 `/opt/adx-fc/bin/{sandboxd,sbox,checkpoint-restore,firecracker}`，内核和 initrd 在 `/opt/adx-fc/artifacts/`。virtiofsd 与 `distill_fs` 在基目录 `tools/`；后者是 S3/OCI rootfs 和 EROFS mount 的实际读取后端，缺失时验收必须在部署前失败。测试使用新建的显式运行目录，固定监听端口因此每个 Pod/VM 同时运行一套；不在已有业务节点直接执行。

```sh
sudo env ADX_FC_BASE=/opt/adx ADX_FC_PROXY_MODE=embedded \
  python3 -u /opt/adx/e2e/firecracker/node.py /var/lib/adx-fc-test/run
```

`ADX_FC_PROXY_MODE` 支持 `embedded` 或 `standalone`。`ADX_E2E_EXECD_IMAGE` 和 `ADX_E2E_ENTRYPOINT_IMAGE` 可指定已发布的 EXECD／入口测试镜像；省略时分别从 `execd.tar` 和 `entrypoint.tar` 启动本机测试仓库。S3、Redis和API测试凭证每次随机生成，配置及密钥不进入公开证据。`collect.py` 只导出证据与组件日志并替换已知测试密钥。

## Execd 主动控制流回归

对支持主动控制流的当前 Execd 镜像，可设置 `ADX_E2E_RUNTIME_CONTROL=1`。驱动为 adxlet 配置网桥网关 `10.231.16.1:19003` 上的 gRPC 监听及节点私有凭据文件；FC guest 默认回连 `http://10.231.16.1:19003`，可通过 `ADX_E2E_RUNTIME_CONTROL_ADDRESS` 与 `ADX_E2E_RUNTIME_CONTROL_LISTEN` 分别指定回连和监听地址。使用同一版本的 ADX 发布包、静态 Execd 与 SDK，不能用旧镜像验证该模式。

另外在基目录准备静态 `checkpoint-probe`。它由本目录的 `checkpoint_probe.rs` 为 guest 架构编译，是通过 SDK 上传到运行环境内的 Unix HTTP 客户端，实际调用 `/run/adx/execd.sock`，不是模拟 checkpoint 后端。ARM64 musl 构建示例：

```sh
rustc --edition=2024 --target aarch64-unknown-linux-musl \
  -C linker=musl-gcc -O checkpoint_probe.rs -o /opt/adx/checkpoint-probe
sudo env ADX_FC_BASE=/opt/adx ADX_E2E_RUNTIME_CONTROL=1 \
  python3 -u /opt/adx/e2e/firecracker/node.py /var/lib/adx-fc-test/control-stream
```

启用后，驱动先执行 `sdk_control_stream.py` 的七项公共 SDK 检查：全网段拒绝下主动 Ready 创建；普通出网阻断而 SDK 命令／文件可用；stateful 全封禁下运行环境内 Unix checkpoint 与持久化完成；切换 stateless 全封禁后再次 checkpoint；reload 的执行身份切换、数据回滚与出网仍阻断；暂停后仅重启 adxlet 并恢复、出网仍阻断；删除后的 Redis 和物理后端清理。策略包含用户最高优先级的双向 Any／`0.0.0.0/0` Deny 和双向默认 Deny。通过临时仅允许探测 peer 的地址与端口确认目标实际可达，恢复全封禁后再次验证。结果写到 `evidence/sdk-control-stream.json`，任一项失败会阻止后续全量 FC 集合。

本地两节点 Docker 驱动同样接受 `ADX_E2E_RUNTIME_CONTROL=1`，将监听地址设为每个容器的可达节点地址。基础功能仍由原 Standalone 用例集合验证。

## Kubernetes 与 Buildkite

`kubernetes.py` 创建带唯一名称/标签的 namespace，部署一个选定 KVM worker 上的特权 Pod，执行与本地相同的26项SDK/生命周期用例。除原有 checkpoint、双克隆和故障场景外，当前集合还要求 S3 rootfs、S3 EROFS mount、独立执行 limit、镜像入口继承、reload、创建/运行期网络策略，以及有/无 checkpoint 的 failover。它保存部署命令、Pod/宿主信息、逐用例日志、结果JSON、JUnit、S3/Redis/实例清理证据。清理检查 namespace UID 与标签；不会删除替换后的 namespace。一个Pod不证明跨节点恢复。

镜像由 `build/e2e/prepare.py --firecracker-kit <dir>` 在当前 ADX 发布包上组合。外部 kit 必须包含 `kit.py` 定义的全部文件，`manifest.json` 包含 `schema_version: 1`、`target`、`sandboxd_revision`、按顺序与 `third_party/sandboxd/source.json` 完全一致的 `sandboxd_patches`，以及文件相对路径到SHA256的 `files` 映射。sandboxd、sbox 和 redis-cli 必须与同次 backend 制品摘要一致。`initrd_provenance` 必须绑定相同的 `source_revision`、相同顺序的 `patches` 和 `artifacts/initrd.img` 的SHA256；缺失或过期的绑定会在打包前失败，避免新的 sandboxd 与旧 guest initrd 混用。

最小 provenance 片段如下（补丁列表必须直接取自 `third_party/sandboxd/source.json`，不可省略或重排）：

```json
{
  "sandboxd_revision": "<source revision>",
  "sandboxd_patches": [
    {"path": "third_party/sandboxd/patches/trusted-guest-metadata.patch", "sha256": "<trusted guest metadata sha256>"},
    {"path": "third_party/sandboxd/patches/firecracker-realtime-restore.patch", "sha256": "<realtime restore sha256>"}
  ],
  "initrd_provenance": {
    "source_revision": "<source revision>",
    "patches": [
      {"path": "third_party/sandboxd/patches/trusted-guest-metadata.patch", "sha256": "<trusted guest metadata sha256>"},
      {"path": "third_party/sandboxd/patches/firecracker-realtime-restore.patch", "sha256": "<realtime restore sha256>"}
    ],
    "sha256": "<sha256 of artifacts/initrd.img>"
  }
}
```

内核、VMM、checkpoint-restore、virtiofsd、distill_fs、MinIO 与测试用 OCI registry 均由kit供应流程准备，此仓库不会在运行节点临时编译或下载浮动版本。

启用独立 `platform-fc-e2e` 步骤需要：

- `ADX_E2E_CHECKPOINT=1`。
- `ADX_FC_KIT_ARTIFACT_BUILD`：包含 `out/buildkite/firecracker-kit/**/*` 的制品构建UUID；或打包worker内可读的 `ADX_FC_KIT_DIR`。
- `ADX_FC_K8S_NODE`：目标worker的 `kubernetes.io/hostname` 标签值。它必须支持KVM并符合kit架构。
- 原有目标 kubeconfig、registry访问和镜像发布配置。

未选择该profile时只有既有基础K8s步骤，不能据此宣称checkpoint验收通过。选中profile后缺kit、KVM或任何必需用例都会失败，不跳过为绿色。当前代码已提供驱动及步骤接线；真实目标K8s的kit供应与运行验收仍待完成。

## 当前验证记录

`out/ci/stage-7/drivers-final.log`：47项部署/制品/清理/汇总测试通过。`fc-r13.log`：入库驱动在Lima ARM64 KVM上对package-v10执行10项真实SDK用例全部通过，包含随机S3凭证与最终配置取证。上述记录不代表目标Kubernetes已经运行。

最新统一package-v11的Lima r14也已通过10项，额外通过公共SDK `node_id=node1` 指定节点。产物为本地未提交工作树构建（manifest的dirty为true），不是已发布或Buildkite验证过的版本。

新增快照场景已由 package-v12 / Lima r15 验证，严格验收器确认13项全部通过，并增加 `snapshots-final.json` 的Deleted/无引用检查。证据位于 `out/ci/pause-resume/fc-r15/evidence/`；当时独立Buildkite步骤使用13项必需用例集合，仍待目标K8s实际执行。

package-v13 / Lima r16再次通过13项；本轮同时修复SDK在 `verify_tls=True` 时向 `wss://` 传入 `ssl=None` 的问题。真实TLS Socket认证订阅回归已加入本地 `interop` 套件，r16全量日志不再出现该连接错误或command-watch不可用回退。


当前驱动增加三项克隆场景，必需集合共16项：公共 SDK 从同一快照创建两个新 Environment，不传镜像/runtime/资源以验证继承，检查 PID/内存计数及可写文件隔离；删除源快照后等待 Redis 目录进入 Deleted 且无引用，再分别暂停、恢复和删除克隆。`sdk/snapshot-collected-before-clone-resume.json` 记录回收顺序，严格验收器要求该证据存在。生命周期操作全部经过公共 SDK；只读 Redis 查询用作清理时序的测试观测。

package-v15/Lima r18 已通过16项及全部最终清理，严格时序证据已验证；该记录不表示目标Kubernetes已执行。


当前驱动验证 Coordinator 的共享回收：注入已无 Environment 引用的捕获清单及真实 S3 半成品，等待 Coordinator 删除，并确认已登记恢复点、未登记对象保持。节点不运行共享删除扫描。克隆使用 Snapshot 时删除返回 `SNAPSHOT_IN_USE`；克隆暂停取得自己的恢复点后才允许回收 Snapshot，然后从各自匿名恢复点恢复。此处描述驱动断言，最新版本实际端到端结果以本次运行证据为准。

测试 VM 的 MinIO 在停止前可能尚未完成其内部 `.minio.sys/tmp/.trash` 回收。S3 最终清单为空证明业务对象已删除，不代表后台垃圾已归还磁盘；反复运行前需要检查可用空间。只可在确认测试服务已停止、证据已保留后清理该次运行的内部垃圾目录，不能删除仍在服务的 MinIO 数据目录。

package-v16 / Lima r20 已通过全部17项和最终清理，证据 `out/ci/pause-resume/fc-r20/evidence/`；r19磁盘水位失败保留在独立日志中。目标Kubernetes仍待正式运行。


当前18项必需用例包含公共SDK设置实例标签、实例硬亲和OR、节点顺序偏好和加权实例反亲和的创建及执行。该用例在单节点上证明SDK→Frontend→Coordinator→Adxlet→真实Firecracker的接线；两个候选节点之间的评分选择、租户隔离和反向反亲和由Rust定向测试验证，不能由单节点FC用例代替。

当前代码把严格集合扩展为26项。Lima ARM64 r16 的SDK组19/19通过；r17、r18使用更新后的组件包，均先通过S3 rootfs、S3 EROFS mount、独立执行limit、入口继承、创建及运行期网络策略等前16项，再在双克隆写入阶段遇到Relay 504。生命周期隔离验收另行验证有／无checkpoint的failover和节点故障契约。上述早期拆分结果不代表同一次26/26严格验收。后续 2026-10-07 的 `firecracker-008` 已在主动控制流模式通过原19项checkpoint和7项节点生命周期，且最终制品及实例清理通过，见[控制流验证记录](../../../docs/testing/runtime-control-stream.md)。该本地结论不代表目标Kubernetes验收。

Lima ARM64 lifecycle r24 的7项故障用例全部通过：backend异常重启、有／无checkpoint的failover、Coordinator不可用时SQLite降级、Adxlet等待Coordinator、心跳过期后的旧会话隔离清理，以及资源观测过期门禁。外层清理确认6个实例全部Deleted且释放资源，runtime inventory、本地checkpoint、S3业务对象、测试进程和network namespace均无残留。证据位于`out/ci/sdk-capability-fc-20260920/fc-lifecycle-r24/`。

## 双节点归属转移

`transfer.py` 在专用KVM主机上使用两套网络命名空间与独立sandboxd验证共享checkpoint跨节点恢复。`ADX_TRANSFER_INTERRUPT_COORDINATOR=1` 注入恢复计划落盘后、执行调用前的Coordinator重启。package-v21/Lima r4六项通过，含同ID新代次、PID/内存/文件保留、旧节点清理与最终无残留，见 [运行说明及证据](../../../docs/testing/firecracker-cross-node.md)。此驱动尚未接入正式Kubernetes步骤。

`ADX_TRANSFER_INTERRUPT_NODE=1` 在backend已Running、Redis仍未提交时重启目标Adxlet，检查旧backend清理后同代次重新恢复。package-v21/Lima r5六项及中断证据全部通过，52项驱动回归通过。

## Coordinator 扩缩容运维验收

在已准备的专用 KVM 主机中使用统一发布包、独立安装的同版本 SDK 与当前
Execd 镜像，选择两套 sandboxd／网络命名空间 fixture 的定向集合：

```sh
sudo env ADX_FC_BASE=/opt/adx-autoscaling ADX_FC_SUITE=autoscaling \
  python3 -u /opt/adx-autoscaling/e2e/firecracker/transfer.py \
  /var/lib/adx-autoscaling/run-001
```

该集合为 Coordinator 配置无鉴权 `127.0.0.1:19192` 管理 HTTP 入口。测试实例使用
1 核／128 MiB，并将节点 RPC 预算设为 120 秒，以覆盖管理暂停等待和真实 VM
启动的累计时间。六项
必需证据覆盖节点／租户／实例过滤和分页、中央与 local-first 暂停后的真实
创建及队列恢复、已有运行实例继续执行、手动恢复和真实转发端口流量唤醒在
管理暂停期间保持 Paused、解除暂停后保留 guest PID 与文件，以及最终实例、
物理后端和对象存储清理。业务生命周期通过已安装的 SDK 调用；运维请求直接
到 Coordinator。`sdk-autoscaling.json` 保存逐项证据，`autoscaling_contract.py`
拒绝缺失用例、暂停期间物理 VM 已启动、缺少 local claim 或清理不完整的结果。

这是一个 KVM 主机内的双逻辑节点定向验收，不等同于默认 checkpoint 集合、
跨主机或 Kubernetes 验收。每次必须使用新的运行目录。


## 本地 checkpoint LRU 验收

对已准备的专用 KVM 主机、统一发布包、已安装的同版本 Environment SDK、静态 Execd 镜像及固定 sandboxd/FC kit，选择缓存定向集合：

```sh
sudo env ADX_FC_BASE=/opt/adx-checkpoint-cache ADX_FC_SUITE=cache \
  python3 -u /opt/adx-checkpoint-cache/e2e/firecracker/transfer.py \
  /var/lib/adx-checkpoint-cache/run-001
```

该集合沿用双网络／挂载命名空间的真实 Firecracker、MinIO/S3、Coordinator、Ingress、Relay 和公共 SDK 链路。首先使用生产默认缓存设置（500 条、50 GiB），随后仅重启节点并将测试阈值降低到 2 条和 1 字节，覆盖独立条目淘汰、满缓存回源、超大快照与多个执行引用共存、运行实例重启对账，以及降低条目上限后的 LRU 重建。暖恢复期间用节点命名空间的防火墙阻断 S3，以证明命中本机副本；业务进程 PID、计数器和二进制文件必须保持。生命周期操作走公共 SDK，Redis／文件系统只用于取证，配额修改和进程重启是显式测试故障注入。

`cache_contract.py` 严格要求七项完整证据及实例、本地快照、对象存储和两个物理后端清单归零。结果为 `evidence/sdk-cache.json`；外层 `evidence/result.json` 还要求测试进程和命名空间清理成功。这个集合不填满实际 50 GiB 磁盘，也不等同于 500 份快照的规模测试。

跨节点集合 `ADX_FC_SUITE=transfer` 继续验证共享快照在原节点失联后的恢复。原节点限额内的无执行引用副本可以保留到下次 LRU 或权威对账；该集合在全局删除后额外重启两个 adxlet，并检查本地文件全部回收，证据为 `cache-reconciliation-cleanup.json`。默认单节点 checkpoint 集合的 `restart_paused.py` 则在所有物理后端停止且权威状态为 Paused 时注入本机缓存丢失，继续验证从 S3 恢复。

### AFS/S3 双执行节点存储验证

`storage.py` 在专用 Linux KVM 主机运行，沿用 `transfer.py` 的两个隔离网络命名空间。
需要同一候选构建的发布包（`ADX_WITH_AFS=1`）、匹配锁定 revision 的 sandboxd、
Firecracker/virtiofsd/kernel，以及含 Python HTTP 服务的工作负载镜像。
存储套件默认设置 `writable_io_engine = "Sync"`，可用
`ADX_STORAGE_IO_ENGINE=Async` 对齐文档中的异步磁盘基线；对比后端必须使用相同值。
`writable_cache_type = "Writeback"` 保持一致；运行时默认
`AsyncDirect` 在该 AFS 挂载上会因缺少 direct I/O 对齐信息而拒绝创建磁盘。

```bash
ADX_FC_BASE=/opt/adx-storage ADX_FC_SOURCE=/source \
ADX_FC_AFS_ROOT=/source/out/afs-fc/afs-services-RUN \
  /opt/adx/client/bin/python -u build/e2e/firecracker/storage.py /var/lib/adx-storage-afs-RUN
```

AFS 使用一个 local-file Meta 和三个独立 DFS 客户端，分别供两个执行节点及 Coordinator
访问同一 namespace；配置为一份必需副本。AFS 服务与执行节点进程独立，执行节点故障
注入不会同时杀死存储服务。此拓扑不证明跨物理主机故障域或 AFS 存储高可用。
运行目录与 AFS 根目录必须全新，驱动保留日志并停止自己的服务、卸载自己的挂载。

取消 `ADX_FC_AFS_ROOT` 可运行 S3＋本地缓存模式（默认独立 MinIO）；提供私有
`ADX_FC_OBS_CONFIG` 可改为真实 OBS。对比必须保留具体服务版本、端点类型和网络位置，
MinIO 结果不能标为 OBS 性能。

每种后端默认分别运行三次 cold 与三次 warm：cold 从独立客户端/无该制品缓存的节点
恢复，warm 在刚完成捕获的原节点恢复。它们分别是跨节点冷恢复与原节点热恢复，
不能据此隔离调度迁移本身的成本；不执行全机 drop_caches。记录暂停发布耗时、首个
真实 HTTP 响应、全部并发响应及延迟触页耗时，同时核对 guest PID、内存状态和文件。
每次发起 12 个请求，交替经过两个 Ingress，必须只产生一次物理恢复。

AFS 模式额外保存实际挂载来源、直接捕获路径、分阶段文件数据读流量及 VMM smaps。
这些观测用于判断运行时是否仍整份读取/复制，接口返回快不等于按需恢复已经通过。
故障验证持久使用保护跨 Coordinator 重启保留，Environment 删除后旧执行仍阻止回收；
原节点完成清理对账后才允许回收。Coordinator 删除系统调用保存在 `coordinator-gc.trace*`。
`fault-progress.json` 记录恢复期间的目录变化，健康目标节点不能被失效处理；真实 OBS 还要求
重启后排空旧恢复操作期间至少两次成功心跳。`recovery-runtime.trace*` 校验被替换 VMM 的退出早于下一次启动。
另将已停止写入的未完成捕获加入持久清单，确认大小为 0、未发布 manifest 的目录也能回收。
原始样本及汇总在 `evidence/sdk-storage.json`，最终完整生命周期裁决在 `evidence/result.json`。

#### 原生 XFS 与读预取对照

仅运行直接 sandboxd 对照时，设置 `ADX_STORAGE_NATIVE_XFS_ROOT` 为已经挂载的原生
XFS 挂载点，并取消 AFS/JuiceFS 配置。驱动通过 `findmnt` 核对文件系统，执行相同的
Full／Incremental 捕获、恢复、真实 HTTP、512 MiB 全量 SHA 和进程连续性校验。
它跳过 ADX SDK 与流量唤醒，结果标记 `native_control_only`；同机可见的 XFS 目录
不能作为生产共享存储后端或跨物理主机恢复证明。

```bash
sudo env ADX_FC_BASE=/opt/adx-storage ADX_FC_SOURCE=/source \
  ADX_STORAGE_NATIVE_XFS_ROOT=/mnt/dedicated-native-xfs \
  ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_DIRECT_HTTP=1 \
  ADX_STORAGE_PROBE=/opt/adx-fc/bin/checkpoint-probe-http \
  python3 -u build/e2e/firecracker/storage.py /var/lib/adx-xfs-control-RUN
```

JuiceFS 私有 fixture 配置可选 `max_readahead_mib`（整数 0–32），映射到客户端
`--max-readahead`。未设置时保留所测版本的默认值；配置与挂载身份均记录实际参数。
预读对照须同时报告首次响应、完整触页和 GET 数据量，避免将少读数据造成的延迟转移
误判为整体加速。当前 cache-size=0 基线下，固定版本已自动禁用块 prefetch。
对比脚本会核对各轮导出的块尺寸、预读与缓存参数，拒绝将同一后端的不同设置混入
一个中位数。
CPU／等待诊断及其测量边界见 [diagnostics/README.md](diagnostics/README.md)。

### 原生盘与完整内存基线探针

`ADX_STORAGE_IO_ENGINE=Async ADX_STORAGE_BENCHMARK=1` 运行存储计时集合，
覆盖冷、热两组及双 Ingress 的 12 路并发唤醒；它不声明完成上述六项生命周期验证。
工作负载为 1 vCPU / 1024 MiB VM，包含 512 MiB 确定性内存；首次 HTTP 不触发
checksum，随后读取全部 512 MiB 并核对宿主机独立计算的 SHA-256，校验耗时单独记录。
ADX 的 `first_http_seconds` 为 12 个请求中最短的请求耗时；`remote_read_bytes.first`、
`first_finished_unix_ns` 和首次 VMM 映射在整组 12 个响应完成后采集，不能作为精确的首个响应边界。
`ADX_STORAGE_REPETITIONS=1` 可保留单次 fresh-client 试验，不能据单次样本计算 P95。

额外设置 `ADX_STORAGE_PROBE=/absolute/path/checkpoint-probe` 时，ADX 创建
Environment 前先运行 `direct_storage.py`。该工具由同一个固定 sandboxd revision 的
`test/e2e/checkpoint-restore` 构建，直接调用 sandboxd；使用同一 guest 镜像、
512 MiB payload、VM 资源和 I/O 配置，验证全量捕获、跨节点恢复、修改 64 MiB、
增量捕获及再次跨节点恢复，并校验完整内存。默认探针通过 `sbox exec` 访问内部 HTTP。
设置 `ADX_STORAGE_DIRECT_HTTP=1` 并提供支持 `-response-file` 的诊断 helper 时，
探针取得实际 SandboxIP，恢复后直接访问 guest HTTP；初始化校验仍使用 `sbox exec`。
证据记录探针类型及 helper SHA-256，两种探针的首次响应耗时分别报告。
证据为 `direct-storage.json`，底层每阶段耗时来自 sandboxd 日志。
直接探针以 Python HTTP 服务作为初始进程，ADX 路径由 Execd 启动同一服务；
两条路径的 guest 启动状态不同，不能用首响应总耗时相减计算 ADX 控制面开销。
全量到增量的直接探针还需要 `ADX_STORAGE_CHECKPOINT_MODE=incremental`；
默认 `full` 模式会拒绝显式增量捕获。该参数在工作负载身份中记录，对照后端必须一致。

原生盘由调用方预先准备，记录 `findmnt`、块设备序列号及运行目录与 OSD 的映射。
驱动不会自动格式化设备；loop/file-backed OSD 和 tmpfs 运行目录不属于原生盘基线。
JuiceFS 的 fresh-client 样本要求每次启动新的客户端；跨节点无该制品缓存与重挂载
客户端是两种不同条件，应分别记录。存储服务端缓存不主动清空。

`diagnostics/rgw_observer.go` 为独立诊断轮提供流式 S3 转发，记录 GET 路径、Range、
Content-Range、实际响应字节及完成状态，不记录 Authorization。
`object_read_profile.py` 按对象计算唯一读取范围与重复读取字节；COW 共享块保留
多个文件归属。尚无文件关联的读量单独标记，不能全部算为 memory 文件读放大。
带 `ADX_STORAGE_FILESYSTEM_TRACE=1` 的诊断轮不参与性能对比。

多个完整成功轮次可使用 `compare_storage.py AFS_EVIDENCE S3_EVIDENCE --output comparison.json`
汇总；该工具拒绝不同发布包、外部运行时或 guest 工作负载混用。
每轮独立重启客户端且 `ADX_STORAGE_REPETITIONS=1` 时，可传入各轮的 `evidence/`；
同一后端会合并冷、热样本，每组仍须至少三次。带直接 sandboxd 探针的轮次还要匹配
探针类型与 helper SHA-256，并通过 Full／Incremental 的完整内存校验。
直接 RPC、直接 HTTP、ADX 首个 HTTP 分别汇总；诊断轮不能混入性能结果。

使用 `ADX_STORAGE_REPETITIONS=1 ADX_STORAGE_TRACE_RESTORE=1` 可运行独立 AFS 诊断，
跟踪目标 sandboxd 及其 Firecracker 子进程对快照 `memory` 的读取和复制，
在首轮 HTTP 完成、显式触页之前停止跟踪。诊断轮仅将目标 AFS 节点日志提高到 INFO，
保存 `dfs.cow_range_committed` 的固定源版本、目标 inode 和范围；性能轮保留 WARN。
`restore-trace-summary.json` 同时记录直接读取字节数、逻辑复制字节数、COW 提交事件和
随后触页的远端读增量。只有元数据 COW 被确认、没有完整数据复制、延迟触页产生新增
远端读取时诊断才通过；不能仅按 `copy_file_range` 的逻辑返回大小推断实际复制。
此轮不参与性能比较。

独立文件系统验证使用 `build/e2e/afs/scripts/dfs/cow.py --binary-dir PACKAGE/bin --root NEW_ROOT`，
覆盖普通文件、非零偏移覆盖和超过 4 GiB 的稀疏分段复制，复制期间源数据读流量必须为 0；
同时验证修改隔离、源删除后副本可读、打开写文件闲置 35 秒后仍可写入和同步，以及正常卸载/退出。

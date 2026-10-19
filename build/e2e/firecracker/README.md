# Firecracker checkpoint 验收

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


当前必需集合增加远端残留回收验证，共17项。暂停成功后，从真实制品读取上传归属标记，注入同会话的半成品文件，并验证当前会话期间保留；杀死 Adxlet 后，等待新会话完成 Redis 权威对账，确认旧会话残留被删除、已登记暂停点仍可恢复、其他节点及无标记对象仍保留。注入的是存储残留，实际 S3 删除、节点重启、checkpoint 恢复均由生产组件执行。测试专用 `checkpoint_gc` 使用零保留期和一秒周期，生产默认仍为24小时和5分钟。逐项证据为 `orphan-gc.json`。

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

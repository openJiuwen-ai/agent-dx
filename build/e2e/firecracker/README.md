# Firecracker checkpoint 验收

这组脚本由独立 Linux VM 或 Kubernetes Pod 内的进程部署共用。运行完整 ADX 发布包、外部 sandboxd/Firecracker、真实 S3 服务与公共 Sandbox SDK。不会用模拟后端替代 checkpoint。运行前要求 `/dev/kvm` 支持 API 12；Linux 内核、EROFS、网络及 FC 制品还需符合所选 sandboxd 版本。

## 运行布局

默认基目录 `/opt/adx`，可设置 `ADX_FC_BASE`。目录需包含：

- `package/`：经过 `build/release/package.py verify` 验证的统一发布包。
- `client/bin/python`：已安装该包 Sandbox SDK 的 Python 环境。
- `e2e/`：本目录所属的 `build/e2e`，以及 `rpc_certificates.py`、发布包验证器 `package.py`。
- `tools/minio`、`tools/redis-cli`、`tools/distill_fs`；本地 OCI tar 模式另需 `tools/docker-registry`、`rrt.tar` 和用于入口继承验收的 `entrypoint.tar`。

外部执行后端在 `/opt/adx-fc/bin/{sandboxd,sbox,checkpoint-restore,firecracker}`，内核和 initrd 在 `/opt/adx-fc/artifacts/`。virtiofsd 与 `distill_fs` 在基目录 `tools/`；后者是 S3/OCI rootfs 和 EROFS mount 的实际读取后端，缺失时验收必须在部署前失败。测试使用新建的显式运行目录，固定监听端口因此每个 Pod/VM 同时运行一套；不在已有业务节点直接执行。

```sh
sudo env ADX_FC_BASE=/opt/adx ADX_FC_PROXY_MODE=embedded \
  python3 -u /opt/adx/e2e/firecracker/node.py /var/lib/adx-fc-test/run
```

`ADX_FC_PROXY_MODE` 支持 `embedded` 或 `standalone`。`ADX_E2E_RRT_IMAGE` 和 `ADX_E2E_ENTRYPOINT_IMAGE` 可指定已发布的 RRT／入口测试镜像；省略时分别从 `rrt.tar` 和 `entrypoint.tar` 启动本机测试仓库。S3、Redis和API测试凭证每次随机生成，配置及密钥不进入公开证据。`collect.py` 只导出证据与组件日志并替换已知测试密钥。

## Kubernetes 与 Buildkite

`kubernetes.py` 创建带唯一名称/标签的 namespace，部署一个选定 KVM worker 上的特权 Pod，执行与本地相同的26项SDK/生命周期用例。除原有 checkpoint、双克隆和故障场景外，当前集合还要求 S3 rootfs、S3 EROFS mount、独立执行 limit、镜像入口继承、reload、创建/运行期网络策略，以及有/无 checkpoint 的 failover。它保存部署命令、Pod/宿主信息、逐用例日志、结果JSON、JUnit、S3/Redis/实例清理证据。清理检查 namespace UID 与标签；不会删除替换后的 namespace。一个Pod不证明跨节点恢复。

镜像由 `build/e2e/prepare.py --firecracker-kit <dir>` 在当前 ADX 发布包上组合。外部 kit 必须包含 `kit.py` 定义的全部文件，`manifest.json` 包含 `schema_version: 1`、`target`、`sandboxd_revision` 和文件相对路径到SHA256的 `files` 映射。sandboxd、sbox 和 redis-cli 必须与同次 backend 制品摘要一致；Firecracker guest `initrd.img` 由未修改的固定 sandboxd revision 构建并随 kit 校验。内核、VMM、checkpoint-restore、virtiofsd、distill_fs、MinIO 与测试用 OCI registry 均由kit供应流程准备，此仓库不会在运行节点临时编译或下载浮动版本。

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


当前驱动增加三项克隆场景，必需集合共16项：公共 SDK 从同一快照创建两个新 Capsule，不传镜像/runtime/资源以验证继承，检查 PID/内存计数及可写文件隔离；删除源快照后等待 Redis 目录进入 Deleted 且无引用，再分别暂停、恢复和删除克隆。`sdk/snapshot-collected-before-clone-resume.json` 记录回收顺序，严格验收器要求该证据存在。生命周期操作全部经过公共 SDK；只读 Redis 查询用作清理时序的测试观测。

package-v15/Lima r18 已通过16项及全部最终清理，严格时序证据已验证；该记录不表示目标Kubernetes已执行。


当前必需集合增加远端残留回收验证，共17项。暂停成功后，从真实制品读取上传归属标记，注入同会话的半成品文件，并验证当前会话期间保留；杀死 Node Manager 后，等待新会话完成 Redis 权威对账，确认旧会话残留被删除、已登记暂停点仍可恢复、其他节点及无标记对象仍保留。注入的是存储残留，实际 S3 删除、节点重启、checkpoint 恢复均由生产组件执行。测试专用 `checkpoint_gc` 使用零保留期和一秒周期，生产默认仍为24小时和5分钟。逐项证据为 `orphan-gc.json`。

测试 VM 的 MinIO 在停止前可能尚未完成其内部 `.minio.sys/tmp/.trash` 回收。S3 最终清单为空证明业务对象已删除，不代表后台垃圾已归还磁盘；反复运行前需要检查可用空间。只可在确认测试服务已停止、证据已保留后清理该次运行的内部垃圾目录，不能删除仍在服务的 MinIO 数据目录。

package-v16 / Lima r20 已通过全部17项和最终清理，证据 `out/ci/pause-resume/fc-r20/evidence/`；r19磁盘水位失败保留在独立日志中。目标Kubernetes仍待正式运行。


当前18项必需用例包含公共SDK设置实例标签、实例硬亲和OR、节点顺序偏好和加权实例反亲和的创建及执行。该用例在单节点上证明SDK→Frontend→Master→Node Manager→真实Firecracker的接线；两个候选节点之间的评分选择、租户隔离和反向反亲和由Rust定向测试验证，不能由单节点FC用例代替。

当前代码把严格集合扩展为26项。Lima ARM64 r16 的SDK组19/19通过；r17、r18使用更新后的组件包，均先通过S3 rootfs、S3 EROFS mount、独立执行limit、入口继承、创建及运行期网络策略等前16项，再在双克隆写入阶段遇到Node Proxy 504。生命周期隔离验收另行验证有／无checkpoint的failover和节点故障契约。上述拆分结果证明新增能力实际经过sandboxd/Firecracker，但不能替代同一次26/26严格验收；完整门禁仍需修复ARM FC双克隆网络问题后重跑并生成新的JUnit和全量清理证据。

Lima ARM64 lifecycle r24 的7项故障用例全部通过：backend异常重启、有／无checkpoint的failover、Master不可用时SQLite降级、Node Manager等待Master、心跳过期后的旧会话隔离清理，以及资源观测过期门禁。外层清理确认6个实例全部Deleted且释放资源，runtime inventory、本地checkpoint、S3业务对象、测试进程和network namespace均无残留。证据位于`out/ci/sdk-capability-fc-20260920/fc-lifecycle-r24/`。

## 双节点归属转移

`transfer.py` 在专用KVM主机上使用两套网络命名空间与独立sandboxd验证共享checkpoint跨节点恢复。`ADX_TRANSFER_INTERRUPT_MASTER=1` 注入恢复计划落盘后、执行调用前的Master重启。package-v21/Lima r4六项通过，含同ID新代次、PID/内存/文件保留、旧节点清理与最终无残留，见 [运行说明及证据](../../../docs/testing/firecracker-cross-node.md)。此驱动尚未接入正式Kubernetes步骤。

`ADX_TRANSFER_INTERRUPT_NODE=1` 在backend已Running、Redis仍未提交时重启目标Node Manager，检查旧backend清理后同代次重新恢复。package-v21/Lima r5六项及中断证据全部通过，52项驱动回归通过。

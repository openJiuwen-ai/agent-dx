# 本地双节点 Firecracker 恢复验收

本页保留既有驱动与历史证据。当前通用跨节点准入要求捕获时的可验证兼容信息，现有采集仅开放默认 rootfs 的 runsc；Firecracker 尚未接入该证据采集，当前版本会安全拒绝其跨节点恢复，不能沿用下文历史通过结论作为当前准出。详见[当前配置边界](../../build/config/examples/README.md)。

`build/e2e/firecracker/transfer.py` 在专用 Linux KVM 主机上启动两套独立的 sandboxd、adxlet及内嵌Relay。两节点使用独立网络命名空间、不同IP池、Unix Socket、cgroup根目录和运行目录；sandboxd具有私有挂载视图，对端运行目录被空目录遮蔽，不能直接读取源节点文件；共享Coordinator、Redis、MinIO与SDK入口。

这可以验证真实执行后端的跨节点归属转移、checkpoint搬运和路由切换，但不是独立物理机/VM故障域，也不能代表正式Kubernetes验收。源节点故障通过暂停其adxlet进程注入，sandboxd及旧实例保持实际存在，随后恢复adxlet以检查旧执行清理。

## 运行条件

沿用 [FC基础驱动](../../build/e2e/firecracker/README.md) 的包、SDK、工具和外部runtime布局。需要root、真实KVM_CREATE_VM/KVM_CREATE_VCPU能力、`ip`、`nsenter`、至少8GiB可用磁盘。主机必须专用于此测试；固定端口和 `10.240.0.0/24` 网络不支持并发运行。

```sh
sudo env ADX_FC_BASE=/opt/adx ADX_TRANSFER_INTERRUPT_COORDINATOR=1 \
  python3 -u /opt/adx/e2e/firecracker/transfer.py /var/lib/adx-transfer-new-run
```

运行目录必须不存在。驱动验证统一发布包，记录包manifest、sandboxd/Firecracker/EXECD摘要、镜像digest与网络拓扑。adxlet使用 `nsenter --net` 切换网络，sandboxd再通过 `unshare --mount --propagation private` 隔离对端运行目录，两者均保留可用的宿主cgroup挂载；`ip netns exec` 重挂载sysfs会导致当前后端误判cgroup版本。

## 必需证据

- 公共SDK创建、写入二进制文件、启动内存计数进程、暂停并生成共享checkpoint，再恢复源实例。
- 源节点心跳失效后，同Environment ID由另一节点的新代次恢复；恢复计划进入完成状态。
- 恢复后的PID不变、内存计数继续增长、文件字节一致。
- 原adxlet返回后，旧backend被清理，目标backend保持运行。
- Coordinator重启后Redis epoch递增、节点完成对账；恢复后的执行代次和runtime ID保持不变。
- 公共SDK删除；Redis终态、两套backend清单、S3清理均核对。停机先清理节点，再停止Coordinator/Redis，最后移除自建网络。

`transfer_contract.py` 严格检查必需用例、最终清单和清理错误；缺少用例不会算通过。`collect.py` 导出组件日志和证据，并去除测试凭据。

## 当前记录

Lima r1未进入用例：sandboxd在 `ip netns exec` 环境下报告 `cgroup v1 controller "cpu" not found in /proc/self/cgroup`。驱动已调整为仅切换网络命名空间。首轮失败日志和清理结果保留于 `out/ci/stage-4/fc-transfer/r1/`，不能算恢复功能失败或通过。

设置 `ADX_TRANSFER_INTERRUPT_COORDINATOR=1` 后，用例仅阻断控制节点到目标adxlet的17001端口，保持目标节点心跳和资源采集正常；待Redis中出现新代次恢复计划后杀死Coordinator，等待epoch递增并确认计划不变，随后放通恢复RPC。该用例覆盖计划落盘后、执行调用前的Coordinator重启；不能替代backend恢复进行中或目标adxlet重启的证据。

驱动回归 `driver-regression.log`：51项通过。r2两节点READY但内部请求被继承的代理设置影响；补齐测试地址NO_PROXY后，r3公共SDK创建及共享checkpoint已通过。r3暂停sandboxd的注入方式同时阻塞了目标资源采集/心跳，等待恢复计划超时；已改为上述仅阻断恢复RPC的注入，失败证据保留。

## r4 真实运行结果

2026-09-16，package-v21、Lima ARM64 KVM，六项全部通过，另包含计划落盘后、恢复RPC执行前的Coordinator重启注入。

- Environment `default-environment-4f4175ba-327e-45aa-b84c-8223a2906ed2`：node1/generation 1 → node2/generation 2。
- 故障注入期间Coordinator epoch 1 → 2，原恢复计划与目标代次不变；成功恢复后再次重启Coordinator，runtime ID和代次保持不变。
- PID `11` 保留，计数器 `9 → 33`，二进制文件逐字节一致。
- 原节点返回后backend清单为空；目标保持一个运行实例，已登记S3制品仍可读。
- 最终两节点库存0、S3清单为空、`cleanup_errors: []`；额外检查无测试进程和网络命名空间残留。

证据：`out/ci/stage-4/fc-transfer/r4/evidence/result.json`、`mid-recovery-coordinator-restart.json`、`runtime-hashes.json`、`topology.json`、组件日志；完整过程在 `r4.log`。51项驱动测试通过。

当前包来自未提交工作树。sandboxd SHA256为 `f1df80fba119e31a5499daecdc40f42b4d0011118be4da72c13c0f1748676cfb`，与固定制品匹配；Firecracker SHA256为 `b0ac325d20f123c63f6cdea8e272db3506fecf8ce87c03e039cd12e060d1d0eb`；Execd SHA256为 `870338367442799b4a0c540e7824a4f36129909909569b15792fa93b74c24458`，与package-v21及本轮OCI制品一致。

## 目标 adxlet 恢复执行中重启

设置 `ADX_TRANSFER_INTERRUPT_NODE=1` 可验证未提交执行的清理契约。用例在目标网络命名空间中仅阻断到EXECD的50090端口，保留sandboxd、资源采集与心跳。待真实backend清单中出现Running执行，同时Redis仍为Paused、recovery.pending=true，再杀死目标adxlet并放通EXECD。

重启后必须满足：节点会话变化；原未提交backend ID消失；同Environment ID、同目标归属代次恢复为一个新的backend ID；PID/内存/文件仍从已登记checkpoint恢复。后续继续执行源节点返回清理、Coordinator重启与最终删除。`node-recovery-before-crash.json` 与 `node-recovery-restart.json` 保存故障点及替换证据。严格验收器会拒绝缺少故障证据、复用旧backend或变更归属代次的运行结果。

### r5 验收结果

2026-09-16，package-v21，`ADX_TRANSFER_INTERRUPT_NODE=1`，六项用例及目标节点中断断言全部通过；52项驱动回归通过。

- Environment `default-environment-a469af4b-ffc5-4a4d-94f6-85d77cc270b4` 从node1/代次1恢复到node2/代次2。
- 崩溃时Redis仍为Paused且pending；实际运行的未提交backend为 `sbox-21f5cda3-a0ff-4516-af39-c48851fa54aa`。
- 重启后节点session改变，原backend清理，新backend为 `sbox-5b22503f-4643-45ae-b9bb-5251130caad8`；归属仍为node2/代次2，最终仅一个执行。
- PID `11`、内存计数 `8 → 36`、二进制文件一致。源节点返回清理、恢复后Coordinator重启与显式删除均通过。
- 两节点库存和S3均为空，`cleanup_errors: []`；额外确认测试进程与网络命名空间无残留。

证据位于 `out/ci/stage-4/fc-transfer/r5/evidence/`，重点为 `result.json`、`node-recovery-before-crash.json`、`node-recovery-restart.json`；完整日志为 `r5.log`，驱动回归为 `node-driver-regression.log`。

r4与r5共同完成阶段4的本地故障恢复验收。此结果仍限定于同一KVM宿主上的隔离节点；正式Kubernetes门禁属于阶段7。

## 真实 OBS 与按流量唤醒

`ADX_FC_SUITE=demand` 选择独立的 `sdk_demand.py` / `demand_contract.py`。验收要求真实 OBS；MinIO 或只完成旧的六项 transfer 用例不能通过该门禁。

```sh
sudo env ADX_FC_BASE=/opt/adx ADX_FC_SUITE=demand \
  ADX_FC_OBS_CONFIG=/run/obs-private/config.json \
  python3 -u /opt/adx/e2e/firecracker/transfer.py /runs/demand-new-run
```

OBS 配置文件必须是权限 `0600` 的普通文件，包含 `bucket`、`region`、`endpoint`、专用 `prefix`、`access_key`、`secret_key`。endpoint 使用区域 HTTPS 地址，例如 `https://obs.cn-north-4.myhuaweicloud.com`。驱动使用虚拟主机寻址，将运行目录名称附加到 prefix，先验证该范围为空，再做真实 SigV4 PUT/GET/DELETE。凭据只写入私有配置与服务环境；导出记录仅含公共存储配置与请求 ID。

demand 严格集合为 12 项；创建和手动 pause 使用显式 RetentionPolicy，业务流量唤醒无需开关。容量回退完成归属转移后，Coordinator 要求存活的原节点重新对账；原节点保持同一 session，退役旧控制器，保留仍属本机的运行实例和目标引用的共享快照。Coordinator 的持久 wake ID 与 adxlet 的执行操作 ID 分层记录。

业务负载是 VM 内的 Python HTTP 服务，通过公共 Ingress 的声明端口接受带唯一 nonce 的请求。响应包含 guest PID、内存中的 nonce 计数和持久文件内容；恢复前后的响应必须对应同一 guest 进程状态，每个请求只执行一次。

初始创建使用 `schedule_affinities` 的软节点亲和（`kind=0, affinity=0`）优先选择 node1，并确认实际归属；这样原节点失效时仍允许 node2 接管。SDK 的 `node_id` 是硬放置约束，不适用于该跨节点故障场景。

| 验证点 | 真实触发与判定 |
| --- | --- |
| 活跃请求与空闲检测 | HTTP 响应延迟超过 idle window；期间连续读取 Redis 和实际 VM 清单，必须保持 Running。 |
| 捕获前新请求 | 非 root 透明代理暂缓实际 Execd 的 idle Prepared 应答；Ingress 请求先形成持久唤醒意图，再放行。必须保留原 backend，不能生成 checkpoint。 |
| 捕获后新请求 | 仅阻断源节点到 OBS 的 HTTPS；确认实际 VM 已停止，再发 HTTP。请求等待实际上传完成并恢复，PID、内存和文件保持。 |
| 并发唤醒 | 12 个真实 HTTP 请求同时唤醒 Paused；原节点仍可用时应保持原归属，最终只有一个 VM，每个 nonce 一次。 |
| 原节点容量不足 | 为专属测试容器设置实际 4 核 cgroup CPU 配额，sandboxd 实测后由 adxlet 上报；在两节点各运行一个占满 CPU 配额的 Firecracker Environment。12 个并发 HTTP 请求先触发健康原节点的 EnsureRunning，并明确得到 NoCapacity；Redis 保留原归属和 local_rejected。释放第二节点的占用 Environment 后才转移，同一唤醒意图完成恢复。必须保持源 session 和占用 VM，目标只有一个新 VM，每个 nonce 一次。 |
| 节点失效 | SIGSTOP 源 adxlet，等待心跳失效；Paused 保持休眠且目标 VM 数量为零。随后 HTTP 才触发另一节点从 OBS 恢复。 |
| 恢复中重启 | 阻断目标 Execd readiness；等实际 backend 为 Running、Redis 仍为 Paused 且 recovery.pending，再 SIGKILL Coordinator 或目标 adxlet。检查意图、归属、session/epoch 和 VM 清理。 |
| TTL 已过期 | 到期后真实 HTTP 必须失败，两节点都不能启动执行。 |
| 恢复跨越 TTL | 到期前接纳请求，真实 VM 恢复后用 readiness 阻断将提交延至到期之后；放行后按既定接纳契约完成。 |
| 永久保留 | 自动永久暂停后让原节点失效并重启 Coordinator；休眠期间不创建 VM。流量跨节点恢复后手动永久暂停，再重启目标 adxlet 并由流量唤醒。两个暂停点都必须没有过期时间，PID、内存和文件保持。 |
| 成功后应答丢失 | HTTPS 代理消费成功的 resume 应答并确认 Redis Running 与物理 VM，再关闭客户端连接。公共 SDK 用同一请求 ID 重试，归属、runtime 和 backend 必须一致。 |

OBS suite 关闭后台 checkpoint GC，避免它与上传阻断竞争；显式删除仍须释放已登记制品。驱动记录实际 package、SDK、Firecracker、sandboxd、kernel、initrd、runtime EROFS 与驱动文件摘要。首次或失败运行应保留错误证据，最终结果以严格验收器、空 VM 清单、空 OBS prefix 和清理记录为准。

Coordinator 重启后，仍存活的 adxlet 清理未提交执行并重新采用权威暂停点时，环境 revision 必须继续递增。Relay 在全量路由替换后仍保留已见版本；回退到旧暂停 revision 会导致后续路由发布冲突。该路径的组件回归使用真实 UDS Relay 的版本检查，同时由上述真实 VM 重启用例验证。

低带宽部署可延长测试 pause 调用预算；OBS fixture 将对象存储请求超时设置为 `AWS_TIMEOUT=240s`。demand 负载使用 128 MiB guest 内存和 32 MiB writable layer。Ingress 的单次唤醒等待仍有 300 秒上限；HTTP 驱动只对 503 用同一个业务 nonce 重试，并在响应证据的 `transient_statuses` 中保留所有失败状态和时间。成功结果代表该业务 nonce 恢复后执行一次；出现 503 时，不能宣称第一次 HTTP 请求被透明保留到恢复完成。

失败运行的测试资源停止后，可使用 `s3_probe.py RUN --cleanup` 删除该运行 prefix 下的对象并中止 multipart uploads。此操作单独记录为 `obs-fixture-cleanup.json`，不会将失败运行改为通过。正常通过仍须先证明实例删除后的 OBS 清单为空。

驱动结束并停止组件后，`demand_physical.py` 根据实际 sandboxd create 标签与 Firecracker restore PID 日志核对每项物理恢复次数。容量回退用例要求原节点恢复次数为 0、第二节点为 1；缺失日志、重复恢复或无法关联归属都会使整体验收失败，结果保存为 `physical-restore-audit.json`。

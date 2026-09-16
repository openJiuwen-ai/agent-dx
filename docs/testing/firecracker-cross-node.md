# 本地双节点 Firecracker 恢复验收

`build/e2e/firecracker/transfer.py` 在专用 Linux KVM 主机上启动两套独立的 sandboxd、Node Manager及内嵌Node Proxy。两节点使用独立网络命名空间、不同IP池、Unix Socket、cgroup根目录和运行目录；sandboxd具有私有挂载视图，对端运行目录被空目录遮蔽，不能直接读取源节点文件；共享Master、Redis、MinIO与SDK入口。

这可以验证真实执行后端的跨节点归属转移、checkpoint搬运和路由切换，但不是独立物理机/VM故障域，也不能代表正式Kubernetes验收。源节点故障通过暂停其Node Manager进程注入，sandboxd及旧实例保持实际存在，随后恢复Node Manager以检查旧执行清理。

## 运行条件

沿用 [FC基础驱动](../../build/e2e/firecracker/README.md) 的包、SDK、工具和外部runtime布局。需要root、真实KVM_CREATE_VM/KVM_CREATE_VCPU能力、`ip`、`nsenter`、至少8GiB可用磁盘。主机必须专用于此测试；固定端口和 `10.240.0.0/24` 网络不支持并发运行。

```sh
sudo env ADX_FC_BASE=/opt/adx ADX_TRANSFER_INTERRUPT_MASTER=1 \
  python3 -u /opt/adx/e2e/firecracker/transfer.py /var/lib/adx-transfer-new-run
```

运行目录必须不存在。驱动验证统一发布包，记录包manifest、sandboxd/Firecracker/RRT摘要、镜像digest与网络拓扑。Node Manager使用 `nsenter --net` 切换网络，sandboxd再通过 `unshare --mount --propagation private` 隔离对端运行目录，两者均保留可用的宿主cgroup挂载；`ip netns exec` 重挂载sysfs会导致当前后端误判cgroup版本。

## 必需证据

- 公共SDK创建、写入二进制文件、启动内存计数进程、暂停并生成共享checkpoint，再恢复源实例。
- 源节点心跳失效后，同Instance ID由另一节点的新代次恢复；恢复计划进入完成状态。
- 恢复后的PID不变、内存计数继续增长、文件字节一致。
- 原Node Manager返回后，旧backend被清理，目标backend保持运行。
- Master重启后Redis epoch递增、节点完成对账；恢复后的执行代次和runtime ID保持不变。
- 公共SDK删除；Redis终态、两套backend清单、S3清理均核对。停机先清理节点，再停止Master/Redis，最后移除自建网络。

`transfer_contract.py` 严格检查必需用例、最终清单和清理错误；缺少用例不会算通过。`collect.py` 导出组件日志和证据，并去除测试凭据。

## 当前记录

Lima r1未进入用例：sandboxd在 `ip netns exec` 环境下报告 `cgroup v1 controller "cpu" not found in /proc/self/cgroup`。驱动已调整为仅切换网络命名空间。首轮失败日志和清理结果保留于 `out/ci/stage-4/fc-transfer/r1/`，不能算恢复功能失败或通过。

设置 `ADX_TRANSFER_INTERRUPT_MASTER=1` 后，用例仅阻断控制节点到目标Node Manager的17001端口，保持目标节点心跳和资源采集正常；待Redis中出现新代次恢复计划后杀死Master，等待epoch递增并确认计划不变，随后放通恢复RPC。该用例覆盖计划落盘后、执行调用前的Master重启；不能替代backend恢复进行中或目标Node Manager重启的证据。

驱动回归 `driver-regression.log`：51项通过。r2两节点READY但内部请求被继承的代理设置影响；补齐测试地址NO_PROXY后，r3公共SDK创建及共享checkpoint已通过。r3暂停sandboxd的注入方式同时阻塞了目标资源采集/心跳，等待恢复计划超时；已改为上述仅阻断恢复RPC的注入，失败证据保留。

## r4 真实运行结果

2026-09-16，package-v21、Lima ARM64 KVM，六项全部通过，另包含计划落盘后、恢复RPC执行前的Master重启注入。

- Instance `default-sandbox-4f4175ba-327e-45aa-b84c-8223a2906ed2`：node1/generation 1 → node2/generation 2。
- 故障注入期间Master epoch 1 → 2，原恢复计划与目标代次不变；成功恢复后再次重启Master，runtime ID和代次保持不变。
- PID `11` 保留，计数器 `9 → 33`，二进制文件逐字节一致。
- 原节点返回后backend清单为空；目标保持一个运行实例，已登记S3制品仍可读。
- 最终两节点库存0、S3清单为空、`cleanup_errors: []`；额外检查无测试进程和网络命名空间残留。

证据：`out/ci/stage-4/fc-transfer/r4/evidence/result.json`、`mid-recovery-master-restart.json`、`runtime-hashes.json`、`topology.json`、组件日志；完整过程在 `r4.log`。51项驱动测试通过。

当前包来自未提交工作树。sandboxd SHA256为 `f1df80fba119e31a5499daecdc40f42b4d0011118be4da72c13c0f1748676cfb`，与固定制品匹配；Firecracker SHA256为 `b0ac325d20f123c63f6cdea8e272db3506fecf8ce87c03e039cd12e060d1d0eb`；RRT SHA256为 `870338367442799b4a0c540e7824a4f36129909909569b15792fa93b74c24458`，与package-v21及本轮OCI制品一致。

## 目标 Node Manager 恢复执行中重启

设置 `ADX_TRANSFER_INTERRUPT_NODE=1` 可验证未提交执行的清理契约。用例在目标网络命名空间中仅阻断到RRT的50090端口，保留sandboxd、资源采集与心跳。待真实backend清单中出现Running执行，同时Redis仍为Paused、recovery.pending=true，再杀死目标Node Manager并放通RRT。

重启后必须满足：节点会话变化；原未提交backend ID消失；同Instance ID、同目标归属代次恢复为一个新的backend ID；PID/内存/文件仍从已登记checkpoint恢复。后续继续执行源节点返回清理、Master重启与最终删除。`node-recovery-before-crash.json` 与 `node-recovery-restart.json` 保存故障点及替换证据。严格验收器会拒绝缺少故障证据、复用旧backend或变更归属代次的运行结果。

### r5 验收结果

2026-09-16，package-v21，`ADX_TRANSFER_INTERRUPT_NODE=1`，六项用例及目标节点中断断言全部通过；52项驱动回归通过。

- Instance `default-sandbox-a469af4b-ffc5-4a4d-94f6-85d77cc270b4` 从node1/代次1恢复到node2/代次2。
- 崩溃时Redis仍为Paused且pending；实际运行的未提交backend为 `sbox-21f5cda3-a0ff-4516-af39-c48851fa54aa`。
- 重启后节点session改变，原backend清理，新backend为 `sbox-5b22503f-4643-45ae-b9bb-5251130caad8`；归属仍为node2/代次2，最终仅一个执行。
- PID `11`、内存计数 `8 → 36`、二进制文件一致。源节点返回清理、恢复后Master重启与显式删除均通过。
- 两节点库存和S3均为空，`cleanup_errors: []`；额外确认测试进程与网络命名空间无残留。

证据位于 `out/ci/stage-4/fc-transfer/r5/evidence/`，重点为 `result.json`、`node-recovery-before-crash.json`、`node-recovery-restart.json`；完整日志为 `r5.log`，驱动回归为 `node-driver-regression.log`。

r4与r5共同完成阶段4的本地故障恢复验收。此结果仍限定于同一KVM宿主上的隔离节点；正式Kubernetes门禁属于阶段7。

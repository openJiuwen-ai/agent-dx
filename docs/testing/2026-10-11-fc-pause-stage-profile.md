# FC 恢复后再次暂停的阶段耗时（2026-10-11）

## 结论与范围

取消 ADX local-only 制品 fsync 后，恢复后再次暂停仍有长尾。本轮逐阶段复现为 26.799 秒；主要阻塞在 sandboxd Checkpoint RPC 内的 Firecracker snapshot 设备排空，内存 write 本身为 1.113 秒。本地制品发布为 0.729 毫秒，权威状态提交为 2.119 毫秒，不是主要瓶颈。

前轮 43.249 秒只有 SDK 总计时，没有后端阶段日志留存，不能把本轮各阶段秒数直接套到那一次。当前证据确认相同负载下的慢路径，不能声称已精确重建历史 43.249 秒。诊断使用 strace/perf 和 debug adxlet，数值用于定位，不是 release 性能基线。

本轮未修改 sandboxd／Firecracker，也没有新增产品协议或配置。计时打印仅加在隔离诊断源码与镜像，生产工作树沿用前轮 LocalCheckpointStore 改动。

## 环境与版本

- 远端 x86 standalone：Linux 6.8.0-137-generic，真实 KVM，宿主 32 CPU／123 GiB 内存。
- ADX 基线：`community/refactor@f33a5025b187ca28317db100f9a30f4bf94c3fae`，叠加取消本地制品 fsync 与隔离计时打印。
- 诊断 adxlet：Rust 1.95.0 debug，SHA256 `03054c5ae6349800b18db60424688d379713f8e8fab22ef890e1ea0d13c90650`。
- AKernel 夹具：`f35e68a5bd370b3606e4aa47b9f4cdca1bd8f53d`。
- sandboxd：`31d0749a85e269396cfc1c469c948345f9610906`。
- Firecracker：`v1.16.1-akernel.3`，fork pin `b9a362d1070ee17991e7388d3b53a9ccce25ecca`。
- 诊断镜像：`akernel-fc-stage-profile:20261011`，ID `sha256:4ce9c2c77a1ba73f6f5bd861773e19c6dea41cb9f9e930566bb853bc19411125`；仅替换 adxlet，其余组件沿用测试夹具。
- 单实例、1000m CPU、3072 MiB VM RAM、2048 MiB 随机驻留数据、256 MiB writable layer。Full checkpoint 包含整个 3 GiB RAM，制品逻辑长度约 3.490 GB。
- checkpoint：local-only，宿主 ext4；filestore：50G loop ext4。恢复的 `memory.live` 与 writable `overlay.ext4` 共用 loop filestore；本轮没有 OBS 上传／下载。
- 同一实例连续两次 pause/resume；恢复后立即再次 pause，没有人为等待写回完成。

## SDK 与 adxlet 分解

下表阶段互不重叠，可相加。SDK 返回耗时与 adxlet 总耗时之差约 5 毫秒。

| 阶段 | 首次暂停 | 恢复后再次暂停 |
|---|---:|---:|
| 前置检查与分配制品位置 | 1.150 ms | 1.086 ms |
| Execd prepare | 13.140 ms | 12.297 ms |
| 退役路由 | 2.223 ms | 2.393 ms |
| sandboxd Checkpoint RPC（含执行停止确认） | 1765.664 ms | 25343.886 ms |
| sandboxd 删除后端／释放镜像资源 | 3266.425 ms | 1099.321 ms |
| 释放节点资源 | 0.686 ms | 0.641 ms |
| LocalCheckpointStore publish | 0.710 ms | 0.729 ms |
| 本地记录更新 | 0.471 ms | 0.496 ms |
| 权威提交与旧制品清理 | 4.134 ms | 333.343 ms |
| adxlet 总计 | 5054.603 ms | 26794.192 ms |
| SDK pause 总计 | 5059.803 ms | 26799.084 ms |

第二次提交中的 `sink.commit` 为 2.119 ms；旧制品清理为 329.537 ms。首轮提交为 2.194 ms、旧制品清理为 0.005 ms。ADX trace 未出现 checkpoint memory／目录 fsync；仅观察到 runtime 日志索引 `.terminated.json.tmp` 的两次 fsync（10.519／2.270 ms），与本地制品持久化不同。

## sandboxd／Firecracker 分解

backend 日志的 `total` 在 `finishCheckpointedSandbox` 前打印，不能当成整个 Checkpoint RPC。最后停止确认及 RPC 余量另列。

| 后端阶段 | 首次暂停 | 恢复后再次暂停 |
|---|---:|---:|
| 布局准备 | 57 ms | 0 ms |
| guest flush 应答等待 | 13 ms | 2000 ms，超时 |
| VM pause | 0 ms（整数毫秒精度） | 0 ms |
| writable overlay 保存 | 263 ms | 232 ms |
| Firecracker snapshot | 1268 ms | 23023 ms |
| seal／manifest／状态 | 3 ms | 3 ms |
| backend 日志 total | 1606 ms | 25260 ms |
| Checkpoint RPC 剩余（含停止确认） | 约 160 ms | 约 84 ms |

这些 backend 阶段是上表 Checkpoint RPC 的内部子项，不能再加到 SDK 总时延上。各阶段日志取整到毫秒，存在少量未单列开销。

第二次 snapshot 的约 23.023 秒又可拆为：

- 等待尚未完成的 block flush：约 21.86 秒。Firecracker 主线程在 `io_cqring_wait`，I/O worker 在 `jbd2_log_wait_commit -> ext4_sync_file -> io_fsync`；在 snapshot 区间约 40 个采样点捕获该等待。
- 写出 3 GiB RAM：两次 write 为 0.754795 + 0.358480 = 1.113275 秒；返回长度合计 3221225472 字节。
- 其余设备／vmstate／快照流程：约 0.05 秒，依据阶段总计扣除等待和 RAM write 得到的残差，并非进一步逐函数计时。

sandboxd 的父进程 strace 使用 `-f`，已跟踪新启动的 Firecracker，故可读取其 RAM write 与恢复 `copy_file_range`。单独 attach Firecracker 返回 PTRACE_SEIZE Operation not permitted，独立输出为空，是因为它已由父进程追踪；不影响上述子进程证据。追踪未包含 read／sendfile 等全部 syscall，因此不能单凭 EXDEV 那一行计算完整 fallback copy 的时长。

## 写回链路

恢复时 trace 捕获 `checkpoint memory -> filestore memory.live` 的 `copy_file_range = EXDEV`。固定 fork 的 `guest_memory_from_shared_file` 在 reflink 不可用时复制整份源文件，再 MAP_SHARED 映射 live RAM。3 GiB copy 产生的脏页尚未全部落盘，第二次暂停就开始了。

随后 guest flush 触发 block flush：

```text
guest sync
  -> Firecracker io_uring fsync(overlay)
  -> loop ext4 journal/barrier
  -> loop driver fsync(backing file)
  -> 宿主 ext4／实际磁盘写回
```

perf 实测 loop ext4 的 fsync（dev 7,0，`iou-wrk-770245`）持续 24.084800 秒，wall 时间约 23:04:35.982—23:05:00.066。其内部 loop worker 对宿主 ext4（dev 259,3，同一 backing inode）的两次 fsync 为 21.168547 + 2.854966 秒；调用栈包含 `do_req_filebacked -> vfs_fsync -> ext4_sync_file`。两个内外层耗时是嵌套关系，不能相加。

guest flush 的 2 秒只结束 sandboxd 等应答；底层同步仍在进行。随后 `VirtioBlock::prepare_save -> drain_and_flush` 等待剩余 I/O，约 21.86 秒被计入 snapshot。固定 fork 的 AsyncIo `drain_and_flush` 在 `drain` 之后还执行 `self.file.sync_all()`，因此 snapshot 路径本身也要求宿主磁盘同步；仅跳过 guest sync 不能据此保证消除后续写回等待。并非内存 dump 自身需要 23 秒。

首份 checkpoint 的后台 `sync_file_range(WRITE)` 还持续了 27.188 秒，结束于 23:04:50.946，跨越首次恢复和第二次暂停。第二份后台调用持续 24.042 秒，SDK pause 已返回后它仍在执行。取消 ADX fsync 只改变 ADX 返回等待，不会取消 sandboxd／内核的写回。

第二次 pause 区间，宿主物理 `nvme0n1` 写入约 4804 MiB，约 179 MiB/s，I/O busy 约 94.5%；snapshot 子区间约 3930 MiB、171 MiB/s。首次恢复区间又写入约 1589 MiB。计数来自 0.5 秒采样，窗口边界取最近样本；未累加 loop、分区和父设备，避免重复计量。进入第二次 pause 时宿主 Dirty 约 4.24 GiB，包含重叠写回；不是该实例独占的指标。不能由 NVMe 名称或 busy 百分比推断具体云盘限额。

此外，后端删除也受到同盘写回影响。首次删除 3.266 秒中，31 次 fsync/fdatasync 合计 3.231 秒；其中 OCI metadata DB 为 3.109 秒。第二次删除 1.099 秒中，同类同步合计 1.044 秒。perf 栈定位到 bbolt 事务及镜像层引用释放。这是独立的次要耗时来源。

## 下一步优化方向

应优先隔离 live RAM 与 writable block 的稳定存储 flush。可评估受内存预算约束的 memfd/tmpfs 共享 backing，或支持 reflink 且布局一致的文件系统，保持 virtio-fs 共享内存、恢复隔离、dirty accounting 与资源释放契约。避免只缩短 guest flush deadline：超时不取消底层 I/O，也不能省去 snapshot 正确性要求的设备排空。

其次评估 OCI 镜像引用释放的事务合并，减少每次 pause 删除过程中的串行 bbolt fdatasync。两项均尚未修改或验证，不能视为已解决。此前有 fsync 的原生 filestore 对照不等于本轮无 fsync 版本的性能承诺。

## 验证与证据

两次 pause/resume 均通过；PID、marker、每 MiB 首尾采样摘要一致，首命令验证均一次成功。Resume API 为 7.627／10.687 秒，至首命令校验完成为 8.323／11.376 秒。最终 backend 仅表头，Redis Environment 字段为空、checkpoint 文件为空、任务容器删除，cleanup exit 0。

本地证据：`out/ci/fc-stage-profile-20261011/`，包括 `phase-breakdown.json`、`samples.jsonl`、`component-logs.log`、`runtime-all.log`、`syscalls.json`、`strace-sandboxd-767251`、`strace-adxlet-767428`、`host-samples.jsonl`、`fsync-duration-summary.json` 和 `perf-script.txt`。perf 与 strace 两种时钟用 105 次同 TID／同设备／同长度 fdatasync 匹配估算偏移，wall 对齐用于毫秒级定位，不能认为两个时钟原生相同。

远端证据：`/root/akernel-worktrees/full-regression-f35e68a-20261010/out/ci/fc-stage-profile-20261011/`。诊断源码独立存于 `/root/adx-fc-stage-profile-20261011/`；未覆盖主线源码。总日志 `/var/log/akernel-fc-stage-profile-20261011.log`。

前轮功能回归与 local-only 成功契约见 [本地发布回归](2026-10-11-local-checkpoint-publication.md)。

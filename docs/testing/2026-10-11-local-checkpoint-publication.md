# Local-only checkpoint 发布回归（2026-10-11）

## 当前契约

Local-only 成功要求后端生成完整、可在所属节点读取的制品并提交恢复点元数据。ADX 不再对本地制品逐文件、逐目录执行 fsync。文件仍经操作系统／执行后端写回，但成功应答不保证突然断电或宿主机崩溃后的制品完整性。暂停、workload checkpoint、上传失败回滚保留和本地可复用快照复制共用该本地存储语义。S3 上传完成和权威元数据提交契约保持。

实现只修改 ADX 的 LocalCheckpointStore；sandboxd、Firecracker 没有修改。

## 来源与环境

- ADX 基线：`community/refactor@f33a5025b187ca28317db100f9a30f4bf94c3fae`，叠加验证时尚未提交的本地存储改动。
- Linux x86_64：`6.8.0-137-generic`，真实 KVM／Firecracker。
- AKernel 测试夹具基线：`f35e68a5bd370b3606e4aa47b9f4cdca1bd8f53d`。
- sandboxd：`31d0749a85e269396cfc1c469c948345f9610906`。
- Firecracker：`v1.16.1-akernel.3`；构建 pin `b9a362d1070ee17991e7388d3b53a9ccce25ecca`。
- 夹具镜像：`akernel-local-checkpoint:20261011`，镜像 ID `sha256:2a938f5cb4fb07bbe967cc47a238e5ada93c7653cf3b1571808e571c340759d2`。
- 仅替换 adxlet；使用 Rust 1.95.0 debug 构建，二进制 SHA256 `9b880436e1a410b539d6fa59eb4561bcee8b2dbf5aa2d2000f08f8dc0e0b1805`。其余组件沿用夹具镜像。
- 64／2048 MiB 常驻随机内存分别配置 1088／3072 MiB VM；制品逻辑长度约 1.409／3.490 GB，checkpoint 存在宿主机 ext4。不存在对象存储下载阶段。

## 验证结果

- TDD red：原实现的语义测试通过，但 syscall 审计发现 15 次制品 fsync，按预期判失败。
- Green：adxlet 全套 168 项通过，0 失败、0 忽略；新增 2 项测试覆盖可读性、存储对象重建、保留、独立复制、源删除、空制品、symlink、特殊文件、根目录外路径及截断拒绝。
- Linux strace 审计真实本地发布／保留／复制路径：观察到制品 IO，强制写回调用为 0。此结论只针对本地制品路径，不包括 SQLite、Redis、sandboxd 自己的持久化。
- `make rust-check JOBS=2` 通过。首次检查遇到共享缓存里其他工作树的协议产物；重新生成当前 protobuf 后通过，未为此修改协议源码。
- 新增边界用例 Target Clippy 通过；文档检查、Python 语法和 `git diff --check` 通过。
- 真实 FC：4 次暂停／同节点恢复成功；每次 PID、marker、内存采样 digest 相等，恢复都在 `node-1`。摘要覆盖每 MiB 首尾各 64 字节，不是全内存逐字节比较。
- 64 MiB 用例第一轮在 Paused 时 SIGKILL adxlet，supervisor 拉起新进程（PID 209 → 395），目录／后端对账后恢复成功。
- 两种内存规模 warm reload 成功；沙箱 Unix checkpoint 完成后源实例继续运行，再修改文件，reload 后文件回到 checkpoint 版本，内存进程校验通过。
- 最终 backend 列表只剩表头，Redis Environment 字段和 checkpoint 文件为空；任务容器已删除，cleanup exit code 为 0。

## SDK 时延

| 常驻内存 | 轮次 | Pause | Resume API | 恢复至首命令校验完成 |
|---|---|---|---|---|
| 64 MiB | 1 | 4.688s | 2.166s | 2.521s |
| 64 MiB | 2 | 9.272s | 3.967s | 4.311s |
| 2048 MiB | 1 | 2.414s | 3.408s | 4.018s |
| 2048 MiB | 2 | 43.249s | 6.758s | 7.476s |

64 MiB 的 Unix workload checkpoint 为 13.972 秒。Warm reload 的 API／首命令时间：64 MiB 为 5.112／5.460 秒，2048 MiB 为 14.902／15.623 秒。

本轮是功能回归兼小样本测量，不是旧新版本使用相同构建配置的配对性能试验。取消 ADX 本地 fsync 已被 syscall 审计确认，但连续恢复后再次保存仍有明显长尾（43.249 秒），不能据首次 2.414 秒认定总体保存瓶颈已解决。本轮没有对这 43 秒逐阶段埋点，因此不能单凭 SDK 总时延分配给某个后端阶段；前轮定位的 sandboxd／FC 写回、恢复内存副本及 guest flush 路径均未在本轮修改。

后续已完成隔离的逐阶段诊断，第二次暂停复现为 26.799 秒，主要等待在 backend block flush／snapshot 设备排空；本地 publish 仅 0.729 毫秒。具体证据与限制见 [FC 暂停阶段定位](2026-10-11-fc-pause-stage-profile.md)，不能将新运行的秒数套到原 43.249 秒。

## 证据与边界

本地证据：仓库 `out/ci/local-checkpoint-20261011/`，包括 red／green syscall 结果、测试和规范检查日志、FC samples.jsonl、身份及清理记录。

远端：`/root/adx-local-checkpoint-20261011/out/ci/local-checkpoint-green/`；FC 证据 `/root/akernel-worktrees/full-regression-f35e68a-20261010/out/ci/fc-local-checkpoint-green-20261011/`，总日志 `/var/log/akernel-fc-local-checkpoint-green-20261011.log`。

本轮没有构建正式发布包、更新 Kubernetes 集群或发布分支。断电耐久性不属于此次 local-only 成功契约；远端 S3 通过组件测试保留其原有上传完成、回滚和缓存语义，本轮没有再次进行真实 OBS E2E。

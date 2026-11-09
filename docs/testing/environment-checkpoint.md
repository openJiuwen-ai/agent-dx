# Environment 暂停与恢复

实现同 Environment ID 的暂停／按需恢复：优先同节点，共享制品允许明确资源不足或节点失效后跨节点恢复。adxlet 持有实例状态机，Coordinator 负责提交结果和更新调度／路由视图；执行后端仍为外部 sandboxd。

## 正常路径

1. 暂停由 API Server 从本地缓存解析归属，携带已认证身份、operation ID 和预期状态版本直达 adxlet。手动恢复和 Ingress 首流量统一进入 Coordinator `EnsureRunning`。
2. adxlet 串行执行本实例的请求。先确认后端支持 checkpoint，使用 Execd 控制接口准备 checkpoint（配置 runtime_control 时走双向 gRPC，否则走 HTTP），再关闭本机路由。
3. sandboxd 完成 `leave_running=false` 的完整 checkpoint。确认源执行已停止、删除成功后，释放本机资源。
4. 存储后端完成制品发布：本地目录检查制品，对象存储完成上传；adxlet 将 Paused 状态、恢复点和过期时间一次提交给 Coordinator。Coordinator 写入 Redis、释放 Shard 占用并撤销发布路由，API Server 才返回暂停成功。
5. 恢复时检查过期时间和制品，优先进行原节点本机资源准入；共享恢复点在明确 NoCapacity 或归属失效后重新调度兼容节点。RPC 结果不确定时保留原归属。目标通过 sandboxd 的 `Start.checkpoint_info` 恢复，后端自行生成物理 ID。
6. Execd 确认新执行身份、重新开放服务，本机绑定完成后，adxlet 提交 Running。Coordinator 增量恢复 Shard 资源占用、发布新路由。

同节点恢复不改变 ownership generation，execution ID 使用增加的实例状态版本区分，例如 `instance-7` → `instance-7-r5`。Execd 校验执行版本前进，Ingress 和 Relay 校验路由版本；旧执行不会借用恢复后的绑定。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `core::checkpoint` | 恢复点、制品引用、最近成功操作的持久化模型 |
| `adxlet::controller::lifecycle` | 串行暂停／恢复、资源释放与重新准入、失败回滚 |
| `CheckpointCooperation` | Execd 控制 prepare／未启动 checkpoint 的 abort（gRPC／HTTP 共用状态机） |
| `RuntimeDriver` | 能力检查、checkpoint、restore；sandboxd 适配器负责物理 ID 和 RPC |
| `CheckpointAccess` | 捕获目录、seal、恢复访问与节点本地缓存释放；共享对象物理删除通过 Coordinator ArtifactReclaimer |
| `coordinator::storage` / Shard | Redis 版本校验、恢复点保存、增量资源记账 |
| `gateway/apiserver` | HTTP 格式转换、缓存归属；恢复走统一 Coordinator 入口 |

`CheckpointAccess::seal` 是存储完成边界。对象存储在此完成上传，通过 `prepare_restore` 下载；本地/S3、缓存引用和跨节点恢复已实现，见 [存储契约](snapshot-storage.md) 与 [跨节点恢复](firecracker-cross-node.md)。

local-only 成功表示后端已生成完整、可在本机读取的制品，并已提交恢复点元数据。adxlet 不主动对本地制品及目录执行 `fsync`；未完成的磁盘写回由操作系统和执行后端处理，不保证突然断电或宿主机崩溃后的制品完整性。该规则同样用于 workload checkpoint、本地回滚恢复点和可复用快照副本；对象存储仍等待完整上传。恢复及重启对账保留本地制品的路径、类型和长度校验，校验失败的恢复点不能用于恢复。

## 配置与部署

adxlet 配置增加可选的 `checkpoint_dir`，例如：

```json
{"checkpoint_dir": "/opt/adx/data/checkpoints"}
```

统一部署配置中填写到 `role: adxlet` 的 `config`。也可设置互斥的 `checkpoint_storage` 选择本地或 S3；两者均未设置时不启用暂停／恢复。该目录必须是 adxlet 与 sandboxd 都可读写、以相同绝对路径访问的节点目录；本地制品需在 adxlet 重启后保留。不要将该目录用作其他组件的文件仓库。

sandboxd 必须支持 checkpoint／restore，并通过 runtime capabilities 提供 Execd 的 checkpoint handoff 和 restore environment 路径。adxlet 从能力信息注入这两个路径。Firecracker 的 Linux/KVM、guest kernel、initrd 和网络条件由运行环境提供；本阶段不会替部署环境启动 sandboxd。

暂停 TTL 控制恢复点过期，不是实例最大存活时间。节点每 30 秒扫描一次，清理动作与实例操作串行：暂停实例过期后删除；已经恢复运行的实例只清理过期制品。显式删除也清理本地恢复点。

## 失败与重试

- API/RPC 断线不会取消已进入实例队列的操作。暂停等节点操作重试保持原始身份/版本；恢复重试复用 Redis 中的固定意图。已暂停对象节点失效后继续休眠，等待需求、TTL 或删除。
- 正常结果记录只保留最近成功操作。Node RPC 拒绝旧预期版本；客户端不能在一个后续操作已改变状态后，把旧请求换成新版本重新执行。
- checkpoint 已完成而 Coordinator 提交失败时，节点按 [SQLite 降级契约](node-lifecycle.md) 保存待补交结果。同一操作重试只补交；Journaled 不返回集群成功，重启后先等待 Coordinator 权威对账。
- 可复用 Snapshot 在同一次节点请求内等待 Journaled 状态达到 Published：只补交相同记录和版本，不重新运行后端 capture。等待占用该次 checkpoint 的剩余预算，后端收到扣除等待时间后的超时。真实归属／版本冲突立即返回；预算耗尽后保留待补交记录和制品引用，不把本地 journal 当作 Snapshot 成功。此规则不放宽 Coordinator 的 CAS、归属和制品引用校验。
- checkpoint 调用前，准备失败可调用 Execd abort；调用后不再假设 checkpoint 未启动。无法确认停止／清理时保留资源占用并报告失败。
- 发布制品失败时，从完整本地暂存尝试恢复运行，并返回暂停失败。回滚或清理失败不会伪装成暂停成功。
- 恢复失败但清理成功时保留恢复点并释放此次本机资源；清理不确定时保留资源，等待处理。自动跨节点恢复仅在本地容量不足、或执行准入 `prepare_execution` 尚未成功返回且报 `Unavailable`、`VolumeBusy`、`NoCapacity`（含准入超时）时保留可重试进度；Coordinator 保留 `recovery.pending` 的目标预留。临时准入错误沿用该 Assignment 重试，明确 NoCapacity 仍可按既有规则拒绝目标后重新选点。checkpoint 读取失败（含有效存储别名下对象缺失）、后端 restore 拒绝、开始 restore 后的初始化超时、readiness 或路由激活失败，在清理确认后转为 Failed，不因错误类型是 `Unavailable` 而再次执行 restore。
- 节点重启后，从 Coordinator 获取完整目录；Paused 记录不会被当作缺失的 Running 实例重建。未持久化且不匹配的执行按既有对账原则清理。
- 替换、过期和删除恢复点时，先提交新元数据，再清理旧制品。提交或清理失败保留待清理记录，后续同步或同操作重试继续完成清理。
- 重启对账期间关闭准入，确认所有执行和结果已对账发布后，清理本地存储根目录中不再被引用的 UUID 制品目录。Coordinator 不可用或对账失败时不执行该清理；共享存储的目录引用与旧会话孤儿清理见 [存储契约](snapshot-storage.md)。

## 验证

组件测试覆盖状态与资源转换、丢失提交回复、过期清理、恢复失败、回滚、真实 sandboxd 协议的 UDS 调用、Execd HTTP 身份切换和 Ingress 版本检查。Redis 集成测试使用独立真实 Redis。
可复用 Snapshot 的真实 SQLite JournalSink 回归还覆盖暂时发布失败后成功、只 capture 一次、原预算耗尽及不重试永久冲突；组件结果不替代并发集群验收。

`platform/coordinator/tests/rpc.rs` 的 `shared_checkpoint` 用例覆盖自动跨节点恢复和休眠后的 Ingress 按需恢复，使用真实 Redis、mTLS、独占 Volume 授权及内存对象存储；断言恢复后原 spec 不变、卷 UID 不变、目标执行独占卷、旧归属提交被拒绝及重复请求不重复恢复。`platform/adxlet/tests/pause_resume.rs` 另注入临时准入错误，验证保留恢复点并在错误解除后只 restore 一次；通过表驱动用例覆盖无到期时间的恢复点在 manifest/数据对象缺失、后端拒绝、restore 初始化超时或准入权限拒绝时进入 Failed，重复恢复请求不再次调用后端。授权后错误场景由真实 Coordinator 完成 Redis 授权后在测试 RPC 包装层替换回复，断言 Paused 提交立即释放独占占用、保存已释放 runtime 身份且后续授权接受该 previous runtime；它不注入调度器内部故障。协议测试单独验证 `Unauthenticated`、`PermissionDenied` 的错误码与原因保留，以及 `Internal` 保留诊断并继续映射为 `Unavailable`。这些用例的 RuntimeDriver 是夹具，不构成真实 sandboxd、共享文件系统或 Kubernetes 验收。

```bash
cargo test -p adxlet -p adx-coordinator -p adx-core -p adx-protocol
cargo test -p adx-execd --test control_http
cargo test -p data-plane-gateway --test coordinator_routes
python3 build/ci/run.py storage --jobs 2 --output out/ci/checkpoint-storage
python3 build/ci/run.py control-rpc --jobs 2 --output out/ci/checkpoint-rpc
```

真实 Firecracker 公共 SDK 用例见 [sdk_checkpoint.py](../../build/e2e/firecracker/sdk_checkpoint.py)。用安装了本次发布包 SDK 的 Python 执行：

```bash
python build/e2e/firecracker/sdk_checkpoint.py \
  --endpoint 127.0.0.1:8443 --token-file /path/to/api-key \
  --ca /path/to/ca.pem --image registry/execd@sha256:... \
  --output /path/to/new-evidence-directory
```

用例检查创建、命令与文件、暂停、恢复后的内存计数器和 PID、二进制文件、删除。可通过 `--restart-command` 指定选定 adxlet 的重启及就绪检查脚本，验证暂停期间重启。进程重启脚本属于部署测试夹具，不通过 SDK 实现。

真实 FC 本地验收与 Kubernetes Buildkite 验收分开记录；组件测试通过不表示后者已通过。

## 可复用快照的发布等待边界

可复用快照与 workload checkpoint 共用后端超时换算：正的剩余预算向上取整到整秒，最少 1 秒；已过期预算不再发起捕获。发布等待仍使用原始截止时间，不因重试重新计时。退避抖动由 Environment ID 与 ownership generation 共同确定，避免同 generation 的环境全部使用同一偏移。

capture 和源状态提交完成后，向 Coordinator 发布目录的 RPC 也使用本次快照操作的剩余预算，不再被普通控制 RPC 的短超时提前截断。调用方与 RPC 传输同时受原 deadline 约束；这不增加目录发布重试，也不将超时解释为写入一定未发生。超时仍保留同一 capture 的待发布进度和引用，供存活 owner 的同操作重试继续处理。真实回环 gRPC 回归覆盖普通 RPC 超时之后、快照预算以内的成功，以及原预算到期、源环境继续运行和不重复 capture 的恢复发布。

准备阶段耗尽预算、且尚未调用后端 capture 时，通过 `abort_unstarted` 撤销准备并保留运行中的源环境；只有撤销无法确认时才沿用失败清理。capture 已成功后的发布失败会由存活的 owner 控制器保留阶段、原操作 ID、原 expected revision、暂存路径和快照元数据；同一请求重试继续发布同一个工件，不重复 capture、已完成阶段或 revision 推进。其他生命周期写操作不能越过这项待发布操作，删除仍可清理。新一次 RPC 可使用新的超时预算，但不能改变原操作的快照类型或名称。这个进度不是跨进程的持久恢复协议：节点进程重启仍按未封存 capture 的对账规则清理或退役执行；持久化的 execution-artifact 引用继续负责保护文件和资源归属。

快照发布进度仍待完成时，运行时事件和定期监控触发的 workload checkpoint 也延后处理：不会为另一个 capture 分配暂存目录、执行 prepare、清理引用或向 Execd 返回完成 ACK。原请求及已记录的截止时间保持不变；快照发布完成后按原预算继续处理，已经过期的 workload 请求仍按原超时规则失败。健康观察和显式删除仍保持原有语义。回归覆盖 runsc 未封存的零大小暂存引用和运行时保留 capture 文件两种情形，并验证发布完成后可以执行后续 workload checkpoint。

只有 `Durability::Journaled` 明确确认本地持久化后才等待重新发布；`sync()` 的错误（包括提交超时或本地 journal 故障）立即向调用方返回，不把不确定的持久化结果当成 Journaled。同一 Environment 的控制器仍串行处理命令，发布等待会占用该命令处理窗口直到成功、错误或请求截止时间；本改动不提供 DELETE、RunDeadline 或健康检查事件抢占快照操作的能力。需要缩短这段等待时应设置较短的快照超时，而不是绕过所有权校验或重复 capture。

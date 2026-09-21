# Checkpoint 存储与可复用快照

`CheckpointStore` 是 Node Manager 的存储边界。已有本地目录与 S3 兼容对象存储适配；新增后端实现该接口，生命周期状态机不依赖具体存储 SDK。

## 配置

Node Manager 可使用原来的 `checkpoint_dir`，或显式选择 `checkpoint_storage`，二者不能同时设置。

```json
{
  "checkpoint_storage": {
    "kind": "s3",
    "alias": "shared",
    "bucket": "adx-checkpoints",
    "region": "us-east-1",
    "endpoint": "https://objects.example.internal",
    "allow_http": false,
    "prefix": "production/checkpoints",
    "root": "/opt/adx/data/checkpoints",
    "cache_budget_bytes": 4294967296
  }
}
```

部署环境通过标准 AWS 凭证环境变量或环境提供的身份机制配置 S3 访问。实例记录只保存存储别名、制品 ID 与字节数，不保存端点和密钥。每个集群使用独立的 bucket/prefix；共享该存储的节点必须为同一别名配置相同 bucket/prefix，并保持节点 ID 和存储别名稳定；本地目录是节点私有的暂存与下载缓存。

## 制品提交与恢复

- 后端先生成完整本地 checkpoint。对象适配器按文件流式上传，记录每个文件的长度、SHA256 和权限；所有文件完成后才写入版本化格式的 manifest，作为发布完成标记。
- 下载按 manifest 限制路径、长度和校验和，不接受符号链接及特殊文件。完整下载后以 rename 暴露缓存目录，避免恢复读取半成品。
- 上传失败保留本地制品，Node Manager 尝试回滚恢复。回滚使用的本地恢复点写入实例结果，保持 local-only 语义；成功上传后的暂存目录在结果全局提交后清理。
- 制品清理与快照复制保留接口边界。复制产生独立制品 ID；源暂停恢复点删除不影响可复用副本。远端文件支持服务端复制，大文件使用有界流式复制。

## 缓存与实际执行引用

缓存按字节预算和最近使用时间淘汰未引用下载。预算被已有引用占满时，新恢复返回暂不可用。暂停实例对账仅验证远端 manifest，不因扫描目录而下载全部暂停实例。

sandboxd 的 Firecracker v2 恢复会直接读取调用方制品中的 state/memory 文件。因此引用要保持到执行停止，不能在 Restore RPC 返回后立即释放。Node Manager 重启后从权威记录重建运行实例的引用；过期恢复点立即失去再次恢复资格，但物理文件和对应记录延迟到运行引用释放后清理。上传失败回滚同样遵守这一规则。

节点失去本地缓存不会丢失远端恢复点。已运行 VMM 仍依赖本机实际文件，这与恢复点是否可跨节点访问是两个问题。

## 快照目录与引用

Master 的快照目录模型、Redis 存储与 mTLS SnapshotService 已实现。API Server 的查询、分页和删除接口经已认证的租户上下文调用该服务；创建快照由 Node Manager 串行控制任务执行；克隆复用调度和恢复链路。每条记录包含创建模板、源节点/执行身份、制品、版本和状态。

- Ready 接受新引用；引用使用恢复实例或节点模板的明确身份，重试不重复增加计数。
- 删除将状态改为 Deleting，拒绝新引用，保留已有引用直到释放。
- 引用归零后才允许物理制品回收；后端确认删除后，再将目录标记为 Deleted。完成请求使用版本复核。
- Redis CAS 同时校验当前 Master epoch 和旧记录内容，旧 Master 不可修改新 Master 接管后的引用。租户检查和不可变制品身份在目录层执行。

## 验证边界

对象存储、文件引用、上传失败回滚已有 Node Manager 行为测试；快照引用、延迟删除、Redis/Master 重启已有真实 Redis 测试。真实 S3＋Firecracker 部署验收已通过，下节列出证据。创建链路已通过组件与真实 Redis/mTLS 测试，统一 package-v12 的 Lima r15 已通过 13 项真实 FC/SDK 验收。从快照创建新实例的实现和本地验证见后文；未登记远端制品回收的契约与验收见后文。


## 本地 S3 验收（2026-09-16）

Lima `adx-fc`，运行目录 `/var/lib/adx-pause-r11`，`package-v9`（基于 `0dde79ad`，包含未提交修改），真实 Firecracker 与独立 MinIO S3 服务。10 项公共 SDK checkpoint／生命周期用例通过。暂停后读取远端 manifest，确认本机上传暂存已清理，再重启 Node Manager；恢复后的内存计数从 91 继续到 239，PID 保持 15。最终所有 Capsule 为 Deleted、资源释放、sandboxd 清单为空、本地 checkpoint 文件与 S3 对象库存均为空，平台正常停止。

证据：`out/ci/pause-resume/fc-r11/evidence/{sdk/result.json,lifecycle/result.json,s3-paused-manifest.json,s3-final.xml,catalog-final.json,inventory-final.txt,result.json}`。完整编译、打包、部署与用例日志为 `out/ci/stage-3/{linux-3,package-3,fc-r11}.log`。MinIO 测试制品为 `RELEASE.2025-09-07T16-13-09Z` Linux ARM64，SHA256 `5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d`。

本轮也修复了 checkpoint 返回与后端源执行退出之间的竞态：不再以一次 Running 查询拒绝暂停，转而等待幂等删除确认后释放容量。r10 为该问题的失败证据，`source-exit-red.log` 为定向复现；`node-green-5.log` 中 74 项节点测试通过。`snapshot-rpc-green-1.log` 验证真实 mTLS、租户隔离、分页与延迟删除，`go-catalog-green-2.log` 验证 API Server 新目录调用链。API Server 目录适配是在 package-v9 之后构建，因此 r11 不作为该 HTTP 目录链路的端到端证据。


本地制品确认丢失的回归：`missing-local-red.log` 复现原实现将 NotFound 当作临时存储故障、导致对账始终关闭；现明确返回 NotFound，权威 Paused 记录转为 Failed。`node-green-6.log` 的75项节点测试与 `clippy-4.log` 通过。API Server依赖裁剪后，`go-catalog-all.log` 的全部Go包测试、vet和API构建通过。

## 快照创建、节点重启与物理清理

`CreateSnapshot` 由 API Server 根据缓存的 Capsule 归属直达 Node Manager。节点先检查 Master 可达，随后在该 Capsule 的串行任务内完成 checkpoint、制品独立复制、目录发布和源实例恢复。成功响应要求快照目录及恢复运行结果均已提交到 Master。创建过程中源实例会短暂暂停；源进程恢复失败会返回失败，不能宣称源实例仍运行。内部中间恢复点使用 24 小时有效期，可复用快照自身不设过期时间。

复制、目录发布或暂停结果提交失败时，只要已有完整 Paused 恢复点，节点会尝试恢复源实例。发布结果不确定时保留复制制品，不能根据一次超时把可能已发布的快照删掉；远端未登记制品由下文的旧进程会话回收器处理。请求 ID 按租户及 Capsule 派生快照 ID，完成结果记录原请求版本；重复调用复用已完成快照，后续生命周期操作之后的旧请求不能重新创建或恢复源实例。

Master 的 `InspectNode` 同时返回 Capsule 及来源属于该节点的 Ready/Deleting 快照。SQLite 补写后重新获取完整目录，节点先校验所有记录再执行清理。本地快照即使源 Capsule 已删除或处于延迟删除阶段，也不会因节点重启被当作孤儿制品移除。

Master 每 5 秒独立扫描删除中且无引用的快照，通过带节点会话的 mTLS RPC 请求源节点清理。维护开关不阻止清理；未完成对账或失联的节点暂不接收请求。Node Manager 确认存储后端删除后，Master 以目录版本及 epoch 复核完成状态。丢失确认、存储错误和进程重启可重试；仍被其他目录或实例恢复点使用的制品拒绝清理。节点不可用期间物理删除会保持待处理。

本轮定向证据：`reconciliation-red.log` / `reconciliation-green.log`、`gc-red.log` / `gc-green.log`、`create-red.log` / `create-green-1.log`、`snapshot-commit-red.log` / `create-final-rust.log`，均位于 `out/ci/stage-3/`。节点 80 项测试、Master 7 项真实 Redis/mTLS 测试、Clippy、全部 Go 测试/vet、SDK 139 项单测及部署驱动 47 项测试已通过。

公共 SDK 的 `Sandbox.get_snapshot`、`list_snapshots`、`delete_snapshot` 支持可选 `connection=ConnectionConfig(...)`，可沿用源实例的地址、凭证和 TLS 校验设置。

### 创建与删除的真实 Firecracker 验收

2026-09-16，统一 `package-v12`（本地未提交工作树，`dirty=true`）在 Lima `adx-fc` 的 `/var/lib/adx-pause-r15` 上通过13项公共SDK/生命周期用例。可复用快照创建后源进程PID保持一致、内存计数继续增加、二进制文件一致；删除源实例后，SDK仍能查询和分页列出快照。删除快照后，Redis目录为Deleted且无引用，sandboxd实例、本地checkpoint文件和S3对象全部清空，停止流程成功。

严格验收器输出 `verified 13`。日志为 `out/ci/stage-3/fc-r15.log`、`fc-r15-acceptance.log`；证据为 `out/ci/pause-resume/fc-r15/evidence/`。该验收覆盖快照创建、查询和删除，不代表从快照创建新Capsule或Kubernetes验收已完成。

package-v13 / Lima r16 再次通过相同13项验收，并确认 SDK 命令订阅不再出现 `ssl=None` 或 `command watch unavailable` 错误。对应日志为 `out/ci/stage-6/{fc-r16,fc-r16-acceptance}.log`，证据为 `out/ci/pause-resume/fc-r16/evidence/`。


## 从可复用快照创建新 Capsule

公共入口为 `Sandbox.create(snapshot, connection=...)`。未指定的镜像、运行时、CPU/内存/磁盘从源快照继承。当前 Firecracker 实现要求显式资源与 checkpoint 的资源规格一致，不做恢复时扩缩容。调用者可指定新名称、环境变量和节点约束；新 Capsule ID 必须不同于源实例。

Master 将 `snapshot_id` 纳入创建规格与幂等校验；加入 Shard 内存队列前在 Redis 持有以目标 Capsule ID 命名的恢复引用。local-only 制品给每个候选节点条件附加源 NODE_ID 约束；共享存储沿正常调度路径选择节点。正在删除的快照仅允许已持有引用的请求继续，禁止新增克隆。Master 重启清理没有持久化分配的旧队列引用，保留已经分配的请求引用。

Node Manager 在串行控制任务内校验来源并复制独立制品，再调用 RuntimeDriver.restore_from。复制失败提交 Failed；创建规格声明了快照却缺少恢复点时拒绝执行。复制成功后的恢复文件保持引用直到执行停止，避免 Firecracker 后续读取时文件被缓存淘汰。目标的恢复点保存 checkpoint 内的源运行身份，供重启和再次恢复校验；之后对目标执行新暂停时，恢复点转为目标自己的运行身份。初次克隆的恢复点跟随实例清理，不单独设置暂停 TTL；显式暂停仍沿用请求 TTL。

sandboxd 适配器把恢复来源写入受控的恢复环境 `ADX_RESTORE_ORIGIN`，普通启动会清除同名用户输入。RRT 校验来源必须等于 checkpoint 内的完整 Capsule/执行/归属代数，并且目标属于新 Capsule；普通恢复保持既有版本校验。通过校验后更新身份、环境和认证信息并重新打开 HTTP/隧道监听。旧源身份的管控请求随后会被拒绝。

结果提交到 Redis 后，Master 维护任务释放源快照引用。删除源实例或源快照均不删除克隆的独立制品；克隆的暂停、恢复和删除继续由其所属 Node Manager 管理。执行完成而持久化前节点故障，仍按已定契约清理与持久化记录不符的执行，不根据未提交信息补造成功结果。

本轮 TDD 证据位于 `out/ci/stage-3/clone-{red,go-red,rust-green,rpc-final,go-final,sdk}.log`。package-v15 / Lima r18 已通过16项真实FC验收。两个新Capsule继承资源规格并保留原PID、内存计数和二进制文件；写入相互隔离。验收先确认源快照物理回收完成，再分别暂停/恢复/删除两个克隆。最终6个实例均Deleted且资源释放，sandboxd清单、本地checkpoint和S3对象全部清空，服务停止成功。

证据目录：`out/ci/pause-resume/fc-r18/evidence/`；严格顺序证据为 `sdk/snapshot-collected-before-clone-resume.json`，汇总为 `sdk/result.json`、`lifecycle/result.json`、`snapshots-final.json` 和 `result.json`。构建/打包/验收日志为 `out/ci/stage-3/clone-{linux-release,package-v15,fc-r18,fc-r18-acceptance}.log`。最终节点暂停/恢复测试21项、真实Redis/mTLS RPC 8项、SDK 139项、驱动6项通过；五个相关Rust包的全目标Clippy通过。该结果为同一Lima节点上的真实FC验证；跨节点迁移和正式Kubernetes验收分别跟踪。


## 未登记远端制品回收

对象上传或复制前先写 `owner.json`，记录节点 ID 和本次进程会话。文件上传完但结果未登记、或上传中途进程退出，都可能留下没有 Redis 权威记录的对象。回收器只处理**同一节点旧进程会话**的残留；当前会话、其他节点、没有归属标记或标记无效的历史对象均跳过。

启动时先恢复 SQLite 降级日志，重新读取 Master 的完整实例及快照目录，再完成执行状态和本机路由对账，最后开启回收。完整目录中引用过的制品在本次启动期间一直受到保护，后续删除由已有实例／快照清理流程负责。Master 不可用或对账未完成时，不启动新一轮回收；当前会话提交失败的制品会留到后续进程重启、完成对账和达到保留时间后再判断。

Node Manager 的 `checkpoint_gc` 配置：

```json
{
  "checkpoint_gc": {
    "enabled": true,
    "min_age_seconds": 86400,
    "interval_seconds": 300,
    "max_artifacts": 100
  }
}
```

所有子对象都达到最小存留时间、且没有本机恢复文件引用，才允许删除。后台任务在成功心跳后触发，单次并发为一，受 `rpc_timeout_seconds` 限制；失败留到后续轮次重试。归属标记最后删除，部分文件删除失败后仍能重新发现。日志输出每轮删除数量或失败原因。本地存储不执行远端扫描。

对象存储凭证需要该 prefix 的 List/Get/Put/Delete 权限。未完成的 S3 multipart upload 不属于普通对象清单，需要部署环境配置后端的过期中止规则。本实现不会自动推断无标记历史文件的归属。

本轮节点测试 85 项、真实 Redis/mTLS RPC 8 项、Clippy 与 FC 驱动测试 6 项通过。定向日志位于 `out/ci/stage-3/orphan-gc/`；统一 package-v16 / Lima r20 已通过17项真实 MinIO＋FC 验收，包含节点重启后的残留回收和原暂停实例恢复。


2026-09-16，r20 的 `orphan-gc.json` 确认当前会话残留受到保护，新会话完成权威对账后删除旧会话残留；已登记 checkpoint、其他节点与无标记对象保持。真实节点日志记录 `remote orphan GC completed: removed=1`，之后原进程 PID、内存计数和文件恢复通过；双克隆及所有节点生命周期场景通过。最终业务对象清单、backend 实例清单和本地 checkpoint 清空，平台停止成功。证据位于 `out/ci/pause-resume/fc-r20/evidence/`，日志为 `out/ci/stage-3/orphan-gc/{fc-r20,acceptance-r20}.log`。

同一 package-v16 的 r19 已通过新增回收场景，但随后克隆因测试磁盘触及 MinIO 最低空闲阈值而失败。确认历史测试进程已停止后，仅清理 r15/r16/r18 的 MinIO 内部 trash，保留历史日志，释放约13GB，再完成 r20 的全部17项验收。r19 失败证据保留，不计为通过。上述均为本地 Lima 验收，正式 Buildkite/Kubernetes 单独跟踪。

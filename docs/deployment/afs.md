# Agent FS（AFS） 部署与配置

Agent FS（AFS） 是 Agent DX 的可选文件系统组件。默认部署配置、默认发布包和默认 supervisor 服务都不包含 AFS 二进制、配置示例或 FUSE 依赖；只有显式设置 `ADX_WITH_AFS=1` 的文件系统包和部署入口才包含这些内容。

## 发布包合同

默认包不得包含：

- `bin/afs-meta`
- `bin/afs-node`
- `etc/examples/afs/`
- Agent FS（AFS） 专用端口、数据目录、挂载点或探针程序

显式文件系统包必须在 `manifest.json` 中记录 `with_afs: true`，并包含 `afs-meta`、`afs-node` 和 `etc/examples/afs/*` 的摘要。测试辅助程序只由验收流程按需构建和放入测试环境，不加入普通试用包。
组包完成后的校验与安装前／暂存后的校验共用随包
`lib/package_manifest.py` 合同；AFS ON、默认 OFF、旧 `with_dfs`、文件清单、摘要和
主机架构不会由两份独立规则分别解释。

## 配置示例

发布包配置示例来自 [build/config/examples/afs/](../../build/config/examples/afs/)，源码级示例保留在 [afs/examples/](../../afs/examples/)。部署层只负责把 AFS TOML 交给 `afs-meta` 和 `afs-node`，以及进程启动、健康查询、正常停止和卸载；它不改写 AFS 自有配置 schema。

Meta 后端优先级：

1. `memory`：一次性演示，重启不保留状态；
2. `local-file`：当前可靠性基线，中心节点可正常重启恢复；
etcd／Redis适配已移除；未来支持需重新设计和验收，不是当前可选后端。

当前默认使用 `local-file`。发布包内 `etc/examples/afs/meta.toml` 已显式设置 `meta_store = "local-file"`，并把 Meta 状态放在 `/opt/adx/data/afs/meta`，固定可恢复路径身份。使用 `memory` 时，验收报告必须明确“Meta 重启后不保留状态”。

### Meta 操作日志与数据目录

本工作树显式选择 `local-file` 时，Meta 在 `data_dir/meta-store/` 使用
`IDENTITY`、恢复清单 `CURRENT` 及操作日志 `wal-<首记录序号>.log`；
首段为 `wal-00000000000000000001.log`，安全回收后不要求首段仍存在。`memory` 使用同一记录合同，
但仅驻内存，重启丢失。每笔记录保存已决定的变化和原结果，持久批次确认后才发布可见状态。
恢复、身份／完整性检查以及恢复屏障完成前，不绑定 gRPC／REST 端口。

首次初始化先持久化空 `INITIALIZING` 标记，再依次安装身份、空 WAL 和初始 CURRENT，
最后移除标记并同步目录，完成前不接纳业务。仅可识别的未完成初始化前缀允许继续。
已初始化目录缺 CURRENT，或目录中有清单及退休集未列出的非空 WAL，均拒绝启动；
单个空预备段须核对身份、头校验和与恢复 tip。整目录被一致回滚且无外部见证不在检测能力内。

**此格式不导入旧 JSON `snapshot`／`wal`。** 非空旧目录会明确拒绝，原文件保留；
不得删除旧文件试图启动，也不得原地把旧格式当成新格式。试用本工作树需指定独立的新数据目录。

三个有限上限可由同名 TOML 字段或 `--meta-…-bytes` CLI 配置（CLI 优先）：

| TOML 字段 | 默认字节 | 边界 |
| --- | ---: | --- |
| `meta_record_max_bytes` | 67108864 | 含信封、结果、hash／CRC 的单帧上限，至少256、不超过u32范围 |
| `meta_batch_max_bytes` | 134217728 | 同批完整帧总字节，不小于单帧上限 |
| `meta_wal_max_bytes` | 1073741824 | 含4096字节头的日志总量，剩余容量须能容纳最大批次 |

单记录还受独立的共享向量容量限制：实体池、变更、原结果及epoch向量共用原合计
262144槽与按类型宽度计算的结构字节上限。写入与恢复采用同一容量规则，不能把
可用编码字节等同于可接纳的条目数，也不能把结构额度当作进程RSS保证。
超限整笔在追加／发布前拒绝，不拆成多个可见提交。
已知WAL额度不足时最多等待5s并唤醒快照；
等待期间准备结果不对外可见，超时返回不可用且不追加。真正追加后的错误仍失败关闭，
由重启确认原结果，不能按错误码猜测是否已写入。5s不包含前方请求的队列等待。
本工作树已有内部显式 checkpoint 安装和重启恢复：`CURRENT` 记录当前／上一恢复点、
所需连续日志段及已退休对象。快照绑定原 store／cluster／incarnation、位置和完整摘要；
恢复只应用后续记录。段按64MiB软目标轮转，完整批次不跨段拆开，合法大批次可超过软目标。
WAL硬上限同时计入保留、待删除和已准备的孤立日志文件，文件数内部上限64。
只有两个保留恢复点都不再需要的封口段，才可在CURRENT持久切换后删除；活跃段不删。
删除中断可按退休清单接续；损坏或外来退休文件保留并继续占额度，不盲删。

当前CURRENT内部格式为v2，最大64KiB；WAL记录信封为v2，状态image为schema2。
不提供早期实验v1清单／记录或schema1 image的自动导入；格式不匹配时拒绝恢复，
保留原目录及证据，不能手改格式或拼接目录。管理记录可推进日志位置而不改变业务revision。
canonical路径已接入历史计账和运行时清理。请求结果及事件历史可通过
[操作日志策略参数](#meta-操作日志有限策略)配置；下表列默认值，待处理输入与准备身份仍为内部边界：

| 对象 | 内部边界 | 达界行为 |
| --- | --- | --- |
| 原请求结果 | 100万条、256MiB owned逻辑计账、24h | 时间／容量任一先到即确定淘汰，持久后发布；原完成时间不延长 |
| 事件历史 | 100万条、64MiB owned逻辑计账、1h | 按完整revision前缀推进持久水位，旧游标返回Compacted |
| 待处理Meta输入 | 1024排队槽及128MiB owned逻辑额度 | 满时在追加／发布前明确拒绝；调用者取消不提前退写者持有额度 |
| 在途／准备／退休身份保护集 | 1088身份及4MiB逻辑额度 | 从入队保护至处理终态，取消不提前退账；退休冲突在入队前拒绝，不豁免历史总额 |

历史树根另计固定64KiB结构预留；每次清理最多4096个请求身份／完整revision组。
定时轮询默认1s，持续请求不会重置到期时间；前方持久I/O仍可能延迟实际执行，
不承诺严格墙钟可见时限。历史字节含完整owned载荷、引用及结构预留，是逻辑上界计账，
不是allocator统计或RSS保证，也不是Node写正文／S3预算。已淘汰身份在窗口外重试
可能重新执行，仍受原业务条件和权限约束，不承诺永久exactly-once。未知持久结果
触发失败关闭并禁止后续管理追加，由重启核对原记录。

canonical读视图当前还有以下内部上限，尚未暴露为部署配置：

| 项目 | 内部默认 | 计账／失效边界 |
| --- | ---: | --- |
| 同时持有视图 | 128 | 每个视图占一个槽，达界拒绝新视图，不阻塞权威提交 |
| 固定状态持有字节 | 8GiB | 包含主实体与历史的owned容量及结构预留，按4KiB向上计费；同一State共享，跨代际保守全额计费 |
| 视图期限／回收轮询 | 30s／100ms | 空闲过期视图主动解引用；执行中引用未结束不提前退账，过期结果报错 |

8GiB是逻辑准入上限，不是预分配、RSS保证或Node正文额度。另有由视图数限制的
有限登记元数据；不能用上述字节数代表整个Meta进程内存。过期或已知权威失败后须
重新取视图。旧Snapshot后端已移除，不保留绕过此准入的旧读取路径。
已返回的独立结果对象不在读视图计账内。watch取数和Meta自有响应队列还有独立内部上限：

| 项目 | 内部默认 | 边界 |
| --- | ---: | --- |
| 单次watch响应 | 16384事件／16MiB owned计账 | 含64KiB基础预留；请求limit=0仍受限；完整revision不可拆开 |
| Meta响应构建／流式队列 | 16槽／512MiB逻辑预留 | 每响应先预留32MiB，覆盖域批次与wire转换期间的共同持有；固定登记元数据的保守上界另为64KiB |
| 响应期限／回收轮询 | 30s／100ms | 取数前准入；无人读取的已安装队列到期主动清空；仍在构建者退出前不退账 |

响应超限时返回此前完整revision；首组本身超限返回容量错误，需恢复扫描或重新配置，
不把容量不足伪造成Compacted。历史确实已压缩时仍按原游标重扫。过期流明确报错，
调用者只能从最后完整处理的批次续接，不能因部分命令已收到就确认整组。
wire计账核对实际Vec／String capacity，不采用protobuf编码长度。新建Meta不依赖运行时；
首次异步准入才启动一个弱引用回收任务，不每响应创建计时任务。
该额度约束Meta自有的构建和流式队列；一元poll返回值单批有界，返回后由调用方／
公共RPC编码与流控持有。tonic/h2／socket缓冲及整个进程RSS不在此额度内。
上表响应额度仍为内部默认；历史策略配置与固定指标已接线，最终资源及完整运行验收
仍待完成，不能据这些软件回归宣称有界历史部署验收通过。
LocalLog锁绑定创建进程，不能把fork继承的对象当成新写者；最后真实持有者释放锁，
同进程clone或尚在执行的后台I/O仍保留锁。正常释放重开不改变持久化身份。

内部checkpoint上限为单份编码1GiB、捕获逻辑计账2GiB、相关文件合计4GiB／8份；
临时文件预留实际磁盘块，隔离证据至多2份。这些不是RSS保证，内部控制项尚未暴露为部署配置。
后台初始软阈值为128MiB日志估计量、100万条记录或60s，轮询1s；段字节估计包含
横跨快照位置的整个段，不是精确增量。观察期限30s，超过期限仍由原任务持有单飞及资源，
不启动第二份。阈值／等待暂为内部策略，未提供管理RPC／CLI。

**后台快照、有界等待、六点进程故障、运行时历史回收、canonical读视图和watch响应／队列约束已有有限回归；配置／指标已接线。正式资源及完整运行验收仍未完成，不能作为持续增长部署的完成版本。**
安全回收删除两个保留恢复点均不再需要的封口段／退休镜像，并可在恢复链验证后回收
未引用、身份及完整校验均有效的本库 checkpoint 镜像。损坏、未知和外来对象保留计账；
不能通过无上限调大参数或手动删除日志解除压力。
旧快照后端、etcd／Redis适配及其依赖已移除；不保留双写、旧格式导入或兼容feature。
这些改动不表示已有 FUSE、物理掉电或性能验收自动适用于新格式。

单机试用前先准备配置、数据、运行时、挂载目录和本机临时证书：

```sh
sudo install -d -m 0755 /opt/adx/config/afs /opt/adx/run/afs /mnt/adx/ownerfs /mnt/adx/dfs
sudo install -d -m 0700 /opt/adx/data/afs/meta /opt/adx/data/afs/node-a
sudo cp /opt/adx/current/etc/examples/afs/meta.toml /opt/adx/config/afs/meta.toml
sudo cp /opt/adx/current/etc/examples/afs/node.toml /opt/adx/config/afs/node.toml
sudo /opt/adx/current/etc/examples/afs/prepare-local-tls.sh
sudo cp /opt/adx/current/etc/examples/afs/deployment-ownerfs-local.yaml \
  /opt/adx/config/deployment.yaml
sudo adxctl validate
```

证书脚本只创建 30 天有效的本机隔离试用 CA、Meta 和 `node-a` 身份，并在已有
任一目标文件时拒绝覆盖；它不分发固定私钥，也不适用于生产或多主机扩容。生产部署
必须由自己的 CA 为每个 Node 签发独立身份，并在 Meta 和 peer 的
`trusted_node_certs` 中把证书精确映射到该 Node ID。不要复制 Node 私钥到其他主机。
证书绑定表不能替代 TLS 配置；启用 OwnerFs 或 DFS 权威的 Meta 缺少 mTLS 时，
必须在监听端口和打开持久后端前拒绝启动。

示例 Node 配置使用 mTLS `https://` endpoint、`/opt/adx/data/afs/node-a` 和
`/opt/adx/run/afs/node-a.sock`。前台运行 `sudo adxctl run` 后，在另一终端用
`sudo adxctl status` 检查 Meta 与 Node 都返回 `status=ready`，并用
`sudo adxctl stop` 正常停止。Supervisor 按 Meta→Node 排序启动；Node 在尚未挂载或
暴露本地 API 前对首次 Meta 注册做有界、可取消重试。预算耗尽、证书/身份拒绝或运行后
心跳/排空失败仍然失败关闭，不会无限重试或盲目重启。

如果启用 FUSE 挂载，先确认 `/mnt/adx/ownerfs` 与 `/mnt/adx/dfs` 为空目录，再在
`node.toml` 中启用对应 `ownerfs_mount` 或 `dfs_mount`。挂载所有者捕获的 canonical
path、mount ID 和 source 同时作为 readiness 身份；挂载被替换时健康检查继续失败关闭。

## OwnerFs workspace bind ON

普通发行默认关闭 workspace bind。当前实际试用场景需要显式开启，示例字段以 `build/config/examples/afs/node.toml` 为准；历史 `native_workspace` 命名只作为兼容入口，不能作为新的核心能力名称。

开启条件：

- 先通过 OwnerFs 正常创建并授权 workspace，再开启 bind；不要在 Home/root/epoch 尚未固定时直接打开开关；
- workspace 必须是已授权的本地 Home workspace；
- bind 源必须是 Home 上的底层真实目录；
- bind 目标必须是 OwnerFs FUSE 根下对应的一级目录；
- 同一 Node 只启用管理员控制的 host bind entry；
- 停机、root/epoch 变化或授权失效前，先停止受管用户并排空引用。

## 运维边界

启动前一次性检查二进制身份、配置 SHA、端口、TLS、数据目录、运行目录、日志目录、挂载目录、FUSE 设备、`fusermount3`/`umount`、旧 PID、旧 UDS、旧挂载和磁盘余量。遇到环境阻塞时保存证据并停止受影响项，不把环境修补混入产品结论。

正常停止应记录进程退出状态，确认 FUSE mount 和 bind mount 已卸载，UDS/PID/临时 claim 无遗留。`local-file` Meta 的恢复检查至少要覆盖写入、sync/close、正常停止、重启、重新打开和内容校验；这不等于 crash recovery、HA、多 Meta 或复杂可靠性通过。

## DFS 外部 S3 连接

这是实验接入，先验证小规模发布／回源闭环，尚不承诺性能或完整故障验收。
在新 Meta 和参与的 Node 中添加相同的身份与后端连接配置；Node 另需凭据。
保留默认 DFS 单副本计数，任何外部后端与多 Node 副本的组合均在启动前拒绝。

```toml
[dfs_external_backend]
store_id = "filesystem-a"
generation = "generation-1"
endpoint = "https://s3.example.com"
bucket = "afs-chunks"
region = "us-east-1"
allow_http = false
unsigned_payload = false
# Node-only; omit on Meta. Must be absolute, owned by the Node process UID,
# regular file without symlink, and mode 0600 or stricter.
credentials_file = "/opt/adx/config/afs/s3-credentials.json"
```

凭据 JSON 包含 `access_key_id`、`secret_access_key` 和可选 `session_token`。
不在示例中提供真实凭据，不把秘密直接放 TOML 或命令行。未指定文件的 Node
显式读取 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 和可选
`AWS_SESSION_TOKEN`；缺失凭据启动失败。`--print-config` 只显示文件路径。
默认 `unsigned_payload=false`，保留 S3 载荷签名。只有管理员确认后端和安全策略支持时才显式设置 `unsigned_payload=true`；该项仍保留 SigV4 请求
认证，只省略 SDK 的载荷签名 SHA256，不替代 AFS 完整 BLAKE3／长度检查，也不
改变成功 fsync 等待对象回执和 Meta 持久确认的要求。它不自动探测 SeaweedFS，
不因 `allow_http=true` 自动启用；泛 S3 后端和既有配置保持原默认。

默认 HTTPS；`allow_http=true` 仅供隔离测试端点。外部服务的认证、bucket
权限、数据同步落盘及数据库持久配置由管理员在服务端保证；HTTP 成功本身
不能证明物理持久性。产品包不部署或打包 SeaweedFS、PostgreSQL 或 JuiceFS。

Meta 的 `meta_store` 是独立配置：默认 `local-file`，也可显式选择易失的 `memory`。
本接入不增加 PostgreSQL Meta。etcd／Redis 选项和连接字段已退役；旧数据需保留原二进制与原目录。
对象 key 身份不允许变更后重新使用同一 Meta 数据目录；配置错误及 GET/PUT
错误向调用方传播。正常停机仍需先排空工作负载和 FUSE 引用。

S3 适配层共用一个 worker、运行时和客户端，固定最多四个实际 PUT；完整
4MiB 块可以提前上传，完成后滚动补位。最多32个活动命令和一个排队命令，
队满等待回压；共享客户端最多保留四个空闲连接。空闲连接池大小本身不是并发限制。

顺序追加以64MiB为一个发布单元，冻结窗口上传期间可按预算余量接纳下一窗口；
普通 write 返回表示已接纳，中间版本稍后发布。独立只读节点读取已发布版本，
同挂载和已有 owner 代理的 dirty read 先完成读入口前已接纳序号的有序发布，
发布失败报错，不能回退旧内容。fsync、fdatasync、可写 flush、同步写
和必要排空必须等待有效对象回执及 Meta 持久确认，不能仅凭 PUT 成功返回。
覆盖写、稀疏写和截断先收口相关窗口，再使用原有提交路径。Meta ACK 未知时保留
原请求身份并阻挡后续 mutation；PUT 失败保留脏数据和可复用的成功回执。

Node 写流水线采用分配前预算：普通数据128MiB，发布保留32MiB，总计160MiB；
共享内容只计一次，失败、准备提交和未知 ACK 期间仍存活的内容不能提前退账。
PUT 直接持有已封口的不可变载荷及其预算租约，不再复制整块 HTTP body；
发布构块使用独立串行保护，仍存活的 transport／retry owner 释放前不得退还额度。
这项预算与缓存容量、缓存 pending、读取缓冲及进程其它开销分开核算，不能作为
总 RSS 上限。失败或超时可能留下未引用的不可变对象，不能直接删除可能被引用的对象。

外部模式的当前恢复范围是读取已经发布的版本；写节点重启后重新取得写权
另行验收。旧写租约有效时，新会话立即复写原文件可能返回 EBUSY，不能将
“退出后可回源读取”理解为“新会话立即恢复写入”。

### 可选的异步内容缓存

仅在 Node 配置中增加以下子表，省略时保持 cache OFF；Meta 不使用此缓存配置。
缓存目录必须专用、绝对路径、由 Node 进程用户持有且权限为0700；父目录须已存在。
不要与 Node 耐久 chunk 目录、源码共享目录或其它服务数据目录混用。

```toml
[dfs_external_backend.cache]
directory = "/opt/adx/data/afs/s3-cache"
capacity_bytes = 268435456
pending_bytes = 37748736
cache_large_write = false
```

`capacity_bytes` 约束当前 store/generation 的 ready 文件及临时文件预留的内容字节，
非零且不超过1TiB；`pending_bytes` 约束被缓存接纳的排队、正在落盘、单个已验证
驻留槽及读者仍持有的分配容量，共享分配只计一次，非零且不大于
capacity。它们不是文件系统配额或进程RSS上限，索引、文件系统块分配、读写工作缓冲
及其它历史目录需另计环境预算。普通填充资源不足时跳过缓存，不阻塞后端确认。

完整GET校验成功后可异步填充，读取优先复用Node共享的单个已验证对象（最多4MiB），
再查pending或加载完整缓存文件。驻留不足时回退原路径，不扩充额度。
外部只读每次 open 在线获取最新版本，版本对象授权可复用最长5秒，同时受
session／grant 截止限制；每次 read 仍检查合法范围、期限和已知失效。Node
停止／epoch 变化关闭复用；Meta 断链时新 open、新对象及到期续期正常失败，
已授权范围只可继续到原截止。未知 Meta 重启不保证瞬时撤销，恢复后重新在线
准入。此固定上限不是新增配置开关；授权元数据最多128项／1MiB，与载荷分账。
来自磁盘的新内容仍安全检查并完整BLAKE3校验。RAM命中
不检查磁盘副本后来发生的变化，下次磁盘加载负责发现损坏并回源。小于4MiB的写入块默认允许填充，完整块
仅在`cache_large_write=true`时填充；后台写盘可与上传重叠，但不产生持久回执。
缓存损坏或读取失败回源S3；S3请求、授权和持久提交错误仍向调用者传播。
正常退出排空并join后台缓存工作，不要求cache fsync。

cache ON 必须按[验收说明](../testing/afs.md#dfs-外部-s3-闭环)登记对应版本的实际 Linux 结果；
不能由配置可解析或工具就绪推导功能／性能通过。SeaweedFS部署继续归外部测试方案。

## FUSE 接收与 DFS 页缓存配置

以下是Node顶层TOML键，也可通过同名连字符CLI选项覆盖；Meta不使用这些挂载参数。

| 参数 | 默认 | 范围与作用 |
| --- | --- | --- |
| `fuse_threads` | `4` | `1..=64`，OwnerFs／DFS接收线程 |
| `fuse_clone_fd` | `true` | 仅多接收线程时生效 |
| `fuse_workers` | `32` | `1..=1024`，有界callback executor，队列容量为worker数×8 |
| `dfs_fuse_keep_cache` | `true` | 仅原生DFS只读、已核对提交身份的open允许保留页面 |
| `dfs_fuse_entry_ttl_ms` | `0` | `0..=60000`，路径entry TTL |
| `dfs_fuse_attr_ttl_ms` | `0` | `0..=60000`，属性TTL；CREATE的单TTL回复取两者较小值 |
| `fuse_max_readahead_bytes` | `1048576` | `0..=16777216`，仅DFS的内核read-ahead请求上限，实际值受协商限制 |
| `dfs_version_cache_entries` | `4096` | `1..=65536`，不可变版本／布局的resident元数据条目数 |
| `dfs_version_cache_bytes` | `33554432`（32MiB） | 标量范围`1..=1073741824`；具备DFS能力的Node还校验与entries相关的最低结构预算 |
| `dfs_outboard_work_wait_ms` | `2` | `0..=50`，共享 proof 工作额度不足时的最长准入等待；0 表示不等待，仍保留完整校验和原全扫回退 |
| `dfs_peer_serve_resident_objects` | `2` | `1..=16`，A端已验证完整对象的LRU条目上限，每对象至多4MiB |
| `dfs_peer_serve_payload_bytes` | `16777216`（16MiB） | `4194304..=67108864`，A端驻留、填充、退役pin及packed/RDMA共用的实际正文capacity上限 |
| `dfs_peer_serve_wait_ms` | `20` | `0..=50`，native gRPC 单入口准入等待期限；0不等待，不增正文或窗口额度 |
| `dfs_source_cache_max_ms` | `5000` | `0..=60000`，源元数据缓存的最长保留时间，另受grant安全截止约束；0不保留且不前瞻 |
| `dfs_source_lookahead_chunks` | `64` | `0..=128`，仅后续源元数据；与mandatory合并请求至多128项；0仅关闭前瞻 |
| `dfs_source_cache_bytes` | `4194304`（4MiB） | `0..=4194304`，最多1024条源元数据的保留估算上限；0不保留且不前瞻 |
| `dfs_readahead_max_bytes` | `0` | `0..=33554432`；0不创建后台fetch任务；product可显式设8388608。这是对象调度距离，未新增正文、proof、连接或磁盘额度 |

TTL均保持0才属于严格路径新鲜度配置；正值是显式宽松模式，rename替换等路径
可能在TTL内继续解析为旧inode，不能宣传严格close-to-open。OwnerFs原Home缓存、TTL、
direct-I/O与mmap策略不由上述DFS参数覆盖。read-ahead不等于对象预取；测试必须
记录有效`read_ahead_kb`和应用open flags，不能用配置请求值冒充协商结果。

DFS复用以完整提交版本ID及长度为准；新open仍在线检查权限和Meta。dirty、provider、
可写或外部S3句柄不取得此复用资格，身份表超限时保守不保留。已经交给内核的页面
不再次经过用户态授权，不能解释成可立即撤回的远端载荷。

提高worker数会增加可同时存活的私有读取结果。原读配置是每操作上限，不是Node全局
RSS配额；A共享正文、B填充、帧、元数据、allocator和内核页缓存须分别核算。
默认线程数也不构成吞吐或时延达标保证。

A端serve正文默认从8MiB调整为16MiB，不能将旧资源上限的结果直接标为新配置证据。
最多8个对象key同时填充，同对象完整结果可共享，partial仍走原范围路径；容量不足
立即返回结构化容量错误，不增加等待队列。控制元数据固定不超过64KiB，独立于正文和
原proof账本。停止后不接受新对象或packed准入，正常排空等待最后引用，首次关闭起
10秒仍未完成则报错。此配置不改变B端磁盘缓存、S3预算或每RPC的独立授权。
健康标准对象的完整proof请求自动选择32KiB覆盖组校验发送，单stream两块合计64KiB
实际载荷仍计入上述A端额度，控制元数据仍受64KiB控制包络约束；没有新增配置或总预算。
帧仍被下游引用时不归池／退账。legacy、partial、缺proof和Header前可选准入不足保留原路径，
Header后出错中止流。该路径不改变RDMA或B端整对象核根要求，不保证单靠此项达到吞吐目标。


源缓存的1秒grant安全余量固定，期限由请求发送前的单调时钟与墙钟锚定，Meta处理／传输
时间不会在回复或安装时被补回。显式配置max超过5000ms不代表授权也获得同样延长；
原grant、Node/session和每个读取上下文的原截止仍分别生效。native与external路径的源元数据
共用这份账本；external授权的原1MiB和S3正文／descriptor预算独立保持。
4MiB不包括当前RPC reply／读取clone或正文。低字节限可能缩小有效条目容量；超大单项
当前仍可依法使用，但不进入缓存，不提供保底例外项。仅可选ID跳过512B以上的值，
最多128项的每份新增advisory字符串正文≤64KiB，String槽与请求／集合副本须另计。

未发布接口统一为`dfs_source_cache_max_ms`，旧`dfs_read_source_cache_ttl_ms`／
`--dfs-read-source-cache-ttl-ms`已移除，会明确报未知字段／参数；请更新为新名，不能继续
依赖旧配置或静默回落。新名按默认值＜TOML＜CLI解析，最终组合再校验。


版本缓存不保存权限或正文，不代替新open的在线检查。entries与bytes双上限均在启动时
生效；bytes按版本、布局、索引的容器capacity、各ID字符串capacity和缓存容器包装
保守估算，不只统计extent数量。超大布局仍能被当前操作使用，但不被缓存保留；淘汰后
在途读者仍可持有其Arc。此上限不等于所有活跃引用、加载临时分配或整个Node RSS上限，
不能据命中率推断新鲜度或授权有效。

超大版本未被保留时，后续需要该布局的每次用户态读取可能重新执行Meta
`get_file_version`、传输完整布局并重建索引，存在比原单项缓存更高的控制成本；
内核页缓存命中不进入该路径。缓存还预扣与entries相关的保守容器开销T，只有
`T + 单项保留估算 <= dfs_version_cache_bytes`时才可能接纳该项。当前64位构建中
T约为`2048 + 384 × dfs_version_cache_entries`字节：默认4096项约1.50MiB，65536项
约24.00MiB；这是实现估算，非实测RSS，也不是固定文件大小阈值。具备DFS能力的Node
按CLI／文件最终合并值校验`bytes >= T + sizeof(ReadOnlyVersion) + 2*sizeof(usize)`，
不足时拒绝启动，并报告该entries配置的minimum。表开销与运行缓存复用同一公式；
这里只是固定字段与Arc头的结构下界，不假定ID字符串长度，也不保证任何真实布局都能缓存。
真实字符串、extent和索引capacity仍逐项计费；超大／碎片化布局超过上限时不新增例外槽，
每次需要该布局的用户态读可能重新加载Meta。可同时减小entries或增大bytes，
并按实际布局估算单项成本；Meta进程及未编译DFS的OwnerFs-only进程不分配该DFS缓存。

## 原生 Peer 可丢弃磁盘读缓存

仅供未配置外部后端的 DFS Node；配置在读者 B 上即可，普通缺省 OFF。
目录必须是独立绝对路径，与 `data_dir`、FUSE挂载点互不嵌套，不含 `..`，由Node进程
独占使用；初始化遇到不明文件或不安全身份应报错，不清理其它目录。

```toml
[dfs_peer_cache]
directory = "/opt/adx/cache/afs/peer"
capacity_bytes = 268435456
```

容量可配置，非零且不超过1TiB；256MiB只是示例。缓存保存不可变对象的临时副本，
近似LRU自动淘汰，条目最多4096；v2文件为4096B固定header、正文和至多8192B的
Bao outboard trailer。ready与pending实际分配受磁盘容量限制，目录及marker开销另计。
旧v1专属缓存条目可退休重建；这不是持久数据格式迁移，不得删除A的唯一持久副本。
回滚到只识别v1的旧Node时，不能直接复用包含v2 ready文件的缓存目录；应先正常停止
Node，再按专属缓存目录的清理合同退休可丢弃条目，或配置新的空缓存目录／禁用可选缓存。
此操作仅针对B的可再生缓存，不得清理A的持久副本、catalog或Meta数据。

`dfs_fetch_max_inflight_objects`默认8，范围1..=8，可由CLI覆盖TOML；限制原生Peer
磁盘缓存的metadata flights，不适用于S3，也不代表8路正文。当前最多两个流式fill
共享原4MiB逻辑transport包络；legacy完整对象fill保持独占，不与流式fill并存。
Node总共只有两个256KiB载荷块、256KiB控制额度；两个流固定复用同一Channel，
两个流共享不超过2MiB的connection窗口，每流窗口也不超过2MiB，解码消息仍限128KiB。
不同连接身份等旧会话
真实排空，不能每流再申请两块缓冲或另开一个连接窗口。私有结果按原每操作读取预算
另计，4MiB不是Node总RSS上限；proof沿用原账本，不新增额度。

每次回填有独立代际pending路径、文件身份、元数据和磁盘预留；ready、retired与pending
实际分配共同受`capacity_bytes`约束。失败删除或外来替换继续收费，不触碰其他活动文件。
最后一个生产者、块或请求释放后才销毁共享缓冲、退还观测reservation；空闲会话不常驻
512KiB正文。外部S3的`pending_bytes = 37748736`仍只属于外部缓存，不可转用。
原生只读句柄在成功交付后检测固定版本上的顺序访问，允许距上次结尾至多1MiB，窗口按
1、2、4逐步增长；随机跳转清零，版本改变重新开始。对象数按每对象4MiB保守折算到
上述距离并封顶8。后台只消费已缓存且未过期的完整对象grant，不查询Meta，不等待
正文、proof、任务或连接额度，前台等待者优先；未命中可用额度直接放弃。外部S3、
dirty和owner代理读取沿用原路径。可丢弃cache须启用，transport还须满足原小窗口限制。

后台只写磁盘sink，以空输出绑定进行完整校验，不申请假4MiB输出；固定控制结构、任务
上下文、字符串capacity与flight继续从原proof账本收费。前台跨对象任务复用原复杂batch
的请求总长scratch，全部join并核权后才复制到私有最终结果；回退前先释放分片scratch。
停止先取消再真实join producer及fetch任务，不以丢弃JoinHandle冒充排空。提交前重查
grant、来源generation、句柄已知拒绝和Node停止；最后检查与rename之间沿用既有极窄
窗口，未引入全局撤销锁；任何后续公开读取仍独立授权，不从Ready继承读取权限。

当前原生读的资源边界如下。重叠包络不能简单相加，配置上限不是实测RSS；实际分配、
在途引用和最后引用退账还须在独立资源诊断中核对。

| 资源 | 当前额度／范围 | 归属与释放 |
| --- | --- | --- |
| A正文 | 默认16MiB，4..64MiB；默认2个resident | 填充、resident、退役pin及packed共用；末正文引用释放 |
| A控制元数据 | 固定64KiB | 有界服务状态与ID，不挪用proof或S3额度 |
| B共享流式包络 | 逻辑4MiB，最多2流、1个实际Channel | 两个256KiB块、256KiB控制额度及原transport积压共享；实际最后owner结束后退账 |
| B前台私有输出／分片 | 原单批最多8MiB，非Node总上限 | 分片合计复用原请求scratch，失败先join和销毁再回退 |
| B后台预读正文 | 不保留完整正文或伪输出 | 仅原共享块到磁盘sink，正常EOF／完整核根后才可安装Ready |
| proof及任务元数据 | 原总账1MiB，resident子限512KiB | A／写pool／B索引、proof、fetch固定状态及任务capacity共用；不是新增两池 |
| fetch任务／对象flight | 默认8，配置1..8 | 唯一FillFlight对象表，任务只有控制和结果；任务数不等于正文并发数 |
| B磁盘 | `capacity_bytes`，示例256MiB | ready、pending、retired及deleting实际分配共同计费，真实删除前不退账 |
| 源／版本元数据缓存 | 默认4MiB／32MiB | 与上述正文分账；当前reply、在途clone及allocator另计 |

要在product测量中启用对象预读，应在Node顶层显式配置
`dfs_readahead_max_bytes = 8388608`，先启用上述专属磁盘cache并用实际二进制
`--config /absolute/node.toml --print-config`确认A/B有效值与配置SHA。contract固定0。
不添加字段时保持默认OFF，不能将旧配置的“AFS prefetch=0”收据作为ON证据。具体
测试启停、参数冻结和结果归档见[验收说明](../testing/afs.md#原生对象调度与磁盘预读回归)。

表内indexed、retired与删除中条目合计最多4096，实际文件删除前磁盘占用不退还；被pin的退休条目
也占容量，同key不能在旧文件仍被使用时重建。容量或控制元数据不足保留安全回退。

索引、entry及proof使用同一原proof ledger的resident子限；每个operation的8KiB
快照控制包络与有界flight使用该ledger的瞬时额度，128个operation是结构上限，
不保证都同时取得快路。该包络按有界容器和字符串容量保守计量，不等于allocator或RSS。
无控制额度的命中可pin并完整核根；不能将安全回退写成命中失败或损坏。
正常停止保留健康ready供恢复；失败pending删除仍可能留到下次独占恢复处理，
不得手动删除身份不明的文件或把它们当作已经释放的空间。

原1MiB proof ledger由同一Node的A store、原生写pool和B cache共用；计入proof实际
capacity、Arc／包装、构建／encode／decode临时分配及证明索引（当前64位构建为32KiB），
在途引用直到最后owner释放才退账。总额的一半（默认512KiB）作为常驻保留子限，
A pinned／Peer正文proof和B proof／索引包装共同受限；不是额外额度或另一池。条目淘汰但
引用仍活跃时不能提前退账。临时验证／写入builder共享剩余总账，超过可用额度允许安全
回退，不保证默认32 workers都取得快路径。容量不足不能当作健康proof损坏并触发反复
重建或写盘；对应准入失败、原生写无proof和范围全扫回退需要分别观察。不挪用正文额度。它不代表进程总RAM；codec、TLS、HTTP/2及内核pagecache
仍按原独立预算核算。这里没有新增正文RAM驻留缓存；可选正文预取仅写入上述磁盘缓存，
须显式开启且继续使用原额度。

本机读、A端Peer范围服务及B磁盘命中统一使用标准BLAKE3根与32KiB覆盖组outboard。
proof包含64B格式／长度／根header和parent CV，4MiB对象最多8192B；命中仍读取、
校验覆盖组；连续对齐范围直读私有输出后用官方range validator验证，仍有32KiB
scratch复制；至多128个无别名范围且有完整中部时，非对齐／多范围直读私有结果并补齐覆盖组边界。其它情况保留原codec，不承诺零复制。本机规范ChunkId的根／长度
须与Meta布局及catalog一致，无需新增Meta RPC；legacy ID或持久副本缺失、旧／坏sidecar
走原完整根扫描。sidecar是可选优化：正文、chunks目录和catalog必需屏障完成后，
安装工作进入有界后台队列，不等待sidecar文件／目录sync再返回ACK。
`dfs_sidecar_install_queue`默认64、范围1..=64，限制queued＋active；每批最多32项。
proof引用和控制容器／ID／路径容量都计入原1MiB ledger，控制容量空闲时仍计费，
最后引用及真实释放后才退账；不新增正文缓存或Node全局写额度。
队满、瞬时元数据不足、停止或安装失败只放弃可选项并计数；安装前的读可全根校验并
按需修复。健康proof逐字节匹配后不重写／rename／file sync，后台批末独立目录sync。
可选proof失败不改变正文、catalog、receipt及成功fsync屏障。该队列不是文件版本提交入口。
停止时关闭入队且固定10秒截止；超时不启动新的可选目录sync，不取消已开始I/O或提前
释放引用；真实join若卡住仍由既有15秒watchdog以124结束，不能当正常排空。
`afs_dfs_sidecar_install_items{state="queued|active|limit"}`与
`afs_dfs_sidecar_install_control_bytes`分别报告项数和有界idle控制容量；
`afs_dfs_outboard_events_total{event="sidecar_install_dropped|sidecar_install_failed"}`
报告可选工作处置事件，不表示写入持久化失败或精确对象完成数。
读侧也可同步修复缺失／损坏的sidecar，安装可能产生文件与目录sync。健康sidecar仅
在固定共享修复锁内比较而不write／rename／sync，避免同对象重复安装；已验证Peer正文
LRU命中不再访问sidecar。修复不持
Peer正文表锁；这条读修复仍同步执行，与写ACK后的可选installer区分。缺容量时走完整根安全读取，不能借修复改变
原正文、catalog或成功fsync合同。

并发范围验证的快路径受原proof总账约束：对齐单范围工作票据36KiB（32KiB scratch
加4KiB元数据余量），非对齐／多范围仍为76KiB。resident接近512KiB且没有其他临时
占用时，剩余约512KiB分别容纳14张／6张票据，实际取决于占用及调度重叠。
准入使用入口固定的截止时间，退账可唤醒等待者，伪唤醒不延长等待。停止时唤醒
等待者，不提前退还仍被引用的分配；builder和常驻proof准入仍不等待。
超过截止时间仍拿不到票据时保留安全全根扫描，例如128KiB范围读4MiB对象可产生
32倍正文读取／hash放大；默认32worker不等于32路均走范围快路径。此退化不重建或
落盘sidecar。性能／容量规划须同时采集全账、resident和全扫事件，不能只看缓存命中率。

符合multi-thread runtime、有界HTTP/2窗口及原读预算条件时，gRPC使用单producer／
consumer与两个256KiB固定载荷块，wire帧仍至多64KiB，块归还后才能复用。
首正文前固定唯一核根者：有预算支持的完整Tree（远端proof或本机构建）时由consumer
计算标准根，并与Meta锚及Completion核对；旧Peer无proof不要求重复计算正文根。
Tree不可用时移除root-only builder，由producer（同步入口为原checksum路径）独占完整
核根。模式在Header固定，不在正文中途切换；实际EOF、join、私有输出绑定和最终授权保持。
`afs_dfs_peer_read_events_total{event="root_fallback"}`统计回退选主次数，
`afs_dfs_read_hash_bytes_total{stage="root_fallback"}`仍是另一条回退扫描的字节，不能混用。
合法缓存覆盖组校验是单独工作，不能从正文唯一核根推断总hash等于应用长度。
有完整中部且无别名的至多128个范围可直接读入私有结果，再补齐覆盖组边界校验；
源／输出别名或小范围保持codec。原77,824B非对齐票据与完整校验不变，
这不新增可配置正文RAM缓存；正式收益需要同条件E2E测量。
两块仍按原预算交接，是否重叠以实际阶段观测为准。原4MiB填充额度与本次私有输出不能超过原单批预算；
不满足条件沿用完整对象缓冲回退，不增加队列、连接数、窗口或线程池。

整个当前chunk获授权时才请求整对象proof；partial保持精确授权正文与range checksum，
由A核对本地可信根覆盖组，不宣称B取得独立整对象证明。
旧Peer缺少可选字段时完整根安全回退；格式、长度或对象绑定错误拒绝。
实际EOF、成功join、完整根／CV、身份和最终授权都通过前，不产生Ready或公开输出。
缓存创建／配额／写盘失败仍以同次RPC完成强校验和合法区间交付；Meta／Peer真实错误仍传播。
文件身份变化使证明失效，未请求覆盖组的静默损坏在读取该组时检测。

正常退出等待活动operation结束，停止后不接纳fill；可丢弃缓存不要求fsync，成功应用
fsync依赖原持久副本与Meta屏障。S3原有窗口、载荷／descriptor预算独立保持，外部写不生成
这份原生写proof；外部S3与Peer缓存协同仍后置，不同时配置两种后端缓存。
这些是实现及部署边界，不表示本候选已完成运行、回归或性能验收。

## 原生写 buffer 与 Node allocator

`dfs_write_buffer_idle_bytes` 为 Node 顶层配置，默认134217728（128MiB），允许0..=1TiB；
0禁用空闲保留。它只限制池中可复用的空闲实际容量，不预分配，也不是 live、RSS 或进程内存硬上限。
未命中池时按需申请，最后引用释放才归还，超基线分配释放给 allocator；停止清空池。
空闲池不改变 S3 的任何预算。原生写入另用下表的 Node live／描述符／上传三账，
原16MiB累计接纳发布阈值退出原生路径；4MiB满块只冻结并排队上传。
原生区间索引仅保留最终可见范围，不保留每次覆盖的历史记录；S3／legacy 仍沿用原日志。
明确分配拒绝尽力返回 ENOMEM 或合法短写，Linux OOM 杀进程仍按崩溃合同处理，
不能保证始终正常返回错误。

| 原生 DFS 参数（CLI 将下划线改为连字符） | 默认 | 合法范围／含义 |
| --- | ---: | --- |
| `dfs_dirty_budget_bytes` | `67108864` | 4..64MiB，Node实际live正文capacity，含仍被持有的已上传正文 |
| `dfs_unpublished_descriptors` | `1024` | 1..1024，且受固定4MiB元数据总额限制，不能靠增大计数绕过字节上限 |
| `dfs_upload_inflight_chunks` | `8` | 1..8，整个Node的真实任务上限，共享live正文 |
| `dfs_optimistic_gc_commit` | 随原生CRC新写默认`true` | 首次写入登记一次代次，省略逐chunk登记；FullSync只原子核对相关代次及session，无关GC变化不拒绝提交 |
| `dfs_crc32c_writes` | 原生DFS Node为`true` | 独立ID＋32KiB分段CRC；支持原复制策略、跨节点读和副本修复。OwnerFs-only、Meta及外部后端缺省不启用 |
| `dfs_background_commit_ms` | `5000` | 1..60000ms，后台发布调度周期，非严格可见时间 |

优先级 CLI＞TOML＞默认，非法值启动报错；仅无外部后端的 DFS 接入此流水线。
固定4MiB元数据内预留64KiB给FIFO和进展，其余在接纳前覆盖描述符和新增发布工作区；
每项实际保留与保守预留分列，字节满可能早于1024项。压力发布只用于描述符额度满，
或本机没有持久副本、必须保留正文而使live满；不得借压力跳过未知提交精确重放。
只有Required策略与实际本机持久ACK核验成功才能退休未发布正文并从本地store按回执根读；
否则保留正文至发布且最后持有者释放。普通write接纳成功不等同已发布，成功fsync、
同步写、可写close/flush和排空仍等待所需对象与Meta持久屏障。

后台写入错误由所属inode保存，各打开handle按自己的错误cursor观察。上传重试或发布已恢复后，
同一handle仍可能收到此前未消费的错误；错误返回不等于本次又发生一次上传故障，
不能据此承诺固定重试次数后必然成功。不同inode不应继承该业务错误。

正常停止的dirty drain是best-effort：按inode推进，首个inode提交失败时返回错误，
后续inode可能尚未发布；调用方不能将该停止报告为排空成功。成功fsync的持久性保证不变，
未同步的dirty写入不承诺在失败停止或强杀／OOM后全部保留。需保证全部写入已同步时，
应先停止应用写入并确认各文件同步成功，再正常停止Node；这不是持FD dirty排空已验收的声明。

`afs_dfs_native_write_resources{resource="..."}`分别报告live／峰值／等待、descriptor实际／
预留、publication scratch预留、control、upload和压力事件；不带inode标签。
这些项存在包含或预留关系，不直接相加；4MiB限新增W2结构，不含P1M既有完整文件布局、
标准RPC、空闲池、allocator或RSS，不因此拒绝原可提交的大文件。已有完整布局并存份数
保持或减少，旧成本及transport临时分配另测；不能把旧布局排除解释成允许增加未计账副本。
RSS另测，不因gauge存在而判资源通过。

Linux Node 默认 feature `node-jemalloc` 安装固定官方 allocator，作用于同一 Node 进程内的 OwnerFs 和 DFS；
AFS Meta 和 ADX 其他进程不受强制替换。依赖许可随 ON 包中的 `third_party/afs-source/NOTICE` 提供。
后台线程在初始化后开启，dirty decay1000ms、muzzy decay0，配置失败启动报错。
`/metrics` 提供写 buffer 与 allocator 的独立统计，allocator每1秒采样；采样失败只使观测无效，
不改变业务行为。GNU Linux 与 musl 的后台线程支持不同，未验证目标不能继承回收结论。

同机制 system allocator 对照仅用于显式 Linux 验证：

```sh
cargo build --locked -p afs --release --bin afs-node
cargo build --locked -p afs --release --no-default-features --features ownerfs,dfs --bin afs-node
```

必须分开保存并核对两个 ELF，不能用后一次构建覆盖前者身份。统一发行仍沿用 ADX 构建入口。
空闲容量基线不能直接当作 RSS 验收值；回收窗口、触页峰值和允许偏差需在测前固定。

## 原生读粗粒度诊断

Node 顶层 `dfs_read_diagnostics_enabled` 默认为 `false`。仅在限定诊断轮显式开启：

```toml
dfs_read_diagnostics_enabled = true
```

使用节点现有 `rest_listen` 地址读取 `GET /v1/diagnostics/dfs-read-timing`；例如 `afs/examples/node.toml` 示例的
`http://127.0.0.1:7501/v1/diagnostics/dfs-read-timing`。未开启时路由返回404。
该入口复用现有管理HTTP监听，不新增鉴权层；监听地址应限于原受控管理网络。

诊断只保留当前进程前256条记录，不轮转、不存正文、路径、inode、用户身份或授权token。
检查 `schema_version`、`overflow`、`clock_failures` 及每项完成／成功状态后再解释数据；
溢出或缺失记录不能用于完整归因。根／outboard工作区间及临时元数据有固定上限，不增加正文缓存或载荷预算。
关闭时不分配诊断journal、不采诊断时钟，仍有轻量状态判断及原有指标，不承诺零指令开销。
正式性能轮关闭该选项，诊断轮单独保存；父子阶段、重叠区间和累计hash耗时不能从E2E直接扣减。
详细边界见[测试说明](../testing/afs.md#原生读粗粒度诊断)。

## Meta 快照升级与回滚边界

OwnerFs 与 DFS 共享 canonical Meta 操作日志，业务记录分开。local-file 持久确认后才发布应答；
后台检查点和历史退休沿用本文件的有限策略。旧 JSON/AFSD 快照、etcd、Redis 后端不再接入，
升级／回滚必须使用匹配的二进制和完整数据目录，参见 [Meta 操作日志有限策略](#meta-操作日志有限策略)。

本版本新增 DFS durability policy、外部对象回执等持久化表示。旧二进制可能无法解析新快照；
不能把替换为旧二进制直接当成可用的回滚。升级前应正常停止相关写入、保留并核验原 Meta 和
数据后端的匹配恢复点及版本身份。回滚只能使用与旧版本匹配的完整恢复点，并明确恢复点之后
写入的处理；不得把旧 Meta 快照与另一代对象数据混用。本版本不提供快照降级转换。

原生Node的`catalog.wal`另有有限尾恢复：只截断物理EOF处无换行的JSON不完整尾部，
保留并同步已核验前缀；完整损坏、非法UTF-8字节、checksum或revision错误仍拒绝启动。
完整无换行事务不在恢复时改写，后续追加补分隔符；不清理目录中的正文孤儿。截断／sync
失败继续报错，不能手工删WAL绕过。原生本地持久请求可合并为一条原格式多记录事务，
校验和磁盘I/O在catalog锁外完成；所有正文、chunks目录、WAL和catalog父目录的必需
屏障成功后才返回各自ACK。同ChunkId的安装／重放／隔离互斥，替换使用新inode。

`dfs_catalog_commit_queue = 256`（CLI `--dfs-catalog-commit-queue`）允许1..=256个
排队加活跃请求。新增committer控制元数据上限1MiB，包含活跃组与未释放flight，不含
正文、既有持久catalog、调用者原有批次或大批独占fallback的原临时元数据；超大原请求
等待排空后整批串行处理，不拆事务、不改变回执。此上限不是全部Node内存或RSS承诺。
`afs_dfs_catalog_commit_resources{resource="requests|reserved_bytes|peak_reserved_bytes|actual_peak_bytes|control_limit_bytes|groups|max_group|failed|exclusive"}`
分别报告请求、控制预留／观测峰值、成功组及失败／独占状态；各指标存在包含关系，不求和。
正式名字已变化、必需屏障未确认后的失败，以及WAL／父目录持久化失败，都会锁存
首个错误到store重开；后续请求不能假成功或继续追加。重开先完成chunks目录屏障，
并在严格恢复后同步已有WAL文件及其父目录；这些屏障失败就拒绝打开store。防止同一
系统内重开后用健康重放跳过未同步的rename或完整但尚未确认同步的WAL。
停止时先完成DFS dirty drain，再关闭committer并等待真实I/O／持有者结束；原进程watchdog
边界保持。成功fsync、未知Meta精确重放及后续修改不能越过的要求不变。

## Node 注册租约期限

Meta 注册回复提供实际已持久化租约的相对剩余期限 `lease_remaining_ms`；Node 从请求开始的
单调时刻计算本地截止时间，保守计入建连和应答等待，最长不超过请求的30秒。
绝对到期时间仍供诊断，不用 Node 与 Meta 的墙钟相减决定读授权。启动与心跳使用同一规则。
缺失／耗尽期限、已过期的读授权或 epoch 变化不能恢复旧授权；真实 session 失效仍按原策略
关停 Node，混合模式的 OwnerFs 也会正常排空。本次不引入 DFS 局部故障隔离。

需配套升级 Meta 与 Node。旧 Meta 未返回相对期限时，新 Node 明确拒绝注册授权，
不回退到跨机墙钟或任意30秒授权。Meta persistence readiness 的判定保持独立。

### Native Peer 准入等待

`dfs_peer_serve_wait_ms`在同一native gRPC请求入口确定一次截止，覆盖RPC槽、
对象flight、控制与正文额度；不是额外传输超时。0不排队，临时压力到期仍返回
resource_exhausted，停止后不允许等待结果生成新的响应。RPC许可等待在原配置期限
内复查stop，预算／flight等待由通知唤醒。该参数不增加容量，不延长授权，不改变
已发送Header后的错误传播和末引用退账。同一截止不因重试、Header前回退或伪唤醒
重置，也不作为已开始I/O、同flight结果或双帧背压的完成时限；原授权／stop／传输期限
继续约束这些工作。大窗口／多连接后置，没有因此启用或扩大共享2MiB窗口。

## Meta 操作日志有限策略

`meta_store` 默认 `local-file`；`memory` 仅用于可丢弃状态。配置中的 `etcd`、`redis`、
`etcd_endpoint`、`redis_endpoint` 不再接受，不静默回退。新二进制只打开新格式的
`data_dir/meta-store`；旧快照目录不自动导入。回滚使用原二进制和匹配的原目录，
不得让旧二进制打开新操作日志目录，也不得覆盖旧数据。

以下参数均遵循 CLI＞TOML＞默认，CLI 将下划线改为连字符；值须大于零。

| TOML 字段 | 默认值 | 含义与范围 |
| --- | --- | --- |
| `meta_wal_segment_bytes` | 67108864 | 软分段目标，至少4352字节且不大于`meta_wal_max_bytes`；原子批次不拆段 |
| `meta_checkpoint_wal_bytes` | 134217728 | 后台checkpoint字节阈值，非额外硬配额 |
| `meta_checkpoint_wal_records` | 1000000 | 后台checkpoint记录阈值 |
| `meta_checkpoint_interval_ms` | 60000 | 时间阈值；不承诺严格完成时刻，必须可表示为有限定时器 |
| `meta_request_retention_ms` | 86400000 | 幂等结果最长保留时间；条数／字节压力可提前清理 |
| `meta_request_outcomes_max` | 1000000 | 幂等结果条数上限 |
| `meta_request_bytes_max` | 268435456 | 幂等结果持有容量的逻辑字节上限 |
| `meta_watch_events_max` | 1000000 | watch历史事件条数上限 |
| `meta_watch_bytes_max` | 67108864 | watch历史事件持有容量的逻辑字节上限 |
| `meta_watch_retention_ms` | 3600000 | watch历史最长保留时间；清理仍以完整revision为单位 |

单记录／批次／总WAL上限仍分别使用 `meta_record_max_bytes`、`meta_batch_max_bytes`、
`meta_wal_max_bytes`。调整总WAL时须同时核对分段目标和原子批次所需空间。
保留策略控制历史结果与事件，不删除当前inode、版本、会话等权威状态。
策略收紧时，恢复阶段先持久化必要清理再接纳服务；放宽策略不复活已清理历史。
以上是逻辑预算和后台阈值，不是RSS或物理掉电验收声明。

### OwnerFs 授权记录维护

Meta 为新 Root 生命周期按前一水位加一持久分配不复用的 epoch，
expected 只作约束，不能抬高分配值；水位与 reservation/原结果原子保存。
无水位的旧状态先从尚存事实引导并持久保存水位，再退休旧结果或授权；这不恢复旧版本已删历史。
正常恢复必须保留原数据目录中的完整身份、checkpoint 和日志链，不能单独删除或编辑水位。
`memory` 整体重启仍按原合同丢状态，不提供跨新 store 身份的旧授权延续保证。
此工作树新增内部水位实体；旧 Meta 不识别的新日志不能用旧程序覆盖启动。回滚沿用
完整目录备份和配套版本规则，不提供自动降级转换。

停用确认不再等待所有历史 grant 实体被删完。后台沿游标每次最多扫描128份、删除32份，
并受原单记录、历史和持久化预算约束；除首次旧水位引导外，未产生删除就不追加管理事务。
当前 Home grant 即使 session 到期仍保留以支持恢复；后继身份切换完成后旧记录可回收。
普通 holder 已过期或被后继 session 替代时可回收，沿用过期 session ID 不允许续回的既有合同。
清理批次会在原单记录字节额度内缩小，不扩大配置上限。
清理暂缓会记录 `meta.authority_cleanup_deferred`，不会重新启用已停用 Root；
清理 I/O 的未知结果仍按原失败关闭合同处理。
旧授权实体清理后，旧日志的实际磁盘回收仍等待原 checkpoint/日志退休条件。
已完成命令与成功匹配 ACK 在 watch／原结果引用退休、无 reservation 且不影响活动 Root
屏障后由同一写者成对持久删除；每轮最多扫描128条、删除16对，另受64KiB键目标与
原事务／历史额度约束。仍被引用、待确认、失败或不匹配的记录保留，不手动删除。
现有 bind 消费者不会自动 ACK，不承诺此类命令均会被回收。
NodeSession 的旧记录承担过期 session ID 不得重用的持久拒绝语义，本轮保留；
NodeSession 与未完成命令的长期增长是剩余生命周期欠账，不把有界历史宣传成全部实体有界。
已有 bind、native FD/mmap 的撤权与排空限制保持。

### Meta操作日志指标

产品Meta和测试用RootCommand启动器把同一个Store的固定指标注册到进程已有的`/metrics`。
Registry不持有Store、State或磁盘句柄；采集不扫描全库、不读盘，不建立新的后台观测任务。

| 指标组 | 计量边界 |
| --- | --- |
| `afs_meta_requests_retained`、`afs_meta_request_bytes`、`afs_meta_events_retained`、`afs_meta_event_bytes`、`afs_meta_history_index_bytes` | 已发布history条数和逻辑owned容量；不等于RSS或实际磁盘占用 |
| `afs_meta_revision`、`afs_meta_log_sequence`、`afs_meta_compacted_revision` | 已发布业务revision、canonical记录sequence及watch水位 |
| `afs_meta_request_evictions_total`、`afs_meta_watch_events_evicted_total` | 本进程启动后由成功持久ACK确认的精确历史删除量，含降额启动清理；不回填重放前历史总数 |
| `afs_meta_wal_batches_total`、`afs_meta_wal_records_total`、`afs_meta_wal_framed_bytes_total` | 成功ACK的追加批次、记录和framed字节；不是保留的WAL磁盘字节 |
| `afs_meta_wal_failures_total`、`afs_meta_wal_capacity_rejections_total` | 追加ACK失败（结果可能未知），以及追加前容量等待到期；不混淆二者 |
| `afs_meta_checkpoints_total`、`afs_meta_checkpoint_failures_total`、`afs_meta_checkpoint_inflight`、`afs_meta_checkpoint_duration_seconds` | 实际owned任务成功、失败、在途及耗时；调用方或observer超时不提前结束计量 |
| `afs_meta_checkpoint_bytes`、`afs_meta_checkpoint_sequence` | 最近成功安装或重开恢复的checkpoint载荷字节和覆盖sequence；不代表目录总占用 |

counter以进程生命周期计，重启归零；恢复的当前状态会初始化gauge。指标是观测副本，
不是canonical权威或线性化读取接口。重复注册返回错误，产品在打开监听前报告该错误。
物理WAL／checkpoint容量仍由日志自身的有限准入和安全回收合同约束，不由counter充当配额。

## 原生 DFS 回收

NodeReplicated DFS 默认启动每 5 秒一次的安全回收；关闭引用有固定 60 秒保护期。
运行中／COW／未完成上传引用仍保护数据；长期打开的 inode 会保守保留全部版本。
失联 Node 不会被当作已删除数据；它恢复后继续确认删除批次。应答成功前本机 catalog 检查点与
删除代次必须已同步落盘。不要在离线 Node 未完成磁盘处置时强行清空 Meta 删除屏障。
外部对象持久化的 DFS 不执行这条物理回收协议。
原生异步上传在接受新写入代次时取得引用，上传前登记块，直至版本发布及异步使用结束后才释放。
本机回收先排空 catalog 安装，再独占执行检查点与 WAL 截断，避免覆盖尚未确认的写入。

引用创建 RPC 使用固定期限；过期返回 `META_DFS_REFERENCE_ADMISSION_EXPIRED`。
未知引用的释放也持久化 60 秒墓碑，阻止乱序的迟到创建。该期限不限制运行中文件／上传的寿命。

## DFS 删除并发与诊断

`unlink`／`rmdir` 的元数据 CAS 冲突会在 Meta 内重新读取并校验，最多尝试 64 次。
每次仍校验调用者权限、目录项和 inode，`rmdir` 重新检查目录是否为空；同一次操作
不会追随已被替换的名字删除另一个 inode。持续竞争或目标替换返回
`META_DFS_NAMESPACE_RETRY`（POSIX `EAGAIN`），权限、非空目录等错误保持原语义。
操作 ID 和请求摘要在内部重试中不变，已提交的删除可重放其原始结果。

定位并发时可临时为 Meta 设置 `AFS_META_NAMESPACE_TRACE=1`。日志记录操作 ID、
调用节点、候选事务版本，以及修改或发生冲突的 inode 版本、head、链接数和属性；
不记录文件内容、xattr 或凭据。`meta_namespace_applied` 只表示候选批次已应用，
不能当成持久化成功回执。默认关闭该诊断，开启后应限制日志访问并在定位后关闭。


## 原生DFS分段CRC32C与写入代次保护

原生DFS Node默认启用`dfs_crc32c_writes`及`dfs_optimistic_gc_commit`。后者在CRC路径表示写入代次保护，不再固定全局GC版本。支持既有desired/sync-required、副本分布及local-copy策略；CRC要求原生存储及代次保护，非法组合在监听服务前拒绝。OwnerFs-only、Meta和外部S3后端的缺省值为false，S3仍走原完整hash路径。本次不提供旧版本数据兼容或迁移。

例如在Node的顶层配置中保留缺省值即可；需要显式选择BLAKE3新写时设置`dfs_crc32c_writes = false`，未单独配置的`dfs_optimistic_gc_commit`随之为false。关闭新写CRC不关闭按持久化算法标记的CRC读取。不要把CRC对象的描述摘要当作正文hash。

新对象ID为`crc32c-v2-<32hex出生代次>-<32hex对象号>`，32KiB分段CRC表与长度、格式一起进入原catalog WAL持久化链。`Crc32cSegmentsV1`承载描述表的小BLAKE3绑定摘要，**不是正文BLAKE3根**；不生成正文Bao/proof，也不按相同正文自动去重。复制接收端重新核验CRC描述后，完成正文、目录及catalog屏障再返回durable ACK。FullSync等待原策略要求的同步收据和Meta持久事务。

首次写入通过已有retain RPC登记一次代次，库存中的同代次chunk在任意副本上均被保护，无需逐chunk登记。FullSync原子核对相关代次引用与owner session，在同一事务内发布版本并退休引用。无关GC状态变化不作废写入；确切代次退休、缺失或session失效仍返回错误。准入有固定时间窗口，退休墓碑覆盖迟到窗口，延迟请求不能复活旧代次；已提交但回复丢失按原operation重放。正常GC、读引用、修复claim和删除代次屏障保持启用。

跨节点读取绑定可信Meta对象及CRC表。服务端验证覆盖到的存储段，只发送获准范围；客户端检查范围CRC，对完整段额外核对Meta绑定的CRC表。非对齐边界依赖可信认证服务端的存储段验证，不扩大发给客户端的grant，也不提供针对恶意服务端的密码学内容证明。缺表、错表、坏正文、截断、错身份、缺Completion均拒绝。复制和修复保持原对象ID及描述，沿用原claim、epoch、收据和幂等重试。

可选BLAKE3 Peer驻留/proof及磁盘缓存不适用于CRC对象；CRC直接走分段验证的范围传输。gRPC与RDMA协议均携带CRC描述，RDMA硬件运行单独验收，不能用全feature编译替代。S3及其缓存策略不在本次变更范围。

`security.capability`在原inode映射检查后本地返回既有不支持错误，其余xattr保留Meta权限与错误语义。Meta的`afs_requests_total`提供`dfs_gc_capture/register/retain/release/collect`、`dfs_commit_file_version`和`dfs_get_xattr`标签，用于核对真实访问次数；不增加逐chunk正文扫描或详细调度采集。

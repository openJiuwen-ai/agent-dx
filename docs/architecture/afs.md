# Agent FS（AFS） 架构边界

Agent FS（AFS） 是 Agent DX 的可选文件系统子系统。它位于根级 `afs/`，
只在显式设置 `ADX_WITH_AFS=1` 时进入文件系统专用构建、测试、带组件发布包
和部署入口；默认 Agent DX 构建、包和部署不包含 `afs-meta`、`afs-node`
或 FUSE 专属系统依赖。

## 架构总览

![AFS 组件与控制、数据路径](../../assets/architecture/afs.svg)

OwnerFs 与 DFS 是平级模式。虚线箭头表示控制与持久元数据，实线表示数据访问；
`afs-meta` 不转发文件正文。DFS 节点复制与外部 S3 是替代路径，不能把图中两条
路径理解为外部模式同时保留 Node 耐久副本。图示描述当前工作树的实现边界，
不代表所有模式已完成运行或性能验收。

## 组件职责

AFS 把用户可见的文件语义和文件字节搬运分开：

| 组件 | 职责 |
| --- | --- |
| `afs-meta` | 命名空间、inode、写租约、文件版本、布局、放置、副本目录和幂等提交结果 |
| `afs-node` | FUSE 会话、OwnerFs Home 访问、DFS dirty 状态、本地 chunk 存储、复制执行、远端读、验证缓存和卸载排空 |
| `afs/common/*` | AFS 域内共享的错误、日志、指标、协议、追踪和传输 crate |
| `afs/client` | DFS 客户端 crate；不作为 Agent DX 平台内部 RPC 适配层 |

`afs/common/*` 是 AFS 域内共享层，不纳入 Agent DX 根级产品基础设施。
Agent DX 平台内部 gRPC 仍按 Environment、Runtime、Snapshot、Route 和 Node
责任拆分；AFS 不把 POSIX/Frontend 适配器接入 Platform 内部 RPC。

## Meta 与数据路径

Meta 是可恢复状态的权威，不代理稳定数据流。Meta 校验已提交状态并准备候选状态，只有持久后端接受后才发布提交结果。读请求先固定一致视图，再选择本地副本、远端副本或缓存来源，避免一次复合读混用多个 revision。

Meta 的 `local-file`／`memory` 路径通过 `Store::open_log` 重放确定的提交记录，
再建立一致的已发布视图。提交在私有结构共享状态中准备，记录持久成功后整体切换视图；
查询只固定已确认版本。日志的独立身份、顺序、前置 hash、CRC 和已声明持久尾都参与恢复核验。
本地重开再次同步 WAL／目录，避免把仅在页缓存里可读的旧进程写入当成持久确认。
首次初始化以持久空 `INITIALIZING` 标记保护身份、空 WAL 和初始 CURRENT 的安装；
精确未完成前缀可继续，未知或非空残留仍拒绝。已初始化目录必须有 CURRENT，
未列入恢复／退休清单的非空 WAL 拒绝；仅允许身份与恢复 tip 精确匹配的单个空预备段。
这能检测清单遗漏和留有新日志的回滚，不能检测整目录自洽回滚而无外部见证的情况。
旧 JSON 快照目录拒绝自动接管；生产仅local-file／memory日志后端，旧快照／etcd／Redis适配及依赖已删除；每个 Store
只选择一条权威路径，不双写。`Store::checkpoint` 可捕获同一已发布 State 及日志位置，
在后台任务中编码自包含状态，持久安装带身份、位置、摘要及 CRC 的快照和 `CURRENT`。
`CURRENT` 同时选择快照和连续日志段；每段头绑定原身份、起始位置及前置 hash，
活跃段的已同步尾标记承接后续提交。重启核对保留恢复链后，从有效快照只应用后续记录；
最新快照损坏时，只有上一快照及所需全部已确认历史均有效才回退，否则拒绝。
调用方取消不会释放仍在工作的安装任务额度。写批次不跨段拆开，段按软阈值轮转；
先持久更新恢复清单，再验证并删除两恢复点均不需要的封口段及旧快照。退休清单支持
删除中断后接续，损坏／外来对象保留并计入有限额度；校验旧正文不持写者锁。
恢复链验证后可回收未引用且经身份、完整摘要和 CRC 核验的本库 checkpoint 镜像；
不采用孤儿镜像作为新的恢复权威，不删除未知／损坏／外来镜像。
后台按记录数、保守段字节量或时间软阈值捕获快照，容量压力也可唤醒。只有写前只读准入
确认尚未追加时才做有界等待，超时拒绝该组且不发布；实际追加错误仍失败关闭，不能按
容量错误码重试未知结果。观察超时不取消仍在工作的快照任务或释放其额度。
易失 memory 安装自包含快照后移除覆盖记录，仍不承诺进程退出恢复。
工作集仍是逻辑计账；六点进程故障、捕获超限拒绝和实际槽位压力下继续提交已有有限回归。
确定的历史清理使用独立管理记录或业务记录中的关联清理，保存原请求完成代际及完整事件
revision 水位；不复制实体正文，不伪造用户操作。纯管理记录只推进物理日志位置。
恢复核对并应用这些决定，不按重启时钟重新选择；自包含快照保存原事件完成时间、保留集、
压缩水位及已移除事件总数，避免清理后重置后续 Node 事件序号。旧 watch 游标明确返回
`Compacted` 和重扫位置，有限请求只复制选中的完整 revision。
canonical单写者按实际owned容量及保守结构预留计量历史，派生索引在恢复时重建，
正常提交只增量计入本次新增项。时间／条数／字节任一先到即选择确定清理；业务关联清理
与业务结果同屏障发布，空闲及持续重复请求期间也由独立到期计时推进管理记录。
待处理输入和准备身份有独立的有限字节准入，身份保护覆盖入队至写者处理终态，
调用方取消不能提前退还写者仍持有的额度；清理选择与身份准入在短锁内互斥，
持久 I/O 不持该锁，正在退休的同身份请求在入队前明确拒绝。
未知ACK后的失败关闭同时阻止后续业务和管理追加。
canonical读视图另有数量、所持状态字节和期限准入；同一不可变状态共享计账，
不同代际保守独立计账。过期空闲视图主动解除状态引用，正在执行的读仍持有额度直到结束，
过期或已知权威失效后不交付成功。此约束不改变业务授权或Node／S3预算，也不覆盖
已返回的独立结果对象。LocalLog只允许创建进程使用写者句柄；最后真实持有者显式释放
文件锁，继承的句柄不能读写权威或解开父进程的锁。
运行时清理后checkpoint／重开，以及降额后先持久清理再准入已有有限回归。
watch在复制前按独立条数／owned字节上限选取完整revision；请求limit为0也不取消硬上限。
超限时只返回此前完整revision，首个revision本身超限则明确报容量错误，不修改持久水位、
不伪造Compacted。已有断档仍通过原Compacted和RecoveryCursor重扫。
Meta的watch RPC在取数前取得独立响应额度；流式队列有固定槽位、代际与期限，并由单个
回收任务主动释放无人读取的过期队列。尚未退出的构建请求仍保留额度，旧流不能释放新代际。
正常EOF／取消退账，超时返回错误，不能把不完整流当成已完成批次推进游标。
上述约束不计入Node/S3预算，也不代表整个进程、调用方已取走结果或tonic/h2缓冲的内存保证。
历史策略配置／固定指标已接线，旧Snapshot后端及etcd／Redis适配已移除。物理掉电、
大场景资源及完整真实产品验收仍未完成，不能宣称持续增长部署已交付。
配置与当前限制见[部署说明](../deployment/afs.md#meta-操作日志与数据目录)。

OwnerFs 面向小规模 Agent workspace。Home 节点把 workspace 保存为普通本地目录；远端节点通过受控 peer 回到 Home 访问文件。远端传输会话本身不授予文件访问权限，每次打开、读写、flush、sync 和 close 都必须校验授权、句柄、权限、root grant、session、Home session 和 fence。

DFS 把已提交数据表示为不可变 chunk。普通写入先进入 inode 的 dirty 状态；sync、同步写标志、close-time flush 或后台策略触发提交，形成新的 `FileVersion`。复制发生在文件布局之下，Meta 只在收到可验证持久 receipt 后提交新版本和副本目录。验证缓存不能自动算作持久副本。

DFS 的 `copy_file_range` 通过元数据 COW 为目标创建独立 FileVersion，重映射指定源范围的不可变 chunk；不读取或重新上传源数据。范围覆盖保留目标其它区域和源稀疏空洞。Meta 核验源与目标属于同一 namespace、布局严格对应声明范围，重试沿用原操作身份。后续写入提交独立 chunk，源目录删除不会使已提交副本失去数据。

文件提交和完整元数据同步遇到事务 CAS 未命中时，会重新验证 head 和写租约身份；同身份续期或并发 inode 属性变化不再永久污染 writer。换主和 head 变化仍拒绝。单次处理的持续竞争返回可重试错误，保留原 operation，避免把暂时竞争带到退出 drain。

所有启用 OwnerFs 或 DFS 权威能力的 Meta 都要求 mTLS 和
`trusted_node_certs`。CA 只证明证书链可信；Meta 还把握手得到的叶证书 DER 精确
映射到一个 Node ID，并校验 RPC 中声明的 caller／holder／owner 身份。DFS-only
部署与 OwnerFs 部署执行同一身份绑定，不能用另一张同 CA 证书冒充目标 Node。
Node 的 Meta、peer control 和 data 诊断请求复用生产 TLS 配置，诊断不建立明文旁路。
这里的 Node ID 绑定发生在服务端对调用方的校验中；客户端仍按 CA 和
`tls_server_name` 验证服务端，不承诺按目标 Node ID 对服务端证书逐一 pinning。

### OwnerFs Root 授权的代际与回收

RootGrant 按 Root、holder node/session 保存。同 session 的重新 acquire 更新原条目；
Root 不存在、epoch/Home session 不匹配或 holder session 被替代时，旧授权不能用于新准入。
新 reservation 使用同一 Meta 写者原子持久分配 epoch：最新分配水位、reservation 和原结果
属于同一事务。分配只取前一水位加一，调用方 expected 不能提高水位；
abort、停用和 grant GC 不回退水位；同 intent 重试保持原 epoch。
此前没有水位的操作日志，从仍保留的 Root/reservation/grant/command/结果引导：
首次分配直接原子保存；若后台维护先运行，则先把已有最大值与本批 grant 清理原子保存。
水位保存前暂停旧结果退休，避免删除最后的 epoch 依据。无法推断旧版本早已删除的历史。
之后水位为独立的 singleton，不为每个已删 Root 保留无界代际列表。

停用精确核对当前 Home grant（包含 Home session/generation），一笔事务移除活动 Root、
写对应撤权命令及原结果，不再枚举全部历史 grant。Root 移除后的 Meta 校验立即拒绝旧授权；
同名新 Root 使用新 epoch，不继承旧权限。Home 的本地拒绝、缓存失效、控制断链处理和
排空顺序保持；这不代表所有 Node 同时收到通知，也不承诺已打开 native FD/mmap 即时撤权。

后台清理在原持久化写者中有界扫描，仅删除已被 Root incarnation、Home 身份或后继
holder session 替代的记录。当前 Root 引用的 Home grant即使到期也保留，以供恢复推进
其 generation；普通 holder 到期可清理，因为现有注册状态机禁止过期 session ID 续回；
不改变该租约合同。选择与准备之间没有 await，删除携带条目 revision
条件，排队的新 acquire/recover/recreate 不能插入其间；后续请求仍按写者顺序执行。
清理也经 canonical 日志、原有历史预算及持久后公开，重启继续收敛。
成功命令及匹配 ACK 只有在 watch／原结果引用已退休、没有 reservation、且不再承担
活动 Root 排空屏障时才成对原子回收；每次最多扫描128条、删除16对，并受原字节预算约束。
NodeSession 保留拒绝过期 session ID 重用所需的记录；待确认、失败、孤立 ACK 及仍有引用
的命令保留。因此有界历史不等于全部权威实体有界，完整生命周期仍有欠账。
逻辑清理不是立即回收旧 WAL 字节，物理回收仍由 checkpoint/退休协议负责。

## DFS 原生写正文所有权

启用原生写入管线时，普通 FUSE 写入和内核页写回先取得 owned 输入，再由有界 callback
worker 按同 fh FIFO 执行，避免上传、发布或正文背压占住接收线程。
worker 将请求正文复制到最终 `ChunkBuilder` 分配。正文准入等待不持 inode 锁或 operation 门禁，
重新取得短操作门禁后复核 generation、序号、位置和 builder，避免等待期间后台提交造成错接。
顺序写优先使用4MiB容量块；满块冻结原分配并排队预上传，不触发 Meta 发布。
提交前仍可原地覆盖可写 builder；已冻结／已提交内容的小覆盖形成普通小 Chunk，
大写不强制按4KiB切分。不拼接正文，也不将 Vec 复制成 Arc 切片。

原生 Node 分开维护 live 正文、未发布描述符和真实在途上传三本账，默认分别为64MiB、
最多1024项且控制元数据总4MiB、8个上传任务。元数据上限含64KiB FIFO／进展预留，
其余容量在接纳前同时覆盖描述符和新增发布工作区；实际保留容量与预留分别观测。
4MiB约束新增W2控制结构，不给既有完整文件布局／标准RPC缓冲增加文件大小硬上限；
已有布局并存份数不增加，其capacity与transport临时分配仍须另行计量，不能宣称全部Node元数据受此限制。
空闲池默认128MiB，独立于 live，不能借作额外接纳额度。共享范围按父分配实际 capacity
计费一次，最后一个读者、上传／复制／RDMA或冻结提交持有者释放后才退账。
FUSE owned 输入使用独立的每 mount 32MiB 预算，复制前取得额度，后端执行结束或未准入
任务丢弃时释放；它不计入原生 ChunkBuilder 的64MiB live账，也不是进程RSS上限。
额度或队列耗尽时接收端仍会背压；worker 排队消除的是单次后端等待直接占用接收线程，
不承诺负载超过限额时仍能立即接纳。关闭时唤醒额度等待者并拒绝新任务。

`afs_fuse_write_duration_seconds{filesystem,source,stage}` 将内核页写回和普通 write 分开，
分别记录输入复制／准入／排队以及后端执行的墙钟耗时。并发调用的累计时间不能与
pause 或恢复的总墙钟时间直接相加，也不能将 execute 全部解释为存储 I/O。

每一代在首次接纳时固定 OperationId，按 `through_seq` 冻结提交；准备旧代上传时可接纳
下一代。进入 File／Metadata 或结果未知阶段后，新的 mutation 不能越过；保留原请求、
OperationId、正文与回执，精确重放后才继续。普通 write 可以先报告已接纳；发布入口是
fsync／fdatasync、同步写、可写 flush／close、最后写者退出、后台调度、批准的压力以及
停止排空。原16MiB累计接纳发布阈值退出原生路径，4MiB满块不是发布入口。
后台默认5s是调度周期，busy时延后补做，不承诺严格可见期限。成功 fsync 仍等待此前
已接纳序号所需的持久对象回执和 Meta 确认，错误原样传播。

Required 策略且同一权威 placement 的实际本机持久 ACK 已验证时，上传后可释放正文，
未发布的本机读取用本地 store 并对照回执根校验；本机无可靠副本时正文保留至发布，
仍有引用不能退账。只有描述符额度压力，或无本机副本导致 live 满时，才提前后台发布。
原生 dirty 区间仍只保留不重叠的最终范围；截断、覆盖和准备失败恢复保持引用安全。
停止先关闭新接纳并唤醒等待者，排空已接纳上传及有序发布，再关闭 catalog 和空闲池。
远端 owner、OwnerFs 和 S3 保留各自调度，S3 的窗口和载荷／描述符预算独立不变。
Node负责运行时和后台调度；独立构造的 `DistributedFs` 需外部驱动既有写回入口。
没有运行时／外部驱动且当前请求无法释放live或描述符额度时，返回结构化错误，不以永久等待冒充背压。

Linux `afs-node` 默认使用官方 jemalloc：后台回收开启、dirty decay1000ms、muzzy decay0。
AFS live/idle、allocator allocated/active/resident/retained 与进程 RSS 是不同指标，
保留虚拟映射不等于保留同量物理页。S3 原有窗口、正文和 descriptor 预算独立保持，
尾块也按保留的真实 capacity 计费；本节不引入正文 RAM 内容缓存或变更读授权。

原生管线的 FUSE 路径包含接收缓冲到 owned 输入、再到 ChunkBuilder 的两次复制；
冻结后不复制提交正文。内核、远端接收、读回和完整 hash 另列。
独立构造且未启用管线的本地 Backend 仍可选择借用写，它会占用该回调的 receiver。
接收线程、worker、任务数与输入字节预算均可形成背压，不能声称新请求总能立即接收。
具体功能、回收及平台验证边界见[测试说明](../testing/afs.md#原生写-buffer-验证)。

## DFS 原生节点读取上下文

未配置外部后端时，新open从Meta取得fresh inode，之后同句柄复用已提交版本视图
最多5秒。截止从fresh请求前的单调时刻起算，并受Node/session原期限限制；命中和
心跳不延长该视图。到期刷新head，已有getattr、本机成功发布或truncate刷新点
使旧视图失效。close-to-open保证新打开读取最新已提交状态，不承诺已打开句柄
每次read即时感知外部修改，也不固定为永久快照；rename/unlink沿用inode及版本
保留规则，不因视图复用扩展GC或文件描述符生命周期。

版本新鲜度和授权独立。Node注册、心跳、epoch变化和停止接入同一有界读取授权，
read开始与私有结果交付前检查；源元数据缓存默认最多5秒，并受原grant到期减1秒
安全余量约束；A端grant缓存仍最多5秒。源缓存不延长任何grant或已捕获Node／读视图期限。
普通Meta断链只允许继续已获且未到期的上下文；新open、
head/source/授权刷新需要Meta时失败即报错。读取拒绝失效对应上下文，Node级
拒绝或epoch变化才停止共享授权，不能由单个grant到期停止所有合法句柄。

B首次缺源时可从同一已核对版本／布局前瞻最多64个后续chunk ID，与当前mandatory ID
合并为至多128项的已有Meta请求。只拿源元数据和grant，不生成正文ops、不预读对象；
512B以上的可选ID跳过，当前mandatory／本机读行为不因此改变。源缓存仍最多1024项，
另以默认4MiB的保守实际保留估算限制两份key、String／Vec capacity及索引表包络；
当前RPC回复／读取clone另计，不能称为Node RSS上限。命中不续期，慢Meta回复消耗从
发送时开始的期限；外部存储的源准入还夹紧原授权截止，不在安装时重新赠送缓存时间。

明确拒绝清除对应version／layout的预装源；观察到同一远端Node／device的epoch变化，
退休相关旧条目，其他peer／scope不误清。已失效前发出的迟回复及其旧命中克隆不能恢复
缓存或直接交付：最多重新请求一次全部当前mandatory授权，持续变化则报Unavailable。
这不是主动撤销协议；尚未知晓的变化仍受既定期限约束。可选批量请求只有精确的
Meta catalog invalid-request错误才允许一次mandatory-only重新验证，权限、认证、冲突及
传输错误原样传播。最终授权、对应对象格式的完整性校验、fsync及close-to-open合同保持。

句柄表和五个按 inode 热状态表使用64分片；句柄状态独立加锁，读取快照共享
`Arc<InodeRecord>`。反向索引只定位目标 inode 的句柄；表锁不跨Meta或Peer RPC。
同句柄过期视图刷新使用单飞结果，等待者仍保留各自的授权与期限，成功刷新不撤销
已经捕获有效提交版本的读取；已知拒绝、关闭及到期仍在私有输出交付前拒绝。
这只说明单个后端/FUSE READ callback使用一个提交版本，不承诺应用一次pread被内核
拆成多个callback后仍是原子快照。source元数据和本地reader分别有界LRU，容量1024/256；
命中只更新淘汰顺序，不延长grant或TTL。淘汰释放cache引用，在途Arc仍可安全完成；
resident条目上限不是全部活跃FD、载荷或进程RSS上限。

不可变版本／布局另按完整版本ID缓存`Arc`，默认4096项及32MiB保守保留字节预算，
两项均可配置；具备DFS能力的Node启动时按最终配置检查表开销加最小结构字段／Arc头，
低于该下界时拒绝启动并给出minimum。此下界不假定ID长度，不能替代真实单项的容量计费。
超大版本可被当前操作使用但不进入缓存。计入版本、布局／索引的
容器capacity、重复ID字符串capacity及缓存容器包装估算；逐出后活跃Arc另行核算，
该预算不是整个Node RSS或加载时峰值上限。首次加载核对
版本、布局和长度，在使用inode时再核对归属。并发miss在锁外查询和建索引，安装时
再次检查canonical条目，不把缓存作为授权或承诺消除初次重复RPC。布局索引在每次版本
物化时构建；缓存命中可复用。超大版本不保留时，后续需要该布局的用户态读可能每次
重新调用Meta `get_file_version`、传输完整布局并重建索引；内核页缓存命中不进入此路径。
这比原先单项版本缓存存在退化角落，不保证超大／碎片化布局的读性能。索引用二分定位
候选；legacy未排序或重叠extent保持原序和溢出检查，不改变序列化格式。
条目上限不等于字节预算，在途Arc及其索引不因淘汰提前释放；mutable dirty布局仍走原路径。

对显式BLAKE3格式、未进入覆盖组流式发送的请求，A端完整BLAKE3通过后按对象单飞共享不可变正文，默认最多2个LRU驻留对象，
每对象最多4MiB。短表锁只管理身份、pin、LRU和最多8个active key；填充、扫描、
sidecar访问及等待均在锁外。完整对象的等待者复用同一次填充，各请求仍独立授权；
partial／无完整对象结果保持各自范围读取，不承诺这些请求也只扫描一次。
`dfs_peer_serve_payload_bytes`默认16MiB（原8MiB的显式容量调整），填充、驻留、
退役但仍被流持有的正文、range回退与RDMA packed按实际capacity共用此额度，
最后引用释放才退账。catalog及物理身份变化使旧代失效重验，旧持有者不能移除新代
健康驻留；保留逐range授权和返回范围的checksum，不预读下一对象。

单飞／驻留控制分配另受固定64KiB包络约束：内联固定槽、实际ID／record字符串capacity、
有界终态错误及Arc包装计入当前／峰值，最后控制持有者释放才退账；不借proof ledger或S3预算。
停止先关闭新准入、唤醒等待者并退休LRU，按首次关闭起的绝对10秒期限等待真实I/O、
正文／控制owner及packed lease；超时报错，不detach。等待后、Header前复核本请求grant
到期和已知停止，不共享授权成功或增加A→Meta授权RPC。
这份A端正文额度与B磁盘填充、帧、扫描scratch及进程其它开销分别核算，不是Node总RSS。

### 原生CRC与写入代次

原生Node新写默认使用独立ID＋32KiB分段CRC32C，正文不参与内容寻址。对象ID包含出生写入代次和随机对象号；小CRC描述绑定长度/格式及分段表，其摘要不等于正文BLAKE3根。正文、目录和catalog原持久屏障先于durable ACK，多副本保持既有同步策略和预算。复制接收与修复核验同一描述并保持原ID，修复仍受原claim/epoch约束。

首次写入登记一次代次，GC以库存对象ID保护同代次未发布正文，省略逐chunk登记。FullSync等待所需同步副本收据，Meta只检查相关代次引用与当前owner session；版本发布和引用退休同事务。无关GC状态更新不拒绝提交，确切引用失效及旧session不能提交。GC计划仍受全Store快照CAS约束；旧删除代次重放屏障、读引用和修复保护保持。准入窗口及墓碑防止迟到请求重新激活退休代次，已确认提交的回复丢失按原operation重放。

本地CRC读核对覆盖段。远端读保持精确grant，由服务端核对覆盖的32KiB存储段，只返回请求字节和范围CRC；客户端核对可信Meta绑定的描述，对完整段独立核对CRC，非对齐边界依赖认证服务端的存储校验。CRC检测意外损坏，不提供Bao对恶意Peer的密码学证明。CRC对象跳过BLAKE3 proof/驻留及可选磁盘Peer缓存，不通过完整正文hash兜底。正常读视图、源选择、重试和私有输出最终授权保持。

### 显式BLAKE3格式的标准根与覆盖组校验

显式BLAKE3格式的本机读、A端Peer范围服务及B端磁盘命中使用官方Bao outboard，按32KiB覆盖组核对
同一个标准BLAKE3根，不采用独立1MiB段摘要或CRC替代根校验。outboard编码包含64B
格式／长度／标准根header及parent CV；4MiB对象最多8192B。结构与根绑定检查不等于
正文已验证，覆盖组读取仍须校验相关路径并仅向私有结果复制请求区间；实际组读取、
hash和交付复制分别计量。连续覆盖组对齐范围直接读入私有结果，借用结果交官方
range validator核对标准根，保留其32KiB scratch复制。含完整中部的非对齐／多范围，
在至多128个无源／目标别名范围下，先一次读入各请求的私有结果，再由借用视图向
同一官方validator提供这些字节；覆盖组的未请求边界直接补读到其32KiB scratch，
完整覆盖组与标准根仍必须全部验证。原非对齐工作票不增；源或目标重叠、范围过多、
不含完整中部的小读保持原codec，不更改覆盖组写入顺序。不宣称零复制或新增正文缓存。
未请求组的静默损坏到读取
该组时检测；文件device／inode／length／mtime／ctime改变使已有证明失效。

本机持久副本的规范内容寻址ChunkId由Meta布局选定的对象提供标准根及长度锚，
再与catalog的ChunkObject核对，不新增Meta RPC，也不信任Peer自报新根。
legacy ID、旧数据、缺失／损坏sidecar或proof预算不足时保留原完整根扫描；
sidecar只帮助校验，不授予权限，不可让损坏正文通过。NodeReplicated写冻结时可
复用同预算内已计算的outboard。正文sync、chunks目录sync、catalog追加和sync仍先于
ACK完成；可选sidecar安装在这组屏障后非阻塞入队，由独立worker处理，不等待文件或
目录sync。`dfs_sidecar_install_queue`默认64，范围1..=64，queued与active合计受限；
每批最多32项。队满、元数据不足或安装失败仅放弃可选优化，不改变ACK／receipt。
proof和有界控制容器共用原1MiB ledger；queue持有原proof引用且禁止其晋升resident，
不复制正文。控制容量空闲时继续计费，shutdown真实释放后才退账；它不等于全部Node内存。
入队时为四张最大范围工作票保留瞬时余量，不给之后的builder承诺永久分区。
worker与读修复共用原修复gate及完整字节健康检查；健康文件不重写／rename／file sync，
批末目录sync独立处理。停止时关闭入队、固定10秒截止；过期不新发可选目录sync，
已开始的I/O仍由worker持有并join，不能提前退账或detach伪造正常退出；外层15秒
进程watchdog仍以124报告失败。安装前的读可完整核根并按需修复，不能假定ACK时proof已存在。
S3写路径不生成这份原生写proof，
原外部窗口、载荷／descriptor预算和有序发布合同独立保持。

本地`catalog.wal`恢复仅丢弃物理EOF处、无换行且JSON确实不完整的尾事务，保留之前
逐条核验的有效字节前缀并同步截断。完整事务即使没有末尾换行也原样接受，下一次追加
补充分隔符；完整语法、UTF-8非法字节、checksum或revision损坏仍拒绝启动，不删除
未入catalog的正文孤儿。恢复格式保持不变；多记录事务只会整条接受或丢弃不完整尾部。

原生本地持久化使用调用者驱动的`CatalogCommitter`，不新增后台提交线程。同ChunkId的
安装、健康重放和隔离共用有界flight；登记和发布使用短锁，正文校验、写入、文件及目录
同步在catalog锁外执行。已就绪的完整请求可合为一条原格式多记录事务；正文先sync，
再完成chunks目录、WAL及其父目录屏障，更新catalog后才分别返回ACK。一个请求不拆成
多个ACK；健康重放仍完整核验已有正文。替换安装发布新inode，不截断在读文件。

`dfs_catalog_commit_queue`默认256，范围1..=256，限制排队与活跃请求；新增控制元数据
另受1MiB上限约束，不持有正文，不借proof或S3预算。超过该额度的原有大批次在已有
flight排空后独占执行原整批路径，保留其既有、随批次增长的元数据成本；不能把1MiB称为
全部catalog、全部Node或RSS上限。正式名字已被安装或替换但必需屏障未确认时，任何后续失败都锁存首个错误；
WAL追加／同步或父目录屏障失败也锁存错误，
后续安装、隔离和健康重放都不能返回新的持久ACK，须重开store恢复。重开时先完成
chunks目录屏障，并在严格校验恢复成功后同步已有WAL文件及父目录；完整记录可解析
不代表前次失败的文件同步已补足。不能仅凭仍可读的新inode或完整JSON返回持久ACK。停止先允许DFS
完成最后dirty提交，再关闭committer准入并等待真实持有者，不提前释放或分离I/O。
Meta提交结果未知时仍先按原OperationId、精确请求、正文和回执幂等确认，后续修改不得越过。

本机和A端完整扫描后也可能修复缺失／损坏的可选sidecar。在固定共享修复分片锁内做
不带sync的完整字节比较，健康文件不重写、不rename、不做文件或目录sync；仅缺坏文件
才安装。已验证Peer正文LRU命中不再访问sidecar。修复可能产生文件与目录sync，但不持有Peer正文表锁；原正文填充
singleflight与额度不变。容量不足不当作损坏，不丢弃健康证明后重建／落盘；安全完整根
扫描仍保留。修复失败只放弃可选优化，不削弱正文／catalog／receipt持久屏障。
固定修复锁及reader结构属于有界运行时开销，不宣称被proof额度覆盖全部Node内存。

### B端可丢弃磁盘缓存与Peer交接

B的`dfs_peer_cache`默认OFF，只存可丢弃磁盘副本，不写入LocalChunkStore副本目录，
不产生DurableReplica或持久ACK。文件v2布局为固定4096B header、正文、proof trailer
（最多8192B）；body offset固定，不把proof写进正文。旧v1缓存条目可在专属缓存目录
退休并重建，不迁移或删除持久副本。缓存仍按namespace及完整ChunkObject隔离，
命中与交付前校验授权、Node/session及期限；close-to-open仍由新open的fresh视图保证。

运行时缓存表锁只保护索引、generation pin、退休、准入与统计；磁盘I/O、校验、Peer调用及
等待在锁外执行。启动恢复在对象发布前独占运行，不将其与运行时并发锁边界混淆。
被pin条目可先摘索引，文件、proof及收费保留到末pin释放；退休删除核对私有文件身份，
失败或外来替换不删除、不退账，旧key退休完成前不复用ready路径。

同对象miss通过有界FillFlight共享填充工作，flight只持有控制信息和结果，不持有正文或
授权成功结论。默认最多8个metadata flight，可用`dfs_fetch_max_inflight_objects`设为1..=8；
这不表示8路正文填充。原生Peer最多两个stream fill共享原4MiB transport包络：
整个Node只有两个256KiB载荷块、一份256KiB控制额度及一个活动Channel；两个流共享
该Channel不超过2MiB的connection窗口，每流窗口也不超过2MiB，解码消息仍限128KiB。
其他连接身份等待当前会话排空；legacy完整对象回退与stream fill互斥。独立hit可与fill并行，
不借用S3 pending额度。逐代pending文件分别保留身份、磁盘及元数据收费，失败删除
不提前退账；最后一个活动持有者释放后销毁共享载荷块，不形成正文RAM缓存。
每个等待者检查自己的授权与期限；提交前已知拒绝不安装Ready。leader个体权限失效不
撤销其他合法请求，缓存错误仍安全回源，真实Peer／授权错误正常传播。正常停止先关闭
准入、唤醒等待者再排空实际持有者，保留健康磁盘条目供恢复；不能把停止成功等同于
全部失败pending已删除，此类可丢弃残留仍由下次独占恢复核验处理。

Meta的ChunkSources可附带当前授权视图中的合法区间；提示缺失或超过128段时保守
沿用原范围读取。只有当前整个对象获授权、填充与私有输出满足原单批预算时，B才
请求当前整对象并设置`include_blake3_outboard`；这不是预取下一对象。A仅对明确请求
且完整对象的响应附outboard，partial仍只返回精确授权范围及原range checksum，
不制造B端完整对象证明，也不扩大grant。partial由A核对可信根覆盖组，B保留原range checksum，
不宣称B独立完成根证明。旧Peer不支持新字段时保留完整根安全回退；
未知格式、超限、截断或标准根／对象身份绑定错误明确拒绝。

`dfs_fetch::ObjectFetcher`以既有runtime阻塞任务调度前台分片及可选预读，复用唯一
FillFlight表；任务上限沿用1..=8，后台无额度即放弃，前台等待者优先。原生只读句柄
固定大小的顺序提示产生1／2／4窗口；`dfs_readahead_max_bytes`默认0，contract保持0，
product可显式设为8388608（8MiB），最多32MiB调度距离；0不建后台任务。
这不扩大预算：两流仍共享同一实际Channel、两块载荷
及控制额度，proof账本覆盖固定状态及按实际capacity估算的任务元数据；后台结果只进入
可丢弃磁盘缓存，不新增正文RAM缓存。
后台以空输出绑定接收完整对象，禁止完整Vec回退；前台多对象分片总长仅复用原复杂batch
scratch，全部join和最终核权成功后才复制输出，失败／回退先释放所有分片。

预读只使用当前缓存的未过期完整对象grant；每帧和提交前重查来源generation、句柄
scope／已知拒绝及Node生命周期。停止取消静默网络等待并join实际producer及fetch任务。
最后核权与磁盘rename沿用既有非原子边界，不建立全局撤销协议；Ready是可丢弃条目，
后续公开输出独立核权。调度计数及元数据峰值为观测，不作为硬准入证明。

满足原窗口及multi-thread runtime条件时，gRPC用既有runtime单producer接收当前对象，
全链仅两个256KiB独占载荷块，wire帧至多64KiB、交接队列至多一项。Header中的proof先
转为有账typed元数据并退役解码Bytes，再随现有control交给consumer。首正文前固定校验
模式：预算支持的完整peer tree由consumer增量计算唯一正文标准根；producer保留顺序、
长度、deadline、Completion与真实EOF验证。缺proof、预算不足或旧Peer保留producer完整
根安全路径，不能中途失败再改校验模式。consumer仍写私有pending、提取范围并构建outboard
后归还块；缓存写失败不跳过完整核根。实际hash分别计量，不能把累计耗时直接相加。
旧诊断`SegmentBlake3`表示consumer的outboard工作，不是独立段摘要。

原4MiB填充额度覆盖原载荷块、控制余量、受限解码及HTTP/2积压，私有输出另按原读预算
计费；窗口过大、current-thread或其它transport保留原缓冲回退，不增加正文RAM缓存。
缓存创建／配额／写盘失败只禁用可选sink，同次RPC继续完整验证和提取，不重复回源。
consumer收到Completion仍不能产生完成凭证；实际EOF和producer成功join后，内部回执
必须匹配Completion长度／摘要及私有sink／输出绑定；consumer模式还须用实际计算根
同时匹配Meta锚定的整对象根和Completion，fallback保留producer唯一完整根；consumer不再另建root-only正文hasher。之后引擎最终授权通过才可rename为Ready及公开输出。late Status、期限、校验、
授权失败不放行；取消仍关闭接收端并真实join，不能提前释放块或proof租约。

A store、原生写pool与B cache在同一Node共用原1MiB proof ledger，不各自新增额度。
其内计入outboard实际capacity、Arc／包装、构建与编码／解码临时工作，以及缓存证明
索引（当前64位构建中4096个指针占32KiB）。共享引用直到最后owner释放才退账；
其中常驻保留最多使用总额一半（默认512KiB），A pinned reader、Peer正文proof和B
proof／索引／包装共用该子限；不是新增一套内存池。已标记常驻的分配直到最后引用释放
才退还总账与子限，淘汰条目不能提前退账。在途工作、writer与验证者仍竞争原总账，
不保证32个并发请求全部取得快路径。对齐单范围工作票预留36KiB（32KiB scratch加
4KiB元数据余量）；非对齐／多范围仍预留76KiB（2×32KiB＋8KiB＋4KiB）。resident
接近512KiB且无其他临时占用时，剩余额度分别约容纳14张／6张票据，实际准入仍取决于
proof加载、writer与请求重叠。范围工作额度不足时按入口固定截止有界等待，默认2ms、
可配0..=50ms；释放与停止通过同一协调锁唤醒等待者，伪唤醒不延期，已持引用不提前退账。
builder／resident准入保持非阻塞。到期未取得票据时保留完整根扫描：例如4MiB对象的128KiB小读，
正文读取／hash可放大至32倍；降级不丢健康proof，也不重建或持久化sidecar。
保留／工作准入失败、原生写没有生成proof及范围验证退回全扫分别计数。该预算不等于整个Node RSS，
codec／TLS／网络编码拷贝仍属于原transport envelope，不冒称已由proof ledger覆盖。
B缓存退休proof后，若已无完整FileIdentity，仍使用完整根扫描；不以只剩dev/inode的身份
启用部分验证。A pinned reader保留完整身份时可用临时proof完成范围验证，不长期驻留。
ready文件的4096B header、正文、trailer与staging实际分配计入磁盘容量；目录、marker等
管理开销另列。近似LRU只淘汰专属可丢弃缓存，不增加持久副本数。

健康sidecar、标准内容ID、请求携带proof能力且读取完整对象时，A端按32KiB覆盖组读入
最终帧、校验到catalog锚定的标准根，再发送；不先扫描完整对象。每个stream最多两个
32KiB缓冲，按实际capacity计入原A正文额度，归还池／排队／tonic编码期间都由同一租约
持有；最后一个Bytes切片释放后才可复用，流结束且所有帧释放后退账，不形成正文驻留cache。
逐组保留catalog／fd身份、grant期限及停止检查，首尾核对路径，完整成功才发送Completion；
后段损坏只可能留下已验证前缀，B仍要求完整对象核根、Completion和EOF后才发布。
缺sidecar、legacy、partial、未请求proof或发送Header前可选准入不足时保留原完整校验路径；
Header后失败中止，不切换来源或伪造完成。观察到的坏可选sidecar在既有修复锁下精确退休，
不删除更新后的proof或健康正文。组读验仍可能与发送和回压交叠，不以累计时长承诺性能收益。

其余原生gRPC路径的64KiB正文帧以Bytes共享已验证对象的合法范围；A附带proof也以Bytes
共享原proof owner，B仅在同ledger准入后复制小proof元数据。生成的Rust API中
`DfsReadRangesHeader.blake3_outboard`与`DfsPutReplicaFrame.data`均使用`Bytes`；这属于
源码API变化，不改变既有wire tag，不能宣称完全无兼容变化。载荷、proof及各自租约
不能因缓存淘汰、producer退出或取消提前退账；最后真实持有者释放后才释放额度。
授权与帧生产继续在同一blocking任务，拒绝在返回stream前传播；原4帧队列、8 read slots、
单RPC总截止和排空合同保持。Bytes共享不等于整个Prost／TLS／网络栈零复制。

DFS Peer客户端协商128KiB HTTP/2接收帧上限，OwnerFiles客户端仍为256KiB。
本策略只作用于客户端接收响应，Node监听端策略不变；帧仍从原connection/stream窗口
扣除，消息编码/解码上限、载荷/控制额度、并发、TLS和截止时间均不变。相对HTTP/2
默认16KiB帧，单个已接收帧最多增加112KiB临时载荷缓冲；在途帧总量仍受原连接和流
窗口约束，不按连接池容量预留128KiB。旧对端可以继续发送小帧。实际DATA帧数量须观察
完整RPC；帧减少不代表TLS记录、TCP分段或墙钟同比减少。

以上描述当前实现及受影响回归边界；具体通过范围须绑定Linux输入和证据，
不代表WI-6正式验收或性能目标已经通过。

## FUSE 接收与页缓存

Node通过公共挂载配置控制接收线程、descriptor clone和callback worker；队列满时等待
容量，普通完成分别唤醒worker或容量等待者，不在接收线程内执行尚未准入的owned任务。
原生管线写入与页写回拥有输入后入队，保留同fh顺序和排空；输入字节预算耗尽时等待，
关闭时唤醒等待者并返回错误。仍选择借用写的 Backend 必须在原回调生命周期内完成，
不能把借用输入放进后台队列。

后端动态确认安全的native只读句柄允许同fh连续READ形成并发批次，排队的write、resize、
flush、fsync及release仍为排他屏障，后续READ不能越过。OwnerFs、S3和可写句柄维持原
排他调度。能力检查本身有元数据查表和授权成本；不将后端局部两次查表说成全链仅两锁。
官方fuser默认协商buffered异步READ；本次没有额外启用ASYNC_DIO，不能将并发单测
外推为O_DIRECT单流或整体E2E提速。

DFS普通I/O沿用cached write-through，不启用writeback。默认entry/attr TTL为0；每次新open
在线观察Meta，原生只读且没有dirty/provider/外部后端时，用完整提交版本ID和已核对长度
决定`FOPEN_KEEP_CACHE`。同inode身份完全相同才保留，缺失、变化、超限或不安全路径均不保留；
成功数据修改撤销该inode的复用资格。身份表最多65536项、版本字符串合计1MiB，另计表结构，
不占用磁盘cache的proof额度，也不保存正文。

64个短表锁管理每inode独立的本地读写锁，覆盖READ实际回复完成和OPEN fresh观察至
回复完成，避免旧READ在新OPEN清页后重新填入旧正文；不同inode不共用读写锁。
表锁不跨后端I/O或等待读写锁，最后引用释放时在表锁内回收条目，避免并发获取时分裂锁身份。
它不是跨节点读许可或撤销协议。内核已收到的页面
没有再次进入用户态授权检查的机会，不能宣传为可立即撤回的载荷；新open和用户态交付
仍遵循权限与授权合同。正TTL是明确的宽松路径模式，不满足严格close-to-open。

OwnerFs使用同一接收／worker配置，其Home缓存、TTL、direct-I/O及mmap协商保持独立。
DFS的版本复用与read-ahead设置不覆盖OwnerFs策略。read-ahead是向内核请求的上限，
实际协商值须记录；它不是下一对象预取。每操作读预算不等于Node全局私有输出配额，
提高worker数可能增加同时存活的输出Vec，需与帧、cache、allocator和RSS分别核算。

## OwnerFs 远端与 bind 数据路径

![OwnerFs 远端访问与本地 workspace bind](../../assets/architecture/afs-ownerfs.svg)

远端 FUSE 经 OwnerFiles 到 Home 访问普通文件，Home 逐操作核验授权和身份。
本地 workspace bind 的源是同一个 Home 底层真实目录，目标是 OwnerFs FUSE
根下对应的一级目录；该子树的访问由 Linux VFS 直接进入底层文件系统。
挂载授权与身份核验属于生命周期，不表示每次 bind 文件操作都回到 Node。
其他未被 bind 覆盖的目录仍走 FUSE；容器不是此能力的前提。

图为逻辑时序，root/Home 查询按需发生，不承诺每次读写的固定 RPC 数。
bind 与远端访问共享底层文件，新打开的可见性及权限仍需按支持范围验证；
图示不承诺已有 FD/mmap 即时撤权或跨节点锁一致性。

## DFS 外部对象持久化边界

![DFS 外部 S3 写入发布与独立读取](../../assets/architecture/afs-dfs-s3.svg)

图示为当前外部 S3 实验路径：PUT 成功与 Meta 持久 ACK 是两个独立屏障。
顺序 write 可与预上传重叠，独立读者只见已提交 head；fsync 强制收口，
成功返回仍要求所有必要对象已获成功回执且 Meta 已持久确认。缓存命中
可省略 GET，但不能省略适用的版本、授权、对象身份和期限检查。
此预上传与 64MiB 发布窗口不作用于 OwnerFs。

可选外部后端配置把新文件系统固定为单副本策略：本地暂存不是耐久副本，
不运行 Node 复制／修复。Node 用官方 S3 客户端执行普通 PUT，成功后返回
绑定 store/generation/key、完整长度和摘要的有类型凭据。Meta 在原版本提交
事务中发布外部对象引用和 inode head；旧 Node 复制凭据与外部凭据不能混用。

S3 默认保留载荷签名；显式 `dfs_external_backend.unsigned_payload=true` 只将 SDK
载荷签名策略改为 unsigned，SigV4 请求认证仍在。AFS 业务完整 BLAKE3、对象长度、
回执身份、错误传播和发布持久屏障不变；该选项不是关闭内容校验，也不自动适配
或部署第三方后端。

S3 桥接复用一个自有线程、runtime 和客户端，容量1的入口队列满时等待；
worker 最多持有32个命令。所有命令共享四个上传许可，单个 PUT 完成即可
调度下一块，不再等待固定四块全部完成。顺序写每满4MiB即预上传，每 inode
以64MiB为一次发布单元；冻结窗口的上传与下一窗口的有界生产可重叠，
按原预算余量接纳，没有额度即回压。最终发布仍通过原 Meta 条件事务串行执行。
普通 write 可先报告已接纳；fsync、可写 flush 和 drain 必须覆盖此前已接纳的
全部相关窗口。Meta 请求在途或 ACK 未知时暂停新修改，保留精确请求重放。
旧窗口 ACK 只推进其序号，保留 active 内容与属性。成功 PUT 回执经身份核验后，
正文可独立退休；最后 transport／retry／read pin 释放才退实际正文账，提交描述
跨 Meta 未知 ACK 保留。Uploaded 仍占逻辑窗口，不作为空洞或扩大窗口额度。
满 active 窗口在前窗仍 Preparing 时推进原提交链，冻结当前窗后才继续接纳；
逻辑窗口的回压不依赖物理正文是否已经释放。
普通 rename 保持独立 Meta 事务，不新增隐式全文件 fsync。窗口首 PUT 前
固定 operation_id，成功回执仅归属本窗口；部分失败保留窗口和成功部分，
Meta 未知确认保留精确请求并阻挡后续 mutation。fsync／可写 flush／同步写
仍等待全部必需回执及 Meta 持久确认。覆盖和截断先安全排空已有窗口，再走
同步修改路径。写侧通过字段私有的 `StagedChunk` 传递已封口身份，HTTP PUT
使用不可变 payload 视图，避免封口后再做一次全量 hash 或 `to_vec`；dirty、
stage、transport、retry 和最终 owner 交接共享有类型预算租约。首次封口仍要
完整计算 chunk 身份，原有预算、fsync、错误传播和退出排空语义不降低；这里
不宣称全链路零拷贝。独立只读客户端只见已提交 head；外部模式同挂载／owner
代理的 dirty read 复用 inode-wide DataSync 屏障，完成入口前已接纳序号的有序
发布后，再走原授权及完整校验读取。失败直接报错，不回退旧版本；读可触发尾部
提前发布，open／getattr 不触发。无 pending 写时不增加 Meta 写事务，非外部
DFS 保留原 dirty overlay。未同步崩溃只恢复最后已提交版本，不新增 WAL 承诺。服务端已接受的
不可变对象不能取消撤销或盲删，正常退出关闭入口、排空并 join worker。

B 在每次 open 在线观察最新已发布 head，建立一致的版本／布局／长度视图。
外部读使用 `AuthorizeExternalVersionRead` 从当前 Meta 状态验证版本归属、
reader session／epoch、对象身份和合法范围，不以旧 MAC 作为续期凭据。
每块完整校验，在最终返回前再次检查期限及已知失效。该路径不伪装
Peer 接收者，也不依赖原 writer session。内容 cache 默认 OFF；可选 Node
本地有界异步 cache，以 store/generation/chunk 身份隔离。完整校验后的内容
先进入有界 pending，再由单后台线程写入临时文件并 rename；正常退出排空。
可选填充先确认任务和pending分配能接纳，再进行磁盘容量淘汰。淘汰只采样
Ready候选中的两个条目，比较1秒单调访问时间桶并选择较旧桶；同桶保留首个
随机候选，不按精确访问顺序打破平局。命中刷新桶，首次Ready和恢复索引按当前
进程时钟初始化，不持久化时间桶。无候选则放弃可选填充，只释放本次所需空间，
不引入95%批量低水位。候选扫描受原条目上限限制，不复制整份候选索引。
删除仍在缓存锁内完成，失败恢复索引资格且不扣减实体容量；磁盘对象删除不能
退还仍由caller pin持有的内存。此策略改善的请求量和性能必须通过实际负载验证。
读仍先获 Meta 授权，损坏缓存丢弃后回源。S3／磁盘读取只通过字段私有、构造时
校验完整长度和 BLAKE3 的只读缓冲区交给读引擎；引擎核对完整 chunk 身份后
复制授权范围，保留最终期限检查。开启cache时，Node共享一个最多4MiB的已验证
对象槽，按完整chunk身份复用相邻请求；授权复用与内容缓存分别限额。驻留、
Ready尚未验证的分配、pending／worker以及槽位替换后仍被读者持有的已接纳分配，共用原pending额度，
按Vec容量去重计费，最后引用释放后才可复用额度。预算不足不保留新槽位，回退
原读取路径。新磁盘加载仍核对私有文件并完整校验；RAM命中不探测磁盘副本之后
的损坏／权限变化，检测延后至下一次磁盘加载。不是每个handle各保留一个对象。
S3 GET 与 Ready 磁盘缓存重载共享每个 store 的私有两线程 BLAKE3 池，使用固定
官方 BLAKE3 的 rayon 接口计算同一个完整摘要；入口最多允许一个对象校验。
Ready重载在单个对象的原分配内分为至多四个1MiB段，使用同一个Hasher按顺序
更新完整摘要；每轮在私有池hash当前段，调用者同时读取下一段的不重叠切片，
scope结束前等待已接纳任务。没有通道或中间正文复制，全部摘要、长度和EOF
验证后才交接；空对象直接完成EOF和空摘要检查，短尾按实际长度处理。
这不预取下一对象、不并行执行同fh回调，不增加对象队列、驻留或载荷额度。
Ready切换对象时，仅在旧resident载荷没有其他reader／fill持有者且容量足够时，
消费其唯一所有权并复用原Vec；容量而非新对象长度继续计费，原Verified身份同时退出。
被pin、容量不合适或预算不足时使用原准入／错误路径，不增加常驻池。
新对象仍必须完整读取、检查EOF和完整BLAKE3，失败不得恢复旧Verified身份。
committed external只读的私有最终结果通过相同最终授权／epoch／失效检查后，
以owned Vec交给FUSE回复，省去staged到另一输出Vec的复制；DFS外部批量读取的借用接口保留
全部范围成功后才修改调用方输出的语义。dirty／remote／其他后端沿用原read路径。
Ready原始分配先预留原pending预算，验证后转移给同一不可变分配；在途任务
退出前不退账，旧reader pin继续计费。预算不足传播容量错误，不删除Ready文件
或绕道回源；启动恢复也沿相同预算，缓存锁不跨读取／校验。调用者仍等待整个
对象验证完成。校验异常关闭入口并保留失败；正常退出先排空已有工作，再显式
关闭并 join 校验线程，失败传回 Node 退出路径。此机制的功能与性能结论须绑定
实际 Linux 候选，不能由线程数或 hash span 缩短推导 E2E 改善。
S3读取观测复用进程Registry，标签仅包含固定phase／source／result，区分排队、
SDK响应头合计、正文首片／累计／最大等流、复制、最终EOF、回复及缓存准入。
GET校验计时包含gate和hash；Ready校验计时包含gate、加载和hash，两者不能
都解释为纯CPU哈希。成功校验字节按来源单列，失败按固定stage／result计数；
失败计数不代表取得全部失败阶段耗时。达到对象长度后仍接纳合法空分片并等待
真正EOF，非空超长、截断和流错误仍拒绝。慢GET阈值50ms，摘要每store每60秒
最多16条，累计慢请求指标不受日志限流截断；摘要无URL、凭据或完整对象路径。
这些边界不揭示SDK内部建连／重试原因，不修改连接池、重试或读取协议。
外部只读句柄复用已观察的不可变版本／layout；重新 open 必须在线获取最新
head，open／getattr 或本挂载修改观察到新版本时一致地刷新关联句柄。旧 FD 不承诺
立即看到远端发布，也不是终身快照；rename／unlink 后仍绑定原 inode。
每次回调保留打开模式、对象范围、Node/session、期限及已知失效检查。

对象授权上下文最长5秒，同时受 grant 和 reader session 截止限制。Node 按 RPC
发出时的单调时刻计算截止，缓存命中和心跳均不能续期。最多128个上下文、
授权元数据预留最多1MiB；被在途读者持有的引用释放前仍计费，包含保守估算的
键、映射开销和有界投影，载荷预算不增。超出保留预算则本次在线核验、不保留
可复用条目。已知对象／版本拒绝使对应条目失效，保守终止与拒绝并发的在途
批次；不会据此终止整个 Node。已知 session／epoch 失效和正常 shutdown 则关闭
本地读准入，并与最终输出串行化；错误不走重新签发或旧授权回退。

Meta 纯传输断链时，已打开视图的未到期合法范围可读本地 cache 或已授权 S3
对象；新 open、新对象和到期续期仍须 Meta。未知 Meta 重启只允许沿用到原截止，
不新增撤销广播／boot watch；持久状态恢复后从当前身份重新准入，不要求旧 MAC
继续有效。状态丢失不能用本地缓存补造目录或持久成功。Node 重启丢弃授权缓存。
原逐批 MAC 核验接口保留其原拒绝语义；dirty 写、稀疏回退和 P2P 保持原路径，
不借此改变写 fencing 或 OwnerFs 授权。输出仍在完整验证和最终检查后一次发布。

缓存额度不充当进程 RSS 或文件系统总配额。小块写允许缓存与 PUT 重叠，但只有 PUT 成功才返回持久凭据。
对象存储部署与服务端持久性属于外部依赖，Meta 状态持久化
仍由 AFS 配置的 Meta 后端承担。详见[配置](../deployment/afs.md#dfs-外部-s3-连接)。

### 文件写租约维护

打开的写文件、脏数据和待确认的提交仍需要有效写租约。DFS 节点通过独立的每秒维护任务，
在 30 秒租约到期前为这些状态续租；该任务不排队等待普通后台写回完成。
续租只更新同一归属的有效期，不改动已经冻结、需要按原身份重放的提交请求。
迟到响应不能覆盖新的归属或缩短有效期，正常关闭且已提交的文件无需继续续租。
Meta 不可用超过租约期限时仍会拒绝旧写入，不通过复活过期租约绕过隔离。

## 本机 SDK usrbio 数据面

`afs-client` 提供与同机 `afs-node` 直连的批量 IO 数据面。控制面仍是 UDS 上的 gRPC：
open、register、iov 与环的生命周期和 sync 都走 RPC，只传身份与布局，从不传文件正文。
数据面是共享内存：SDK 创建持久 iov（memfd，区块为 4 KiB 到 64 MiB 的 2 的幂）并把
fd 经 fd broker 一次性交给 node；之后读写把正文放进 iov 区块，通过共享提交/完成环
（64 字节槽位，Release/Acquire）提交 `(reg_id, offset, iov_offset, len)`，一次操作既没有
RPC，也没有逐次操作的 fd 传递。提交与完成各有一个跨进程 eventfd 门铃：SDK 提交后敲
提交门铃唤醒 node 的环 worker，node 每投递一批完成项敲一次完成门铃，SDK 侧用 AsyncFd
阻塞等待，不做固定间隔轮询。诊断穿刺路径（每次操作一个 memfd 加一次性 fd broker）仍
保留，与常驻数据面分开，受进程内并发上限约束。

区块归属在协议里固定：结果明确（完成项带字节数或错误码）归还空闲池；结果未知（等待
超时、完成项对不上号）永久 forfeit，避免 node 仍在读写的内存被复用；提交前失败按
Drop 归还。环的凑批策略由 `io_depth` 决定：正数必须攒满这么多提交才处理且不受
`timeout_ms` 约束，负数凑不满时最多等 `timeout_ms`，零有多少处理多少；必须攒满的目标
大于环项数时创建即被拒绝，单发接口不得用在必须攒满的环上。单条 IO 失败以带原
`userdata` 的完成项返回，环继续可用；只有控制面错误才可能让整条环进入不可用状态。

数据面不改变 DFS 元数据与副本路径：SDK 的路径解析与 FUSE 走同一后端路径，权限与
目录项语义一致；环内读写只携带注册编号，注册把句柄与调用身份绑定。连接是同机
owner-only UDS，SDK 身份取 node 实际 uid/gid，越权请求照常失败。环活动计数经
`RingStats` 与 Prometheus 指标导出，仅用于观察，不参与正确性判断。

## OwnerFs workspace bind mount

OwnerFs workspace bind mount 是当前实际试用场景的优先能力，默认 OFF。开启后，它把 Home 上 workspace 的底层真实目录 bind 到独立原生 `ownerfs_workspace_egress_root` 下的同名一级目录。egress root 必须在 OwnerFs FUSE 挂载和 Node 数据目录之外；FUSE 子树仍保留，不以 bind 覆盖。把 FUSE 目录自身 bind 到别处不满足此设计。

核心实现只负责挂载、身份核验和卸载；runc、容器启动、执行、停止和探针属于适配层。ON 场景仍需保留授权、Home/root/epoch 核验、数据新鲜度、close-to-open、权限、错误传播、正常卸载和受管引用排空。当前不承诺已有 native FD、mmap、描述符转移或二级 clone 的即时撤权。

## Checkpoint 共享存储接入

Platform 的 `adx-checkpoint-storage` 用普通文件接口访问 DFS 挂载，不依赖 AFS RPC crate。
adxlet 直接捕获到共享目录，恢复时把该目录交给 sandboxd；不会先下载整个 checkpoint。
Coordinator 挂载同一 namespace，统一管理共享文件的回收；节点仅管理执行文件访问。
匿名恢复点跟随 Environment，可复用 Snapshot 使用中删除返回 Sandbox 列表。
运行时文件依赖与挂载的实际验证边界见[存储流程](../testing/snapshot-storage.md)。

## DFS 历史与数据块回收

NodeReplicated DFS 的回收由 Meta 判定可达性、AFS Node 删除本机块。Platform Coordinator
仍负责匿名 checkpoint／Snapshot 的共享目录生命周期；AFS 的回收只处理目录删除后失去引用的
inode、FileVersion、LayoutRoot 与数据块，两层职责不混用。

- 有链接的 inode 当前版本是保留根。打开的文件／目录使用持久引用；文件的在途访问共享同一引用，
  最后一个使用者退出才排队释放。有效写租约、尚未提交的上传与复制任务也构成保留根。
- 最后一个写句柄关闭后，即使数据已提交且序号相同，也后台提交剩余元数据；提交结果未确认时保留
  原请求和写租约，确认干净后停止续租，避免已 unlink 文件因时间戳脏状态永久保留历史。
- COW 目标布局直接引用共享 chunk；源文件删除不改变目标的所有权。`parent_version` 仅记录血缘，
  不作为永久保留历史的理由。打开的 inode 保守保留全部版本，关闭后再回收历史。
- 新引用的创建请求带固定的 30 秒准入期限，重放不刷新期限；Meta 最多接受 60 秒的剩余期限，
  容纳时钟差。期限只限制创建请求，已打开文件和长上传的保护不受它限制。
  对尚未出现的引用释放也必须持久化墓碑，防止释放先到、创建后到；墓碑保护期结束后，
  原创建请求已过期，不能重新激活引用。
- 正常关闭先提交释放，默认再等待 60 秒，覆盖已签发的 30 秒读取授权。心跳超时不释放引用。
  同一 Node 的新 session 接管后，旧 session 引用最早在旧授权截止时间加 60 秒后失效。
- Meta 先持久化带代次的删除批次和待确认 Node 列表，再删除不可达元数据；该批次中的 chunk
  在所有相关 Node 确认前禁止重新发布。同内容重新上传需等屏障完成。
- Node 删除块和校验旁文件，持久化 catalog 检查点及已完成代次后应答；重启或应答丢失重试不重复
  删除新上传的同内容块。catalog WAL 随检查点截断。Node 清单还会发现无主上传残留。

每批最多 1024 个 chunk，Node 每 5 秒推进，引用释放每次最多 64 个。不可达 Node 会保守阻塞
当前物理删除批次，不以心跳失联推定磁盘已清空。DFS 外部对象持久化模式目前不启用本机块回收；
其对象删除需要独立的外部存储回收合同，不能直接套用 NodeReplicated 协议。

## Meta 保留窗口与 Watch 重新同步

Meta 沿用 canonical log 的有限历史策略：默认幂等结果最长保留 24 小时，Watch 历史最长保留
1 小时，条数与字节预算可提前触发清理。配置使用 `meta_request_retention_ms`、
`meta_watch_retention_ms`、`meta_watch_events_max` 等平坦字段，具体默认与恢复合同见
[部署说明](../deployment/afs.md#meta-操作日志有限策略)。清理决定经日志持久化后发布，Watch 按完整
revision 截断；快照 GC 的读视图和相关键 CAS 保留 canonical 的期限与容量检查。

旧 Watch 游标得到 Compacted 与恢复游标。RootCommand 调用自动从同一份只读视图取得
当前 session 的待处理命令和 revision，随后从该 revision 增量跟进；历史截断不会丢掉
仍待执行的撤销命令。`GET /v1/store` 提供实体数量、结果／事件数量、水位和 GC 屏障进度，
用于观测收敛，管理端口应限制在受信网络。

## 后置能力

当前不承诺以下能力：完整 POSIX、复杂可靠性、多 Meta、高可用、etcd/Redis
后端验收、大规模长时间运行、RDMA 性能专项、跨节点 `fcntl/flock`、阻塞锁
等待取消、bind/native 与远端 FUSE 锁域协同。bind 本机 ext4 锁和单挂载
内核回退不能宣传为分布式锁。

支持范围、验证边界和性能目标见 [Agent FS（AFS） 演进计划](../development/afs-plan.md)。

写流水线的 normal 128MiB 租约分别归属原 partial dirty 分配、pending Vec 容量
及 sealed Arc；封块后 dirty 与 HTTP/retry 共享 sealed 租约，旧 partial read pin
仍持自己的租约。32MiB publisher 仅预留 Vec→Arc 的瞬时转换峰值，不能长期
借给下一窗口。额度在分配前取得、最后一个持有者释放后归还；descriptor
维持独立1MiB／32项上限，缓存 pending36MiB不混用。

## Native Peer 有界准入等待

Native gRPC read在入口固定一次`dfs_peer_serve_wait_ms`截止，RPC槽、flight和
control/payload不足共用该期限，不按重试或Header前回退重新计时。默认20ms、0禁等，
过期只禁止继续等待，现成容量仍可立即取得；已开始的I/O、同flight结果和R-4帧背压
继续受原授权／stop／传输合同约束。budget独立通知锁不嵌套获取cache表锁，最后
真实owner释放才扣款唤醒；停止先关闭准入再排空。RDMA packed保持原立即准入，
不增加连接窗口、正文或proof额度。原预算下的两流共享Channel不等于多连接；16MiB
connection／4MiB stream等大窗口方案继续后置，不能移除现有≤2MiB的流水线准入条件。

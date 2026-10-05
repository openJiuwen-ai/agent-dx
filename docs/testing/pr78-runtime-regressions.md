# PR78 运行可靠性回归

## 创建失败后的资源释放

ADX 在调用 sandboxd Start 时预留本机资源。Start 失败、超时、应答丢失或成功载荷无效，均由 adxlet 将创建判定为 Failed，不重复启动同一执行。该判定不要求 sandboxd 增加应答标记。

创建进入 Failed 即释放本机标量资源和 GPU/NPU 预留，提交 `resources_held=false` 后尝试清理。清理与尚在等待的 Start 客户端任务串行；查询或删除失败时继续重试，不重新占用已经释放的资源。提交不可用时独立重试补写，清理重试不释放其他实例的预留。Failed 控制器在释放资源后继续定期清理迟到后端，不接纳为 Running，也不重复提交相同 Failed 状态。adxlet 重启通过完整权威目录对账清理残留。公开 API 的应答丢失与后端 Start 失败分开：前者仍复用原 Environment 身份查询或重试。

`platform/adxlet/tests/sandboxd_rpc.rs` 通过真实 gRPC/UDS 与模拟 sandboxd 覆盖失败清理、迟到后端和客户端超时；`platform/adxlet/tests/lifecycle.rs` 覆盖清理／提交失败时释放资源、整卡复用及资源已释放后的 Failed 巡检。这些是组件契约用例，不是实际运行时端到端证据。

## 2026-10-04 实际运行时故障回归

从干净 ADX `2e15df5` 源码构建 Linux ARM64 adxlet，SHA256 为 `b01bb4a98bad51104b5e4874d4522723c81d0af4f1d0297042bcaaa9a1246039`，覆盖已记录的两节点 runc fixture。sandboxd 使用 `eceda17` 修复二进制，SHA256 为 `0d28a3c1afe1ba92b73a789375e46834dc4b12b2dff62be18840e88eae35c852`。其他组件沿用 fixture，属于最新 adxlet 的真实 SDK 故障链路回归，不代替完整正式发布包验收。

gRPC/UDS 代理分别注入执行前 Start 错误、真实 Start 成功后切断应答、切断应答并使 Delete 不可用，以及超时后才执行的迟到 Start。四例均进入 Failed、`resources_held=false`，每例仅一次后端 Start。Delete 不可用期间可观察到一个真实残留后端；取消故障后巡检清空。迟到后端也被巡检清理，没有变成 Running。SDK 对公开创建请求的重试不会再次执行后端 Start。

随后正常 SDK command、文件读写和删除通过。逐例显式删除后 Redis 主目录消失，最终 Redis environment 字段为空、两节点 backend 为空，fixture 清理通过。注意这验证了显式删除和后端清理，当时未实现 Failed 元数据自动 GC；当前契约见 [Failed GC](failed-environment-gc.md)。证据保存在本地 `pr78-failed-create-release-20261003/evidence/`，包含 `failed-create-release.json`、`start-proxy-events.jsonl`、`fix-identity.json` 和 `result.json`；组件回归 369 通过、63 忽略，忽略的 Redis/RPC 用例不计通过。

## Reverse tunnel WebSocket 关闭

Execd 转发真实的 WebSocket close code 和 reason。空关闭帧在内部表示为 1005、空 reason；SDK 将其还原为空关闭帧，不能把保留状态码 1005 写入网络。传输错误或未收到 close 的 EOF 作为错误处理，不能伪装成正常 1000 关闭。隧道通道失败会向连接端返回 1011。

Rust Socket 用例验证两个方向的 1008/code-reason 透传；Python SDK 的真实 WebSocket 用例验证空关闭帧。2026-10-03 的 standalone 验收加载修复后的 Execd，并在实例内校验执行文件 SHA256。direct/tunnel 两条路径分别在并发 1、8、16 下运行 25 次连接，覆盖 0、1 KiB、64 KiB、1 MiB 二进制消息、ping，以及 1000/fixture-finished 关闭握手；全部通过并完成资源清理。证据为本地 `pr78-fixes-20261002/green-005.log`。

## 验证边界

2026-10-03 的本地测试使用 ARM64 Lima Ubuntu 24.04、Linux 6.8 和独立的两节点 Docker fixture。原报告使用 x86、48 CPU 和 AKernel fork gVisor；本地 ARM64 上游 gVisor 运行结果不能证明该 fork 的 gofer 并发故障已修复。运行时选择、bootstrap 来源和实际执行文件 SHA256 必须随证据记录。sandboxd 的守护进程重启与 supervisor stop 是两个不同操作，后者仍按显式停机契约删除本机实例。

## 原报告逐项闭环边界

核对日期：2026-10-05。原报告对应 AKernel `52d0e2f`、ADX `5e62b9f`、sandboxd `31d0749`。以下将修复、未复现和交付验收分开；源码发布不等于正式包或集群已升级。

| 原报告问题 | 当前证据 | 尚未闭环 |
|---|---|---|
| 后台命令短 wait、自然退出 PTY 对象保留 | AKernel `6953519` 修复；271 SDK 单测、永久 standalone 两项和 100 个自然退出 PTY 通过 | 新 SDK 正式制品及部署 pin 整合 |
| Start 失败／应答丢失后持续持有资源 | 创建 Failed 即释放；清理与提交失败、Journaled 降级、GPU/NPU 复用和 UDS 迟到应答均有组件测试 | 同一正式制品及部署 pin 整合；Failed 元数据 GC 已接入，正式包与部署版本须单独核对 |
| runsc gofer `EBUSY` | cn-north-4 六场景累计 8,189 次生命周期成功，未复现 | 原 x86/Linux 6.8/fork runsc 同条件复现及 main A/B；根因未定位 |
| 混合压力文件截断／EOF | 原负载 runc 38,618 个读写周期、2,400 次生命周期通过；集群 runsc 22,859 个周期无错误 | runsc 混合仅提交 1,106/2,401 创建时隙，未完成 20 create/s；原 EOF 根因未定位 |
| tunnel WebSocket close 丢失及并发消息 EOF | 关闭帧已修复；runc 和修复制品 runsc 的 direct/tunnel C1/C8/C16 各50连接、200消息、2,048 ping/pong通过 | 原报告并发EOF根因未定位；不能由当前消息矩阵通过推断其根因已解决 |
| sandboxd 重启后 runsc 实例丢失 | sandboxd 停机保留工作负载已修改；runc 与 cn-north-4 runsc 的同 backend／IO 恢复通过 | 正式制品/pin 整合与部分启动隔离资源最终清理 |
| Kata guest 缺少 devpts、PTY／CLI 不可用 | sandboxd `6976ee9` 配对修复；本地 ARM64 KVM/QEMU 七项完整 PTY/CLI 矩阵通过 | 原 x86 平台配对 |
| bpfnat 动态 block 后控制面失联 | 真实 TC/BPF 测试及 Firecracker＋bpfnat block/clear 通过 | runsc＋bpfnat 控制端口放行和持续控制链路 |
| S3 根对象空 prefix 忽略覆盖配置 | sandboxd 修复；空／非空 prefix 的 HEAD/Range 读取通过 | 真实 AWS S3／OSS 签名、ACL 和网络互操作 |
| 旧 wait／rrt.sock 文档 | AKernel 当前源文档已同步 wait 契约和 execd.sock | 随正式制品校验部署文档与 pin |

终态 Failed 控制任务与 Redis 主目录由 Coordinator 按保留期自动回收，默认十分钟；待重启／恢复场景受保护，清理失败会重试。最小删除回执保留十分钟，详见 [GC 契约](failed-environment-gc.md)。创建尾延迟仍需独立优化；首请求缓存缺失503已通过本页第十九轮修复回归，长期及更高负载尚未验收。测试部署私有SWR凭证接入及resources/queue入口404已修正并通过公开链路验收。GPU 实机、多节点亲和、整节点故障与共享 checkpoint 跨节点恢复、长期 soak 属于原报告未覆盖的验收边界，不列作已复现缺陷。

## 2026-10-04–05 本地与测试集群补充

本地 Kata 可以验证完整交互链路。实际 KVM/QEMU、Kata runtime-rs 的 native PTY、SDK 退出码、输入/resize/Ctrl-C、独立会话、close 终止远端进程与实际 AKernel CLI handler 均通过，实例内 Execd SHA256 为 `0ade7ed7b16c47221a412653038378c2820206bf8174c249100f0c140c7dd95e`。该 ARM64 验收不等同于原 x86 平台配对。本地 ARM 上游 runsc 在启动阶段遇到 SIGSYS/readv，与原报告 fork runsc 的 gofer EBUSY 不同；既有集群 EBUSY 未复现及未达成的混合发压速率仍按前文边界记录。

测试集群公共 resources/queue 404 已定位到 Ingress 部署配置缺少 `/global-scheduler` 前缀，修正后两接口 HTTP 200；源 Helm 模板已有该配置。Kubernetes imagePullSecret 不自动成为 sandboxd 仓库凭证，测试部署已复用现有 Secret 的 Docker auths 填充挂载的 registry_auths.json 并逐节点重载，验收日志不包含凭证值。

历史集群 Redis 快照只扫描独立 environment 键，遗漏 `adx:{akernel-adx-test}:control:v1` hash 内字段，不能证明实例目录为空。新审计发现 9,909 条 Deleted、1 条 Failed，Deleted 序列化值合计 42,366,500 字节，均不持有资源。旧版本 InspectNode 返回约9.78MB，超过客户端默认4MiB解码上限。最新源码的原子启动回收已通过三个真实 Redis 旧记录退役/同名重建测试；该行为的正式包与部署版本仍须独立核对。adxlet 对账失败日志保留实际 RPC 错误，避免仅报告目录不可用而丢失原因。

最新路由发布实现由提交事件触发，10ms 有界合并；200ms tick 用于异常视图恢复。不能继续把“事件触发尚未实现”列为源码缺口，也不能由源码通过推断旧集群的短暂503已经消除。历史 C128/C256 使用100m CPU请求，单节点8CPU最多同时预留80个，尾延迟包含中心排队，需与容量内的启动、就绪及发布延迟分别测量。

Environment 状态为 Pending、Starting、Running、Pausing、Paused、Resuming、Deleting、Deleted、Failed。资源持有、待重启与checkpoint字段另行判断；终态 Failed 支持自动元数据 GC，仍不能用“资源已释放”代替“元数据已清空”；保留期与恢复保护见 [GC 契约](failed-environment-gc.md)。

2026-10-05 在 `akernel-adx-test` 使用 `fdccbfa` 构建的 Coordinator 验证二进制（SHA256 `93e8dd64d34427324f3b4b938ebd6d36f734cf6f5e3ab9d2eb06cb4779595d80`）执行启动迁移：9,910条Deleted被原子退役，control hash从9,915字段降至5字段，仅余1条Failed和节点／控制字段，两节点均恢复routable。测试未手动HDEL；替换位于Pod可写层，重建恢复旧镜像，不算正式升级。原Coordinator对SIGTERM未退出，SIGKILL后supervisor重启一次；优雅退出挂起仍是新发现，尚未修复。

测试集群 runsc 使用 sandboxd `eceda17` x86_64 验证二进制：daemon重启前后 backend ID `sbox-e50cbefd-cafe-4ccb-93dc-afd1a2aade1e` 一致，marker 文件和 command 读写均通过，重启后首个 IO 验证耗时约 94ms（驱动已先等待 3 秒，不代表完整故障恢复耗时）；私有SWR digest镜像的创建、命令与文件读写通过（首次创建1.780s）。这些属于修复制品的实际运行时实验，恢复原sandboxd binary/NAT后仍须检查物理后端及元数据，不以SDK handle.close/kill的返回代替显式删除验收。

测试集群 bpfnat独立NAT在Linux5.10加载成功，但当前sandboxd ACL依赖Linux5.17+的`bpf_loop`，其程序被5.10拒绝，daemon无法启动；因此runsc＋bpfnat组合未完成，需满足ACL前提的节点。临时NAT与sandboxd binary已恢复。runsc旧运行时直连WebSocket C1/C8/C16全部通过，tunnel C1仍复现1005/空reason的关闭帧问题，需验证包含已修复Execd的运行时制品。

容量内C1/C8/C32共41次创建后首次raw HTTP command均200，首次请求16–61ms；C8创建p50/max为489/576ms，C32为916/1010ms。daemon重启后的首个C1样本34.4s，包含尚未拆分的准入恢复/等待；这些不是纯Start性能或长期P99证据，创建尾延迟仍未达到既定目标。

启动就绪需要单列：当前 sandboxd 的 `Healthy()` 依赖 manager housekeeping 发出的初始健康信号，首次周期为 35 秒。实验中 daemon 重启后三秒就创建会返回 NOT_SERVING／Unavailable；SDK 对原身份重试不会把已失败创建重新启动。后续压力驱动应先用实际 gRPC health 的 SERVING 状态确认就绪，再计算稳定运行的创建延迟。首个 34.4 秒样本与该启动窗口吻合，但尚未完整拆分各阶段，不能把它当作单次 sandboxd Start 的耗时。

同一批 41 条日志可配对拆分：sandboxd 收到 Start 到 adxlet 后端身份映射，C1 约 123ms、C8 中位/最大 199/216ms、C32 为 461/577ms；身份映射到 Running 提交，分别为 109ms、168/272ms、319/420ms。后段包含执行服务就绪、绑定和提交，不能归因为单一模块；同机墙钟日志差值也不代替完整客户端延迟或压力 P99。C1 在 daemon 启动约 35 秒后的首轮健康检查后才进入 Start，说明其主要耗时在启动就绪等待；稳定并发下的 Start 和后续阶段延迟仍需优化。


### Failed 重启对账与显式删除

本轮实际测试发现：旧 `InspectNode` 排除了不占资源、不等待重启的 Failed；节点重启后失去对应控制器，DELETE 返回节点未管理该实例，SDK 把404视作已删除，但 Redis 完整记录仍保留。当前目录保留 Failed，包括已失效的归属。adxlet 对账只清理旧执行，不恢复 Running；存储校验继续拒绝失效归属重新占资源或发布 Running。

测试先验证原过滤行为失败，再验证修复通过；真实 Redis/mTLS 覆盖心跳失效、返回对账、旧 Running 提交被拒绝、Deleted 提交退役。在测试集群加载 `fdccbfa` 加该修复的 Coordinator（SHA256 `cf55aacfcc21a27e0fd01800c10e6100719968c28f86c082ea8c1cd46c39f62f`），节点重启后显式 DELETE 返回200，对应Failed字段消失，保留1条测试前已有Failed，没有手动HDEL。该轮修复补齐显式删除；随后另行接入 [Failed 自动 GC](failed-environment-gc.md)。

同名重建还须同步调度器：归属失效时内存 `retired` 标记禁止旧请求再次入队；显式删除持久化成功后必须清除该标记。当前 Deleted 提交确认无调度占用后解除标记，未删除 Failed 不受影响。回归覆盖 Failed 身份禁止重排、Deleted 后重新入中心队列、仍有待调度工作时拒绝清除；全套27项真实Redis/mTLS RPC通过。


### runsc 修复制品消息矩阵

2026-10-05 第十七轮在测试集群加载静态MUSL Execd（SHA256 `67a90a41d61b7dce2a0242cdc0b70eb455e0a12fe2062921edfeae9a19c758cb`），实例内再次校验SHA。direct/tunnel各C1/C8/C16共六组，50连接、200二进制消息、2,048 ping/pong均通过，关闭均为1000/fixture-finished，清理与配置恢复完成。原运行时的close丢信息已复现，修复制品该矩阵通过；原混合负载／并发消息EOF根因仍未定位。

临时EROFS最初遗漏正式构建要求的 `-E noinline_data`，GNU与MUSL版本均加载失败；补齐该参数后MUSL启动及消息矩阵通过。正式ADX `build/runtime/rootfs.py` 与AKernel `builder/runtime.Dockerfile` 原本已有该参数，不修改sandboxd源码来规避该制品问题。证据为本地 `pr78-local-closure-20261004/cluster-closure-017.log` 与 `attempt-017/cluster-evidence.tar`。最新Coordinator验证二进制SHA256为 `7038fbf0c17295c8aed13da5a322138b978ee8276b3cbf32916806f22c9d4f98`，包含本轮Failed目录及同名中心重建修复；它仍是测试Pod层替换，不代表正式镜像发布。


### 暖启动扩大样本中的路由竞态

第十八轮在实际SERVING和节点准入后计划C1/C8/C32各三轮，共123次。只完成42条样本，SDK构造的只读process.list已出现路由缓存缺失503，随后一次请求三次重试均Broken pipe，驱动退出失败。首轮C1/C8/C32创建中位数分别约723/572/1151ms，最大为723/617/1347ms；它们包含SDK内部路由就绪检查及重试，不能当成纯Start或P99。42个完成样本的首次raw command通过，不能据此抹去SDK构造时的503或将123样本标成通过。

本轮恢复后独立核查两节点backend为0、默认profile和原adxlet二进制恢复、supervisor重启预算为0，Redis保留测试前已有的1条Failed。证据为本地cluster-closure-018.log、attempt-018/cluster-evidence.tar和restore-final-audit.json。修复前stream-only resolver在增量到达前立即返回Unavailable；这一缓存到达窗口与观测的503吻合，新增到达契约测试已先失败。Broken pipe的具体根因仍需单独定位，不能由503归因替代。

Ingress 当前对缓存缺失最多等待50ms内对应订阅路由的到达，命中立即返回；先订阅再重读，避免漏过同时到达的增量，无关事件不延长截止时间。超过窗口仍返回Unavailable，不增加Coordinator点查或SDK重试。9项路由契约、48项Ingress单测和workspace严格Clippy通过；集群复测单独记录，组件测试不证明实际负载已经消除503。

第十九轮加载包含该修复的合并API Server／Ingress二进制，实际执行文件SHA256为`aef160483ad73c0dadd448fbdcebfb3c955eaae11975360659cc7a671b8151f4`。C1/C8/C32各三轮123样本全部创建和首次raw HTTP command通过，日志缓存缺失503与Broken pipe均为0；首次命令16.5–97.4ms。创建中位数/最大分别为472/837ms、579/667ms、1037/1279ms，仍超过既定目标。第十八轮Broken pipe未再次出现，根因仍未独立定位；不能以本轮通过宣称其根因修复。此次实验不更新AKernel PR，不代表正式包或生产部署升级。证据为本地attempt-019/cluster-closure-019.log及cluster-evidence.tar。

独立恢复审计确认两节点物理backend均为0，原adxlet SHA、默认rootfs/bootstrap和supervisor预算恢复；Redis仅有测试前已有1条Failed，活动两节点available/routable，未手动删除已有记录。恢复与最终审计日志分别为restore-audit-019.log、final-audit-019.log。

## C1 性能口径核对（2026-10-05）

2026-09-27 Build #116 的原 HTTP Create 基准，隔离 C1 每次创建后删除：100 次 P50 104.088ms、P95 161.323ms、P99 179.749ms；create 返回后的路由传播单独计时，P99 115.584ms。2026-09-28 Build #121 另有单次 C1 62.089ms，样本仅一条，不能据此声称稳定 P99 为62ms。此前百毫秒级是完整 HTTP Create 到 SSE Running，并非 sandboxd Start 单阶段。

第十九轮的 C1 中位数471.7ms来自 AKernel SDK Sandbox 构造，包含后续 process.list 等就绪访问；使用节点192.168.10.179、自定义镜像、runtime limit 500m/512MiB。旧基准固定另一节点、复用 HTTP 连接，runtime limit 1000m/2GiB。不能把这两种边界直接比较并宣布创建性能回退。仍需在相同节点、镜像、资源限额和计时边界下补配对测试，分别报告 raw Create 和 SDK 构造。

## Failed 自动 GC（2026-10-05）

Coordinator 默认保留终态 Failed 600 秒，支持 `failed_retention_seconds` 配置。失败时间持久化、回收批次有界；在线节点先清理后端再退役，失效归属原子退役后由返回节点对账清理。待重启／恢复受保护，完整契约见 [Failed GC](failed-environment-gc.md)。

真实 Redis storage 全套34项、真实Redis/mTLS RPC全套29项、adxlet lifecycle 28项通过，workspace fmt／严格Clippy通过。Lima ARM64 两节点 standalone 使用真实 runc、Redis和公开Sandbox SDK，注入Start成功后丢失应答及Delete不可用：创建Failed且资源释放，超过3秒测试保留期仍保留无法清理的元数据；解除故障后自动GC，无显式DELETE；12.676秒收敛到后端与Redis Environment为空，同名重建generation 1→2，命令调用及删除成功。fixture清理通过。

实际GC验证使用当前源码的Coordinator/adxlet覆盖既有隔离runtime fixture，不代表正式镜像或生产集群已升级。sandboxd故障代理沿用前轮制品，本次未修改sandboxd。证据位于本地 `pr78-local-closure-20261004/failed-gc-standalone-evidence/`，包括身份摘要、逐步日志、`failed-create-release.json`与`result.json`；`failed-gc-standalone.log`保留构建和全程输出。

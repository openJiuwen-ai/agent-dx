# DFS / JuiceFS 双口径对照驱动

该目录只包含可维护的测量工具。产品由外部 fixture 启停；SeaweedFS、PostgreSQL
的部署、实验数据和主机路径不属于此目录。驱动不会创建 VM、格式化磁盘、部署后端、
全局停止进程或修改后端参数。

Meta 专项使用独立的[显式基线辅助程序](meta-bench.md)，不复用文件读写的计时
或将 Store 层结果当成 FUSE／RPC 性能结论。

## 测量边界

- `contract`：双方 `O_RDONLY | O_DIRECT`，记录并核对 `read_ahead_kb=0`、
  `prefetch=0`。用于自身读引擎的同条件比较。
- `product`：双方 `O_RDONLY`，使用测前固定的真实缓存/预读配置并记录具体值；
  如启用候选新增预读，须显式配置和验明生效，不能以旧默认冒充开启。
  单独报告页缓存收益，不能用于声称历史 O_DIRECT 路径提速。
- A 提前准备相同内容及 SHA-256；A/B 控制进程和 TCP 通道提前建立；两条等待连接在 GO 前通过 Linux pidfd 阻塞，
  GO 后不启动 SSH/Lima/Python 轮询或主动采集。ready 并预备完成后，
  一律静置 30 秒；读取前的指标快照在 A 创建文件之前完成。
- A 默认按 4MiB 调用写入，可通过固定的 `write_block_bytes` 调整；`write-1g` 场景必须为 1MiB。
  执行文件 `fsync → close → rename → 父目录 fsync` 后通知 B。
  B 每次重新打开，以 4MiB `readv` 读到 EOF，并核对长度及完整 SHA-256。
- 主 E2E 是 **A 创建开始到 A 核验 B 首读 SHA/EOF ACK**，由 A 单一单调时钟直接测量。
  `total_with_repeat_ns` 另列，包含固定一秒复读等待。二者都不做阶段求和或 S3 扣减。
- A/B 指标在首读之前、首读 ACK 之后、复读 ACK 之后采样。中间快照使用固定复读间隔，
  不计入应用 read 或首读 E2E。A/B 均记录快照原始耗时；快照本身或首读 ACK 边界至快照完成超过半秒时，该轮失败并保留时长证据，不允许靠延长固定复读间隔通过。
  快照包含各自单调时钟；差分不代表所有跨组件累计耗时都在关键路径上。
  兼容字段 `metric_deltas.*.first` 的实际区间由 `metric_window_boundaries` 明确：A 是 publish＋first，
  B 是 BEGIN 至首读 ACK，均不是严格隔离的单次 read；repeat 区间也包含固定等待，不能据此把写入 RPC 归因成读 RPC。
- 每种系列、每个标签恰好三轮，串行执行；默认测前固定顺序 AB/BA/AB。原始值、中位数、max/min 全部保留；
  稳定性门槛为 `max/min <= 1.10`，不满足时不追加样本。`completed` 与 `stable` 分别登记。

## 1GiB 场景与写入边界

`scene` 默认 `e2e`，沿用已有 64MiB 等规模的数据和完整流程。`cold-read-1g`、
`write-1g` 要求 `size_bytes=1073741824`；两者仍完成写入发布、首次读和复读，
不会为了取得局部带宽取消内容、长度或 EOF 核对。冷读场景以首次读为判据，
冷缓存状态仍由外部 fixture 的全新命名空间及挂载证明，场景名称不等于冷缓存证明。

`write-1g` 固定 `write_block_bytes=1048576`。`writer.publish.write_and_fsync_ns`
使用一次单调时钟区间：打开文件后、第一次写循环之前，至成功文件 fsync 返回。
区间包括写循环、回压和文件 fsync，排除 open、close、rename、目录 fsync。
原 `publish_ns` 和完整 E2E 保持原边界。输出同时保留 open 完成、写起点、文件 fsync
完成及整个发布完成的时间点、应用块大小和真实 `os.write` 次数；短写重试单独计入次数。
这些是应用调用，不代表 FUSE 回调的尺寸或次数。与本地盘分母比较时必须使用相同边界。

写端在计时前准备一整份数据：1GiB 场景会由测试进程持有 1GiB 正文，独立于 Node 产品预算，
还需计入客户端、页缓存及其它服务。`workload.driver_prepared_payload_bytes` 明确记录该项。
可用 `nodes.*.min_available_memory_bytes` 在所有节点启动前检查 Linux `MemAvailable`；
它只是准入下限，不会调整任何内存上限，也不能替代 fixture 对启动后峰值的预算核验。

## P2 必需指标

仅 AFS label 配置 `metric_profile="p2"`，并提供各一个 A、B metrics endpoint。
JuiceFS label 不配置该字段，不要求其导出 AFS 指标。每个节点分别核对 before、
after_first、after_repeat 三个快照，不允许 B 的指标补足 A 的缺项。
必需项覆盖 G-P2-6 的 A 授权次数、B 拿源次数、producer/consumer/覆盖组校验字节、
cache_to_output、cache 正文读取次数及 FUSE read 次数；正常与回退所需原始 hash stage
分别保留。outboard 的四个 ledger gauge 和五类事件也须实际存在。

`metric_evidence` 输出节点、各快照必需值、原始 outboard gauge 和逐项缺失清单。
缺失、非有限值或缺失快照均为“证据不完整”，使本轮失败；已完成的应用结果及原始快照
仍保留，正常停止并核对清洁状态，保留失败数据，不自动重试。观察到的 0 才是 0；
不会用缺失的 label 构造 0。P1M 的 `PeerDiskCache::with_read_work_metrics` 会发布实际的
cache_to_output 初始统计，不能假定未配置此缓存的场景也存在该值。

原文保留全部 outboard 指标，四个 gauge 另按采样点提取；可下降的 gauge 不做计数差分。
三次边界采样不是持续峰值采样，不替代资源诊断 run。完整性通过也不等于 G-P2-6 的
机制门槛通过，不能从聚合计数自行推断各阶段都在关键路径上。

R-7 候选可另外设置 `fetch_metric_profile="r7"`，必须同时选择 `metric_profile="p2"`。
此时 B 的三个快照还须存在 `afs_dfs_fetch_events_total` 的全部固定 event：
`scheduled`、`skip_disabled`、`skip_budget`、`skip_grant`、`skip_busy`、`joined`、
`cancelled`、`failed`，以及 `afs_dfs_fetch_resources` 的 `jobs`、`metadata_bytes`、
`metadata_peak_bytes`、`job_limit` 四个 resource。前者单位为事件次数，不保证每种分支
都会发生；后者分别是任务数、字节、字节、任务上限。原始 gauge 单独保留，不做计数差分，
三个快照不构成任务峰值或准入正确性的证明。未创建调度器时 `job_limit` 可以是实测0。
缺少任何固定 label 使证据不完整，不能补零。旧 P1M 不选择该 opt-in，不强求新指标。

## 使用

复制 [config.example.json](config.example.json)，填入当前环境的固定二进制 SHA、
外部 fixture、挂载、资源预算和有效配置。示例不是可直接运行的部署配方。

```sh
python3 build/e2e/afs/perf/dfs_compare.py --config /absolute/private/config.json --validate-only
python3 build/e2e/afs/perf/dfs_compare.py --config /absolute/private/config.json --series both
python3 -m unittest discover -s build/e2e/afs/perf -p 'test_*.py' -v
```

`--labels afs-baseline,juicefs` 固定执行顺序；`--rounds` 仅用于明确覆盖配置，正式运行仍必须
是三轮。诊断配置必须 `diagnostic=true, rounds=1`，使用独立输出目录；其证据不混入正式样本。
该工具就绪或单测通过不代表基线、诊断或功能验收通过。

## 配置与外部 fixture 合同

JSON 字段如下：

| 字段 | 要求 |
|---|---|
| `orders` | 可选，每轮完整 label 排列；默认 AB/BA/AB，执行前固定，不按结果调整 |
| `scene` | `e2e`（默认）、`cold-read-1g`、`write-1g`；后两项固定 1GiB |
| `write_block_bytes` | 默认 4MiB；正数、4096 对齐、至多 4MiB；`write-1g` 固定 1MiB |
| `run_id` | 无路径分隔符的独立标识。实际每轮为 `<run_id>-<series>-<label>-<round>` |
| `output` | 源码树外绝对路径，必须不存在或为空；失败不覆盖旧结果 |
| `nodes.a/b/c.transport` | `ssh` 或 `limactl shell VM --` 的 argv。SSH 逐参数 shell quoting，无模板 eval |
| `nodes.*.command_prefix` | 默认空；仅允许显式 `["sudo", "-n"]`，用于固定为与 FUSE 挂载相同的 root 测试身份 |
| `nodes.*.root` | Linux 上独立控制根；每轮创建其下唯一目录，与挂载数据目录分离 |
| `nodes.*.min_free_bytes` | 启动前最低容量。必须计入正文、副本、缓存和临时产物 |
| `nodes.*.min_available_memory_bytes` | 可选，启动 fixture 前的 Linux `MemAvailable` 下限；1GiB 写端须额外计入测试正文 |
| `nodes.*.dependencies` | 预检查的可执行文件名 |
| `nodes.*.ports/process_names` | 不能已被使用的端口及精确进程 comm 名称；不得列入共享后端进程 |
| `nodes.*.binaries` | `{path,sha256}` 数组；启动前完整校验。客户端、Meta、参考版本由配置提供 |
| `control` | 明确的 B 监听/连接地址、A 源地址及独立 TCP 端口；HMAC 密钥逐轮生成、不写入证据 |
| `labels.*.mounts.a/b` | 当前系列的空白测试挂载根，目标文件固定 `test.bin` |
| `labels.*.identity` | 候选/参考 commit、tree、二进制目录等身份，原样记录 |
| `labels.*.settings.contract/product` | 实际预读、预取及其他固定配置；`ready` 必须逐项确认一致 |
| `labels.*.budgets` | Node、Peer 磁盘缓存、载荷、descriptor、VM 等预算；fixture 核验，驱动记录 |
| `labels.*.page_cache_initial_state` | 初始页缓存状态及清场方式，不能笼统写“冷缓存” |
| `labels.*.external_backend` | 外部 S3/Meta 版本与连接配置身份；认证仅引用外部私密配置文件 |
| `labels.*.metrics` | `{node:a或b,name,url}`，节点回环 HTTP `/metrics` 地址；保留原文和差分 |
| `labels.*.metric_profile` | 可选 `p2`，启用上述逐节点、逐快照完整性检查；JuiceFS 留空 |
| `labels.*.fetch_metric_profile` | 可选 `r7`，仅在 `metric_profile=p2` 时启用 B 调度事件及资源快照完整性检查；旧基线留空 |
| `labels.*.metric_counter_patterns` | 可选，单调计数器名称匹配，默认 `_total/_bucket/_sum/_count`；原始 gauge 不做工作量差分 |
| `labels.*.hooks` | 下列六组顺序命令。每条为 `{node:a/b/c/controller,argv:[...]}` |

每个 label 必须提供：

1. `preflight`：只读核对候选身份、配置、目录空白、实际挂载/资源/端口和环境占用；输出紧凑 JSON。
2. `start`：只启动该 run 的 AFS 或 JuiceFS 客户端和必要的自有 Meta，不能部署/修改第三方后端。
3. `ready`：输出单一 JSON `{ "ready": true, "effective_settings": { ... } }`，有效配置必须匹配配置文件。
   AFS `prefetch` 的0/1必须由候选实际解析的对象预读配置导出，不能固定写0或仅回显
   期望值；启用时还须在fixture证据保留确切字节距离、A/B配置摘要和实际ELF身份。
   contract必须确认真实OFF，product启用须测前显式冻结。具体Node配置写入由外部
   fixture负责，通用驱动不生成TOML或部署产品。
4. `stop`：正常停止本 run。`{successful}` 为 `true` 时可先在挂载仍有效时删除 `test.bin` 并 fsync
   父目录，再卸载/排空；为 `false` 时保留复现数据。不得停止共享服务或按名称全局 kill。
5. `cleanup`：仅在测量及正常停止成功时调用，清理本 run 的可再生数据。S3 对象/collection 只经
   外部已核验接口回收；不能直接删除其底层文件。证据、失败和配置必须保留。
6. `verify_clean`：输出单一 JSON `{ "clean": true, ... }`，核对无本 run 进程、挂载等残留；
   不要求保留的失败数据消失。清理失败使整轮不能通过。

argv 允许 `{run_id}`、`{label}`、`{series}`、`{round}`、`{run_root}`、`{mount}`、
`{size_bytes}`、`{successful}`、`{diagnostic}`。它们只是逐参数替换，不执行表达式、shell
或命令替换。`controller` 在编排主机执行，其他命令在对应 Linux 节点执行。fixture 必须
验证这些值的授权范围，不能把它们交给 eval。hook 默认180秒，`hook_timeout_seconds` 可显式覆盖。

任何失败都会保存当前轮的结果和已取得的原始证据，然后停止本次序列，不重试；其他独立序列
由调用方另建 run 决定。控制进程按 PID/start_ticks/boot_id/命令身份定向停止，拒绝误杀。
控制工具按内容 SHA 在每节点复用一份，不逐轮复制源码快照；每轮只保留控制状态和日志，密钥退休。大型结果不入源码树。配置/日志可能含基础设施地址，
请使用私密输出目录，并在分享前核对内容。

## 验证局限

单测仅覆盖驱动合同，不证明 Linux FUSE 行为、实际性能或第三方后端回收。实际运行须同时保存
固定二进制/配置身份、fixture 准入及恢复证据。当前驱动输出原始指标和差分；某个版本没有尺寸
直方图或相应计数器时应标为缺失，不能伪造零值或宣称验收完整。旧基线与增加低开销尺寸观测的
变体须分别记录二进制身份。

仅验证控制协议时，可在 Linux ext4 数据目录运行 `python3 build/e2e/afs/perf/smoke_protocol.py --root /absolute/fresh/smoke`。
该用例只跑64KiB普通文件系统、两种open模式和pidfd等待，不运行FUSE，也不采用正式30秒静置，不能作为产品或性能验收。

诊断轮次（`diagnostic=true`）额外记录B端完整read的单调起止和实际readv区间（含EOF），最多128条、溢出另计；正式轮次不保留该列表。区间复用原计时边界，完整read仍包含应用SHA256、open和close。实际FUSE回调仍须从Node记录关联，不能假设应用4MiB等于一次FUSE请求。跨节点时钟不得直接相减，诊断时延不作为正式性能成绩。

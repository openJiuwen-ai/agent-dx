# Meta 基线驱动（显式运行）

`afs-meta-bench` 是测试辅助 example，不是 Meta 产品进程，不进入普通交付包。
它调用现有公开 Store API，另提供 DfsService 创建／重试／查询的功能检查；
不实现 P3 新状态引擎，也不测 FUSE、网络 RPC 或正文传输。

仅在 Linux 构建和运行，沿用 ADX 公共工具链与既有缓存：

```sh
cargo build --locked -p afs --example afs-meta-bench --release
cargo test --locked -p afs --example afs-meta-bench
```

每次运行须提供真实的完整源码 commit、包括未提交工具改动的源码清单 SHA-256；
工具会另行计算自身 ELF 的 SHA-256。外部证据还须记录工具链、镜像、内核、
CPU/内存/磁盘及挂载身份、进程占用和完整命令。驱动自身不能证明环境满足 NVMe 条件。

## 数据与计时边界

数据集参数为 `--dataset s|m|l10|l`。S/M 含版本、布局、chunk、placement和copy
元数据；L10/L 没有 chunk。业务文件之外有一个公共根 inode，实体分类数量明确输出。
chunk 根来自固定 4MiB 内容的真实标准 BLAKE3，但不创建正文文件，不承诺这些合成
副本实际存在。此工具的数据只是 Meta 基准夹具，不能作为数据副本／持久性验收。

- `--phase seed` 只允许新数据目录，每批最多256个文件，完整文件的inode、dentry、
  版本、布局、chunk、placement、copy在同一笔事务内生成；公共根只写一次。
  全夹具仍默认最多300万业务实体，按全量计数准入，超出明确拒绝，不缩小规模。
  生成和提交时间分别累计，另输出seed墙钟、批数及最终revision，不混进请求时延。
  S仍为十万文件／700001个业务实体，但批次各有outcome、revision与历史；
  这与旧版单笔seed的历史分布不同，不能称初始历史完全相同或改写旧结果。
  末尾显式安装同一已提交状态的checkpoint，单列sequence与耗时；这是测量前准备，
  不计入请求成绩。若后台checkpoint已持有单飞，只对明确的busy拒绝等待最多30秒，
  然后捕获新的已提交状态；真实编码/持久化错误立即失败，不做I/O重试。失败时不写成功fixture标记。
  不为工具调大产品record/history/WAL额度；中途失败保留目录，不原地续跑或重置。
- `--phase measure --operation create|commit|read|watch`：四次固定预热后开始测量。
  请求构造在逐次计时之外，计时从调用 Store API 前到完整返回；请求失败／超时保留，
  整轮标为失败并退出非零。Store create是合成事务，不等同于独立客户端的真实RPC create。
  watch沿用旧基线的“当前seed末尾无新事件”轮询：游标从fixture的最终seed_revision取得，
  输出实际watch_after_revision；返回非空事件或Compacted则报错，不能混作更便宜的空轮询。
- `--phase recover` 在新进程中重开该目录，计时包括 Store 的后端加载和状态恢复，
  并核对最后一个seed inode。schema v2分别记录backend_open_ns、store_open_only_ns、
  total_store_init_ns；旧v1 recovery_ns仅含Store::open，未包含此前后端open的第一次load。
  完整进程墙钟由外部collector记录。这只是正常重开，不能代替崩溃矩阵或完整恢复内容核验。
- `--phase service-smoke` 用真实 DfsService 验证创建、原身份重试及lookup；只证明功能。

输出为新建的 JSON 与同名 CSV。缺失指标为 null／空列，不填0；既有输出不覆盖。
`--qualification-inodes N` 只用于小规模驱动资格检查，始终标记 qualification-only，
不能以此通过正式 S/M/L 门槛。测量使用精确的纳秒时延频数表，保留每次观测及重复次数，按nearest-rank
计算分位数；不同纳秒值的预算为262144个（各worker等分），超出后明确失败，
不抽样、不量化时延、不丢长尾。该上限限制统计内存，不限制重复观测次数。

`--diagnostic` 单列canonical append累计调用、decided记录的诊断JSON字节及append耗时；不是实际framed磁盘字节，也不包含checkpoint总成本，不用于正式成绩。旧snapshot候选的结果保留原定义，不追溯换算。
累计耗时包含重叠，不与E2E相加或相减，不能用其反推fork／JSON的各自时间。
尚未测量的阶段在输出中明确列为未测。

## 一次小规模资格检查

在仓外创建新的空run目录，设置`SOURCE_COMMIT`、`SOURCE_MANIFEST_SHA256`和`BIN`
为本轮真实值；不得把示例变量当成身份核对。以下每个phase有独立输出：

```sh
"$BIN" --phase seed --dataset s --qualification-inodes 64 \
  --store "$RUN/store" --output "$RUN/seed.json" \
  --source-commit "$SOURCE_COMMIT" --source-manifest-sha256 "$SOURCE_MANIFEST_SHA256"
"$BIN" --phase measure --dataset s --qualification-inodes 64 \
  --operation read --seconds 1 --store "$RUN/store" --output "$RUN/read.json" \
  --source-commit "$SOURCE_COMMIT" --source-manifest-sha256 "$SOURCE_MANIFEST_SHA256"
```

正式运行不传qualification选项，每轮至少30秒、候选冻结；三轮中位和max/min≤1.10。
每个写入轮次须从等价的初始fixture开始，明确记录数据增长，不能把上轮修改后的目录
当成同一初始状态。campaign及整改预算沿用已批准计划，不由此工具自行追加轮次。
24h长稳暂停不改变资源有界和正常错误传播的功能要求。

## 有界阶段诊断

`--phase diagnose --diagnostic` 只执行两波、每波4个小commit；不按成绩延长、
不进入正式三轮。复用现有Store span，仅安装测试侧Subscriber，不改产品源码／依赖。
每个span最多128条，记录batch_id、单进程monotonic边界与busy时间；异步await
使用first_enter至last_exit，busy只计实际进入。同步clone/sort的span变量仍可被持有，
因此不能把created至drop当作该组件耗时。commit_batch的完整生命周期另记。
输出每个调用区间和原始阶段记录；按区间并集核对关键路径，重叠不直接相加。
溢出、未闭合span或请求错误使诊断无效，不拿ON结果承诺OFF收益。

诊断在开Store前另行重建同形seed，只计算当前JSON的key与VersionedEntity编码
字节（含revision／entity），请求回执另列；不含snapshot外壳和历史事件重复，
不能把它称为完整磁盘字节或P3未来codec大小。运行须用可再生的seed副本，
保持封存的OFF三轮和原seed不变。M／L暂缓不因诊断而放开。

## 有界资源核算

Linux可显式运行现有Store测试中的资源报告，不启动Meta服务或正式性能campaign：

```sh
cargo test --locked --release -p afs --lib --all-features \
  meta::store::tests::p3_a77_owned_memory_and_real_wal_resource_report \
  -- --exact --nocapture --test-threads=1
```

输出`P3_A77_RESOURCE_REPORT`：两个64/128 inode＋chunk元数据夹具，
按表中真实key/value的owned capacity、内联大小和现有im/Arc预留核算，
并将逐项合计与State总账交叉验证；这些是逻辑估算，不是allocator或RSS。
字段、字符串capacity及架构不同会改变结果，不把单个夹具的每实体数字作为固定预算。

实际LocalLog文件增长与完整frame大小交叉核对，checkpoint文件与image正文分别记录，
通过checkpoint＋后续记录恢复相同字段。WAL比值的分母是canonical Record编码字节，
分子是实际文件长度增量；初始4096 B段头另列。它不包含ACK槽覆盖写、
设备写放大或CURRENT/checkpoint成本，不从这个比值推断E2E时延。
该诊断不替代S/M/L、进程故障、RSS或正式性能验收；各自保留原始版本与结果。

## 已封存 seed 的完整核对

旧`--phase recover`保留原来的末尾inode检查和计时定义；不追溯修改旧结果。
`--phase audit-seed`只接受未追加测量写入的、按256文件一批生成的seed副本。
它按批重建预期值，核对每个预期实体的全部字段、每实体mod revision、
每批原RequestOutcome以及前后business revision；不会重放或提交请求。
请求结果若已超保留期而缺失会明确停止，不能据此断言丢失已确认数据，
也不能用过期请求进行一次新写入来伪造恢复证明。

先保存seed的文件身份并复制到独立guest ext4目录，连同同名`.fixture.json`；
原seed保持封存。以下变量必须来自实际编译、文件清单和本轮新建路径：

```sh
"$BIN" --phase audit-seed --dataset s --store "$RUN/seed-copy" \
  --output "$RUN/audit.json" --source-commit "$SOURCE_COMMIT" \
  --source-manifest-sha256 "$SOURCE_MANIFEST_SHA256"
```

输出分别保留Store加载时间和完整核对时间；核对中的生成、完整BLAKE3摘要构造
及逐项查询不混入加载时间。此入口验证预期seed实体及原结果，不枚举任意额外实体，
不覆盖全部history/node_epochs、负载阶段新ACK、真实强杀、掉电或恢复期间的服务隔离。
因此它不能单独通过完整G-P3-5，也不追加或替代正式性能三轮。

### S seed 的进程强杀与已 ACK 后缀核对

辅助测试 `tests::seed_audit_s_process_crash_preserves_ack_suffix_and_retry` 默认忽略，必须显式提供已准入、独立物理复制的 S seed；它不属于默认基准或发布包。`AFS_P3_S_RECOVERY_DIRECTORY` 指向本次专属目录，目录需包含 `owned`（精确内容 `owned physical S seed copy\n`）、`seed-copy/` 及对应 `seed-copy.fixture.json`。不得将原始封存目录作为此输入。

在已有 Linux 构建环境显式构建 example 测试后，使用 Cargo 实际报告并校验 SHA 的测试 ELF：

```sh
AFS_P3_S_RECOVERY_DIRECTORY="$owned_run_root" "$verified_test_elf" \
  --exact tests::seed_audit_s_process_crash_preserves_ack_suffix_and_retry \
  --ignored --nocapture --test-threads=1
```

测试先由子进程在 checkpoint 391 后追加一笔确定事务，保存调用方已收到 ACK 的结果，再由父进程实际 SIGKILL 并 wait。重开后要求原 700001 实体及 391 原结果不变，新增两项实体和一项结果匹配；精确重试不能改变结果、业务 revision、日志 tip 或存储文件正文。现有 `audit-seed` CLI 继续拒绝非原始 seed，后缀核对只在该独立测试中使用声明版本。

原始事务包含 `RequestAbsent`：精确重试按既有 Store 合同返回 `ConditionFailed { existing_outcome: Some(原结果), .. }`，不要求再次 `Committed`；仍须证明原结果、revision、tip 和存储文件字节不变。

结果和子进程日志保留在专属目录；成功保存证据并核验原资产后才清理本次可再生副本，失败保留副本并停止。该测试只覆盖一笔已 ACK 后缀的进程强杀，不覆盖物理掉电、旧负载后全部 ACK、M/L、网络服务准入或正式恢复时间门槛；不消费或替代性能 campaign。

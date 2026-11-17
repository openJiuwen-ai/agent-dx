# OwnerFs 原生目录名纳管设计

状态：设计草案，待评审。机制已在双机真机验证通过（见 §12），代码仅存在于 spike 分支
`spike/ownerfs-adopt-native-name`（`c98c9e75`，**不可合入**，见 §7 的七个必修项）。
本文描述的是目标设计；凡标「待实现」的条目，当前分支上并不存在。

纳管入口已按评审意见改为 **afs-node 启动扫描**（§6），不由业务方调用 API。spike 里纳管
由 API 触发，但 ④–⑦ 的本地动作与启动扫描完全相同，§12 的真机数据仍然成立；差异只在触发
方式与护栏，已在 §6 单独展开。

## 1 需求与约束

业务方现以 autofs 在本机目录下存放数据，形如：

```text
/data/business/test001/...      业务方现有数据
/data/business/test002/...
```

接入 AFS 后 AFS 不认这两个名字，因为 OwnerFs 把每个 workspace 的物理数据目录命名为
`root-<hex(name)>-e<epoch>`：

```text
<storage_root>/root-74657374303031-e3    ← test001 的物理目录
```

业务方提出的约束是逐级收紧的，最终形态决定了方案：

| 阶段 | 业务方诉求 | 可行性 |
| --- | --- | --- |
| 一 | 不想拷贝数据 | 可以：`rename(2)` 零拷贝，inode 不变 |
| 二 | 连 `rename` 也不接受 | 只剩「原地认名」：AFS 必须直接接受 `test001` 这个目录名 |

第二级约束是本设计的出发点。它意味着 **live 数据目录名必须等于 workspace 名**，
不能再携带任何 AFS 内部编码。

## 2 结论

纳管不是新功能，是把「造目录」改成「认目录」。

`root_data_dir()` 返回名字本身之后，一个已经存在的 `test001` 恰好就是一个合法的
root 数据目录，于是纳管退化成「写一条 catalog 记录 + Meta 激活」——`rename` 与 `copy`
都从链路上消失。

支撑这件事的关键洞察是：**代次标签不必活在 live 路径里**。`rename(2)` 可以同时改目录
和改名字，所以删除时一步完成「原名 live → canonical trash」：

```text
rename("<storage>/test001", "<storage>/.ownerfs-trash/root-74657374303031-e3")
```

代次信息一点没丢，只是搬到了 AFS 私有的那一侧。于是：

| 位置 | 名字形态 | 谁依赖它 |
| --- | --- | --- |
| live 数据目录 | `test001`（原名） | 业务方、NFS 导出、运维 |
| trash 目录 | `root-<hex>-e<epoch>`（canonical） | AFS 回收链路 |
| catalog 记录 | `name` 以 hex 落盘 | 崩溃恢复 |
| RootId | `root-<hex>` | Meta、proto、outlet 快照 |

`RootId` 保持 hex 不变，因此 **proto、Meta 存储、state image、hash domain、golden
全部不动**，这是本方案比「让 Meta 记住物理目录名」便宜一个量级的原因。

## 3 已确定的设计决策

| # | 决策 | 理由 |
| --- | --- | --- |
| D1 | live 数据目录名 = workspace 名（原名） | 业务方唯一硬约束；也是纳管零动作的前提 |
| D2 | trash 目录名 = `root-<hex>-e<epoch>`，由 `rename(2)` 一步完成 | 代次必须在 trash 里可自解释，`collect_trash` 不依赖 catalog |
| D3 | `RootId` 不变，仍是 `root-<hex(name)>` | 避免 proto/Meta/state image/golden 全链 bump |
| D4 | epoch 继续是 Meta 分配的全局单调、永不重用水位 | 回收判定「这个代次还有没有人认」的唯一依据 |
| D5 | 纳管由 **afs-node 启动扫描**驱动（声明式），不经由业务方调用 API | 评审结论：接入现场配好存储根、重启即纳管，不要求业务方逐目录调接口 |
| D6 | 纳管不传 uid/gid/mode，直接沿用目录的实际属性，一个字节都不改 | 属性是业务方既有的，AFS 无权也不需要改写 |
| D7 | 保留名从 `.pub`/`.quarantine` 扩为拒绝 `.ownerfs-` 前缀 | 存储根与业务目录同层后，`.ownerfs-roots/`、`.ownerfs-trash/`、锁文件都在这一层 |
| D8 | 同名重建后，跨机旧路径指向新 workspace（POSIX 语义） | 接受；见 §5 |
| D9 | 非 UTF-8 名字全链走 `OsString`，catalog 不再落盘 `data_dir` | 真机上非 UTF-8 名字会让节点起不来（P0） |
| D10 | 启动纳管的 Home 恒为本节点，且与 `ownerfs_mkdir_load_balance` 配置互斥 | 扫描到的目录只在本机磁盘上，HRW 选别的节点必然造空目录（P5）；互斥后本期不需要改 proto |
| D11 | 不提供 `root-*` legacy 目录兼容分支 | 系统未上线（`AGENTS.md`），存量 AFS 环境直接重置 |

## 4 核心机制

### 4.1 三条命名公式

改动集中在这三个函数，其余调用点因为都经过它们而自动保持一致。

| 函数 | 现状 | 目标 |
| --- | --- | --- |
| `root_data_dir(name, epoch)` | `root-<hex>-e<epoch>` | `name` 本身（`epoch` 参数保留但不再参与，待实现时决定是否删除） |
| `pending_data_dir(reservation)` | `root-<hex>-e<epoch>`（直接拼 id） | 从 `root_name_from_id(id)` 解码后走 `root_data_dir` |
| `trash_name(id, epoch)`（新增） | 无（原先是同一相对路径） | `root-<hex>-e<epoch>` |

`root_data_dir` 是 `pub(crate)`，outlet mirror 与 API 侧都用它交叉校验路径公式，
所以改一处即全链生效。真机已验证这条：`/public/test001` 的 target 与物理目录逐字节相等。

### 4.2 代次与隔离

epoch 由 Meta 的 `RootEpochWatermark` 加一分配，单调递增、永不重用，所以 `-e7` 里的 7
在集群内唯一，不是「这个名字的第 7 代」。

删除被拆成「快隔离 + 慢回收」：递归删几万文件不能一直握着 workspace 锁，所以
`isolate_and_deactivate` 只做三件事——核对 Meta 的 epoch 与本机 Home 会话 →
`rename_to_trash` → `detach`。rename 是同一文件系统内的原子元数据操作，代价与目录大小无关
（代码显式比 `dev()`，跨挂载点直接拒绝）。

隔离后三个判定点全部仍从 trash 名反解 `(root_id, epoch)`：

| 判定点 | 作用 |
| --- | --- |
| `placement(record)` | 同时看 live 与 `trash/<canonical>`；Live / Isolated / Missing / Conflict，两处都在即 fail-closed |
| `finish_isolated_roots()` | 只有 `record.epoch == epoch` 且 `Isolated` 才继续撤权 |
| `collect_trash()` | Meta 当前 epoch ≤ 条目 epoch **且** catalog 不引用该 epoch 才真删 |

同名重建为什么安全：重建后 catalog 里这个 `root_id` 已是新 epoch，于是
`record.epoch == epoch` 对 trash 里的旧条目为假，旧垃圾不会被当成当前代次去
`deactivate_root`；而 `recover_local_root` 遇到 `Isolated` 直接返回
`META_OWNER_ROOT_BUSY`，隔离过的代次不可复活。

真机实测（`chattr +i` 卡住回收，制造「上一代删不掉」）：同名重建照样 `201`、拿到
epoch 5，新代可写、旧代数据在 trash 里完整；解除 immutable 后 20 秒回收，**新代毫发无损**。

### 4.3 `placement` 与 `trash_entries` 必须分开算

这是改动里最危险的一处，因为错了不报错、只是垃圾永远删不掉。

现状 `placement` 用同一个 `record.data_dir` 拼 live 和 trash 两侧路径（rename 不换名，
所以相对路径相同）。改名后两侧必须用不同公式：

```rust
let live  = disk_root.join(record.data_dir.as_path());       // 名字本身
let trash = trash_path.join(trash_name(&record.id, record.epoch)?);  // canonical
```

同时 `trash_entries()` 里有一处**反算校验**：从目录名反解出 `(id, epoch)` 后，重新用
`root_data_dir(name, epoch)` 算出期望路径并与目录名比对。公式一改就全部对不上——但不会
报错，只是 `continue` 跳过，于是回收永久失效、trash 无限增长。spike 的单测直接抓到了它。
正式实现里这一处必须改成拿 `trash_name` 反算。

### 4.4 认领流程（启动扫描驱动）

```text
① 业务方现有目录      <storage_root>/test001        （AFS 不参与）
② afs-node 启动       RootManager::open → reconcile_on_startup（既有记录，fail-closed）
③ 扫描存储根          adopt_pre_existing_roots：命中 test001（判定规则见 §6.2）
④ Meta 预留代次       reserve_root → epoch=N（Home = 本节点，D10）
⑤ 本地认领            prepare_local_root：命中已存在目录，跳过 mkdir/chmod/chown
⑥ 写 catalog 凭据     .ownerfs-roots/root-<hex>.json
⑦ activate_prepared   state=ready
⑧ 挂载 + 发布         recovered_roots() 已含新纳管者；250ms 内 /public/test001
```

第 ⑤ 步是纳管的实质。原 create 会 `mkdir` + `chmod` + `chown` 三步都做；目标实现先探目标
路径，命中已存在目录就置 `adopted = true` 并跳过这三步，只做两件不改变数据的事：

- `open_dir` 拿一个目录 fd 用于 fsync；
- 检查 `dev` 与父目录一致（挂载点不能 rename，必须在纳管时拒掉，否则第一次删除才炸 `EBUSY`）。

第 ⑧ 步不需要额外接线：`recovered_roots()` 返回整个 roots 表，纳管时插入的 root 会被
assembly 的挂载循环自动取到。这是「启动纳管」能以极小代价落地的主要原因。

真机证据（`demo-adopt`，mode 0755，含 `2024Q1/orders.csv`；当时由 API 触发，但 ④–⑦ 的本地
动作与启动扫描完全相同）：

| 检查点 | 纳管前 | 纳管后 |
| --- | --- | --- |
| 目录 inode | 2883630 | **2883630** |
| 文件 inode | 2884672 | **2884672** |
| mode | 755 | **755** |
| mtime | 2023-08-09 14:05:00 | **2023-08-09 14:05:00** |
| sha256 | `6b8a69fd…2126` | `6b8a69fd…2126` |

跨到 node-b 读 `/public/demo-adopt/2024Q1/orders.csv`，**inode 仍是 2884672**、sha256 一致。
NFS 透传 inode，这是「没有拷贝」最硬的证据，可直接交付业务方。

### 4.5 崩溃窗口

启动扫描天然带重试：**每次启动都会重新扫一遍**，所以纳管不需要 intent 标记文件，也不需要
「intent 必须在 pending 清理之前消费」这类顺序约束。各窗口行为：

| 崩在 | 磁盘状态 | 重启后行为 |
| --- | --- | --- |
| ④ 之前（扫描中） | 业务目录 + 无 Meta 记录 | 下次启动重扫，重新纳管 |
| ④ 之后 ⑥ 之前 | 业务目录 + Meta pending + 无 catalog | pending 分支 `abort_root`，`rmdir` 失败于 ENOTEMPTY → **warn 不阻塞**；随后重扫以新 epoch 重新纳管 |
| ⑥ 之后 ⑦ 之前 | catalog 有记录 + Meta pending | 同上；`record.epoch < reservation.epoch` 时 `retire_fenced_root` |
| ⑦ 之后 | 一切一致 | 正常恢复；扫描因「catalog 已有活跃记录」跳过 |

第二、三行是必须改的一处：master 的 reconcile pending 分支对残留目录直接
`self.remove_abandoned_dir(&data_dir)?`（`root.rs:1857`），`?` 会让**节点起不来**。
好消息是 `remove_abandoned_dir` 只调 `rmdir`（spike `root.rs:2044`），**不递归**，所以那个
窗口不可能删掉业务数据。目标实现把它改成与另一分支一致的 warn + 保留 catalog 证据
（spike 已在 1943 处改为 warn）。

> 幂等由 catalog 记录承担：同一代次重试时 `persist_prepared_root` 对相同
> `(root, epoch, prepare_id)` 幂等，冲突则拒绝覆盖。所以重复扫描不会产生两代 root。

## 5 契约变更：跨机同名重建

epoch 从 live 路径里消失后，跨机行为变化。这是本方案唯一一处语义倒退，需要评审确认。

| 场景 | 现状（hex + epoch） | 改后（原名） |
| --- | --- | --- |
| 创建 `test001` | `/public/test001 → users/root-…-e3` | `/public/test001 → users/test001` |
| 删除后同名重建 | 旧链接变成断链（fail-closed） | 路径不变，**旧消费者静默读到新 workspace** |

本机、FUSE、以及持有 fd 的进程都不受影响（POSIX：打开的描述符仍指向旧 inode）。
只影响「还在引用旧 `/public/test001` 的远端消费者」。

已确认接受此变化（D8）。若后续有业务不能接受，可给纳管根加一档「不发布到 `/public`」，
只在本机 FUSE 可见——代价是跨机不可用，机制上只是 `plan()` 里跳过该 root，不需要额外状态。

## 6 纳管入口：启动扫描（声明式）

评审决定：不让业务方调用 API 纳管。接入语义改为「配好存储根 → 重启 → 自动纳管」，
`POST /v1/workspaces` 保持原语义（只创建），不新增 `adopt` 字段。

这个方向省掉五块改动，代价是把风险面从「接口契约」搬到「启动期批量动作」。

### 6.1 省掉的改动

| 原方案要做的 | 启动扫描后 |
| --- | --- |
| `adopt` 请求字段 + 三档 409 语义 | 不需要，API 契约零改动 |
| uid/gid/mode 从目录实际属性回填（真机上 0750 撞 0755 的那个 409） | 不需要——扫描根本不传这三个参数 |
| `validate_owner` 放宽以支持中文名 | 不需要——扫描不经过 HTTP |
| Meta 加 `pinned_home_node_id`（proto + 快照 + golden） | 不需要——改为配置互斥（D10），零 proto 改动 |
| 显式 adopt 入口 + intent 崩溃标记 | 不需要——每次启动重扫，天然幂等重试 |

### 6.2 扫描规则

`adopt_pre_existing_roots()` 遍历存储根的直接子项，逐项判定：

| 条件 | 处理 |
| --- | --- |
| 名字以 `.ownerfs-` 开头 | 跳过（保留名，D7） |
| 不是真目录（普通文件 / 软链） | 跳过 + WARN |
| `dev` 与父目录不一致（挂载点） | 跳过 + WARN（rename 进 trash 会 EBUSY） |
| `7 + 2*len(name) + 20 > 255` | 跳过 + ERROR（P6） |
| catalog 已有该 `root_id` 的活跃记录 | 跳过（已纳管，走既有恢复路径） |
| catalog 有同 `root_id` 的 pending 记录 | 跳过，交给 reconcile 与下一次启动 |
| Meta 已有该 `root_id`，Home 是本节点 | 跳过（走既有恢复，不新纳管） |
| Meta 已有该 `root_id`，Home 是别的节点 | 跳过 + WARN（另一端才是数据主，需人工裁决） |
| 其余 | 纳管 |

最后两行是双机场景的关键：同一个名字在两个节点都有目录时，Meta 只会授一个 Home，另一个
节点必须跳过而不是抢占——两边都有真实数据时只能人工裁决，自动决定等于随机丢一边。

### 6.3 时序与位置

```rust
// RootManager::open_with_catalog
manager.reconcile_on_startup()?;      // 既有语义：记录不合规 → fail-closed
manager.adopt_pre_existing_roots();   // 新增：单目录失败只 WARN，不阻塞启动
Ok(manager)                            // recovered_roots() 已含新纳管者
```

两条原则必须写死，并在测试里固定：

- **既有 catalog 记录的问题 fail-closed**（现状不变）；
- **扫描发现的新目录 fail-open**（跳过 + 日志），否则一个怪目录就能让节点起不来。

### 6.4 护栏

无条件扫描等于「存储根下的任何目录都变成 workspace」。以下五条是硬要求：

| # | 护栏 | 理由 |
| --- | --- | --- |
| G1 | 开关 `[ownerfs] adopt_on_startup`，默认 `false` | 行为变更必须显式开启，不能随二进制升级自动生效 |
| G2 | 与 `ownerfs_mkdir_load_balance` 互斥，同时开启则配置校验失败 | 扫描到的目录只在本机磁盘上，HRW 选别的节点必然造空目录（P5） |
| G3 | 上限 `adopt_max_roots`（默认 1000）：超限则**一个都不纳管** + ERROR | 存储根误配到 `/data` 时，宁可完全不起作用也不要批量接管 |
| G4 | 摘要日志 `ownerfs.adopt.summary`：`scanned/adopted/skipped/failed` | 批量动作没有摘要等于黑盒 |
| G5 | 单目录失败只 WARN，不进重试队列；下次启动重扫 | 失败不应累积成持久状态 |

G3 的「超限不纳管而不是截断纳管」是刻意的：截断会让运维以为「纳管了前 1000 个」，而实际
纳管集合依赖目录遍历顺序，不可预测。

### 6.5 代价：启动时长

本地实测 245ms/目录（含 reserve RPC + catalog fsync + activate RPC），1000 个目录约 4 分钟，
这段时间 VFS 尚未挂载。缓解顺序：

1. 去掉每目录重复的 `sync_root`（现在 prepare / activate 各调一次）；
2. 批内并发（reserve 可并发，catalog 写入串行）；
3. 首次接入用一次性本地命令预热 catalog（不经 API，仍是本地动作）。

**需要评审给一个可接受的启动时长上限**，否则上千目录的现场会变成「重启即长时间不可用」。

### 6.6 仍然需要 API 的部分

删除。非空 workspace 的递归删除只走 `DELETE /v1/workspaces/{owner}`（FUSE `rmdir` 对非空
目录返回 `ENOTEMPTY`）。评审否决的是「用 API 纳管」，不是删除 API 本身。

但要注意：`validate_owner` 仍拒绝中文名与 `.ownerfs-` 前缀开头的名字。扫描纳管会造出这类
root（扫描按字节判定，不经过 `validate_owner`），于是**造出删不掉的 root**。所以一旦允许
扫描纳管非 ASCII 名字，删除入口的校验必须同步放宽，否则 P4 那类「只能重置节点」的场景会从
手工误操作变成常态。

## 7 必须修复的问题

真机把 spike 的缺陷暴露得很干净。以下七项全部是**评审门槛**，未修完不可合入。

### P0 · 非 UTF-8 名字使节点无法启动（阻塞级）

建一个 `bad\xffname` 后重启 node-a 直接死：

```text
FailedPrecondition: local root catalog identity is not canonical
```

根因：catalog 的 `data_dir` 按 JSON 字符串落盘，`\xff` 被写成 U+FFFD
（`bad\357\277\275name`），重启时 `root_data_dir` 重算值与落盘值不等 → fail-closed。

修法：`data_dir` 不再是持久字段，改为从 `name` 派生（`name` 已按 `name_hex` 落盘）。
同一名字当时也不发布到 `/public`，且每 250ms 刷一条 `unrenderable_target`（见 P3）。

**单测没抓到，因为单测只用 UTF-8 名字。** 正式实现必须补一个非 UTF-8 名字的重启回归测试。

### P1 · 启动扫描必须有护栏

误配存储根（例如指到 `/data`）会让一次启动接管几百个无关目录，而「删除 workspace」是把
目录搬进 trash、约 20 秒后真删。这是本方案唯一可能造成批量数据位置变更的路径。
必须实现 §6.4 的 G1–G5，缺一条都不建议上线。

### P2 · 扫描失败不能阻塞启动，也不能静默

两类失败必须分开：catalog 记录不合规 → fail-closed（现状）；扫描发现的新目录不满足条件 →
WARN 跳过 + 计入摘要。没有摘要日志时，「为什么这个目录没被纳管」只能靠 grep 全量日志。

### P3 · 非 UTF-8 名字不发布 + 日志刷屏

spike 里 `Action::Publish.target` 和 `PresentEntry::Symlink.target` 是 `String`，非 UTF-8
名字无法表达，于是 `target_path` 返回 `None`、`plan()` 直接跳过该 root，外加每 250ms 一条
WARN。目标实现两处都改 `OsString`，`classify()` 的 `read_link` 一侧同步改；`symlinkat`
本身接受任意字节，内核侧没有障碍。

### P4 · `.ownerfs-` 前缀的目录能被认领

`RESERVED_ROOT_NAMES = [".pub", ".quarantine"]`（`root.rs:2206`）不覆盖 AFS 自己的内部
目录。真机上 `mkdir /mnt/ownerfs/.ownerfs-trash` 成功把 **AFS 的 trash 目录认领成了
workspace**：catalog 写入、`/public/.ownerfs-trash` 被发布、FUSE 可见。

更麻烦的是它删不掉：`DELETE /v1/workspaces/.ownerfs-trash` 被 `validate_owner` 拦下
（首字符必须是字母数字），FUSE `rmdir` 对非空目录返回 ENOTEMPTY，递归删除只走 API。
只能重置节点。后果推演：它被删除时 `rename_to_trash` 会把整个 trash 搬进
`.ownerfs-trash/.ownerfs-trash/`，所有待回收数据跟着搬家。

修法：`validate_root_name` 改为拒绝 `.ownerfs-` 前缀。**不能塞进 `root_id_from_name`**——
那个函数是纯编码，reconcile 拿它核对存量记录，加策略会让一条历史记录把节点变成不可启动。

### P5 · 负载均衡会把数据纳管到错误节点（数据「看起来丢了」）

`ownerfs_mkdir_load_balance` 的行为：

| 取值 | Home 怎么选 | 纳管后果 |
| --- | --- | --- |
| `false`（默认） | 返回**发起请求的节点** | 只有从数据所在节点调用 API 才正确 |
| `true` | HRW：`blake3("afs-ownerfs-placement-v1" ‖ root_id ‖ node_id)` 取最大者 | 完全不看数据在哪，也不看请求来自哪个节点 |

实测（数据预置在 node-b 的存储根，从 node-a 的 API 纳管）：

| 检查点 | 结果 |
| --- | --- |
| API 返回 | `201 Created`，`state=ready`，`node_id=node-a` |
| node-a 存储根 `drift-a` | **空目录**（子项数 0） |
| node-b 原址数据 | 完好，ino 673928，mtime 2022-01-01 |
| `/public/drift-a/data.txt` | `No such file or directory` |
| node-b 的 FUSE 视图 | `drift-*` 一个都没有 |

业务方看到的是「纳管成功、API 显示 ready、目录存在、打开是空的」，且从此再也纳管不进来
（Meta 里该 `root_id` 已被空目录占用）。这比数据漂移更严重。

而且 HRW 是确定性的，**重试无法自愈**：删掉后从 node-b 重新创建同名，Home 仍落在 node-a，
因为 `placement_score` 只依赖 `root_id` 与节点集合。运维只能「删 root → 改开关 → 换节点重来」，
每次重建消耗一个新 epoch。

修法（本期）：配置校验拒绝 `adopt_on_startup` 与 `ownerfs_mkdir_load_balance` 同时开启
（D10）。启动扫描的 Home 天然是本节点，不需要挑；互斥让本期完全不必改 proto。

若将来两个能力必须共存，再引入 `pinned_home_node_id`（扫描纳管显式指定、跳过 `select_home`），
那是独立的一期。

> 附带发现：关闭负载均衡时 `prepare` 永远走 `if *home == self.config.id` 的本地分支，
> **node→node gRPC 从未被使用**。开启后两节点互相请求全部失败于
> `afs.transport.rejected: received corrupt message of type InvalidContentType`。
> 上负载均衡之前这条通道必须单独验收，不能假定「双机跑通过」就等于它通（此前验证走的是 NFS）。

### P6 · 名字长度上限必须在纳管前校验

trash 名是 `root-<hex>-e<epoch>`，长度 `5 + 2n + 2 + digits`。u64 epoch 最多 20 位，
因此 `27 + 2n ≤ 255` → **名字 ≤ 114 字节**。

现状 live 名用同一公式，所以今天超过 114 的名字本来就会在 `mkdir` 处 `ENAMETOOLONG`；
改名后 live 名变短、看起来上限放宽到 255，但**删除时会炸**——这是把失败从创建期挪到删除期，
必须避免。做法：`validate_root_name` 里显式校验 `7 + 2*len + 20 ≤ 255`，超限即拒绝纳管，
错误信息指明是「trash 命名空间限制」而非目录名限制。

业务方若有 115–255 字节的目录需要纳管，本期不支持；后续可引入 catalog 旁路索引（trash 名
改为不依赖名字的编号），不在本设计范围。

## 8 改动清单

集中在 OwnerFs 本地模块与配置校验。**proto、Meta 存储、state image、hash domain、golden
全部零改动**——这是启动扫描方案相对 API 方案最大的收益。

| 文件 | 改动 | 量级 |
| --- | --- | --- |
| `node/vfs/ownerfs/root.rs` | 三个命名公式、`placement`、`rename_to_trash`、`trash_entries` 反算、prepare 的 adopt 分支、**新增 `adopt_pre_existing_roots()` 与 §6.2 判定**、reconcile pending 非空 warn、`validate_root_name`（前缀 + 长度）、摘要日志 | ~380 行 |
| `node/vfs/ownerfs/catalog.rs` | `LocalRootRecord` 去掉 `data_dir` 持久字段改为派生；新增 `adopted: bool`；`RECORD_VERSION` bump | ~45 行 |
| `node/outlet/mirror.rs` | `target_path` 返回 `OsString`；`Action::Publish.target`、`PresentEntry::Symlink.target`、`classify()` 全链 `OsString` | ~80 行 |
| `node/outlet/live.rs` | `read_link` → `OsString`、`symlinkat` 调用点、target 比较 | ~30 行 |
| `config/ownerfs.rs` | `adopt_on_startup` / `adopt_max_roots`、与 LB 互斥校验 | ~40 行 |
| `node/api/workspace.rs` | **不改**（不新增 `adopt` 字段） | 0 |
| `meta/*` + `meta.proto` | **不改**（P5 改为配置互斥） | 0 |
| 测试 | 扫描矩阵（保留名/非目录/挂载点/超长/双节点冲突/超限/幂等/部分失败）+ inode 不变 + 非 UTF-8 重启 + trash 反解 + 崩溃窗口；重写约 18 项失效断言 | ~350 行 |
| 文档 | `docs/architecture/afs.md` 目录命名段落、`docs/deployment/afs.md` 存储根 / autofs / 开关前置 | 3 段 |

合计约 **900 行**（测试约 1/3）。与 API 方案（约 950 行）基本持平，但评审面显著变小；
增加的是**启动期行为的验收责任**，这部分不能靠单测替代，必须真机跑。

## 9 验收矩阵

### 9.1 本地（`cargo test -p afs --lib`，必须全绿）

| 项 | 断言 |
| --- | --- |
| 认领零动作 | 认领前后目录 `(dev, ino)`、文件 inode、内容、mode、mtime 全部不变 |
| 扫描规则 | §6.2 的九条判定逐条有测试：保留名、非目录、挂载点、超长名、已活跃、pending、Meta 本节点、Meta 他节点、正常纳管 |
| 扫描开关 | `adopt_on_startup=false` 时一个都不纳管；与 LB 同时开启时配置校验失败 |
| 扫描上限 | 目录数超过 `adopt_max_roots` 时**一个都不纳管**并打 ERROR，而非截断 |
| 扫描幂等 | 连续两次启动只纳管一次；纳管后第二次启动因「catalog 已有活跃记录」跳过 |
| 扫描失败隔离 | 单个目录纳管失败（如 Meta 返回冲突）不影响其余目录，也不阻塞启动 |
| 代次隔离 | 删除后 live 名字立即释放；trash 出现 `root-<hex>-e<N>`；同名重建落在原名；`collect_trash` 只回收旧代 |
| 崩溃窗口 | 四个窗口分别重启后行为符合 §4.5 表格，且**任何窗口都不会递归删除非空目录** |
| 非 UTF-8 | 名字含 `\xff` 可创建、可发布、可重启、可删除；两个仅差非法字节的名字不塌成一条 |
| 保留名 | `.ownerfs-` 前缀、`.`、`..`、含 `/` 一律拒绝 |
| 长度 | 115 字节名在 create 与启动扫描处被拒，错误信息指向 trash 命名空间 |
| 挂载点 | `dev` 与父目录不一致时纳管被拒 |

spike 当前状态：`node::vfs::ownerfs` + `node::outlet` 共 385 项，367 通过 / 18 失败。
18 项里 11 项是机械断言（期望 target 从 `root-61-e7` 变 `a`），5 项是契约变更（换代不再
republish、非空残留改 warn+defer、空残留被复用），2 项是 P3 的真实缺陷。

### 9.2 真机（已完成，环境保留在 `/home/lzq/afs-adopt`）

| 项 | 结果 |
| --- | --- |
| 单目录纳管 | inode / mode / mtime / sha256 全部原样 |
| 跨机读 | node-b 经 `/public/<name>` 读到，**inode 一致**，`fs_type=nfs4` |
| 跨机写 | 中文目录 `业务数据001` 从 node-b 写入成功 |
| 删除改名 | strace 抓到 `rename(".../test001" → ".../.ownerfs-trash/root-74657374303031-e3") = 0` |
| 旧代残留时重建 | `201`，epoch 5，新代可写，旧代隔离；解除 immutable 后 20s 回收且新代无损 |
| 批量 | 20 个目录 / 1040 条目，inode 全一致，**平均 245ms/个**（200 个约 50 秒） |
| 野目录 | 24 个未纳管目录在全新 Meta 下重启后全部保留，FUSE 不可见、不被删 |
| 重启自洽 | catalog `data_dir` 与物理目录一致，root 仍可用 |

### 9.3 待真机补验（P0–P6 修完后）

1. 非 UTF-8 名字的完整生命周期（含跨机，见 §11 风险）。
2. **「重启即纳管」端到端**：24 个野目录的现场，开开关重启后只纳管符合规则的目录，
   inode 全部不变，`/public` 与 FUSE 视图与摘要日志一致。
3. 双节点同名冲突：两个存储根各放一个同名目录，先启动的一方拿到 Home，另一方跳过 + WARN。
4. `adopt_on_startup` 与 `ownerfs_mkdir_load_balance` 同时开启时节点拒绝启动。
5. 启动时长实测（1000 个目录的量级），确认可接受的窗口。
6. autofs 在场时的纳管（必须验证「停 map」是硬前置）。
7. node→node gRPC 通道单独验收（P5 附带发现）。

## 10 部署前置与回退

### 10.1 前置（缺一不可）

1. **停 autofs 的 map**。否则 automounter 会在目录被纳管后重新挂载，使目录变成挂载点，
   后续 trash 隔离直接 `EBUSY`。
2. **OwnerFs 存储根指向现有数据的父目录**（不是每个子目录各挂一个）。否则 `rename` 会
   `EXDEV`，trash 隔离做不成。
3. 核对同文件系统：

   ```bash
   stat -c '%d %n' <数据父目录> <数据目录> <ownerfs存储根>
   ```

4. 核对保留名与长度：

   ```bash
   ls <数据父目录> | grep -E '^\.ownerfs-'
   ls <数据父目录> | awk '{ if (length($0) > 114) print }'
   ```

5. **打开开关并确认互斥**：`[ownerfs] adopt_on_startup = true`（默认关闭，G1），且确认
   `ownerfs_mkdir_load_balance` 未开启（G2，同时开启会让节点拒绝启动）。
   纳管发生在**各节点自己的**存储根上，所以不要再用「把请求发给哪台节点」来判断 Home。

6. **先小样本验证再全量**：把 `adopt_max_roots` 临时调到较小值，确认「超限不纳管」的护栏
   生效，再放开。存储根误配是本方案唯一能造成批量影响的操作（G3）。

### 10.2 回退

格式与旧实现**不兼容**：旧环境的 `root-*` 目录在新二进制下无法被识别为 root 数据目录，
且不提供 legacy 分支（D11）。回退方式为「回退二进制 + 重置 AFS 环境」，不做数据兼容。
这依赖「系统未上线」的前提；若上线后需要再改命名，必须另做迁移方案。

## 11 风险与未决

| # | 项 | 状态 |
| --- | --- | --- |
| R1 | 非 UTF-8 名字的跨机可用性未验。NFSv4 要求 UTF-8，NFSv3 字节安全；本地已验证可用，跨机未测 | 待验；若不可用，需在发布时降级（本机可用、跨机不发布） |
| R2 | 名字 ≤114 字节是硬约束，来自 trash 命名空间 | 待业务方确认存量目录是否有超长名字 |
| R3 | 中文名/non-ASCII 名可被扫描纳管，但 `validate_owner` 仍拒绝它们 → **造出删不掉的 root**（§6.6） | 待定：删除入口同步放宽，或扫描默认跳过非 ASCII |
| R4 | 空目录删除直接 `rmdir`、不产生 trash（与命名无关的既有行为）；非空目录的递归删除只能走 API，FUSE `rmdir` 返回 `ENOTEMPTY` | 需在运维文档写明 |
| R5 | **误纳管**：存储根配错时一次启动接管几百个无关目录；「删除 workspace = 搬进 trash 并约 20 秒后真删」，误纳管后再误删等于数据位置变更 | 靠 G1/G3 护栏；建议上线首周保持 `adopt_max_roots` 保守 |
| R6 | **启动时长**：245ms/目录，1000 个约 4 分钟，期间 VFS 未挂载 | 需评审给可接受上限（§6.5） |
| R7 | **双节点同名**：两边都有真实数据时必须人工裁决，系统只会让先启动的一方拿到 Home | 需运维手册写明裁决步骤 |
| R8 | Meta root 数量随纳管目录数线性增长；已确认 `root_command` 无界泄漏（约 1.9KB/删除） | 长期问题，与本期无关但会被放大 |

## 12 验证记录

- 环境：`/home/lzq/afs-adopt`，node-a `192.168.2.101`（10.94.217.10）、
  Meta + node-b `192.168.2.213`（1.94.246.161），端口 183xx，与既有 `afs-v1111` 隔离。
- 二进制：spike 分支 `c98c9e75`。
- 脚本：WSL `/root/adopt-verify/`（`deploy.sh`、`preload.sh`、`start.sh`、`api-create.sh`、
  `api-delete.sh`、`trash-reclaim.sh`、`generation.sh`、`reclaim-gen.sh`、`bulk-adopt.sh`、
  `internal-name.sh`、`drift.sh`、`deterministic.sh`、`cleanup.sh`）。
- 环境当前状态：负载均衡已恢复默认关闭，`lbprobe*` / `drift-*` 残留已清理；
  存储根保留 24 个野目录，Meta 为空。

## 13 实施顺序建议

1. P4 + P6（保留名前缀 + 长度校验）——量小、独立、可先合。
2. P0 + P3（非 UTF-8 全链 `OsString` + catalog 不落盘 `data_dir` + 版本 bump）——先写失败测试。
3. 命名公式三处 + `placement` + `rename_to_trash` + `trash_entries` 反算——这一步跑通后，
   纳管的本地动作才成立。
4. `adopt_pre_existing_roots()` + 护栏 G1–G5 + 摘要日志——主体工作，按 §6.2 的九条规则
   逐条写测试。
5. 按新契约重写 18 项失效断言，跑 `cargo clippy` / `cargo fmt --check`。
6. 真机改造现有 `/home/lzq/afs-adopt` 现场，验证「重启即纳管」：24 个野目录只按开关纳管、
   双节点同名冲突跳过、超限不纳管、启动时长与摘要日志。

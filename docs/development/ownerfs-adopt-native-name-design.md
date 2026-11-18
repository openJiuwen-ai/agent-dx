# OwnerFs 原生目录名纳管设计

状态：设计草案，待评审。机制已在双机真机验证通过（见 §12），代码仅存在于 spike 分支
`spike/ownerfs-adopt-native-name`（`c98c9e75`，**不可合入**，见 §7 的必修项）。
本文描述的是目标设计；凡标「待实现」的条目，当前分支上并不存在。

纳管入口已按评审意见改为 **afs-node 启动扫描**（§6），不由业务方调用 API。spike 里纳管
由 API 触发，但 ④–⑦ 的本地动作与启动扫描完全相同，§12 的真机数据仍然成立；差异只在触发
方式与护栏，已在 §6 单独展开。

第二轮评审否决了两条护栏：**「与 `ownerfs_mkdir_load_balance` 互斥」**（纳管完成后新建目录
仍需要负载均衡）与**「目录数上限」**（数量拦不住「位置配错」）。前者的替代是让 Meta 的
reserve 接受一个输入侧标志 `pin_home`，使「纳管」与「负载均衡」正交；后者不再有替代机制，
改为部署前置的人工核对（§10.1）。取舍记录见 §6.4。

第三轮评审调整了两处范围：

- **`RecoverRoots` 批量接口纳入计划**（§6.5）。启动是 O(N) 次 recover txn，每次重启都付，
  属于长期成本；作为独立提交，不阻塞纳管主体。
- **非 UTF-8 名字的支持整体延后**，本特性做完后单独提 PR（P0、P3）。本期改为在**创建与
  扫描两个入口拒绝非 UTF-8 名字**（P0′，约 15 行），因此不再需要 catalog 记录格式改造和
  outlet 全链 `OsString`（省约 155 行）。前提是「现场不存在非 UTF-8 目录名」，这是业务侧
  已确认的事实；中文等合法 UTF-8 名字不受影响。

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

`RootId` 保持 hex 不变，因此 **Meta 的实体编码、state image、hash domain 全部不动**。
proto 有两处新增：

| 新增 | 性质 | 持久影响 |
| --- | --- | --- |
| `ReserveRootRequest.pin_home`（§6.4） | 输入侧标志 | 无——不落盘，不进快照 |
| `RecoverRoots`（§6.5） | 批量换绑会话 | **唯一的 Meta 持久格式触点**：`OperationResult` 需要新变体，落在 WAL 的 request outcome 里。不影响实体编码与 state image，但需独立 bump 版本、重生成 golden，并单独评审 |

这是本方案比「让 Meta 记住物理目录名」便宜一个量级的原因。

## 3 已确定的设计决策

| # | 决策 | 理由 |
| --- | --- | --- |
| D1 | live 数据目录名 = workspace 名（原名） | 业务方唯一硬约束；也是纳管零动作的前提 |
| D2 | trash 目录名 = `root-<hex>-e<epoch>`，由 `rename(2)` 一步完成 | 代次必须在 trash 里可自解释，`collect_trash` 不依赖 catalog |
| D3 | `RootId` 不变，仍是 `root-<hex(name)>` | 避免 proto/Meta/state image/golden 全链 bump |
| D4 | epoch 继续是 Meta 分配的全局单调、永不重用水位 | 回收判定「这个代次还有没有人认」的唯一依据 |
| D5 | 纳管由 **afs-node 启动扫描**驱动（声明式），不经由业务方调用 API | 评审结论：接入现场配好存储根、重启即纳管，不要求业务方逐目录调接口 |
| D6 | 纳管不传 uid/gid/mode，直接沿用目录的实际属性，一个字节都不改 | 属性是业务方既有的，AFS 无权也不需要改写 |
| D7 | 创建路径的保留名从 `.pub`/`.quarantine` 扩为拒绝 `.ownerfs-` 前缀；启动扫描再收紧一档：跳过**所有** `.` 开头的名字与 `lost+found` | 存储根与业务目录同层后，`.ownerfs-roots/`、`.ownerfs-trash/`、锁文件都在这一层；扫描是「猜哪个目录该变 workspace」，猜错一次就到不了头，所以宁可严格 |
| D8 | 同名重建后，跨机旧路径指向新 workspace（POSIX 语义） | 接受；见 §5 |
| D9 | **本期不支持非 UTF-8 名字**：创建路径与启动扫描都拒绝（P0′）；全链 `OsString` 与 catalog 不落盘 `data_dir` 延后为独立 PR（P0/P3） | 业务侧确认现场不存在非 UTF-8 目录名；入口拒绝能让 catalog 里永远只有合法 UTF-8，从而规避「节点起不来」而不必改记录格式 |
| D10 | 启动纳管的 Home 恒为本节点，由 `ReserveRootRequest.pin_home` 显式指定，与 `ownerfs_mkdir_load_balance` **正交**（可同时开启） | 扫描到的目录只在本机磁盘上，HRW 选别的节点必然造空目录（P5）；但 LB 只应作用于新建目录，不该作用于「已经躺在磁盘上的目录」。该标志不落盘，因此不 bump 状态版本 |
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
`remove_abandoned_dir` 只调 `rmdir`、**不递归**，所以非空业务目录删不掉；但**空目录会删成功**，
而改名后 `data_dir` 就是业务目录本身。因此目标实现比「warn」更强：

- 启动期清理（reconcile pending、zombie 世代、`retire_fenced_root`）**一律不 `rmdir` live
  目录**，只 warn 并保留 catalog 证据；
- `rmdir` 只允许出现在 trash 内的 `root-<hex>-e<epoch>` 条目，以及用户显式删除（空目录删除）
  这两条路径上。

崩溃窗口因此最多留下一个空目录，下次启动可被重新扫描纳管，不会删除任何业务目录。

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
| Meta 加**落盘**的 `pinned_home_node_id`（state image + 快照 + golden） | 不需要——`pin_home` 只是 reserve 的输入侧标志，不进入任何持久实体（§6.4） |
| 显式 adopt 入口 + intent 崩溃标记 | 不需要——每次启动重扫，天然幂等重试 |
| 非 UTF-8 全链 `OsString` + catalog 记录改造（P0/P3） | 不需要——入口拒绝（P0′）让这类名字进不了 catalog；真要做时按 §7 的修法单独提 PR |

### 6.2 扫描规则

`adopt_pre_existing_roots()` 遍历存储根的直接子项，逐项判定：

| 条件 | 处理 |
| --- | --- |
| 名字以 `.` 开头（含 `.ownerfs-*`） | 跳过（保留名，D7） |
| 名字是 `lost+found` | 跳过（文件系统自建目录） |
| 不是真目录（普通文件 / 软链） | 跳过 + WARN |
| 名字不是合法 UTF-8 | 跳过 + WARN（P0′，本期不支持非 UTF-8 名字） |
| 名字是合法 UTF-8 但含非 ASCII 字节 | 跳过 + WARN（R3 裁决：默认只纳管 ASCII 名字） |
| `dev` 与父目录不一致（挂载点） | 跳过 + WARN（rename 进 trash 会 EBUSY） |
| `7 + 2*len(name) + 20 > 255` | 跳过 + ERROR（P6） |
| catalog 已有该 `root_id` 的记录（活跃或 pending） | 跳过；active 走既有恢复路径，pending 交给 reconcile 与下一次启动 |
| Meta 已有该 `root_id` 的 active 记录，Home 是本节点 | 跳过（走既有恢复，不新纳管） |
| Meta 已有该 `root_id` 的 active 记录，Home 是别的节点 | 跳过 + WARN（另一端才是数据主，需人工裁决） |
| Meta 只有该 `root_id` 的 pending reservation（无 active、无本地 catalog） | **不跳过**：旧会话已失效时 Meta 以新 epoch 接管（纳管成功）；旧会话仍存活时返回 `ROOT_BUSY`，计入 `failed` |
| 其余 | 纳管 |

最后两行是双机场景的关键：同一个名字在两个节点都有目录时，Meta 只会授一个 Home，另一个
节点必须跳过而不是抢占——两边都有真实数据时只能人工裁决，自动决定等于随机丢一边。

#### 例子 1：一次干净的首启扫描

```text
/home/agentos/.users/            ← ownerfs_data_dir
├── .ownerfs-roots/              ← AFS 私有 catalog（RootManager::open 创建）
├── .ownerfs-root-catalog.lock   ← AFS 私有进程锁（RootManager::open 创建）
├── .ownerfs-trash/              ← AFS 私有回收区（首次删除后出现）
├── test001/                     ← 纳管（test001 → root-7465737430303031）
├── test002/                     ← 纳管
├── .git/                        ← 跳过：点前缀
├── lost+found/                  ← 跳过：文件系统自建
├── notes.txt                    ← 跳过：不是真目录
├── 客户资料/                    ← 跳过：非 ASCII
├── bad\xffname/                 ← 跳过：非 UTF-8
└── aaaa…（115 字节）/           ← 跳过：trash 名会超过 255 字节
```

摘要：`scanned=11 adopted=2 skipped=9 failed=0`。

`scanned` 是 `read_dir` 的原始条目数，`skipped` 也包含 `.ownerfs-roots/`、
`.ownerfs-root-catalog.lock` 这类 AFS 私有条目，所以即使布局完全干净也不会是 0；
`skipped` 的准确口径就是 `scanned - adopted - failed`。运维核对应结合每个条目的
判定日志（`ownerfs.adopt.*`），只看计数无法区分「保留在扫」和「被跳过」。

#### 例子 2：名字冲突与残留

| 条目 | Meta / catalog 状态 | 结果 |
| --- | --- | --- |
| `test003/` | 本地 catalog 已有 active 记录 | 跳过；`reconcile_on_startup` 走既有恢复，不重新分配 epoch |
| `test004/` | Meta active，Home = `node-b` | 跳过 + WARN `ownerfs.adopt.foreign_home`；本节点不抢占 |
| `test005/` | Meta 只有 pending，原会话已失效 | 以新 epoch 接管并纳管；旧 reservation 被覆盖 |
| `test006/` | Meta 只有 pending，原会话仍存活 | 计入 `failed` + WARN `root_busy`，不纳管；等对端结束或人工处理 |

#### 例子 3：只有坏目录时的重启

存储根里只有 `bad\xffname/` 时：第一次启动全部跳过、节点正常挂载；第二次、第三次启动
结果相同，目录与内容始终保留，不会因为扫描失败而拒绝启动。

### 6.3 时序与位置

```rust
// node/assembly/ownerfs.rs，open 返回后、recovered_roots() 之前
let roots = RootManager::open(...).await??;   // 既有语义：记录不合规 → fail-closed
if cfg.ownerfs_adopt_on_startup {
    // 与 open 一样放进 spawn_blocking：内部有 Meta RPC
    adopt_pre_existing_roots(roots.clone()).await;  // 单目录失败只 WARN，不阻塞启动
}
let recovered = roots.recovered_roots();      // 已含新纳管者
```

扫描放在 assembly 而不是 `RootManager::open` 内部：配置只由 assembly 持有，给 `open` 增加
开关参数会让 28 处测试调用点全部改签名，收益为零。两条调用顺序等价，都在挂载之前。

两条原则必须写死，并在测试里固定：

- **既有 catalog 记录的问题 fail-closed**（现状不变）；
- **扫描发现的新目录 fail-open**（跳过 + 日志），否则一个怪目录就能让节点起不来。

### 6.4 护栏与取舍

无条件扫描等于「存储根下的任何目录都变成 workspace」。评审后保留三条：

| # | 护栏 | 理由 |
| --- | --- | --- |
| G1 | 开关 `[ownerfs] adopt_on_startup`，默认 `false` | 行为变更必须显式开启，不能随二进制升级自动生效 |
| G2 | 摘要日志 `ownerfs.adopt.summary`：`scanned/adopted/skipped/failed` | 批量动作没有摘要等于黑盒，事后无法回答「为什么这个目录没被纳管」 |
| G3 | 单目录失败只 WARN，不进重试队列；下次启动重扫 | 失败不应累积成持久状态 |

#### 6.4.1 被否决的两条护栏

| 原护栏 | 否决理由 | 现状 |
| --- | --- | --- |
| 与 `ownerfs_mkdir_load_balance` 互斥 | 纳管是一次性动作，负载均衡是长期配置；互斥等于要求集群永久关掉 LB 才能完成接入，接入完成后新建目录又需要它 | 改为 `pin_home`，两者**正交**，可同时开启（见 6.4.2） |
| 目录数上限 `adopt_max_roots` | 数量拦不住「位置配错」：`storage_root` 误写成 `/data` 时下面可能只有 4 个目录，任何阈值都放行；而合法场景有 3000 个目录时阈值又把它整体禁掉。两头都不对，因为它拦的是数量，错误实际在位置 | 删除，不设替代机制。风险由 G1（默认关闭）与部署前置人工核对（§10.1 第 5 条）承担 |

#### 6.4.2 `pin_home`：让纳管与负载均衡正交

`select_home()` 现在只有两种行为：`load_balancing=false` 返回发起节点，`=true` 按 HRW
挑节点、完全不看数据在哪。纳管需要的是第三种：**发起节点就是数据所在节点，无条件尊重它**。

所以给 `ReserveRootRequest` 追加一个 `bool pin_home`（字段号 10）：

| 场景 | `load_balancing=false` | `load_balancing=true` |
| --- | --- | --- |
| 纳管（`pin_home=true`） | Home = 发起节点 | Home = 发起节点 |
| 新建（`pin_home` 缺省） | Home = 发起节点 | Home = HRW 选中者 |

关键点是 `pin_home` **不落盘**。`RootReservationRecord.home_node_id` 本来就是 Home 的权威
来源，pin 只是「第一次提交时怎么选」的输入；之后所有读取路径（`recover_root`、outlet 快照、
跨机转发的 `RootLocation`）都只看 `home_node_id`。因此 state image、快照版本、hash domain、
golden 全部不动，proto3 追加字段对旧调用方是透明的。

两个实现约束：

- 节点侧本来就是 `preferred_home_node_id = self.node_id`，`pin_home` 只是让 Meta 尊重这个
  名为 "preferred" 的字段，而不是覆盖它；
- 同一 `request_id` 重放时以**已提交的** `home_node_id` 为准，`pin_home` 不参与幂等比对
  （它不在持久实体里）。重放方拿到的是既有 reservation，不会因为改传 `false` 而换 Home。

### 6.5 代价：启动时长与 `RecoverRoots`

**先纠正口径。「245ms/目录」是 API 路径的成本，不是纳管的成本。** 真机拆分：

| 操作 | 实测 |
| --- | --- |
| `POST /v1/workspaces`，真实创建 | 220–245ms |
| 同一个 POST，幂等返回 | 5ms |
| `GET /v1/workspaces/{owner}` | 3.6ms |
| `fsync`（存储根 / Meta 数据目录） | 1.2–1.5ms |

差值几乎全在 `api/workspace.rs::wait_outlet()`：创建结束后每 25ms 轮询，等 outlet mirror 的
250ms tick 把 `/public/<name>` 发布出去（API 契约要求 `access_path` 返回即可用）。启动扫描
不经过这条路径，每个 root 是 4 次本地 fsync + 1 次 reserve txn + 1 次 activate/recover txn，
量级 **15–25ms**，1000 个约 **15–25 秒**，不是 4 分钟。

因此两条「缓解」不做：

- **批内并发**：瓶颈不在并行度；reserve 并发还会撞 `RootEpochWatermarkEquals` 这个全局
  水位条件，必须自带重试，收益 2–3× 且引入新失败模式。
- **一次性预热 catalog**：`recover_root` 每 root 一 txn 与 catalog 无关，预热只省首次那一次，
  之后每次重启照付 O(N)，却引入「Meta 已 pending + 本地已有 catalog + 未 activate」的新中间态。

真正该做的是把 O(N) 次 txn 压成 O(1)：**`RecoverRoots` 批量接口**。一次 txn 内换绑该节点
全部 Home 会话，条件为 `NodeSessionCurrent` + 每个 root 一条 `EntityEquals(Root)`，mutation 为
每个 root 的 `Put(Root)` + `Put(RootGrant)`。收益是 N 次 WAL `sync_data` 与 N 次 RPC 往返降为
**每批一次**：1000 个目录省 1–3 秒，N 上万时才是主要收益。它**每次重启都受益**，不只首次接入，
所以值得做；但它不是启动时长的阻塞项，作为独立提交，可与纳管主体并行。

实现要点（评审重点）：

- `OperationResult` 必须新增 `RootGrants(Vec<RootAccessGrant>)`：`access_generation` 由 Meta
  生成，节点无法自行构造，批量返回必须带 N 个 grant。这是本设计**唯一触及 Meta 持久格式**
  的地方（WAL 的 request outcome），需要独立 bump 版本、重生成 golden，不与纳管改动混提交。
- **分批上限 256**：单 txn 带 N 个条件与 2N 个 mutation，WAL 单帧不能无界。
- **单批失败降级为逐条 recover**：某个 root 在启动期间被并发删除时整批条件失败，不能因此
  让同批其余 root 全部恢复不了。
- **幂等与条数校验**：幂等键沿用 `node/session + request_id`，重放返回同一批 grant；返回
  条数与请求不一致时，缺的那个按 reconcile 的 zombie 语义处理（目录已不存在则 deactivate，
  见 R11），不能当成功。

启动期仍是「VFS 挂载前完成」，所以窗口内新连接会失败；按 15–25ms/个估算，1000 个目录的
不可用窗口在 30 秒以内，不需要额外的时长护栏。

### 6.6 仍然需要 API 的部分

删除。非空 workspace 的递归删除只走 `DELETE /v1/workspaces/{owner}`（FUSE `rmdir` 对非空
目录返回 `ENOTEMPTY`）。评审否决的是「用 API 纳管」，不是删除 API 本身。

但要注意：`validate_owner` 仍拒绝中文名与 `.ownerfs-` 前缀开头的名字。若扫描纳管非 ASCII
名字，就会造出**删不掉的 root**。本轮裁决取「扫描默认跳过非 ASCII」（§6.2），因此
`validate_owner` 保持现状；将来要支持中文名时，必须同步放宽删除入口的校验，否则 P4 那类
「只能重置节点」的场景会从手工误操作变成常态。

## 7 必须修复的问题

真机把 spike 的缺陷暴露得很干净。按第三轮评审分成两组：

| 组 | 项 | 门槛 |
| --- | --- | --- |
| 本期必修 | P1 / P2 / P4 / P5 / P6，以及新增的 P0′ | 未修完不可合入 |
| 延后，独立 PR | P0 / P3（非 UTF-8 全链 `OsString` + catalog 记录改造） | 本期用 P0′ 的入口拒绝规避 |

P0 与 P3 的修法保留在文档里，供独立 PR 直接取用；两节都补了「为什么可以延后」的前提。

### P0 · 非 UTF-8 名字使节点无法启动（本期延后，由 P0′ 规避）

建一个 `bad\xffname` 后重启 node-a 直接死：

```text
FailedPrecondition: local root catalog identity is not canonical
```

根因：catalog 的 `data_dir` 按 JSON 字符串落盘，`\xff` 被写成 U+FFFD
（`bad\357\277\275name`），重启时 `root_data_dir` 重算值与落盘值不等 → fail-closed。

修法：`data_dir` 不再是持久字段，改为从 `name` 派生（`name` 已按 `name_hex` 落盘）。
同一名字当时也不发布到 `/public`，且每 250ms 刷一条 `unrenderable_target`（见 P3）。

**本期不做**：业务侧确认现场不存在非 UTF-8 目录名，而 P0′ 在创建与扫描两个入口就把这类
名字挡住，catalog 里只可能有合法 UTF-8，本缺陷不可达。catalog 记录格式改造因此可以延后，
与 P3 一起作为独立 PR。

**单测没抓到，因为单测只用 UTF-8 名字。** 独立 PR 必须补一个非 UTF-8 名字的重启回归测试。

### P0′ · 非 UTF-8 名字在两个入口拒绝（本期必修）

非 UTF-8 支持延后之后，必须有东西保证它进不了 catalog，否则 P0 的「节点起不来」会从
「不支持的特性」变成「一次误操作炸节点」。两个入口，同一判定 `name.to_str().is_none()`：

| 入口 | 位置 | 行为 |
| --- | --- | --- |
| FUSE / 内部创建 | `validate_root_name`（`root.rs:2310`） | 返回 `NODE_VFS_INVALID`（EINVAL），错误信息指明「名字必须是合法 UTF-8」 |
| 启动扫描 | §6.2 判定表新增一行 | 跳过 + WARN `ownerfs.adopt.non_ascii_name`（非 UTF-8 与非 ASCII 合并为同一事件）+ 计入摘要 `skipped` |

两个实现约束：

- **不能塞进 `root_id_from_name`**：它是纯编码，reconcile 用它核对存量记录，加策略会让一条
  历史记录把节点变成不可启动（与 P4 同一个理由）。
- 合法 UTF-8 的中文名不受影响（真机 `业务数据001` 端到端已验证）；只有非法字节序列被拒，
  这正是 `String` 与 `OsString` 的分界。

测试两条：FUSE 建 `bad\xffname` 返回 EINVAL；存储根里只放一个 `bad\xffname` 目录时，
连续两次启动都正常，摘要满足 `adopted=0 / failed=0 / skipped == scanned`（`scanned` 含
`.ownerfs-roots/`、锁文件等 AFS 私有条目），坏目录原样保留。对应单测
`adoption_survives_restarts_with_only_a_non_utf8_directory`。

### P1 · 启动扫描必须有护栏

误配存储根（例如指到 `/data`）会让一次启动接管几百个无关目录，而「删除 workspace」是把
目录搬进 trash、约 20 秒后真删。这是本方案唯一可能造成批量数据位置变更的路径。

评审后护栏收缩为 §6.4 的 G1–G3（默认关闭 + 摘要日志 + 单目录 fail-open），**系统侧不再有
自动阻断误配的机制**（数量上限被否决，理由见 §6.4.1）。这意味着一半责任转移到部署流程：
首次开启前必须人工 `ls` 一遍存储根（§10.1 第 5 条）。不接受这个前提就不应开 G1。

### P2 · 扫描失败不能阻塞启动，也不能静默

两类失败必须分开：catalog 记录不合规 → fail-closed（现状）；扫描发现的新目录不满足条件 →
WARN 跳过 + 计入摘要。没有摘要日志时，「为什么这个目录没被纳管」只能靠 grep 全量日志。

### P3 · 非 UTF-8 名字不发布 + 日志刷屏（本期延后）

spike 里 `Action::Publish.target` 和 `PresentEntry::Symlink.target` 是 `String`，非 UTF-8
名字无法表达，于是 `target_path` 返回 `None`、`plan()` 直接跳过该 root，外加每 250ms 一条
WARN。目标实现两处都改 `OsString`，`classify()` 的 `read_link` 一侧同步改；`symlinkat`
本身接受任意字节，内核侧没有障碍。

**本期不做**：P0′ 之后 `/public` 里不可能出现非 UTF-8 目标，`unrenderable_target` 这条刷屏
日志同样不可达。与 P0 合并为同一个独立 PR，一并交付跨机验证（NFSv4 要求 UTF-8、NFSv3 字节
安全，见 R1）——只改本地发布而不验跨机，等于把一个未知风险留在生产里。

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

修法（本期）：`ReserveRootRequest` 追加 `bool pin_home`，纳管路径置位，`select_home()` 在置位
时直接返回发起节点。两种 LB 配置下纳管都落在数据所在节点，新建目录照旧走 HRW，因此两者
可以同时开启（§6.4.2）。

`pin_home` 不进入任何持久实体，所以不必 bump 快照版本或重生成 golden。不要用「落盘的
`pinned_home_node_id`」实现：`RootReservationRecord.home_node_id` 已经是权威来源，额外存
一份会在重建 epoch 时产生两个真值。

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

集中在 OwnerFs 本地模块、配置校验，以及 Meta 的两处 proto 新增。**Meta 的实体编码、state
image、hash domain 不动**；唯一触及持久格式的是 `RecoverRoots` 的 `OperationResult` 变体，
它按 §6.5 单独提交、单独评审。

| 文件 | 改动 | 量级 |
| --- | --- | --- |
| `node/vfs/ownerfs/root.rs` | 三个命名公式、`placement`、`rename_to_trash`、`trash_entries` 反算、prepare 的 adopt 分支、**新增 `adopt_pre_existing_roots()` 与 §6.2 判定**、reconcile pending 非空 warn、`validate_root_name`（前缀 + 长度 + **非 UTF-8 拒绝，P0′**）、摘要日志 | ~395 行 |
| `node/vfs/ownerfs/catalog.rs` | **不改**（不加 `adopted`，schema 不变，`RECORD_VERSION` 不 bump） | 0 |
| `node/outlet/mirror.rs` / `node/outlet/live.rs` | `mirror.rs` 的 `target_path` 改为 `<name>`（原硬编码 `root-<hex>-e<epoch>`，不改则 `/public` 全部断链）；`live.rs` 不改 | ~10 行 |
| `config.rs` + `config/ownerfs.rs` | 扁平键 `ownerfs_adopt_on_startup`（默认 false，Node 角色）与 `ownerfs_data_dir`（默认 `<data_dir>/ownerfs`，绝对路径，Node + OwnerFs） | ~25 行 |
| `node/assembly/ownerfs.rs` + `node/api/workspace.rs` | 存储根从写死的 `data_dir.join("ownerfs")` 改为 `cfg.ownerfs_data_dir`；**API 契约不改**（不新增 `adopt` 字段），仅内部就绪校验同步 | ~4 行 |
| `node/api/workspace.rs` | 就绪校验的 `(dev, ino)` 比对改读 `ownerfs_data_dir`；HTTP 契约零改动，但漏改会让换根后的 `wait_outlet` 永久失败 | ~2 行 |
| `crates/protocol/proto/meta.proto` + `meta/owner_roots.rs` + `node/rpc/meta.rs` + `meta/store/txn.rs` | `ReserveRootRequest.pin_home`（字段号 10）、`select_home` 短路分支、节点侧透传 | ~35 行 |
| 同上（独立提交） | `RecoverRoots`：proto 消息、批量 txn（分批 256 + 逐条降级）、`OperationResult::RootGrants` 变体、节点侧 reconcile 调用点、版本 bump + golden 重生成 | ~150 行 |
| 测试 | 扫描矩阵（保留名/非目录/挂载点/超长/双节点冲突/幂等/部分失败）+ `pin_home` 两种 LB 配置 + **P0′ 入口拒绝** + 批量 recover（成功/降级/重放/条数不符）+ inode 不变 + trash 反解 + 崩溃窗口；重写约 18 项失效断言 | ~380 行 |
| 文档 | `docs/architecture/afs.md` 目录命名段落、`docs/deployment/afs.md` 存储根 / autofs / 开关前置 / 非 UTF-8 限制 | 4 段 |

合计约 **990 行**（测试约 1/3），其中批量 recover 约 150 行可独立交付。与 API 方案（约 950 行）
基本持平，但评审面显著变小：API 契约零改动，Meta 只多两个 proto 消息；增加的是**启动期行为的
验收责任**，这部分不能靠单测替代，必须真机跑。

## 9 验收矩阵

### 9.1 本地（`cargo test -p afs --lib`，必须全绿）

| 项 | 断言 |
| --- | --- |
| 认领零动作 | 认领前后目录 `(dev, ino)`、文件 inode、内容、mode、mtime 全部不变 |
| 扫描规则 | §6.2 的每一条判定都有测试：点前缀、`lost+found`、非目录、非 UTF-8、挂载点、超长名、已活跃、pending、Meta 本节点、Meta 他节点、正常纳管 |
| 扫描开关 | `adopt_on_startup=false` 时一个都不纳管；`true` 时按 §6.2 纳管 |
| 负载均衡正交 | `ownerfs_mkdir_load_balance=true` 时：纳管根的 `home_node_id` 为发起节点（`pin_home` 生效），同批新建目录仍按 HRW 分散到两个节点；`pin_home` 缺省时 HRW 行为不变 |
| 扫描幂等 | 连续两次启动只纳管一次；纳管后第二次启动因「catalog 已有活跃记录」跳过 |
| 扫描失败隔离 | 单个目录纳管失败（如 Meta 返回冲突）不影响其余目录，也不阻塞启动 |
| 代次隔离 | 删除后 live 名字立即释放；trash 出现 `root-<hex>-e<N>`；同名重建落在原名；`collect_trash` 只回收旧代 |
| 崩溃窗口 | 四个窗口分别重启后行为符合 §4.5 表格，且**任何窗口都不会递归删除非空目录** |
| 非 UTF-8 入口拒绝（P0′） | FUSE 创建 `bad\xffname` 返回 EINVAL；存储根里只放一个 `bad\xffname` 目录，扫描跳过 + WARN，连续两次启动正常，摘要 `adopted=0 / failed=0 / skipped == scanned`，目录保留 |
| 非 ASCII 名字 | API 拒绝非 ASCII；扫描遇到中文名跳过 + WARN，不纳管 |
| 批量 recover | N 个 root 一批换绑成功，`access_generation` 各自 +1；单 root 冲突时降级逐条且其余成功；同 `request_id` 重放返回同一批 grant；返回条数少于请求时缺的那个走 zombie 分支 |
| 保留名 | 创建路径：`.ownerfs-` 前缀、`.`、`..`、含 `/` 一律拒绝；扫描路径：所有 `.` 开头与 `lost+found` 一律跳过 |
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
| 删除改名 | strace 抓到 `rename(".../test001" → ".../.ownerfs-trash/root-74657374303031-e3") = 0` |
| 旧代残留时重建 | `201`，epoch 5，新代可写，旧代隔离；解除 immutable 后 20s 回收且新代无损 |
| 批量 | 20 个目录 / 1040 条目，inode 全一致，**平均 245ms/个**（此数是 API 路径含 `wait_outlet` 的口径，非纳管成本，见 §6.5） |
| 野目录 | 24 个未纳管目录在全新 Meta 下重启后全部保留，FUSE 不可见、不被删 |
| 重启自洽 | catalog `data_dir` 与物理目录一致，root 仍可用 |

### 9.3 待真机补验（本期项修完后）

1. **P0′ 端到端**：存储根里放一个 `bad\xffname` 目录 + 一个中文目录，重启后前者跳过、后者纳管；
   连续两次重启都不受影响（这正是 P0 原本会炸的场景）。
2. **「重启即纳管」端到端**：24 个野目录的现场，开开关重启后只纳管符合规则的目录，
   inode 全部不变，`/public` 与 FUSE 视图与摘要日志一致。
3. 双节点同名冲突：两个存储根各放一个同名目录，先启动的一方拿到 Home，另一方跳过 + WARN。
4. `ownerfs_mkdir_load_balance=true` 下的纳管：野目录全部落在各自节点（不出现空目录、
   inode 不变），随后新建的目录仍按 HRW 分到两端。
5. 启动时长实测：1000 个目录的量级，分别量逐条与批量 recover，确认窗口在 30 秒以内且批量
   确实把耗时曲线拉平。
6. autofs 在场时的纳管（必须验证「停 map」是硬前置）。
7. node→node gRPC 通道单独验收（P5 附带发现）。

## 10 部署前置与回退

### 10.1 前置（缺一不可）

1. **停 autofs 的 map**。否则 automounter 会在目录被纳管后重新挂载，使目录变成挂载点，
   后续 trash 隔离直接 `EBUSY`。
2. **OwnerFs 存储根指向现有数据的父目录**（不是每个子目录各挂一个）。存储根由
   `ownerfs_data_dir` 指定，默认 `<data_dir>/ownerfs`；纳管时必须显式改到数据父目录，
   否则扫描的是默认私有目录。跨设备时 `rename` 会 `EXDEV`，trash 隔离做不成。
3. 核对同文件系统：

   ```bash
   stat -c '%d %n' <数据父目录> <数据目录> <ownerfs存储根>
   ```

4. 核对保留名与长度（`.` 开头的目录不会被扫描纳管，需人工确认是否漏了业务目录）：

   ```bash
   ls -A <数据父目录> | grep -E '^\.'
   ls -A <数据父目录> | grep -E '^lost\+found$'
   ls -A <数据父目录> | awk '{ if (length($0) > 114) print }'
   ```

5. **人工核对目录清单**：`ls -1 <storage_root>`，确认里面只有要纳管的业务目录。数量上限已按
   评审删除，系统**不会**自动阻断误配的存储根（§6.4.1）；误纳管 + 误删是本方案唯一能造成
   批量数据位置变更的路径。建议先在单节点开启、看过摘要日志后再推到第二个节点。

6. **打开开关**：`ownerfs_adopt_on_startup = true`（默认关闭，G1）。纳管发生在**各节点
   自己的**存储根上，所以不要再用「把请求发给哪台节点」来判断 Home；
   `ownerfs_mkdir_load_balance` 可以保持开启，纳管走 `pin_home`、新建目录照旧走 HRW（§6.4.2）。

### 10.2 回退

格式与旧实现**不兼容**：旧环境的 `root-*` 目录在新二进制下无法被识别为 root 数据目录，
且不提供 legacy 分支（D11）。回退方式为「回退二进制 + 重置 AFS 环境」，不做数据兼容。
这依赖「系统未上线」的前提；若上线后需要再改命名，必须另做迁移方案。

`RecoverRoots` 的 `OperationResult` 新变体同属一次性格式变更（R10）：只向前，不提供「新 WAL
给旧二进制读」的降级路径。两个变更都应在同一个发行窗口上线，避免「回退了二进制却读不了
WAL」这种最尴尬的组合。

## 11 风险与未决

| # | 项 | 状态 |
| --- | --- | --- |
| R1 | 非 UTF-8 名字**本期不支持**（P0′ 在两个入口拒绝）；将来支持时还需确认跨机可用性——NFSv4 要求 UTF-8、NFSv3 字节安全 | 随独立 PR（P0+P3）一并交付；本期只需保证入口拒绝的报错可读 |
| R2 | 名字 ≤114 字节是硬约束，来自 trash 命名空间 | 待业务方确认存量目录是否有超长名字 |
| R3 | 中文名/non-ASCII 名可被扫描纳管，但 `validate_owner` 仍拒绝它们 → **造出删不掉的 root**（§6.6） | 已裁决：扫描默认跳过非 ASCII（§6.2），`validate_owner` 不变；将来支持时需同步放宽删除入口 |
| R4 | 空目录删除直接 `rmdir`、不产生 trash（与命名无关的既有行为）；非空目录的递归删除只能走 API，FUSE `rmdir` 返回 `ENOTEMPTY` | 需在运维文档写明 |
| R5 | **误纳管**：存储根配错时一次启动接管几百个无关目录；「删除 workspace = 搬进 trash 并约 20 秒后真删」，误纳管后再误删等于数据位置变更 | **无自动阻断**：数量上限已按评审删除，靠 G1（默认关闭）+ §10.1 第 5 条人工核对 |
| R6 | **启动时长**：15–25ms/目录（不含 API 的 `wait_outlet`），1000 个 15–25 秒，期间 VFS 未挂载 | 批量 recover 后进一步下降（§6.5）；不需额外时长护栏 |
| R7 | **双节点同名**：两边都有真实数据时必须人工裁决，系统只会让先启动的一方拿到 Home | 需运维手册写明裁决步骤 |
| R8 | Meta root 数量随纳管目录数线性增长；已确认 `root_command` 无界泄漏（约 1.9KB/删除） | 长期问题，与本期无关但会被放大 |
| R9 | 以 `.` 开头的存量目录不会被扫描纳管（D7 的扫描侧规则）；业务方若有这类目录，需人工改名或另加白名单 | 部署核对时一并检查 |
| R10 | `RecoverRoots` 新增 `OperationResult` 变体，旧二进制读新 WAL 会失败（只增不改的枚举变体也不保证降级可读） | 与 §10.2 的回退策略一致（回退即重置）；需要单独发版说明 |
| R11 | zombie 判定用 `metadata().unwrap_or(false)`，把 IO 错误退化成「目录不存在」，会直接 `deactivate`（`Delete`，无 tombstone） | 待定：改为非 NotFound 即 fail-closed；并加部署前置「`storage_root` 下不允许子挂载点」 |

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

1. P4 + P6 + **P0′**（保留名前缀 + 长度校验 + 非 UTF-8 入口拒绝）——量小、独立、可先合。
   P0′ 先合的价值：后续所有实现与真机测试都不必再担心「造出一个非 UTF-8 root 把自己弄挂」。
2. 命名公式三处 + `placement` + `rename_to_trash` + `trash_entries` 反算——这一步跑通后，
   纳管的本地动作才成立。
3. `pin_home`（proto 字段 + `select_home` 短路 + 节点透传）——量小，但它是「负载均衡可以
   保持开启」的前提；先做掉可以让后续真机验证都在 LB 开启下进行。
4. `adopt_pre_existing_roots()` + 护栏 G1–G3 + 摘要日志——主体工作，按 §6.2 的规则逐条写测试。
5. 按新契约重写 18 项失效断言，跑 `cargo clippy` / `cargo fmt --check`。
6. 真机改造现有 `/home/lzq/afs-adopt` 现场，验证「重启即纳管」：野目录只按开关纳管、
   双节点同名冲突跳过、LB 开启时仍落本节点、启动时长与摘要日志。
7. **`RecoverRoots` 批量接口**（独立提交，可与 1–6 并行）——先写批量/降级/重放测试，再换
   节点侧 reconcile 的调用点；版本 bump + golden 重生成单独一个提交，便于审阅与回退。
8. 非 UTF-8 全链（P0+P3）——本特性合入后另开 PR，补非 UTF-8 重启回归与跨机 NFS 验证。

## 14 实现决策记录（2026-11-17）

| # | 议题 | 决定 |
| --- | --- | --- |
| D-1 | outlet mirror 是否零改动 | **必须改**：`target_path` 改按 name 构造；原文「mirror 本期不改」与实现不符 |
| D-2 | catalog 区分 active / pending 记录 | 不区分，判定合并为「catalog 已有该 root_id 记录 → 跳过」 |
| D-3 | catalog 新增 `adopted` | **不加**；schema 不变，`RECORD_VERSION` 不 bump |
| D-4 | 启动清理与空业务目录 | 启动期清理**不 `rmdir` live 目录**：pending / zombie / `retire_fenced_root` 全部改为 warn 并保留目录。pending 由 Meta abort 清理；zombie 与 fenced 代次从本地 catalog 丢弃记录（否则后续扫描会因「已有记录」跳过，目录永远无法重新纳管）。`rmdir` 只用于 trash 条目与显式删除 |
| D-5 | R3 非 ASCII | 扫描默认跳过非 ASCII；`validate_owner` 保持现状 |
| D-6 | 配置键 | 扁平键 `ownerfs_adopt_on_startup`（Node 角色，默认 false），不引入 `[ownerfs]` 表 |
| D-7 | 扫描接线位置 | assembly 在 `RootManager::open` 返回后、`recovered_roots()` 之前调用；`open` 签名不变 |
| D-8 | `RecoverRoots` 批量接口 | 本轮不做，不触碰 `OperationResult` / WAL 格式；作为独立提交另行交付 |
| D-9 | 同名重建遇到旧代次目录 | 本地 catalog 有该 root 记录 → **复用**目录（只 chmod/chown/sync，不删除内容）；无记录且目录非空 → `IO_DIRECTORY_NOT_EMPTY`，不静默接管 |
| D-10 | 存储根此前不可配置 | 新增扁平键 `ownerfs_data_dir`（Node 角色、绝对路径、默认 `<data_dir>/ownerfs`）。原文 §10.1 要求「存储根指向数据父目录」，但实现写死 `cfg.data_dir.join("ownerfs")`，部署面无法把根指到 `/home/agentos/.users`；`node/api/workspace.rs` 的就绪校验同步改用该键 |

由此产生的已知残留：

- 同一进程内同名重建（catalog 已有该 root 的旧代次记录）**复用**原目录并保留内容，不再
  `remove_dir` 后重建（`prior_preparation` 分支）。
- 若旧代次目录**已无 catalog 记录**（例如启动 reconcile 已丢弃记录、或数据目录由外部留下）
  且非空，同名 `create` 会因 `ENOTEMPTY` 失败并保留原数据，不会静默接管。正常「删除后重建」
  不受影响：删除会把 live 路径 rename 进 trash。需要接管时显式开启 `ownerfs_adopt_on_startup`。
- 启动崩溃窗口可能留下空目录，由下一次扫描重新纳管。

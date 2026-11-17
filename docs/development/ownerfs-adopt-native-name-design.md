# OwnerFs 原生目录名纳管设计

状态：设计草案，待评审。机制已在双机真机验证通过（见 §12），代码仅存在于 spike 分支
`spike/ownerfs-adopt-native-name`（`c98c9e75`，**不可合入**，见 §7 的六个必修项）。
本文描述的是目标设计；凡标「待实现」的条目，当前分支上并不存在。

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
| D5 | 纳管是显式动作（`adopt`），`create` 撞名一律不接管 | 否则别人手建的目录会被一次 create 吞掉 |
| D6 | adopt 允许省略 uid/gid/mode，省略即从目录实际属性回填 | 业务方 200 个目录权限不可能统一，精确匹配会让纳管变成逐个 `stat` |
| D7 | 保留名从 `.pub`/`.quarantine` 扩为拒绝 `.ownerfs-` 前缀 | 存储根与业务目录同层后，`.ownerfs-roots/`、`.ownerfs-trash/`、锁文件都在这一层 |
| D8 | 同名重建后，跨机旧路径指向新 workspace（POSIX 语义） | 接受；见 §5 |
| D9 | 非 UTF-8 名字全链走 `OsString`，catalog 不再落盘 `data_dir` | 真机上非 UTF-8 名字会让节点起不来（P0） |
| D10 | 纳管必须固定 Home（`pinned_home_node_id`），不走 HRW | 负载均衡会把 Home 选到数据不在的节点，造成「纳管成功但目录是空的」（P5） |
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

### 4.4 认领流程

```text
① 业务方现有目录           <storage_root>/test001   （AFS 不参与）
② POST /v1/workspaces {"owner":"test001","adopt":true}
③ Meta 预留代次            reserve_root → epoch=N（D10：Home 已固定）
④ 本地认领                 prepare_local_root：命中已存在目录则跳过 mkdir/chmod/chown
⑤ 写 catalog 凭据          .ownerfs-roots/root-<hex>.json
⑥ activate_prepared        state=ready，FUSE 可见
⑦ outlet 发布（250ms 内）   /public/test001 → <import_root>/<home>/users/test001
```

第 ④ 步是纳管的实质。原 create 会 `mkdir` + `chmod` + `chown` 三步都做；目标实现先探目标
路径，命中已存在目录就置 `adopted = true` 并跳过这三步，只做两件不改变数据的事：

- `open_dir` 拿一个目录 fd 用于 fsync；
- 检查 `dev` 与父目录一致（挂载点不能 rename，必须在纳管时拒掉，否则第一次删除才炸 `EBUSY`）。

真机证据（`demo-adopt`，mode 0755，含 `2024Q1/orders.csv`）：

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

认领比 create 多一个窗口：目录在原位、catalog 还没写。各窗口的重启行为：

| 崩在 | 磁盘状态 | 重启后行为 |
| --- | --- | --- |
| ① 之后 ② 之前 | 业务目录 + 无 Meta 记录 | 完全忽略，可重试 |
| ② 之后 ④ 之前 | 业务目录 + Meta pending + 无 catalog | pending 分支：`abort_root`，`rmdir` 失败于 ENOTEMPTY → **warn 不阻塞**（已改） |
| ④ 之后 ⑤ 之前 | 同上 | 同上 |
| ⑤ 之后 ⑥ 之前 | catalog 有记录 + Meta pending | 同上，且 `record.epoch < reservation.epoch` 时 `retire_fenced_root` |
| ⑥ 之后 | 一切一致 | 正常恢复 |

第二行是必须改的一处：master 的 reconcile pending 分支对残留目录直接
`self.remove_abandoned_dir(&data_dir)?`（`root.rs:1857`），`?` 会让**节点起不来**。
好消息是 `remove_abandoned_dir` 只调 `rmdir`（spike `root.rs:2044`），**不递归**，所以那个
窗口不可能删掉业务数据。目标实现把它改成与另一分支一致的 warn + 保留 catalog 证据
（spike 已在 1943 处改为 warn）。

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

## 6 接口设计

纳管复用现有 `POST /v1/workspaces`，通过字段区分语义，不新增端点：

```jsonc
// 创建（现状语义不变）
{"owner": "test001", "uid": 1000, "gid": 1000, "mode": "0750"}

// 纳管（目标）
{"owner": "test001", "adopt": true,
 "uid": 1000, "gid": 1000, "mode": "0755"}   // 三个可选，省略则从目录实际属性回填
```

| 字段 | 创建 | 纳管 |
| --- | --- | --- |
| `adopt` | 省略 / `false`；目标路径已存在 → `409` | `true` 且目录已存在 → 认领 |
| `uid`/`gid`/`mode` | 必填语义（默认 0700），mkdir 后 chmod/chown | 可省略，省略则取 `stat` 实际值 |
| 目标路径不存在 | 创建 | `409`（纳管必须命中已有目录，不隐式创建） |
| 目标路径非目录 | `409` | `409` |
| 目标为挂载点（`dev` ≠ 父目录） | 不适用 | `409`，提示改 bind 到存储根 |

`adopt: true` 时必须显式携带 `uid`/`gid`/`mode` 才校验；省略即接受目录现状。这样
`uid/gid/mode differ from request` 这一类 409 只会在调用方主动指定且不一致时出现。

> P2：`validate_owner` 目前只允许 `[A-Za-z0-9._-]` 且 ≤64（`workspace.rs:950`），与
> 「workspace 名是任意字节串」的既有契约矛盾。中文名只能走 FUSE `mkdir`（已验证端到端可用）。
> 纳管入口若只有 HTTP，中文目录会卡在第一步。

## 7 必须修复的问题

真机把 spike 的缺陷暴露得很干净。以下六项全部是**评审门槛**，未修完不可合入。

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

### P1 · 纳管请求的 uid/gid/mode 被精确匹配

传 `0750` 而目录是 `0755` → `409 uid/gid/mode differ from request`；改 `0755` 立刻成功；
不传则默认 `0700` 一样撞。见 §6 的 `adopt` 语义。

### P2 · 中文名走不了 HTTP API

见 §6。需要决定：放宽 `validate_owner`（改为只校验「非空、无 NUL、非 `.` / `..`、
≤ NAME_MAX」），还是明确「中文名只支持 FUSE 入口」。**建议前者**，与既有文档契约对齐。

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

修法：

```rust
ReserveRootInput {
    preferred_home_node_id: String,          // 现状：只是入口节点
    pinned_home_node_id: Option<String>,     // 新增：adopt 显式指定，跳过 select_home
}
```

Meta 侧校验该节点 session live 且 `ownerfs_enabled` 后直接授权，不走 HRW。adopt 入口由部署
脚本传入「数据所在节点」，或由节点自查存储根是否有同名目录、查到才允许纳管。

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

全部集中在 3 个模块，不碰 proto、Meta 存储、state image、golden（P5 的 `pinned_home_node_id`
是唯一例外，需要 proto + Meta 记录 + 快照）。

| 文件 | 改动 | 量级 |
| --- | --- | --- |
| `node/vfs/ownerfs/root.rs` | 三个命名公式、`placement`、`rename_to_trash`、`trash_entries` 反算、prepare 的 adopt 分支、reconcile pending 非空 warn、`validate_root_name`（前缀 + 长度）、`remove_abandoned_dir` 保持 rmdir 语义 | ~300 行 |
| `node/vfs/ownerfs/catalog.rs` | `LocalRootRecord` 去掉 `data_dir` 持久字段改为派生；新增 `adopted: bool` | ~40 行 |
| `node/outlet/mirror.rs` | `target_path` 返回 `OsString`；`Action::Publish.target`、`PresentEntry::Symlink.target`、`classify()` 全链 `OsString` | ~80 行 |
| `node/outlet/live.rs` | `read_link` → `OsString`、`symlinkat` 调用点、target 比较 | ~30 行 |
| `node/api/workspace.rs` | `adopt` 字段 + 可选权限回填；`validate_owner` 放宽（P2） | ~60 行 |
| `meta/*` + `meta.proto` | `pinned_home_node_id` 全链 + 快照 + 版本 bump + golden 重生成 | ~150 行 |
| 测试 | 新增：inode 不变、非 UTF-8 重启、trash 名反解、超长名拒绝、`.ownerfs-` 前缀拒绝、挂载点拒绝、三个崩溃窗口；重写：约 18 项按新契约失效的断言 | ~300 行 |
| 文档 | `docs/architecture/afs.md` 的目录命名段落、`docs/deployment/afs.md` 的存储根与 autofs 前置 | ~3 段 |

粗略合计 **~950 行 diff**（其中测试约 1/3）。真机验证脚本在 WSL `/root/adopt-verify/`，
可作为集成验收基线复用。

## 9 验收矩阵

### 9.1 本地（`cargo test -p afs --lib`，必须全绿）

| 项 | 断言 |
| --- | --- |
| 认领零动作 | 认领前后目录 `(dev, ino)`、文件 inode、内容、mode、mtime 全部不变 |
| 代次隔离 | 删除后 live 名字立即释放；trash 出现 `root-<hex>-e<N>`；同名重建落在原名；`collect_trash` 只回收旧代 |
| 崩溃窗口 | 五个窗口分别重启后行为符合 §4.5 表格，且**任何窗口都不会递归删除非空目录** |
| 非 UTF-8 | 名字含 `\xff` 可创建、可发布、可重启、可删除；两个仅差非法字节的名字不塌成一条 |
| 保留名 | `.ownerfs-` 前缀、`.`、`..`、含 `/` 一律拒绝 |
| 长度 | 115 字节名在 create/adopt 处被拒，错误信息指向 trash 命名空间 |
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
2. `pinned_home_node_id` 生效后，从错误节点发起纳管必须被拒，而不是静默建空目录。
3. autofs 在场时的纳管（必须验证「停 map」是硬前置）。
4. node→node gRPC 通道单独验收（P5 附带发现）。

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

5. **固定 Home**：纳管请求必须发到持有数据的那台节点的 workspace API 端口
   （`[ownerfs_workspace_api] listen`，不配则该路由不挂载、`/v1/workspaces` 返回 404）。
   长期由 P5 的 `pinned_home_node_id` 兜住。

### 10.2 回退

格式与旧实现**不兼容**：旧环境的 `root-*` 目录在新二进制下无法被识别为 root 数据目录，
且不提供 legacy 分支（D11）。回退方式为「回退二进制 + 重置 AFS 环境」，不做数据兼容。
这依赖「系统未上线」的前提；若上线后需要再改命名，必须另做迁移方案。

## 11 风险与未决

| # | 项 | 状态 |
| --- | --- | --- |
| R1 | 非 UTF-8 名字的跨机可用性未验。NFSv4 要求 UTF-8，NFSv3 字节安全；本地已验证可用，跨机未测 | 待验；若不可用，需在发布时降级（本机可用、跨机不发布） |
| R2 | 名字 ≤114 字节是硬约束，来自 trash 命名空间 | 待业务方确认存量目录是否有超长名字 |
| R3 | 中文名入口：放宽 `validate_owner` 还是只走 FUSE | 待定（P2 建议放宽） |
| R4 | 空目录删除直接 `rmdir`、不产生 trash（与命名无关的既有行为）；非空目录的递归删除只能走 API，FUSE `rmdir` 返回 `ENOTEMPTY` | 需在运维文档写明 |
| R5 | 纳管 N 个目录是串行 RPC + fsync，245ms/个；规模到万级需要批量接口 | 本期不做，记录在案 |

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

1. P0 + P3（非 UTF-8 全链 `OsString` + catalog 不落盘 `data_dir`）——先写失败测试再改。
2. P4 + P6（保留名前缀 + 长度校验）——量小、独立、可先合。
3. P1 + 显式 `adopt`（含 `LocalRootRecord.adopted` 落盘，修重试绕过校验）。
4. P5（`pinned_home_node_id`）——唯一需要动 proto 和 Meta 的部分，单独一个提交。
5. 按新契约重写 18 项失效断言，跑 `cargo clippy` 与 `cargo fmt --check`。
6. 真机按 §9.3 补验，产出验收报告。

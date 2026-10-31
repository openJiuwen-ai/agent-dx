# 跨节点 Workspace 出口（Outlet）设计规范 v2 —— fail-open 直达路径

## 0. 版本说明

- 本规范是跨节点 Workspace 共享能力的**简化重设计**，目标基线为 **master 分支**
  （master 已具备 OwnerFs、Meta root 生命周期 `ReserveRoot/ActivateRoot/DeactivateRoot`、
  `NodeSession` 心跳，`afs/src/node/vfs/ownerfs/root.rs` 的 `root_data_dir()` 是
  物理目录命名的唯一权威）。
- 本规范**不基于** `feat/afs-outlet-on-master` 分支上的 outlet 实现（探针、事件流、
  发布状态机）；该实现作为设计对照保留在原分支，不进入本规范的依赖或迁移范围。
- 相对 v1 设计（`docs/afs-outlet-cross-node-spec` 分支
  `afs/outlet-cross-node-spec-v1.md`）的核心取舍：**放弃运行中持续健康门禁
  （fail-closed），接受 fail-open**，换取发布面从状态复制机退化为纯路径推导。
- 设计输入：集群规模 **≤ 10 节点**（全网格挂载表 ≤ 90 条，静态 fstab 维护
  可接受，见 R2 定案），**数百个 workspace**（容量计算见 §5.5）。
- 文中"硬规则"为验收合同，违反即实现错误；"建议"为实现自由度。
- **v3 修订**（相对本文件 v2 初稿）：
  1. NFS 由 `hard` 改为 **`softerr`**（§6）——`hard` 会让进程进入不可 kill 的
     D 状态，同时使 `umount` 失效，故障时既不能有界失败也不能卸载修复；
  2. 新增 **`root_revision` 校验位**（§4.2）替代每轮全量传输，作用域限定在
     Root 实体，不受 DFS 事件污染；**明确不做事件游标**（§4.3 给出对比）；
  3. 新增 **NFS 参数硬要求**与挂载点权限（§6）；
  4. 新增 **存量 `/home/agentos/users` 迁移**前置条件（§6.4）；
  5. 保留 D5（target 带 epoch），**不引入 home 侧稳定名**——理由见 §7.3；
  6. 明确 `ListActiveOutletRoots` 会返回死 home 的 root（recovery 语义不做
     存活过滤），消费方不得据此推断 home 活着（§4.4）；
  7. 新增 **§6.5 NFS 导出绕过 AFS 授权**与 **H18 整树导出**——数据面不经过
     `afs-node`，`validate_peer_root_access` 不参与，可见性边界由
     `/etc/exports` 决定（R8）。

## 1. 需求概述

集群内多个节点，workspace 在某一个节点（home）上创建，其余节点的容器使用
**同一个平坦名字**访问它。

- 使用方（起容器的一方）不感知节点拓扑，不调用 Meta 数据接口之外的任何东西；
- 数据权威唯一（home 节点本地盘），不做数据复制；
- 本机访问与跨节点访问共用一套路径公式；
- 消费方需要在启动容器前有一个廉价的可用性检查手段。

## 2. 设计决策记录

| # | 决策 | 理由 | 放弃的东西 |
|---|---|---|---|
| D1 | 部署方铺统一挂载布局 `/imports/<node>/users`，本机 bind、远端 NFS | 路径公式全局有效；本地/远端差异从代码分支降级为挂载类型 | 挂载表的 N² 静态配置由部署工具维护 |
| D2 | fail-open：不做运行中健康门禁 | 持续门禁需要探针线程 + 副本 + 摘牌对账（v1 约 3000 行）；真实用例是启动前一次性检查 | "home 死后名字立即消失"语义 |
| D3 | 视图即推导：`/public` 由 250ms 全量 diff 驱动，不维护内存副本 | 磁盘 symlink 就是状态；恢复 = 重新 diff | 事件流、游标、退避、Compacted |
| D4 | 镜像 worker 永不 stat target | 远端 target 在 softerr 挂载上仍可能阻塞至 `io_failure_bound` | target 存在性前置校验（由消费方启动门禁承担） |
| D5 | 镜像 target 直接携带代次（`-e<epoch>`），**不设 home 侧稳定名** | 换代 = 镜像文本变化 = 一次原子 rename；同名重建无歧义（epoch 直可见） | 换代传播的最终一致性窗口（由 H11 宽限期兜住，见 §7.3） |
| D6 | NFS 用 `softerr` 而非 `hard` | `hard` 使进程进入不可 kill 的 D 状态且阻塞 `umount`，故障时无法有界失败也无法卸载修复 | 极少数"静默重试直到恢复"的连续性（改为有界失败，由消费方重试） |
| D7 | 传输层用 `root_revision` 校验位，**不用事件游标** | 空闲每轮 20 B；不需要游标、Compacted 回退、生产级唤醒原语 | 事件级增量（ε 级延迟，但本场景不可观测，见 §4.3） |
| D8 | 不做后端抽象；NFS 判定收敛到单一模块（编译期，不是 trait） | 首版只有 NFS；抽象等到第二个实现出现再引入 | 可插拔后端 |

## 3. 核心抽象与不变式

### 3.1 三个平面

- **数据面**（部署方，静态）：每个节点把全部成员树挂进统一布局
  `/imports/<node>/users`；本机为 bind，远端为 NFS。挂载细节见 §6。
- **控制面**（Meta，已有 + 一个新 RPC）：root 记录
  `{name, home_node_id, epoch, state}` 与节点心跳表，见 §4。
- **名字面**（每节点镜像 worker，新增）：tmpfs `/public` 下的
  `name → target` symlink，见 §5。

### 3.2 路径公式（全方案唯一的"地址计算"）

```
target(/public/<name>) = "/imports/" + home + "/users/" + data_dir_text(name, epoch)
```

- `data_dir_text(name, epoch)` 必须调用与 OwnerFs 同源的命名函数
  （master `afs/src/node/vfs/ownerfs/root.rs` `root_data_dir()`，
  产出 `root-<hex(name)>-e<epoch>`）。**硬规则 H1**：命名不得出现第二份实现；
  镜像 worker 与 OwnerFs 共享同一 crate 函数。
- 该公式在所有节点上产出同一文本。本地/远端的区别只由消费节点的挂载表
  在解析时决定，任何代码不得依据"本机是否为 home"产生分支
  （**硬规则 H2**：节点侧 outlet 组件禁止持有 `is_local_home` 判断）。

### 3.3 不变式

- **H3**：AFS 永不执行 `mount`/`umount`；挂载缺失/类型漂移只告警。
- **H4**：镜像 worker 的全部输入 = Meta Active 快照 + `public_root` 目录列表。
  禁止读取 `/imports` 下任何内容（含 stat）。
- **H5**：`/public/<name>` 的放置与摘除必须是原子的（staging 目录 + rename）。
- **H6**：workspace 名是单个普通路径分量；非法名拒绝，不落盘。

## 4. Meta 契约

master 的 root 生命周期与 `NodeSession`（含 `is_live_at_unix_ms`）原样复用，
root 记录**不新增字段**。新增一个只读 RPC，带可选的变更校验位。

### 4.1 `ListActiveOutletRoots(known_root_revision) -> { roots, root_revision }`

```proto
message ListActiveOutletRootsRequest {
  string request_id = 1;
  // 0 表示"无本地快照，要全量"。非 0 时若与 Meta 当前值相等，
  // 服务端返回 unchanged=true 且不带 roots。
  uint64 known_root_revision = 2;
}

message ListActiveOutletRootsReply {
  repeated RootLocation active_roots = 1;
  uint64 root_revision = 2;
  bool unchanged = 3;
}
```

- 返回当前 Active 状态的 root 集（`root_id`、`root_epoch`、`home_node_id`）。
- **没有游标、没有事件、没有 Compacted**——全量快照即恢复机制。
- `unchanged=true` 时 `active_roots` 为空，客户端**必须**沿用本地快照，
  不得解读为"没有 root"。
- 服务端要求调用方持有当前 live 会话（复用现有姿态）。
- **wire 不含 `home_session_id`**：镜像不用它，而它占 payload 约 1/3。
  需要 session 信息的消费者走 `ListOwnerRoots`，不复用本 RPC。

### 4.2 `root_revision`：作用域正确的校验位

在 `State` 增加 `root_revision: u64`，**仅在写路径中对 Root 系实体递增**：

```rust
// state.rs 现有 Put/Delete 分支内，紧邻 state.events.push
if matches!(key, MetaKey::Root { .. } | MetaKey::RootReservation { .. }) {
    state.root_revision = state.root_revision.wrapping_add(1);
}
```

**硬规则 H13**：`root_revision` 的递增条件只允许是上述两个 key 变体。
禁止改用全局 `state.revision`。

理由（这是本设计最容易被做错的地方）：`state.rs` 的每个 `Put`/`Delete`
都会推全局 `revision` 并追加事件，**包括 DFS 的每个 inode 写、GC 引用更新、
复制任务状态**。任何基于全局 revision 的"有没有变"判断，在有 DFS 流量的
集群里会持续判定为"已变"，校验位退化为 `always false`，方案失效。
`root_revision` 的作用域必须严格限定在"改变 Active root 集合"的实体上。

`RootReservation` 计入递增是保守选择：reservation 不可发布（§3.3），但它
之后必然转为 Root 或消失，提前失效快照只会多一次全量传输，不会漏发布。

### 4.3 为什么不用事件游标

事件游标（`WatchRoots(after_revision, wait)` long-poll + 基线 + `Compacted`
回退）与 `root_revision` 校验位的对比如下。设定：10 节点、500 workspace、
Root 集合每天变化约 200 次。

| 维度 | 全量快照 250ms | **`root_revision` 校验位 250ms** | 事件游标 long-poll |
|---|---|---|---|
| 空闲单轮传输 | 60 KB | **20 B** | 空批仍计入 `WATCH_RESPONSE_BASE_BYTES`（64 KB 预算） |
| 集群空闲出口 | 2.4 MB/s | **800 B/s** | ~1 KB/s |
| 变化时传输 | 60 KB 全量重传 | 60 KB（一次） | 仅变化事件（~120 B/条） |
| 感知延迟 | ≤250ms | ≤250ms | 提交 + RTT（~1ms） |
| 恢复路径数 | 1（首轮扫描） | **1**（计数器失配即全量） | **2**（基线 + `Compacted` 回退） |
| Meta 侧新增 | 无 | 一个 `u64` + 6 行递增 | 生产级唤醒原语 + 事件过滤 + `wait` 裁剪 + 慢消费者策略 |
| 客户端状态 | 无 | 一个 `u64` | `after_revision` + 本地集合 + 回退逻辑 |
| 估计代码量 | 0（T1 已有） | **~60 行** | **~300–400 行** |

**结论**：校验位在空闲流量上与游标几乎持平（800 B/s vs 1 KB/s），在复杂度
上是它的 1/6，代价是 250ms 的发布延迟。

250ms 在本场景不可观测：workspace 激活后，容器 provisioning 要走调度、
拉镜像、挂 rootfs、起进程，量级是秒。游标用一整套协议栈买来的 249ms，
落在这个流程的噪声里。**反过来，`Compacted` 造成的第二条恢复路径是
可观测的风险**——"状态可能从两处重建"正是副本不一致类 bug 的来源。

**Meta 侧扫描不构成理由**：`ActiveRoots` 扫描 500 条约 50–200 µs，
250ms 一轮即 0.02%–0.08% CPU。若将来 workspace 上到万级（约 8% CPU），
再重新评估游标方案；届时本节的对比表是决策依据。

**可选优化（非必需）**：Meta 侧可用 `root_revision` 兼作快照缓存键
（未变则复用上次序列化的 `Arc<[u8]>`），把空闲开销降为 O(1) 比较。
首版不实现，等实测显示扫描成为瓶颈再加。

### 4.4 `Active` 的语义边界（消费方必读）

- **"Active" = `MetaEntity::Root` 记录存在**。`DeactivateRoot` 直接
  `Delete(MetaKey::Root)`，没有软删除状态位，因此快照不需要状态过滤。
- **`RootReservation` 不构成 Active**，不可发布。
- **本 RPC 不做存活过滤**：已死 home 的 root 仍在返回值中（这是 recovery
  语义，与 `list_owner_roots` 一致）。消费方**不得**用 `lstat /public/<name>`
  通过来推断 home 活着——那只能证明"Meta 曾激活且镜像已同步"。

这条边界是 fail-open 的核心：名字的语义是"曾发布过"，不是"此刻可用"。
需要"此刻可用"判断的消费方，自行做穿透检查（§7.2 可选增强）或查
`NodeSession`。

## 5. 节点组件：镜像 worker

### 5.1 收敛循环（核心逻辑，纯函数可测）

```
维护 cached_revision: u64 = 0（进程内，不持久化）

每 outlet_mirror_interval_ms（默认 250ms）：
  reply = Meta.ListActiveOutletRoots(cached_revision)
  if reply.unchanged:
      continue                                  # 磁盘与期望态都没变，零动作
  cached_revision = reply.root_revision
  active  = reply.active_roots                  # 期望态
  present = read_dir(public_root) 过滤保留名     # 现状态
  对 active - present 的每个 root：
      want = 路径公式(root)                     # §3.2
      staging 写 symlink → rename 原子亮出      # H5
  对 present - active 的每个名字：
      unlink（幂等容忍 ENOENT）
  对 public_root 中非 symlink 实体（消费方违约 ghost）：
      rename 进 .quarantine/<截断名>-<随机hex>   # 证据保留，最小实现
```

**硬规则 H14**：`unchanged=true` 时**不得**执行任何 diff 或 unlink。
该分支的语义是"服务端保证 Root 集合自 `known_root_revision` 以来未变"，
把它当成"没有 root"会导致 `/public` 被整体清空。

**硬规则 H15**：`cached_revision` 只放在进程内存。进程重启后从 0 开始，
第一轮必然是全量——这也是唯一的恢复路径（H9）。

注意 `unchanged` 分支仍会让"消费方违约 ghost"存活最多一轮：worker 不扫描
`public_root`，因此看不到 ghost。这可以接受（ghost 只会被下一轮全量清理，
而全量只在 Root 集合变化时发生）。若需严格清理，见 §5.5 的可选定期全量。

### 5.2 硬规则

- **H7**：任何一轮失败（Meta RPC 错误、tmpfs IO 错误）只告警并跳过本轮；
  已放置的 symlink 保持不动，下一轮全量重扫自然收敛。无重试队列、无退避。
  **禁止**因 Meta 不可达清空或批量摘除 `/public` 现有条目（"粘性"语义，
  TC8 验证）；镜像以最后一次成功快照为准持续服务。
- **H8**：快照与磁盘列表之间不存在一致性窗口承诺——diff 中途 Meta 变化
  由下一轮修正，本轮结果必须仍满足幂等。
- **H9**：worker 不消费事件、不维护游标、不持久化任何状态；进程重启后
  第一轮 diff 即完成恢复。
- **H10**：`is_log_transition` 类日志限流仅用于连续失败场景（attempts 计数
  可保留在本地变量，不落结构化状态）。
- **H11**：**换代宽限期**——home 侧废弃旧代物理目录（`-e<epoch>` 旧值）前
  必须保留其存活至少 `outlet_generation_grace_ms`（**默认 120000ms**）。
  原因：镜像 target 直接携带代次（D5），各节点 symlink 的更新是最终一致的；
  同时 NFS 目录缓存使新代可见性滞后至多 `actimeo`。宽限期内旧代目录必须
  可解析，否则窗口内新启动的容器会 bind 到已消失的旧代（ENOENT）。
  取值约束：`grace ≥ interval_ms × 10` 且 `≥ actimeo × 2`，取较大者
  （默认 `max(2500, 120000) = 120000`；推导见 §7.5）。
  宽限期过后旧代目录的删除归 OwnerFs 生命周期常规回收，不属本规范。

### 5.3 启动序列

1. 校验 `outlet_public_root` 是已挂载 tmpfs（mountinfo，非 tmpfs 即 fail-fast）；
2. 确保 `.pub/`、`.quarantine/` 私有目录（0700）存在；
3. 清扫 `.pub/` 残留；
4. 进入收敛循环（第一轮即全量恢复）。

### 5.4 mountinfo 只读诊断

启动时与每 60s 解析 `/proc/self/mountinfo`：

- `outlet_public_root` 非 tmpfs → 启动失败（同上）；
- `/imports/<node>/users` 缺失 → `warn outlet.topology.path_missing`；
- 本机条目非 bind/ext4 类、远端条目非 `nfs*` → `warn path_drift`。

诊断不参与任何决策（H3），不得阻塞收敛循环（独立任务或复用阻塞池）。

### 5.5 容量与参数依据

参数不是拍的，按下列假设计算。设 10 节点、500 workspace、
Root 集合每天变化约 200 次。

| 项 | 计算 | 结果 |
|---|---|---|
| 单次全量 payload | 500 × (40 B `root_id` + 8 B epoch + 40 B home + 帧开销) | ≈ 60 KB |
| 空闲单轮（`unchanged`） | 20 B | 20 B |
| 每节点空闲 | 4 轮/s × 20 B | **80 B/s** |
| 集群空闲出口 | 10 × 80 B/s | **800 B/s** |
| 集群空闲每日 | 800 B/s × 86400 | **69 MB** |
| 集群空闲每年 | | **25 GB** |
| 对比：无校验位 250ms | 10 × 240 KB/s | 2.4 MB/s（207 GB/天） |
| Meta 扫描开销 | 500 条 × 200 µs / 250ms | 0.08% CPU（单 Meta） |

`interval_ms = 250` 的依据是延迟需求（由 §7.2 的重试循环覆盖 ≤250ms 的
镜像延迟），**不是**成本妥协——成本在 §4.3 的对比表里已经解决了。
不得为提高间隔而牺牲发布延迟：两者不是同一个权衡。

**可选定期全量**：若需要清理 ghost，可加 `full_rescan_interval_ms`
（默认 60000），到期时把 `cached_revision` 置 0 强制全量。首版不实现。

## 6. 部署合同

部署方负责（AFS 只校验，见 H3）：

1. **数据卷**：每节点本地盘挂载 OwnerFs 数据根；
2. **统一挂载布局**：每节点对全部成员（含自己）：
   - `/imports/<self>/users` = bind mount 自身数据根（fstab `bind`）；
   - `/imports/<M>/users` = NFS4 挂载，参数见 §6.1；
   - 导出为**静态整树导出**，fsid 基于数据盘 UUID（nfsd 默认），
     保证重启后文件句柄稳定；
3. **节点身份固定**：IP 与导出配置在节点生命周期内不变；
4. **`/public` tmpfs**：部署方提供并挂载，**必须不含存量真实目录**（§6.4）；
5. **成员表变化**：由部署工具重生成各节点 fstab 并 `mount -a`
   （N² 挂载表是本设计的已知运维成本，见 §13）。

### 6.1 NFS 挂载参数（硬要求，不是建议）

| 要求 | 理由 | 参数 |
|---|---|---|
| 失败可中断 | `hard` 会让进程进入不可 kill 的 D 状态，**同时使 `umount` 失效**——故障时既不能有界失败也无法卸载修复，只能重启节点 | `softerr`（不用 `hard`，也不用 `soft`） |
| 失败有界 | 单次 I/O 必须在可预期上限内以错误结束 | `timeo` / `retrans` |
| 版本与识别一致 | 挂载诊断按 `fs_type` 判定，需固定版本 | `nfsvers=4.2` |
| 吞吐可扩展 | 单连接是吞吐上限 | `nconnect≥4` |
| 陈旧窗口可声明 | 换代可见性滞后需要可解释，不能靠猜 | `actimeo=60` |
| 重启后可恢复 | 节点重启后不依赖人工挂载 | `bg` 或 `x-systemd.automount` |

**硬规则 H16**：`softerr` 是硬要求。选择 `hard` 会使"故障时卸载修复"这条
恢复路径整体失效，与 §8 故障语义表冲突。

`actimeo=60` 的后果要显式声明：NFS 客户端目录属性缓存最长 60s，因此
**换代后新代可见性可能滞后至约 2×actimeo（120s）**。H11 宽限期的默认
120000ms 正是据此推出，见 §7.5。

### 6.2 挂载点权限

- `/imports/<M>/users` 挂载点 **0555**：只约束 host 命名空间视图，防止
  host 进程绕过 NFS 直接写入。容器经 `mount --bind` 进入后按目标目录自身
  权限读写，两者不矛盾。
- `/public` 为 tmpfs，**不暴露给容器**；若必须暴露，以 `ro` 挂载。
  否则容器进程可在 tmpfs 上伪造/删除 symlink，污染名字面。

### 6.3 AFS 的角色边界

AFS **永不执行 `mount`/`umount`**（H3），只解析 `/proc/self/mountinfo` 做
只读诊断（§5.4）。挂载表是部署方的责任与状态，不是 AFS 的。

### 6.4 存量 `/home/agentos/users` 迁移（启用前置条件）

`/home/agentos/users/<workspace>` 是**现役对外契约**
（`gateway/README.md:766`、`platform/sdk/environment/python/README.md:480`），
目录里现在装着真实用户数据。而本设计的 `/public` 必须是 tmpfs。

**同一路径不能既是 tmpfs 名字面又保留真实目录。** 因此：

**硬规则 H17**：启用 outlet 前必须完成存量迁移——把
`/home/agentos/users/*` 的真实数据迁到各 home 的
`/imports/<home>/users/`，再将该路径改为 tmpfs。迁移完成前不得启用镜像
worker，否则会因 tmpfs 覆盖而丢失用户数据。

迁移时点与灰度方式见 R6（开放问题）。

### 6.5 NFS 导出绕过 AFS 授权（重要边界）

**事实**：B 侧对 `/public/<name>` 的读写不经过 `afs-node`。数据路径是

```
B 容器 → 内核 NFS client → A 的内核 nfsd → A 的本地 ext4
```

`afs-node` 进程全程不在数据路径上。因此 AFS 的授权校验
（`validate_peer_root_access` → `Meta.ValidateRootAccess`）**完全不参与**，
B 侧也不需要、不会获得任何 `RootGrant`。

**代码级依据**（供审查复核）：

- `validate_peer_root_access`（`root.rs:715`）的唯一入口是 P2P gRPC handler
  （`node/rpc/data.rs`）经 `OwnerFs::{lookup,getattr,read,write}(peer_node_id,
  access: &PresentedRootAccess, ...)` 调用；所有调用点都是 `peer_*` 函数。
- 该函数的第一个参数是 `PresentedRootAccess`，含 `fencing_token`、
  `access_generation`、`holder_session_id` 等字段。NFS 协议（filehandle +
  AUTH_SYS/Kerberos）**没有位置携带该结构**。
- AFS 不实现 nfsd（`afs/src/` 无 `nfsd`/`exportfs`/`mountd` 代码），也不执行
  非 bind 的挂载（`bind_mount.rs` 的 `libc::mount` 第三参数恒为 `null`，
  只用 `MS_BIND`/`MS_PRIVATE`）。

**推论一（授权面不变）**：B 在 AFS 的授权模型里始终是"未被 grant 的 peer"。
`access_generation`、`fencing_token`、`revoke` 语义均不受影响。B 能读到数据
的唯一原因是部署方在 A 的 `/etc/exports` 中导出了该目录——这是**基础设施层
的权限决定，不是 AFS 的授权决定**。

**推论二（导出范围即可见性边界）**：既然 NFS 绕过 AFS 授权，那么
`/etc/exports` 的导出粒度就是**实际生效的可见性边界**，无法由 AFS 收紧。

**硬规则 H18**：导出单位必须是整个 `users_root`（整树导出），
不得按 workspace 逐个导出。理由与后果：

- 当前设计不存在"按节点过滤 workspace 可见性"的语义——名字面就是
  Meta Active 集合，所有节点看到同一份（§3.1、§4.4）。整树导出与此**自洽**。
- 因此 **H18 的前提是"不做 per-node 可见性"**。若将来需要"某 workspace 只对
  某些节点可见"，该需求**无法在 NFS 层实现**，必须在 AFS 的 P2P 路径上做，
  或改用能表达 per-client 可见性的传输——这会推翻本规范的数据面选择。
  届时按新需求另立规范，不在本设计上加装。

需部署方确认的两点：

1. `/etc/exports` 的客户端范围应等于集群成员集合（默认 `*(ro,sync,
   no_subtree_check)` 或按成员 IP 列举，二选一由运维决定）；
2. **不得对非成员主机导出**。NFS 层没有 workspace 级授权，导出范围一旦放宽，
   等同把全部 workspace 数据面暴露给该主机。

## 7. 消费方契约

### 7.1 挂载方式

- 容器创建时 bind `/public/<name>`（或 platform 持有 home 时直接 bind
  `/imports/<home>/users/<data_dir_text>`）；
- 内核解析时经本节点挂载表自动选择本地 bind 或 NFS 通道；
- **禁止将 `/public` 整棵目录 bind 进容器**：symlink target 是宿主机路径
  `/imports/...`，容器内不存在该布局，整棵挂入会导致全部 symlink 断链。
  只允许逐条 bind `/public/<name>`（runc 在宿主机解析 symlink，语义正确）。

### 7.2 启动前门禁（一次性检查，替代 v1 持续门禁）

**最小门禁（必选，1 条命令，永不挂死）**：

```
lstat /public/<name> 成功      # test -L 或等价，摸 tmpfs，不跟随 symlink
（带 ~3s 重试循环，覆盖 ≤ interval 的镜像同步延迟）
```

防住：创建竞态（未 Active/镜像未同步）、过期引用（已下线）、名字拼错。
注意这三种情况即使不设防也只是容器创建报干净 ENOENT——lstat 的价值是
给消费方一个**可干净重试的信号**，而非阻止事故。

防不住：home 死亡、挂载漂移——前者由 fail-open 语义兜底（§7.3），
后者由 §5.4 mountinfo 告警 + bind 失败双重兜底，消费方无需额外命令。

**可选增强（+1 条命令，仅当调度方需要在启动前识别死 home 时）**：

```
timeout -s KILL 3 ls /public/<name>/ >/dev/null 2>&1
```

穿透 symlink 触发一次 NFS READDIR，把"home 已死"挡在容器创建流程之外。

`softerr` 挂载下这条从**必需**降为**可选**：`softerr` 保证 I/O 在
`timeo × retrans` 内以错误返回，宿主机上 bind 源路径解析会**失败**而不是
永久阻塞（D 状态）。因此即便不做此检查，最坏结果是容器创建报一个有时限的
错误，调度器可重试。

保留它的价值仅在于**提前**失败：不检查则失败发生在 runc 挂载阶段，错误
更难归因；检查则错误出现在 provisioning 的显式判定点。

`-s KILL` 的理由仍然成立：即使 `softerr`，某些路径仍可能进入较长等待，
`SIGTERM` 不保证杀得掉，必须用致命信号。

**语义边界**：lstat 只证明"Meta Active 且镜像已同步"，不证明"数据此刻
可达"；检查通过与容器实际挂载之间存在固有的 TOCTOU 窗口，由 fail-open
兜底（挂载失败 → 容器启动失败，属预期行为）。

### 7.3 运行中语义（fail-open，已接受的代价）

- home 死亡：**名字不消失**；已挂容器的 IO 在 `timeo × retrans` 内有界失败
  （`softerr`）而非永久挂死；新挂载尝试失败，消费方可重试；
- 下线：镜像摘 symlink；已挂容器继续（钉住的 inode），新挂载 ENOENT；
- 换代：见下面 §7.4。

### 7.4 换代为什么不采用 home 侧稳定名

另一种可行设计是在 home 侧维护 `users/<name>` → `users/<name>-e<epoch>`
稳定 symlink，让 `/public` 指向稳定名、换代对 `/public` 透明。本规范
**不采用**，理由是它在 NFS 上引入了更难处理的问题：

| 维度 | target 带 epoch（本规范 D5） | home 侧稳定名 |
|---|---|---|
| 换代对 `/public` 的影响 | 一次原子 rename（文本变化） | 无（稳定名挡住） |
| 换代窗口 | ≤ interval + 一次 RPC（250ms 级） | 理论上零 |
| **同名重建歧义** | **无**——epoch 直可见，新旧代是不同的路径 | **有**——同名删除后重建，NFS 客户端在 `2×actimeo`（120s）内可能解析到**上一代** |
| 换代窗口的兜底 | H11 宽限期（定时器，简单） | 需要 `reclaim_probe` 判活 + 旧代保留窗口 ≥ stale window（复杂） |
| 陈旧写风险 | 无（窗口内只是拿到旧代，不是错代） | **有**——窗口内的写会落进上一代目录，并随旧代回收而丢失 |
| 消费方心智 | `-e<epoch>` 可见，便于排障与归因 | 路径不含代次，陈旧观测难以归因 |

关键判断：**"换代多等 250ms"与"同名重建后 120s 内可能读写错代"不是同一
量级的代价**。前者是延迟，后者是正确性。而且 `actimeo=60` 是为了吞吐必须
保留的参数，不能靠 `actimeo=0` 消除这个窗口（元数据开销会放大一个数量级）。

因此本规范选择：**换代靠 H11 宽限期兜住 250ms 窗口，同名重建靠 epoch
直可见保证无歧义。**

### 7.5 换代窗口的精确表述

要区分两种"滞后"，它们成因不同、后果也不同：

| 滞后 | 成因 | 量级 | 后果 |
|---|---|---|---|
| **镜像传播滞后** | 各节点 symlink 尚未更新到新 epoch | ≤ interval + 一次 RPC（250ms 级） | 新容器取得**旧代** target |
| **NFS 目录缓存滞后** | 客户端缓存了目录属性 | ≤ `actimeo`（60s） | 新代目录的可见性延后 |

两者叠加，最坏情况是新容器在换代后约 120s 内仍拿到旧代。由 H11 宽限期
（默认 120s）保证旧代目录此期间仍可解析，因此行为是"拿到旧一代数据"，
**不是断链，也不是错代**——旧代是真实存在过的、数据完整的一代。

**约束（H11 补充）**：`generation_grace_ms` 取
`≥ max(interval_ms × 10, actimeo × 2)`。默认
`max(250×10, 60000×2) = 120000ms`（2 分钟）。
调大 `actimeo` 或 `interval_ms` 时必须按此式重新核对（TC18 覆盖）。

## 8. 故障语义表

| 故障 | 现象 | 恢复 | 人工操作 |
|---|---|---|---|
| home 节点宕机 | 远端消费在 `timeo×retrans` 内有界失败（`softerr`）；名字保留 | home 回归后内核 NFS 自动续传 | 无 |
| home 节点重启 | 同上；本机 /public 由首轮 diff 重建 | 同上 | 无（挂载表需 fstab 持久化，§6） |
| 消费节点重启 | 挂载表清空 | fstab 挂回 + 镜像首轮重建 | 无 |
| Meta 失联 | 镜像本轮跳过，现有 symlink 保持（H7） | 重连后下轮收敛 | 无 |
| 挂载表漂移（部署方漏配/错配） | 告警 `path_missing`/`path_drift`；访问失败 | 部署方修复 | 是 |
| activate 中途崩溃 | 物理目录或 Active 记录存在而另一者缺失 | activate 幂等重试（复用 master 生命周期）+ 镜像下轮对齐 | 无 |
| 换代窗口内消费（symlink 未更新） | 新容器取得旧代 target（H11 宽限期内可解析） | 宽限期后旧代回收；数据为旧代快照，属预期 | 无 |
| `softerr` 触发 EIO | 容器内 I/O 返回错误而非挂死 | 上层重试；home 恢复后成功 | 无 |
| `root_revision` 误用全局 revision | 校验位恒失配，退化为每轮全量 | 违反 H13，需修复 | 是（代码缺陷） |

## 9. 配置面

TOML 三路径 all-or-none（沿用现有校验模式，任一缺失即配置拒绝）：

```toml
[fs]
outlet_users_root  = "/mnt/adx/users"     # 本机数据根（bind 源，仅用于校验与文档一致性）
outlet_import_root = "/mnt/adx/imports"   # 统一布局挂载点根
outlet_public_root = "/mnt/adx/public"    # 表现层 tmpfs

[outlet_mirror]
interval_ms = 250                    # 可选，默认 250；仅影响发布延迟，不影响数据面
generation_grace_ms = 120000         # 可选，默认 120000；H11 旧代目录宽限期
                                     # 约束：≥ max(interval_ms×10, actimeo×2)
# full_rescan_interval_ms = 60000    # 可选，默认不启用；定期强制全量以清理 ghost
```

- **H12**：节点侧 outlet 组件不读取 `outlet_users_root` 内容（数据路径
  唯一入口是挂载表）；该路径仅参与启动校验。
- master 现有 `experimental_ownerfs_workspace_bind`（本机 bind 加速）与本
  特性正交，互不依赖。

## 10. 与 master 现有代码的锚点

| 锚点 | 位置（master） | 用途 |
|---|---|---|
| `root_data_dir(name, epoch)` | `afs/src/node/vfs/ownerfs/root.rs`（`root-<hex>-e<epoch>`） | 路径公式同源命名（H1） |
| `ReserveRootInput.preferred_home_node_id` | `afs/src/meta/owner_roots.rs` | home 在 Reserve 时定死 |
| `NodeSession.is_live_at_unix_ms` | 同上 | 节点心跳权威（已有，本规范不新增消费者） |
| `ActivateRoot/DeactivateRoot` | 同上 | Active 集变更来源，无需修改 |
| 本机 `LocalRootSnapshot`/启动对账 | `root.rs` | activate 崩溃中间态恢复，已有机制 |
| mountinfo 解析 | master 无 outlet 侧实现，feat 分支 `parse_exact_mountinfo_line` 可移植 | §5.4 诊断 |

**master 上不存在因此需要新建的**：节点侧 `outlet/` 模块（mirror + mountinfo
诊断）、Meta 两个只读 RPC、配置节、部署脚本示例。master 上不存在因此**不需要
删除**的：无（greenfield）。

## 11. 任务分解

| # | 任务 | 内容 | 验收 |
|---|---|---|---|
| T1 | Meta 只读 RPC + 校验位 | `ListActiveOutletRoots`（proto + owner_roots 实现 + 现有会话校验）+ `State.root_revision` + `MetaRead::ActiveRoots` | 单元测试：Active 集投影、非 live 会话拒绝、**reservation 不泄漏**、**deactivate 后消失**、**稳定排序**、`root_revision` 仅随 Root 系实体递增（DFS 事件不改变它） |
| T2 | mountinfo 诊断 | 解析 + 三类告警（缺失/类型漂移/tmpfs 校验） | 纯函数测试（构造 mountinfo 文本） |
| T3 | 镜像 worker | diff 纯函数 + 收敛循环 + `unchanged` 短路（H14）+ 原子放置 + ghost 隔离 + staging 清扫 | 纯函数表驱动测试（§12 TC1-TC7 的离线版）+ `unchanged` 分支测试 |
| T4 | 配置与接线 | 三路径 all-or-none 校验、`node.rs` 启动接线、`[outlet_mirror]`、H11 取值校验 | 配置校验测试、启动 fail-fast 测试、grace 下限校验测试 |
| T5 | 部署资产 | 两节点部署脚本（fstab 示例含 `softerr` 等参数、nfsd 导出示例含**导出 IP 与成员表一致性检查**、挂载点 0555）、`docs/deployment/afs.md`、架构文档、README、**存量迁移步骤（H17）** | `python3 build/docs/check.py`、`git diff --check`、导出范围核对脚本（TC20） |
| T6 | 双节点运行验证 | §12 全部用例 | 验证报告（明确"非多 VM/HA 证明"的边界声明） |

提交按 conventional commits + 单条 Signed-off-by；T1-T4 一组（行为），
T5-T6 一组（资产与证据），不混入无关变更。

## 12. 验证用例

| # | 用例 | 期望 |
|---|---|---|
| TC1 | A 创建 workspace → B 上 `/public/<name>` 出现 | ≤ interval + 一次 RPC 延迟 |
| TC2 | B 容器 bind `/public/<name>` 并读写 | 数据与 A 一致（close-to-open） |
| TC3 | A 本机容器 bind 同名路径 | 走 bind，无网络（性能对照） |
| TC4 | 同名换代（epoch 7→8） | 运行中容器仍读 e7；新容器读 e8；symlink 文本变化 |
| TC5 | Deactivate | symlink ≤ 一轮摘除；已挂容器继续 |
| TC6 | A 宕机 | B：名字保留、IO 挂死；A 回归后自动续传 |
| TC7 | A 重启 | 挂载表自动挂回；`/public` ≤ 一轮重建；B 自动续传 |
| TC8 | Meta 重启 | 镜像跳过轮次后收敛，symlink 无闪断 |
| TC9 | 部署漂移（移除一条 import 挂载） | `path_missing` 告警；AFS 不补挂 |
| TC10 | ghost 目录占位 | 隔离进 `.quarantine/`，下轮发布成功 |
| TC11 | 非法名 / 保留名 | 拒绝，public_root 无残留 |
| TC12 | 启动门禁（lstat） | Active 发布后 lstat 重试内通过；未发布/已下线/拼错名 → 失败且即时返回（不穿透、不挂） |
| TC12b | 启动门禁（可选穿透检查） | A 宕机时 `timeout -s KILL 3 ls /public/<name>/` 3s 超时退出；A 活着时毫秒级通过。注意：`softerr` 下不检查也会在 `timeo×retrans` 内有界失败，本用例验证的是"失败出现在 provisioning 判定点而非 runc 挂载阶段" |
| TC13 | 换代窗口（H11） | epoch 7→8 后、B 的 symlink 更新前启动新容器：bind 成功且读到 e7 数据（宽限期内）；宽限期后旧代目录被回收 |
| TC14 | `/public` 整棵挂入容器（违约） | 容器内 symlink 断链；逐条 bind 正常——文档禁令与现象一致 |
| TC15 | `root_revision` 作用域（H13） | 只创建 DFS entity（不碰 Root）：`root_revision` 不变，带校验位的请求返回 `unchanged=true` |
| TC16 | `unchanged` 短路（H14） | 构造 `unchanged=true` 但 `/public` 有 symlink：**不得**被清除；`/public` 内容逐字节不变 |
| TC17 | 服务端 `unchanged` 判定 | 首次请求（`known=0`）返回全量 + `root_revision=N`；再请求 `known=N` 返回 `unchanged`；Root 变更后 `known=N` 返回全量 + `root_revision>N` |
| TC18 | H11 取值约束 | `interval_ms` 或 `actimeo` 调大至违反 `grace ≥ max(interval×10, actimeo×2)` 时，启动失败并指明约束 |
| TC19 | NFS 路径不经 AFS 授权（§6.5) | 在 B 上对 `/public/<name>` 做 IO：A 的 `afs-node` 日志无 `validate_root_access` / `ValidateRootAccess` 调用；B 侧无任何 `RootGrant` |
| TC20 | 导出范围核对（H18） | 部署检查脚本：`/etc/exports` 的客户端集合与成员表一致；加入非成员 IP 时脚本报错 |

## 13. 风险与开放问题

- **R1 fail-open 的运维语义**：故障排查依赖"名字在但数据摸不到"的可识别性。
  缓解：§7.3 写入文档；告警体系覆盖 path_missing/drift。**运维约定**：
  `path_missing`/`path_drift` 告警的响应优先级应高于用户报障——悬空 symlink
  的消费方失败（bind ENOENT）总是晚于告警出现，先看告警能省一轮排障。
- **R2 N² 挂载表（已定案）**：设计输入为集群 ≤ 10 节点，全网格 ≤ 90 条
  静态挂载，部署工具重生成 fstab + `mount -a` 的成本可接受。**明确不引入
  autofs 懒挂载变体**；若未来集群规模超出 10 节点，重启此评估（fail-open
  下 autofs 的门禁/缓存缺陷不再适用，届时仍是合法选项）。
- **R3 NFS 目录缓存**：换代可见性滞后按 `actimeo=60` 推导为 ≤60s，已并入
  H11 取值约束（§7.5）。T6 需实测确认该推导，若实测滞后显著超过 60s，
  说明挂载参数未生效，属部署缺陷而非设计缺陷。
- **R4 TOCTOU**：启动门禁通过后、挂载完成前 home 死亡的窗口。已接受
  （fail-open），文档明示。`softerr` 下该窗口的最坏结果是容器创建报有时限
  的错误，不是无限挂死。
- **R5 与 v1 的关系**：v1 的 fencing、grant/revocation 生命周期仍由 master
  现有 root 生命周期承担，本规范不覆盖、不削弱；如未来需要"健康时立即摘名"
  的强语义，另立规范，不在本设计上加装。
- **R6 存量 `/home/agentos/users` 迁移（阻断性）**：H17 是启用前置条件。
  迁移时点、灰度方式、回滚方案未定。风险在于该路径是现役契约，迁移期间
  新旧两种形态不能共存（同一路径不能既是 tmpfs 又保留真实目录）。
  **在 R6 关闭前，本特性不得在生产启用。**
- **R7 `root_revision` 的实现纪律（H13）**：这是唯一"写错也能跑、只是
  退化"的地方——误用全局 `revision` 不会报错，只会让校验位恒失配、
  流量退化为 2.4 MB/s。缓解：TC15 用例 + 代码注释指向本节。审查时须
  特别核对递增条件的作用域。
- **R8 NFS 导出即可见性边界（H18，接受）**：NFS 数据面绕过 AFS 授权
  （§6.5），因此 AFS 无法对 B 侧访问做 workspace 级授权，
  `access_generation` / `fencing_token` / `revoke` 对 NFS 路径无效。
  当前设计中这**不构成矛盾**（不存在 per-node 可见性语义）。真正的风险是
  **导出范围被放宽**：一旦 `/etc/exports` 包含非成员主机，等同把所有
  workspace 数据暴露给它，且 AFS 侧无从察觉（数据路径上无 AFS 组件）。
  缓解：H18 写入部署合同；部署检查脚本核对导出 IP 列表与成员表一致
  （T5 交付物）。此项**无法在代码层防御**，属部署合同风险。
  若未来需要 per-node 可见性或 workspace 级撤权即时生效，必须放弃 NFS
  数据面，改走 AFS P2P 路径——那会重新引入本规范刻意舍弃的探针与门禁成本。

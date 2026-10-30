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
  可接受，见 R2 定案）。
- 文中"硬规则"为验收合同，违反即实现错误；"建议"为实现自由度。

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
| D4 | 镜像 worker 永不 stat target | 远端 target 在 hard mount 上，stat 可能永久阻塞 | target 存在性前置校验（由消费方启动门禁承担） |
| D5 | 镜像 target 直接携带代次（`-e<epoch>`），不设 home 侧稳定名 | epoch 是 Meta Active 记录的一部分，换代 = 镜像文本变化 = 一次原子 rename | home 侧稳定名维护、换代通知 |

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
root 记录**不新增字段**。新增一个只读 RPC：

### 4.1 `ListActiveOutletRoots() -> { roots: [{name, home_node_id, epoch}] }`

- 返回当前 Active 状态的 root 集。无游标、无事件、无 Compacted——全量快照
  即恢复机制。
- 服务端要求调用方持有当前 live 会话（复用现有姿态）。
- 实现建议：直接读 owner_roots 的 Active 视图，不加缓存层。

**不新增** `ListLiveNodes`：启动门禁（§7.2）最终采用纯 lstat（+可选穿透
检查），不依赖 Meta 心跳查询；节点死活的判断由穿透检查（数据通路实测）
或心跳的已有消费者承担，Meta 不为门禁新增接口。

## 5. 节点组件：镜像 worker

### 5.1 收敛循环（核心逻辑，纯函数可测）

```
每 outlet_mirror_interval_ms（默认 250ms）：
  active  = Meta.ListActiveOutletRoots()        # 期望态
  present = read_dir(public_root) 过滤保留名     # 现状态
  对 active - present 的每个 root：
      want = 路径公式(root)                     # §3.2
      staging 写 symlink → rename 原子亮出      # H5
  对 present - active 的每个名字：
      unlink（幂等容忍 ENOENT）
  对 public_root 中非 symlink 实体（消费方违约 ghost）：
      rename 进 .quarantine/<截断名>-<随机hex>   # 证据保留，最小实现
```

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
- **H12**：**换代宽限期**——home 侧废弃旧代物理目录（`-e<epoch>` 旧值）前
  必须保留其存活至少 `outlet_generation_grace_ms`（默认 60000ms）。
  原因：镜像 target 直接携带代次（D5），各节点 symlink 的更新是最终一致的
  （≤ interval + 一次 RPC）；宽限期内旧代目录必须可解析，否则窗口内新启动的
  容器会 bind 到已消失的旧代（ENOENT/挂死）。宽限期过后旧代目录的删除归
  OwnerFs 生命周期常规回收，不属本规范。

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

## 6. 部署合同

部署方负责（AFS 只校验，见 H3）：

1. **数据卷**：每节点本地盘挂载 OwnerFs 数据根；
2. **统一挂载布局**：每节点对全部成员（含自己）：
   - `/imports/<self>/users` = bind mount 自身数据根（fstab `bind`）；
   - `/imports/<M>/users` = NFS4 挂载（`hard,bg`，或
     `x-systemd.automount` 懒挂载）；导出为**静态整树导出**，fsid 基于数据盘
     UUID（nfsd 默认），保证重启后文件句柄稳定；
3. **节点身份固定**：IP 与导出配置在节点生命周期内不变；
4. **`/public` tmpfs**：部署方提供并挂载；
5. **成员表变化**：由部署工具重生成各节点 fstab 并 `mount -a`
   （N² 挂载表是本设计的已知运维成本，见 §13）。

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

穿透 symlink 触发一次 NFS READDIR。**必须 `-s KILL`**（Linux 2.6.25+
NFS 阻塞操作可被致命信号中断，默认 SIGTERM 不保证杀得掉）。其唯一
不可替代的价值：home 死亡时，无门禁直接启动容器，宿主机上 bind 源
路径解析会阻塞 runc（D 状态、"挂住"而非"失败"，调度器无法重试）；
穿透检查把这种挂死挡在容器创建流程之外。

**语义边界**：lstat 只证明"Meta Active 且镜像已同步"，不证明"数据此刻
可达"；检查通过与容器实际挂载之间存在固有的 TOCTOU 窗口，由 fail-open
兜底（挂载失败 → 容器启动失败，属预期行为）。

### 7.3 运行中语义（fail-open，已接受的代价）

- home 死亡：名字不消失；已挂容器的 IO 挂死（hard mount）直至 home 回归；
  新挂载尝试失败或挂死；
- 换代（同名新一代激活）：home 侧新物理目录就绪后，镜像的 target 文本随
  epoch 变化并在下一轮原子替换 symlink。**已运行容器 bind 钉住旧代 inode**，
  继续安全使用；新启动容器取得新一代。NFS 客户端目录缓存可能使新代可见性
  滞后（close-to-open 后收敛），不做更强承诺。**窗口承诺**：各节点 symlink
  未更新的间隙内（≤ interval + 一次 RPC），新容器可能取得旧代 target——
  由 H12 宽限期保证旧代目录此时仍可解析，行为是"拿到旧一代数据"而非
  "拿到一个断链"；
- 下线：镜像摘 symlink；已挂容器继续（钉住的 inode），新挂载 ENOENT。

## 8. 故障语义表

| 故障 | 现象 | 恢复 | 人工操作 |
|---|---|---|---|
| home 节点宕机 | 远端消费挂死/ESTALE；名字保留 | A 回归后内核 NFS 自动续传 | 无 |
| home 节点重启 | 同上；本机 /public 由首轮 diff 重建 | 同上 | 无（挂载表需 fstab 持久化，§6） |
| 消费节点重启 | 挂载表清空 | fstab 挂回 + 镜像首轮重建 | 无 |
| Meta 失联 | 镜像本轮跳过，现有 symlink 保持 | 重连后下轮收敛 | 无 |
| 挂载表漂移（部署方漏配/错配） | 告警 `path_missing`/`path_drift`；访问失败 | 部署方修复 | 是 |
| activate 中途崩溃 | 物理目录或 Active 记录存在而另一者缺失 | activate 幂等重试（复用 master 生命周期）+ 镜像下轮对齐 | 无 |
| 换代窗口内消费（symlink 未更新） | 新容器取得旧代 target（H12 宽限期内可解析） | 宽限期后旧代回收；数据为旧代快照，属预期 | 无 |

## 9. 配置面

TOML 三路径 all-or-none（沿用现有校验模式，任一缺失即配置拒绝）：

```toml
[fs]
outlet_users_root  = "/mnt/adx/users"     # 本机数据根（bind 源，仅用于校验与文档一致性）
outlet_import_root = "/mnt/adx/imports"   # 统一布局挂载点根
outlet_public_root = "/mnt/adx/public"    # 表现层 tmpfs

[outlet_mirror]
interval_ms = 250                    # 可选，默认 250；仅影响发布延迟，不影响数据面
generation_grace_ms = 60000          # 可选，默认 60000；H12 旧代目录宽限期
```

- **H11**：节点侧 outlet 组件不读取 `outlet_users_root` 内容（数据路径
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
| T1 | Meta 只读 RPC | `ListActiveOutletRoots`（proto + owner_roots 实现 + 现有会话校验） | 单元测试：Active 集投影、非 live 会话拒绝 |
| T2 | mountinfo 诊断 | 解析 + 三类告警（缺失/类型漂移/tmpfs 校验） | 纯函数测试（构造 mountinfo 文本） |
| T3 | 镜像 worker | diff 纯函数 + 收敛循环 + 原子放置 + ghost 隔离 + staging 清扫 | 纯函数表驱动测试（§12 TC1-TC7 的离线版） |
| T4 | 配置与接线 | 三路径 all-or-none 校验、`node.rs` 启动接线、`[outlet_mirror]` | 配置校验测试、启动 fail-fast 测试 |
| T5 | 部署资产 | 两节点部署脚本（fstab 示例、nfsd 导出示例）、`docs/deployment/afs.md`、架构文档、README | `python3 build/docs/check.py`、`git diff --check` |
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
| TC12b | 启动门禁（可选穿透检查） | A 宕机时 `timeout -s KILL 3 ls /public/<name>/` 3s 超时退出（SIGKILL 可中断 hard mount 等待）；A 活着时毫秒级通过 |
| TC13 | 换代窗口（H12） | epoch 7→8 后、B 的 symlink 更新前启动新容器：bind 成功且读到 e7 数据（宽限期内）；宽限期后旧代目录被回收 |
| TC14 | `/public` 整棵挂入容器（违约） | 容器内 symlink 断链；逐条 bind 正常——文档禁令与现象一致 |

## 13. 风险与开放问题

- **R1 fail-open 的运维语义**：故障排查依赖"名字在但数据摸不到"的可识别性。
  缓解：§7.3 写入文档；告警体系覆盖 path_missing/drift。**运维约定**：
  `path_missing`/`path_drift` 告警的响应优先级应高于用户报障——悬空 symlink
  的消费方失败（bind ENOENT）总是晚于告警出现，先看告警能省一轮排障。
- **R2 N² 挂载表（已定案）**：设计输入为集群 ≤ 10 节点，全网格 ≤ 90 条
  静态挂载，部署工具重生成 fstab + `mount -a` 的成本可接受。**明确不引入
  autofs 懒挂载变体**；若未来集群规模超出 10 节点，重启此评估（fail-open
  下 autofs 的门禁/缓存缺陷不再适用，届时仍是合法选项）。
- **R3 NFS 目录缓存**：换代的可见性滞后未定量。开放：T6 实测后决定是否在
  文档标注建议的 `actimeo` 上限。
- **R4 TOCTOU**：启动门禁通过后、挂载完成前 home 死亡的窗口。已接受
  （fail-open），文档明示。
- **R5 与 v1 的关系**：v1 的 fencing、grant/revocation 生命周期仍由 master
  现有 root 生命周期承担，本规范不覆盖、不削弱；如未来需要"健康时立即摘名"
  的强语义，另立规范，不在本设计上加装。

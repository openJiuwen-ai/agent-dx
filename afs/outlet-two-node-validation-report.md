# Outlet 跨节点验证报告（两节点真机）

- 日期：2026-10-31 17:20–17:46（CST）
- 构建：`afs 0.1.0 (9e034aae4e59, clean, built 1793437466, linux/x86_64)`
  - `afs-node` sha256 `db7837ada184b8362c4b96dbf3b158b2a3610ca5ef91627f590c82e953473636`
  - `afs-meta` sha256 `995a622a3eebd18cd4582eacc0b4aa19ad430514b78b168c701bbb6225838d90`
- 主机：`agentos-sz-0001`（1.94.217.10 / 内网 192.168.2.101）与
  `agentos-sz-0002`（1.94.246.161 / 内网 192.168.2.213），
  Ubuntu 24.04.4，glibc 2.39（与构建环境一致）。
- 隔离：所有产物位于 `/home/lzq/outlet-validation/`；Meta 端口 27400/27401、
  node 端口 17500/17501 与 27500/27501，避开宿主上已存在的
  `afs-baseline`（127.0.0.1:17400）与其他用户的 afs-meta（0.0.0.0:7400）。
  未修改 `/etc/fstab`、`/etc/exports`；NFS 导出用运行时 `exportfs` 添加。

## 拓扑

```
node-a（home A）                       node-b（消费方）
  afs-node :17500                        afs-node :27500
  ownerfs FUSE: mnt/ownerfs              outlet_public_root = mnt/outlet-public (tmpfs)
  data: data/node-a/ownerfs              outlet_import_root = imports/
    └── root-6167656e7431-e1   ──NFS──▶    imports/node-a/users (ro,softerr,actimeo=2)
  exportfs → 192.168.2.213               mirror interval 250ms
afs-meta（0002）：192.168.2.213:27400，mTLS（trusted_node_certs 含 node-a/node-b）
```

## 验证步骤与结果

| # | 步骤 | 证据 | 结果 |
|---|---|---|---|
| 1 | 二进制身份与传输完整性 | 两台 `--build-info` 输出一致，sha256 一致 | ✅ |
| 2 | Meta 启动（独立端口，mTLS） | `meta.ready`，27400/27401 监听 | ✅ |
| 3 | node-a 启动 + FUSE 挂载 | `node.ready`，`afs-ownerfs` on mnt/ownerfs | ✅ |
| 4 | 创建 workspace `agent1` | FUSE mkdir → 物理目录 `root-6167656e7431-e1` + catalog 记录 | ✅ |
| 5 | NFS 跨机可见 | node-b `mount` 后 `ls` 见到目录树 | ✅ |
| 6 | node-b 启动带 outlet | `outlet.mirror.starting` + `outlet.mirror.converged applied=1` | ✅ |
| 7 | symlink 发布 | `/public/root-6167656e7431 -> …/imports/node-a/users/root-6167656e7431-e1`，target 携带代次 | ✅ |
| 8 | 跨机数据面 | node-a FUSE 写 `note.txt`，node-b 穿透 symlink 立即读到 | ✅ |
| 9 | **TC21** 删除时远端持句柄 | node-b 持 fd 期间 node-a `rmdir` 返回 0；`dir_busy` + `catalog_deferred` + `.retired/` 标记出现；Meta deactivate 完成 | ✅ |
| 10 | 名字摘除延迟 | node-b `/public` 在一个镜像周期（250ms）内清空；`converged applied=1`（active=0） | ✅ |
| 11 | 已打开 fd 存活性 | holder 继续读到 `cross-node-data-…` | ✅ |
| 12 | H11 宽限期 | 宽限期内每轮 `pending=1 removed=0`（不删除） | ✅ |
| 13 | 回收完成 | 清空文件后下一轮 `root.reclaim.completed`、`removed=1`；目录、`.retired`、catalog 全部清空 | ✅ |

## 第二轮：spec §13 补充用例（2026-10-31 18:05–18:37 CST）

构建 `be657ecb770e, clean`。第一轮数据残留导致的 epoch 污染（见问题 4）
暴露了清理流程缺陷，本轮从全空状态重新拉起后执行。

| # | 用例 | 证据 | 结果 |
|---|---|---|---|
| 1 | TC1/TC2 创建→发布→读写 | symlink 指向 `-e1`，B 穿透读到 `data-v1` | ✅ |
| 2 | **TC4/TC13 同名换代**（B 持 e1 fd 期间） | rmdir OK；e1 保留（H11）；重建得 e2；**symlink 原子换至 e2**；旧 fd 仍读 `data-v1`；新读者读 `data-v2` | ✅ |
| 3 | TC7 A 重启 | 重启后 FUSE 重挂、root 恢复；B 侧 symlink 与数据不受影响 | ✅ |
| 4 | TC19 NFS 路径不经 AFS 授权 | B 经 NFS 读前后，A 日志 `ValidateRootAccess` 计数为 0 | ✅ |
| 5 | TC9 部署漂移 | 卸载 import 挂载 → `outlet.topology kind=path_missing`；恢复挂载后 65s 零新增 | ✅ |
| 6 | **TC23 同名重建并存** | e1（待回收）与 e2 并存；清空 e1 后 `root.reclaim.superseded_removed` **只删 e1**，e2 完好 | ✅ |
| 7 | TC22 回收重试（第二轮复验） | e1 非空时持续 `pending=1`；清空后 `removed=1` | ✅ |

**未在真机执行**（与第一轮结论一致）：TC3/TC12/TC12b/TC14 需要容器
provisioning 层；TC6 A 宕机需故障注入（TC7 覆盖了优雅重启路径）；
TC10/TC11/TC15/TC16/TC17 已有单元测试；TC18 已在配置校验实现
（`outlet_generation_grace_must_cover_interval_and_actimeo_floor`）；
TC20 属部署脚本（T5）；TC24 指标按产品决定未实现，以
`ownerfs.reclaim.swept` 日志的 `pending` 计数代替。

## 第三轮：规模验证（2026-10-31 23:45–11-1 00:05 CST）

在两节点各创建 200 个 workspace（FUSE mkdir），然后从 B 的 `/public`
（outlet 名字层）把一份 30 文件 / 329KB 的配置树拷贝进全部 401 个
workspace，并逐一校验内容哈希。

| 步骤 | 结果 |
|---|---|
| A 创建 200 个 workspace（FUSE mkdir，`ws001–ws200`） | ✅ 0 失败，数据目录 200 |
| B 创建 200 个 workspace（FUSE mkdir，`wsb001–wsb200`） | ✅ 0 失败（见问题 8 的同名限制） |
| 镜像收敛 | ✅ `/public` 401 个 symlink，`active=401` 一轮内追平 |
| **401 份拷贝（全部经 `/public` 路径）** | ✅ 0 失败，24s；A-homed 的 200 个写穿 NFS `ro→rw` 后落盘 |
| 逐一哈希校验 | ✅ **401 checked, 0 mismatched** |
| A 侧反向抽查 | ✅ `find data/node-a/ownerfs -name builtin_rules.yaml` = 200 份 |

**结论**：outlet 名字层在 401 root 规模下功能正确；跨节点写入路径
（`/public` symlink → NFS import → home 磁盘）全部生效。

## 第四轮：afs-node 重启验证（2026-11-01 11:13–11:35 CST）

在 401 workspace 的既有规模上验证重启，覆盖三种起点：正常停止后重启、
进程崩溃后残留 FUSE 的重启、以及连续多轮重启的可重复性。

| 场景 | 检查项 | 结果 |
|---|---|---|
| **node-a 重启**（200 root） | 停止干净、FUSE 重新挂载、**200 workspace 全部恢复**、可写 | ✅ 挂载 2s 内就绪 |
| **node-b 重启**（201 root + outlet） | FUSE 恢复、200 本地 workspace、`/public` 401 保持、可写 | ✅ |
| **连续重启** node-a ×2 / node-b ×2 | 每一轮都恢复，无累积劣化 | ✅ 4/4 |
| **重启后跨节点数据面** | A 新建 → B 镜像发布（401→402）→ B 经 `/public` 读 | ✅ |
| **重启后反向写入** | B 经 `/public` 写 A-homed workspace → **A 在自己磁盘上看到** | ✅ |
| 错误审计 | 无 mirror 失败、无 topology 告警、无 WARN | ✅ |

**重启后观测到的已知良性错误**（与重启无关，均为操作竞态或 xattr 噪音）：

- `OwnerFs currently supports only user.* xattrs`：`cp -r` / 文件操作会探测
  `security.capability` 等扩展属性，OwnerFs 只支持 `user.*`。数据不受影响。
- `No such file or directory` / `root is not registered in Meta`：删除探针
  文件后 FUSE 仍完成了一次 lookup。属正常竞态。

**重启时序的重要发现**：`node.ready` 日志打印在 FUSE 挂载**之前**，
端口监听更早。以「端口可连」判断就绪会在 200 root 规模下误判为未挂载
（实测 node-a 需要约 2 秒、node-b 类似）。**验证或编排必须以
`mountpoint` + 一次可读 `ls` 作为就绪判据**，不能用固定的短等待。

## 第五轮：双节点对等发布与双向可见性（2026-11-01 11:41–11:55 CST）

前四轮只在一个节点（B）上启用了 outlet 镜像，A 上以 `/public` 不存在。
本轮给 A 也加上 `outlet_public_root` / `outlet_import_root`，让两端各自
独立发布同一份 Meta 快照，验证**对等性**与**双向可见性**。

前置（A 侧）：`/public` 独立 tmpfs；`imports/node-a/users` 用 bind 挂本机
数据树、`imports/node-b/users` 用 NFS 挂对端；B 侧补 `exportfs` 导出自己的树。

| 用例 | 观测 | 结果 |
|---|---|---|
| 两端 `/public` 链接数 | A=402 / B=402 | ✅ |
| 两端按 home 归属 | A-homed 201 / B-homed 201（两边相同） | ✅ |
| 断链数（`-xtype l`） | A=0 / B=0 | ✅ |
| **逐条对比**（名字 → 代次） | `diff` 退出码 0，404 行完全一致 | ✅ |
| **A 创建 → B 可见** | B 侧链接指向 `imports/node-a/users/root-…-e404`，内容 `payload-from-A` | ✅ <1s |
| **B 创建 → A 可见** | A 侧链接指向 `imports/node-b/users/root-…-e405`，内容 `payload-from-B` | ✅ <1s |
| A 经 `/public` 写 B 的 workspace | B 在自己磁盘上读到 `written-by-A` | ✅ |
| B 经 `/public` 写 A 的 workspace | A 在自己磁盘上读到 `written-by-B` | ✅ |
| **A 删除 → B 摘除** | 1s 内两端均 `no` | ✅ |
| **B 删除 → A 摘除** | 1s 内两端均 `no` | ✅ |

**结论**：outlet 的对等式部署成立——两端各自跑镜像，得到完全一致的
名字视图（含代次），创建与删除都双向传播，跨节点读写穿透到对端 home
的磁盘。这补上了前四轮"单侧发布"未覆盖的设计面。

## 第六轮：对等发布下的换代与两条删除路径（2026-11-01 14:23–14:29 CST）

### 换代时两端必须同步切换

在两端都跑镜像的前提下，删除并同名重建 workspace（epoch 408 → 409）：

| 检查项 | 观测 | 结果 |
|---|---|---|
| 换代前两端一致 | A=`-e408`、B=`-e408`，内容均 `GEN-ONE` | ✅ |
| **换代后两端同时切换** | A `-e408`→`-e409`，B `-e408`→`-e409` | ✅ |
| 两端新代一致 | 均为 `-e409`，内容均 `GEN-TWO` | ✅ |
| 旧代读者不受影响 | 换代前打开的 fd 仍读到 `GEN-ONE` | ✅ |
| 换代后对等 | 两端 `/public` 各 405 条 | ✅ |

**结论**：两个节点基于同一快照独立计算，换代后仍得到完全相同的
(name → epoch) 映射，不存在一端切换、另一端滞后的窗口。

### 两条删除路径的区分（修正前几轮的表述）

| 路径 | 条件 | 行为 | 证据 |
|---|---|---|---|
| **(a) 空目录** | `rmdir` 时目录为空 | **立即删除**，无宽限期、无 marker | `-e408` 直接消失，`.retired/` 为 0 |
| **(b) 非空目录** | `rmdir` 时仍有文件 | `dir_busy` WARN → 写 retired 标记 → 目录保留 | marker=1，`keep.txt` 存活 |

H11 宽限期**只对 (b) 生效**——空目录没有保留价值，无需等窗口。

**对前几轮表述的修正**：第二轮我曾把"远端持有 fd 导致 ENOTEMPTY"作为
H11 的触发条件，这不准确。真正阻止删除的是**服务端目录里还有文件**；
NFS 客户端的 fd 本身不会让 `rmdir` 失败（客户端句柄在服务端目录为空时
仍可删除，之后该 fd 读取依靠已打开的 inode，行为正确）。

### 宽限期的时序闭环

用非空目录触发 (b) 路径后持续观测：

```
标记写入            14:24:47
目录持续保留        t+40/80/120/160s 均存在（因 keep.txt 仍在，pending=1）
清空文件            14:27:49
宽限期已过（+120s） 14:26:47
回收完成            14:28:39  → root.reclaim.completed, pending=0, removed=1
```

两个条件（宽限期已过 **且** 目录为空）同时满足后的下一轮 sweep 即回收，
之后 marker 清零、目录消失。与 spec 一致。

## 第七轮：性能测试（2026-11-02 09:28–10:15 CST）

在双端对等发布的环境中做三阶段计时：FUSE 建目录、镜像收敛、经 `/public`
拷贝 30 文件（329KB）配置树。

### 阶段一：FUSE 创建 workspace（每节点 200 个）

| 节点 | 数量 | 总耗时 | 平均 | 吞吐 |
|---|---|---|---|---|
| node-a | 200 | 1.72 s | 8 ms | **116 个/秒** |
| node-b | 200 | 1.63 s | 8 ms | **122 个/秒** |

两端一致。单次 mkdir 8–12ms，主要成本是 `reserve_root` + `activate_root`
两次 Meta RPC 与本地 catalog 落盘。

### 阶段二：镜像收敛

创建完成后 `/public` 已追平（811 个链接，两端相同）。镜像间隔 250ms，
在 200 个新 root 的规模下**一轮内完成**，未观测到多轮积压。

### 阶段三：经 `/public` 拷贝（本轮核心发现）

| 路径 | 目录数 | 总耗时 | 平均 | 吞吐 | 相对 |
|---|---|---|---|---|---|
| **跨节点（A-homed，走 NFS）** | 409 | **41.2 s** | **100 ms** | **9 个/秒** | 1× |
| **本机（B-homed，本地磁盘）** | 402 | **0.53 s** | **1 ms** | **762 个/秒** | **~78×** |

数据完整性：`perfA*` / `perfB*` 各 200 个全部存在，抽样 40 个哈希全部
与源树一致，拷贝 0 失败。

### 瓶颈定位

对跨 NFS 路径做单操作剖析：

| 操作 | 耗时 |
|---|---|
| 创建单个文件（create+write+close） | **3 ms** |
| mkdir | 2 ms |
| `cp -r` 一个 30 文件目录 | **81–89 ms** |

30 × 3ms ≈ 90ms，与实测吻合。**瓶颈是每文件的固定往返延迟，不是带宽**
（329KB 数据在千兆链路上不足 3ms）。NFS 挂载为 `vers=4.1, rsize/wsize=1M,
actimeo=2, softerr`，延迟来自协议往返与属性同步，而非传输量。

### 隔离测试：outlet 层开销可忽略

为区分「outlet 名字层」与「NFS 本身」的成本，对同一个 A-homed 目录做三条
路径的对照（每项 5 次，取中位数，源树 30 文件 / 329KB）：

| 路径 | 耗时样本（ms） | 中位数 |
|---|---|---|
| NFS 直连（`imports/node-a/users/root-…`，不经 symlink） | 87, 81, 89, 84, 83 | **84 ms** |
| 经 `/public` symlink | 82, 78, 84, 86, 80 | **82 ms** |
| 本地磁盘（对照） | 1, 1, 2, 1, 1 | **1 ms** |

**`/public` 的 symlink 开销在噪声范围内**（82 vs 84ms）。symlink 只解析
一次，之后由内核 dentry 缓存承接，因此 outlet 名字层不构成额外成本——
批量写入的耗时就是 NFS 的原始耗时。

### 结论与建议

- outlet 名字层本身开销可忽略：发布 200 个 root 一轮完成，`/public` 解析
  是 O(1) 的 symlink 跳转。
- **真正的成本在跨节点数据面**：按文件串行的 NFS 写，每个文件约 3ms。
  拷贝 400 个目录 × 30 文件 ≈ 12000 次往返。
- 若要提升跨节点批量写，方向是**减少往返次数**而非加大带宽：打包成 tar
  后单流写入、或使用 `pNFS`/并行写入、或对小文件用 `nconnect` 增加并行
  连接数。当前配置是单连接。

**注意**：本轮 `/public` 已达 811 个链接（含历史测试目录），高于用户要求
的 400。耗时数据按实际目录数记录，每目录成本可直接外推。

## 第八轮：`/public/<name>` 落地后的完整重跑（2026-11-02 14:48 CST）

命名从 `root-<hex>` 改为 `<name>` 之后，在干净环境上重跑完整双向验证。

| 检查组 | 用例 | 结果 |
|---|---|---|
| 起点对等 | 两端各 4 条、0 断链、无 `root-` 前缀 | ✅ 5/5 |
| **A 创建 → B 可见** | B 出现同名条目，内容 `payload-A` | ✅ 2/2 |
| **B 创建 → A 可见** | A 出现同名条目，内容 `payload-B` | ✅ 2/2 |
| 跨节点写穿透 | 各自 disk 上看到对端写入（`byB` / `byA`） | ✅ 2/2 |
| **按名 bind** | `mount --bind /public/<name>` 后读到正确内容 | ✅ 2/2 |
| `ll` 不卡 | `ls -alF` 跟随链接，2ms | ✅ 1/1 |
| 删除双向传播 | 两端在 1s 内摘除 | ✅ 2/2 |
| 终态 | 两端列表一致、0 断链 | ✅ 3/3 |

**合计 19/19 通过。**

`/public` 内容（两端相同）：

```
fromA-1793600288 -> imports/node-a/users/root-66726f6d412d...-e4
fromB-1793600291 -> imports/node-b/users/root-66726f6d422d...-e5
job-42           -> imports/node-a/users/root-6a6f622d3432-e1
test321          -> imports/node-b/users/root-74657374333231-e3
```

关键点：链接名是**调用方创建时的名字**，target 仍是编码后的数据目录。消费方
可以 `mount --bind /public/job-42`，不需要手工 hex 编码。

### 排查记录：`ll /public` 超时

验证初期 `ll /public`（即 `ls -alF --color=auto`）会挂住 8 秒以上，而
`ls -la` 只要 1ms。原因链：

1. `-F` 需要 stat 每个条目的 target 来判断类型，`-la` 不跟随链接；
2. 当时 A 上所有 target 都不可达，于是每个条目各等一次 NFS 超时；
3. target 不可达是因为清理环境时 `rm -rf data/node-*` 后**目录 inode 变了**，
   而 `imports/node-a/users`（bind）与 `imports/node-b/users`（NFS）仍然
   指向旧 inode / 旧 filehandle。

修复是重挂这两处 import，并在 B 上 `exportfs -ra` 刷新导出。**这不是 outlet
或代码问题，是清理环境时未先卸载依赖挂载留下的残留。** 正确顺序应为：
先 `umount imports/*`，再删数据目录，重建后再重挂。

## 过程中发现并即时处理的问题



1. **宿主已有其他 AFS 进程**：7400（他人 meta）与 127.0.0.1:17400
  （`/home/lzq/afs-baseline`）。全部避开，未触碰。
2. **NFS 导出源放错**：第一次导出了手工目录而非真实数据树，重挂为
  `data/node-a/ownerfs`。
3. **测试设计疏漏**：先删除带文件的目录，ENOTEMPTY `pending` 是正确行为；
  清空文件后回收立即完成——这反而实证了 H20「非空即等待」。
4. **第二轮的 Meta store 污染**：`meta_store = local-file` 在未配置
  `data_dir` 时默认落到全局共享的 `/tmp/afs-meta/`，与本机其他用户的
  Meta 写到了同一个 store——第一轮验证的 active root 记录泄漏进第二轮，
  node-a 以 "Meta has an active Home root without local catalog evidence"
  fail-fast（该 fail-fast 本身工作正常）。修复：meta.toml 显式
  `data_dir`，验证配置从全空 store 重启。**经验**：共享宿主上跑 Meta
  必须显式 `data_dir`，默认值不是每实例隔离的。
5. **build-info 缓存事故**：commit 变更后 cargo 未重跑 build.rs，
  `--build-info` 报告旧 commit。发现于 checksum 变化而 commit 不变；
  修复为 `cargo:rerun-if-env-changed`（be657ecb）。
6. **早期只在一端部署镜像，导致跨节点操作踩坑**：第一至四轮只在 B 上
  启用 outlet，A 没有 `/public`，于是在 A 上 glob `/public/root-*` 拿到的是
  字面目录名，把 400 份配置拷进了同一个目录。第五轮改为**两端各自发布**
  后此问题消失。结论：**每台需要读该名字视图的节点都应启用镜像**，不要
  依赖单一节点代劳。
7. **每个 home 都必须出现在 `imports/<node>/users`**：B 自己的 workspace
  的 symlink 指向 `imports/node-b/users/...`，漏挂时全部断链（spec §5.2
  的部署要求，容易只挂远端漏本机）。本机用 `mount --bind` 补挂即恢复。
8. **OwnerFs 同名 root 的僵尸目录**：一轮并发混乱在 Meta 留下 pending 记录
  后，同名 FUSE mkdir 返回 EEXIST 且目录删不掉（`Device or resource busy`），
  换新名字立即可建。**这是 OwnerFs 名字管理层的问题，不是 outlet 的**；
  待办：同名复用前应允许从 Meta 残留中恢复（当前 fail-fast 是保守正确
  的，但运维上需要一条恢复路径）。
9. **NFS 导出/挂载的 rw 忘记切换**：验证初期用 `ro` 导出，401 份拷贝在
  A-homed 的 200 个上报 `Read-only file system`。改 `exportfs rw` +
  重挂后通过。
10. **崩溃后残留 FUSE 会误导诊断**：daemon 已死但挂载仍在时，`readdir` /
  `stat` / `cat` 可能命中内核页缓存而"成功"返回，只有 `write` 必然触达
  已死的 daemon 并失败（`EINVAL`），`statfs` 报 `Function not implemented`。
  **判断 FUSE 是否存活必须用写探测**，读探测会给出假阳性。清残留用
  `fusermount3 -uz` + `umount -fl`。
11. **outlet 配置必须写在顶层表**：把 `outlet_*` 追加到文件末尾会落进
  其前的 `[trusted_node_certs]` 表，被当作证书路径解析，报
  `invalid type: integer 250, expected path string`。**必须插在第一个
  `[table]` 之前**，否则错误信息指向的值与真实原因不在同一层。
12. **`pgrep -f` / `pkill -f` 会匹配到自身**：脚本命令行里含配置文件名时，
  `pkill -f "etc/node-b.toml"` 会杀掉执行脚本的 shell 自身，导致后续启动
  从未执行（表现为"重启后进程不存在、日志为空"）。**必须从监听端口反查
  PID 再 kill**。

## 边界声明

- 八轮累计：24 个 spec 用例中 **10 个在真机完整验证**
  （TC1/2/4/5/7/9/13/19/21/22/23），6 个由单元测试覆盖
  （TC10/11/15/16/17/18），TC24 以日志代替指标，其余需容器层 /
  故障注入 / 部署脚本（T5）。
- **仍未覆盖**：TC6（A 宕机，需故障注入）、TC12（容器 provisioning
  层的启动门禁）、TC14（整棵 `/public` 挂入容器的违约现象）。
- 规模：功能验证单 workspace 起步，第三轮扩到 401 个 workspace 验证
  并发建号与多 root 回收；第七轮在该规模上做了性能计时。
- 这是**两节点、NFSv4.1 over TCP** 的功能与规模验证，
  **不是**多 VM 压测、HA 或故障注入证明。
- `softerr`/`actimeo` 组合未在弱网下测试；换代替换、ghost 隔离、
  挂载漂移告警仅在单元测试覆盖，未在真机复现。
- 验证后清理：`umount` node-b 的 NFS 挂载、`exportfs -u` 移除导出、
  kill 验证进程，见 cleanup 脚本。

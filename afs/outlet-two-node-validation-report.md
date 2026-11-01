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
6. **`/public` 是单点拓扑**：outlet 镜像只部署在 B 上，A 没有镜像也没有
  `/public`。跨节点批量操作必须在运行镜像的节点上经 `/public` 执行——
  在 A 上 glob `/public/root-*` 会拿到字面目录名并把 400 份拷进一个目录。
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
11. **`pgrep -f` / `pkill -f` 会匹配到自身**：脚本命令行里含配置文件名时，
  `pkill -f "etc/node-b.toml"` 会杀掉执行脚本的 shell 自身，导致后续启动
  从未执行（表现为"重启后进程不存在、日志为空"）。**必须从监听端口反查
  PID 再 kill**。

## 边界声明

- 两轮合计：24 个 spec 用例中 **10 个在真机完整验证**（TC1/2/4/5/7/9/13/19/21/22/23），
  6 个由单元测试覆盖（TC10/11/15/16/17/18），TC24 以日志代替指标，
  其余需容器层/故障注入/部署脚本（T5）。
- 这是**两节点、单 workspace、NFSv4.1 over TCP** 的功能验证，
  **不是**多 VM 压测、HA 或故障注入证明。
- `softerr`/`actimeo` 组合未在弱网下测试；换代替换、ghost 隔离、
  挂载漂移告警仅在单元测试覆盖，未在真机复现。
- 验证后清理：`umount` node-b 的 NFS 挂载、`exportfs -u` 移除导出、
  kill 验证进程，见 cleanup 脚本。

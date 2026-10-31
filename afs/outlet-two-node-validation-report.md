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

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

## 过程中发现并即时处理的问题

1. **宿主已有其他 AFS 进程**：7400（他人 meta）与 127.0.0.1:17400
  （`/home/lzq/afs-baseline`）。全部避开，未触碰。
2. **NFS 导出源放错**：第一次导出了手工目录而非真实数据树，重挂为
  `data/node-a/ownerfs`。
3. **测试设计疏漏**：先删除带文件的目录，ENOTEMPTY `pending` 是正确行为；
  清空文件后回收立即完成——这反而实证了 H20「非空即等待」。

## 边界声明

- 这是**两节点、单 workspace、NFSv4.1 over TCP** 的功能验证，
  **不是**多 VM 压测、HA 或故障注入证明。
- `softerr`/`actimeo` 组合未在弱网下测试；换代替换、ghost 隔离、
  挂载漂移告警仅在单元测试覆盖，未在真机复现。
- 验证后清理：`umount` node-b 的 NFS 挂载、`exportfs -u` 移除导出、
  kill 验证进程，见 cleanup 脚本。

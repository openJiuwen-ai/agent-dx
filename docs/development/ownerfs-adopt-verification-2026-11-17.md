# OwnerFs 启动纳管 · 双机真机验证（2026-11-17）

被测代码：分支 `recover`，提交 `220bb0e9`（`feat(ownerfs): adopt pre-existing directories by native name`），
二进制取 `target/debug/afs-node` / `afs-meta`（`md5 107bec5f…` / `bf48a5e6…`，构建于 21:02，构建标识
`18526256b-dirty`）。

| 项 | node-a | node-b |
| --- | --- | --- |
| 机器 | `1.94.217.10`（`agentos-sz-0001`，192.168.2.101） | `1.94.246.161`（`agentos-sz-0002`，192.168.2.213） |
| 角色 | Node（gRPC 18310 / REST 18311 / API 18312） | Node（18320/18321/18322）+ Meta（18300/18301） |
| 部署目录 | `/root/afs-v2` | `/root/afs-v2` |
| 存储根 | `ownerfs_data_dir=/home/agentos/.users` | 同 |
| 关键开关 | `ownerfs_adopt_on_startup=true`（节点）；`ownerfs_mkdir_load_balance=true`（Meta） | 同 |
| 对端地址 | `advertise_endpoint="https://192.168.2.101:18310"` | `advertise_endpoint="https://192.168.2.213:18320"` |

> `advertise_endpoint` 这一行是排查后补的，缺了它 node → node 通道会退化成明文（§2.2）。导出侧
> 另有 `insecure` 要求（§2.1）。两处修完后，§1.7 的跨机创建 / 跨机读才第一次真正跑通。

数据：node-b 上新造 10 个一级项（`test001`/`test002`/`empty-dir`/`perms-01` + 6 个边界项），
node-a 上沿用 11-13 遗留的 12 个业务目录（`u01-normal`…`u12-owner`）。

## 1 通过项

### 1.1 启动纳管

```text
node-b: scanned=12 adopted=4 skipped=8 failed=0     # 4 个目录合计 ~40ms
node-a: scanned=14 adopted=12 skipped=2 failed=0    # 12 个目录合计 ~133ms（≈11ms/个）
```

跳过项与规则逐条对上：

| 项 | 日志 | 说明 |
| --- | --- | --- |
| `.hidden-dir`、`.ownerfs-roots`、`.ownerfs-trash` | （静默跳过） | 点前缀 |
| `lost+found` | （静默跳过） | 文件系统自建 |
| `regular-file.txt` | `ownerfs.adopt.not_a_directory` | 非目录 |
| `zz-bind-target` | `ownerfs.adopt.mount_point` | `dev` 与父目录不同 |
| `业务数据001` | `ownerfs.adopt.non_ascii_name` | **所有非 ASCII 都跳过**（见 §3.1） |
| 150 字节长名 | `ownerfs.adopt.name_too_long` | 超过 trash 编码上限 |

`epoch` 从 1 起单调分配，`root_id` 仍是 `root-<hex>`；catalog 记录形如
`{"version":1,"root_id":"root-656d7074792d646972","name_hex":"656d7074792d646972","epoch":4,"data_dir":"empty-dir",…}`。

### 1.2 零迁移

纳管前后逐条比对 `inode / mode / uid / gid / mtime / sha256`，16 条全一致（含硬链接
`origin.txt`↔`hard.txt` 链接数、软链 `link-to-b`、`setgid` 目录 `perms-01`）。经历多轮重启、
增删、LB 操作后复核仍全 OK：

```text
test001                              OK      test001/./deep/c/d.bin   OK
test001/./hard.txt                   OK      test001/./origin.txt     OK
test001/./readme.txt                 OK      test001/./sub/b.txt      OK
empty-dir                            OK      perms-01/./setgid.txt    OK
```

存储根里 `root-*` live 目录数恒为 0。

### 1.3 幂等与重启

- 二次启动 node-a：`adopted=0 skipped=16`；三次启动 node-b：`adopted=1`（只纳管了我测试用的
  `zz-bind-target`，见 §3.4）。
- 节点重启后 catalog 驱动 reconcile，原 epoch 全部恢复，没有重新 reserve，也没有 `root-*`
  空壳目录冒出来。

### 1.4 `pin_home` 与负载均衡正交

- 16 个纳管根的 `home_node_id` 全部等于数据所在节点（node-b 的 4 个都在 node-b，node-a 的
  12 个都在 node-a）——LB 开着也没被 HRW 挪走。
- LB 开启下新建 10 个 workspace：**node-a 3 个 / node-b 7 个**，目录都落在 home 节点，且
  名字就是原名（`lb2-new-01`…`lb2-new-10`）。
- 创建请求打到非 home 节点时返回 503（`wait_outlet` 等不到对端发布），同一名字改打 home
  节点即 201——HRW 是确定性的，重试到对端就能成功。
- 当时 503 的真实原因见 §2.2（node → node 明文）与 §2.1（跨机 NFS 挂不上）；两处修复后
  「HRW 把 Home 选到对端」这条路完全打通，见 §1.7。

### 1.5 删除与同名重建

```text
DELETE /v1/workspaces/test002            -> 200
.ownerfs-trash/root-74657374303032-e1    -> 里面 f1.log…f5.log 完好（宽限期）
25s 后                                   -> trash 清空，live 目录消失
test001                                  -> inode/sha256 未变（未受影响）
POST 同名 test002                        -> 201, epoch=26, inode=657740（原 679439），空目录
```

### 1.6 FUSE 入口命名校验

| 输入 | 结果 |
| --- | --- |
| `bad\xffname` | `EINVAL` |
| `.ownerfs-evil` | `EINVAL` |
| 115 字节名 | `EINVAL` |
| `业务数据002` | 创建成功（合法 UTF-8 不受限） |

### 1.7 修复后补验：跨机创建与跨机读写

之前被 §2.1 / §2.2 挡住、无法验证的两条，修复后补上了：

- **跨机创建**：30 个全新名字一次跑完 **30/30 成功、0 失败**，HRW 分布 node-b 17 / node-a 13，
  每个目录都落在自己的 home 节点且用原名；两节点近 3 分钟 `FUSE request failed` 与
  `received corrupt message` 均为 0。
- **跨机读**：node-b 建 workspace 写入 `f1.txt`，node-a 经 `/public` 读到同一 md5
  （`4240b717…`）、同一 mtime、内容正确；反方向 node-a 写 `f2.txt`、node-b 读，md5
  `c2e22dfa…` 一致。
- 两个节点的 `/public` 都能看到全部 30 个名字，说明 import 挂载与名字发布都收敛了。

（`/public` 是 FUSE 视图，inode 与 NFS 侧不同是正常的；零迁移的 inode 一致性结论仍以 §1.2 为准。）

## 2 已定位并修复的两处故障（均与被测改动无关）

这两项一开始都表现为「环境问题」，事后都定位到了具体机制。第一项是**部署态**问题（导出选项），
第二项是**产品缺陷**（传输层 scheme 判定），已提修复。

### 2.1 跨机 NFS 挂载 `Operation not permitted`：`noresvport` 撞上 `secure` 导出

根因是两个默认值互斥，不是内核或 nfsd 的问题：

| 一侧 | 取值 | 来源 |
| --- | --- | --- |
| 客户端 | `vers=4.1,hard,timeo=600,retrans=2,`**`noresvport`**`,nosuid,nodev` | `config/outlet.rs::DEFAULT_MOUNT_OPTIONS` |
| 服务端 | `/home/agentos/.users *(...)` 隐含 **`secure`** | `/etc/exports.d/workspace-storage.exports`（agentos 的 deploy.sh 管理） |

`noresvport` 让客户端用非保留端口连 2049，而 `secure` 要求保留端口，服务端直接拒绝，客户端表现为
`mount.nfs: Operation not permitted`（rc=32）。本机上其余四个 AFS 导出都带 `insecure`，只有
`.users` 这份没有——所以「以前 AFS 能用、这次不行」。对照组：

| 客户端选项 | 结果 |
| --- | --- |
| `vers=4.1` + `noresvport` | `Operation not permitted` rc=32 |
| `vers=4.1` 去掉 `noresvport` | 挂载成功 |
| `vers=3` + `noresvport` | `access denied by server while mounting` |
| `vers=3` 去掉 `noresvport` | 挂载成功 |

排查中还有一层误导：`/var/lib/nfs/etab` 里留着一条先前用 `exportfs -o` 加进去的 `secure` 条目，
与 `/etc/exports` 里的新条目冲突，`exportfs -ra` 报 `incompatible duplicated export entries`
并且**返回 0**，导出实际没进内核——这也是「`exportfs` 说成功了但挂不上」的来源。

修复：在 `/etc/exports.d/workspace-storage.exports` 的 `.users` 行加 `insecure`（与其余 AFS 导出
一致），删掉 `/etc/exports` 里的重复行，清掉陈旧 `etab` 后重启 nfs-server。复验：两机双向
`mount.nfs -o vers=4.1,...,noresvport` 均 `rc=0`，能列出对端数据目录。

> 附带纠正：`/proc/fs/nfsd/exports` 在这台内核上只打印表头，即使导出正常工作也不显示条目。
> 不能拿它判断导出是否生效，要看 `showmount -e` 与实际挂载。

**长期修复应落在 agentos 的 `workspace-storage` 导出模板上**（补 `insecure`），否则重建环境会复发；
或者把 `noresvport` 从 `DEFAULT_MOUNT_OPTIONS` 里去掉，但那是为了免特权端口，不建议动。

### 2.2 node ↔ node 通道全明文：`http://` scheme 让 tonic 跳过 TLS

**不是 node ↔ Meta**（元结论修正）。真正的失败链路是：FUSE `mkdir` → `reserve_workspace` →
Meta 用 HRW 把 Home 选到**另一个节点** → `peers.prepare_root()` 走 node → node gRPC → 失败。

证据链：

1. 成功的 root 全部 `home = 本节点`；失败的 root 在**两个节点上都没有 catalog、也没有目录**，
   说明它死在「本地落盘之前」；
2. 失败率与并发、间隔都无关（背靠背 15/30、隔 3 秒 10/20，都是 ~50%），等于「HRW 命中对端的
   概率」;
3. 对端日志在同一毫秒出现 `afs.transport.rejected: received corrupt message of type
   InvalidContentType`（收尾统计 149 次）——这是 **TLS 端口收到明文 HTTP/2 preface** 的固定报错；
4. tonic 源码（0.14.6 `channel/service/connector.rs:55`）：
   ```rust
   let is_https = uri.scheme_str() == Some("https");
   if is_https { tls.connect(..) }   // 否则明文，即使配了 tls_config
   ```
5. 节点注册到 Meta 的 `grpc_addr` 来自 `advertised`，默认是 `format!("http://{}", cfg.grpc_listen)`，
   而 `SecurityManager::endpoint_uri()` 在加密开启时**原样返回**，于是在途地址一直是 `http://`。

Meta 那条链路正常，只因为 `meta_endpoint` 是手写配置、明确写了 `https://`。

修复（两层）：

- **部署层（已真机验证）**：两个 node.toml 显式写 `advertise_endpoint = "https://<grpc_listen>"`。
  复验：50 次新名字创建 **28/28 成功（其余 22 次是上一轮已存在的同名 EEXIST）**，随后 30 个全新
  名字 **30/30 成功、0 失败**，HRW 分布 node-b:17 / node-a:13，两节点近 3 分钟
  `FUSE request failed` 与 `corrupt message` 均为 0。
- **代码层（已提交 `6c99e309`）**：`endpoint_uri()` 让 scheme 跟随策略——加密开启时把
  `http://` 升为 `https://`。这本来就是 `connect_lazy` 注释里「there is no plaintext fallback」
  该有的语义，缺失时任何漏写 scheme 的部署都会静默降级成明文。补了三个单测覆盖
  加密 / 仅认证 / 关闭三种模式。附带把 `docs/deployment/afs.md` 里 `advertise_endpoint` 那行
  补上「开启加密时必须用 `https://`」。

> 另外解释一个此前的误判：Meta 日志里「每 5–10 秒一条 `afs.transport.authenticated`」不是病因。
> 实测完全空闲 60 秒新增 14 次握手、发 60 个请求也只新增 12 次，频率由时间驱动、与失败率无关。

## 3 需要改文档或代码的点

### 3.1 非 ASCII 名字：实现比设计更严

`adopt_entry()` 的判定是 `std::str::from_utf8(bytes).is_err() || !bytes.is_ascii()`，即**所有
非 ASCII 名字（含中文）一律跳过**，日志 `ownerfs.adopt.non_ascii_name`。

- 设计文档 §6.2 写的是「名字不是合法 UTF-8 → 跳过」，中文应当可以纳管；
- `docs/deployment/afs.md` 写的是「非 ASCII/非 UTF-8 名跳过」，与实现一致。

真机表现：`业务数据001` 被跳过，但同样的名字经 FUSE 创建是允许的（见 §1.6）。也就是「新建的
中文 workspace 能用，存量中文目录纳管不了」。要么改设计文档，要么放开 `is_ascii()` 只留 UTF-8
判定（catalog 的 `data_dir` 是 JSON 字符串，合法 UTF-8 落盘无损）。

### 3.2 挂载点护栏只在扫描那一刻有效

`zz-bind-target` 首次扫描被正确判为挂载点跳过；机器重启后 tmpfs 消失，它变成普通空目录，
第三次启动就被纳管了（epoch 25）。部署前置里「`storage_root` 下不允许子挂载点」这条要写成
硬约束，否则「挂载没起来」与「业务目录」在扫描时无法区分。

### 3.3 catalog 仍落盘 `data_dir`

记录里保留了 `data_dir`（如 `"empty-dir"`）。在入口拒绝非 UTF-8 之后它不会再有损，
但与「catalog 不再落盘 `data_dir`」的说法不一致，评审时容易误解。

## 4 复跑方式

脚本在 WSL `/root/adopt-v2/`：

| 脚本 | 作用 |
| --- | --- |
| `deploy.sh` | 停旧进程、铺目录、装二进制与配置 |
| `start.sh {meta,node-a,node-b,all}` | 用 `systemd-run` 起进程（脱离 ssh 会话） |
| `status.sh` | 双机 workspace 视图、home 归属、import 挂载、`/public` 条目数 |
| `seed-data.sh <host>` | 造存量数据并写基线 `baseline.tsv` |
| `verify-nochange.sh <host>` / `final-check.sh` | 与基线逐项比对 |
| `test-delete.sh` / `test-lb2.sh` / `test-fuse-names.sh` | 删除重建 / LB 分散 / 命名校验 |

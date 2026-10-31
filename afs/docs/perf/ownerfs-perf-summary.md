# OwnerFs 本地 FUSE 性能优化 —— 实施摘要

> 分支 `perf/m3a-openat`（基线 `a19b3041`），作者 `linziqi <linziqi1@huawei.com>`，未 push。
> 完整过程、失败记录与方法论见 `ownerfs-smallfile-write-perf-report.md`（668 行）。
> 本文只回答两件事：**做了什么**、**效果如何**。

## 一、结论先行：规模 × 并发 × 参照实现

单位：files/s（4 KiB 小文件）。优化前 = `a19b3041`，优化后 = 当前分支，均为 release 构建。
除矩阵表外，其余每格为多轮中位；门槛列给出该组 `max/min`（<=1.10 通过）。

### 1.1 单进程与 4 并发（2k/10k/20k）

| 场景 | ownerfs 优化前 | **ownerfs 优化后** | 提升 | bindfs | 本地 ext4 |
|---|---|---|---|---|---|
| 单进程写 2k | 556 | **6,873** | 12.4x | 12,598 | 114,972 |
| 单进程写 10k | 537 | **7,056** | 13.1x | 15,928 | 79,161 |
| 单进程写 20k | 540 | **6,573** | 12.2x | 16,715 | 84,680 |
| 4 并发写 2k | 1,298 | **13,822** | 10.6x | 6,187 | 30,076 |
| 单进程读 2k 热 / 冷 | 20,454 / 3,686 | **28,132 / 3,646** | +37% / 持平 | 45,253 / 4,157 | 453,621 / 4,576 |
| 单进程读 20k 热 / 冷 | 16,225 / 3,870 | **18,676 / 3,705** | +15% / 持平 | 40,212 / 4,129 | 396,474 / 4,671 |
| 4 并发读 2k 热 / 冷 | 69,874 / 10,388 | **85,548 / 12,899** | +22% / +24% | 93,221 / 15,041 | 699,292 / 17,146 |

### 1.2 规模 × 并发矩阵（10k / 50k / 100k × 4 / 8 / 16，单轮）

每格：新建语料 → 读（热）→ `drop_caches` → 读（冷）。

**写**

| 规模 | 并发 | **ownerfs** | bindfs | ext4 |
|---|---|---|---|---|
| 10k | 4 / 8 / 16 | 13,654 / 12,703 / **21,916** | 7,212 / 7,232 / 6,211 | 12,535 / 22,622 / 8,951 |
| 50k | 4 / 8 / 16 | 14,089 / 20,673 / **21,976** | 7,738 / 6,377 / 6,930 | 10,871 / 9,978 / 9,228 |
| 100k | 4 / 8 / 16 | 13,675 / 21,473 / **21,842** | 5,608 / 5,049 / 5,185 | 11,354 / 9,262 / 8,904 |

**读（热，页缓存命中）**

| 规模 | 并发 | ownerfs | bindfs | ext4 |
|---|---|---|---|---|
| 10k | 4 / 8 / 16 | 62,692 / 87,210 / **124,852** | 80,464 / 52,967 / 90,607 | 1.20M / 2.02M / 2.20M |
| 50k | 4 / 8 / 16 | 61,348 / 88,606 / **131,975** | 86,481 / 106,291 / 98,186 | 1.48M / 2.50M / 3.75M |
| 100k | 4 / 8 / 16 | 62,111 / 90,656 / **127,903** | 89,222 / 109,308 / 98,899 | 1.54M / 2.80M / 3.97M |

**读（冷，真实首次读）**

| 规模 | 并发 | **ownerfs** | bindfs | ext4 |
|---|---|---|---|---|
| 10k | 4 / 8 / 16 | 12,773 / 24,917 / **33,319** | 7,996 / 19,539 / 26,029 | 18,700 / 36,004 / 62,177 |
| 50k | 4 / 8 / 16 | 13,560 / 25,145 / **48,993** | 11,932 / 20,215 / 33,023 | 18,538 / 36,606 / 46,174 |
| 100k | 4 / 8 / 16 | 13,753 / 25,449 / **45,944** | 11,671 / 19,794 / 35,017 | 18,788 / 36,051 / 63,145 |

### 1.3 六条结论

1. **写：ownerfs 优于 ext4、远超 bindfs。** 100k/16 并发时 21,842 vs ext4 8,904（2.5x）、vs bindfs 5,185（4.2x）。
   ext4 与 bindfs 在并发新建同目录文件时被目录/位图锁与 dirty 回写拖住（ext4 从单进程 115k 掉到 9k），
   而 ownerfs 的写落页缓存（close 不同步，见 §1.4 的语义说明），并发反而把用户态开销重叠掉。
2. **冷读（真实首次读）已接近 ext4、全面优于 bindfs。** 100k/16 并发 45,944 vs ext4 63,145（73%）、
   vs bindfs 35,017（1.31x）；50k/16 并发甚至反超 ext4（48,993 vs 46,174）。冷读瓶颈是磁盘，
   固定开销被 I/O 等待掩盖。
3. **热读是唯一明显落后的档位（比 ext4 慢 12-30 倍），且这是 FUSE 架构固有成本**：
   每次读都要把数据从内核拷到用户态再写回 `/dev/fuse`，而 ext4 命中页缓存是零拷贝。
   不改变数据通路（splice/sendfile 直通）就无法消除，优化空间在写与元数据路径。
4. **并发扩展良好**：热读 4→8→16 并发为 62k→88k→128k（2.06x），冷读 13k→25k→46-49k（3.5x）。
   写侧在 8 并发饱和（约 21.5k），与 `fuse_threads=4` 加每 fh 写序一致。
5. **规模不影响吞吐**：10k / 50k / 100k 三档几乎重合（±5%），100k 文件无退化。
6. **两处此前的判断需要更正**（见 §1.5）。

### 1.4 语义前提（引用数字前必读）

- **写数字包含 close 不同步**：私有 local root 上 close 不 `fdatasync`，写落内核页缓存，
  等价 ext4 默认（`data=ordered`）：进程崩溃不丢，**掉电可能丢最近若干秒**。
  需要 close 后掉电不丢时用 `AFS_OWNERFS_CLOSE_SYNC=1`，此时写回到 556 / 1,298（单进程 / 4 并发）。
- **读数字分热冷**：热 = 数据在页缓存；冷 = 读前 `drop_caches`。两者差 3-7 倍，不可混用。
- **1 GiB 大文件**：不 fsync 1,749 MB/s（vs bindfs 384、ext4 3,642）；加 fsync 后 255 MB/s，
  与裸 ext4 238 同量级。**1,749 不代表持久化吞吐。**

### 1.5 被推翻的两个判断

| 此前说法 | 实测结果 | 更正 |
|---|---|---|
| 「scratch 全局锁把读的并发扩展压到 3.06x」 | 16 并发热读达 128k = 单进程 28k 的 4.5x；改成线程本地 buffer 后吞吐仅 +1.3% | 该锁是代码坏味道与潜在上限，**不是本负载的瓶颈**；扩展比 3.06 只是「4 客户端 ÷ 单进程」的算术结果 |
| 「热读随规模退化 34%（28k→18k @20k）」 | 单次遍历 10k/50k/100k 三档吞吐重合；退化只在**同一语料反复读、跨越 1 s TTL 边界**时出现（每读多 1-2 次 FUSE LOOKUP） | 退化与"文件数"无关，与"重复读的间隔是否跨 TTL"有关；`AFS_OWNERFS_CACHE_TTL_MS` 调到 30s 可消除（20k 时 16.4k→24.6k） |

口径：release 构建、同机、同客户端 `/tmp/iobench3`（无 fork）、每格前 `drop_caches`。
矩阵表为单轮（3 组 × 3 档 × 3 目标，单轮约 5 分钟）；单进程表为 3-4 轮中位。
正式对外引用矩阵数字前应补到 >=3 轮并满足 `max/min <= 1.10`。

## 二、必须先说的前提（否则数字不可用）

1. **此前所有数据基于 debug 构建，已作废。** 同一份代码：debug 2259–2470 files/s、release 5191–6043 files/s，**差 2.6 倍**。bindfs 是 `-O2` 的 C，拿 debug 比它是错的。
2. **客户端决定结论。** 用 `for i; head -c 4096 > f$i` 这类 shell 循环时，每文件 fork+exec 固定吃掉 580–860 µs，会把 7 倍差距压成 2 倍。此后一律用自写 C 客户端 `/tmp/iobench3`（open/io/close 分段计时）。
3. **尖峰必须剔除。** 冷页缓存下 `open` 偶发 70–90 ms（ext4 冷元数据读延迟，非我方代码），对比时按 `worst open > 20 ms` 剔除该轮。
4. **口径**：release、每轮 `drop_caches`、≥4 轮取干净轮中位、部署前三重校验（md5 + 二进制 marker + build.info 版本）。

## 三、做了什么（3 个提交）

### 提交 1 `cff2c71e` —— 存储层路径解析

| 问题 | 改动 | 效果 |
|---|---|---|
| 每次按路径打开子项都要 `/proc/self/fd/<fd>/<name>` 重开，内核多走一遍 procfs + 符号链接 | `open_child_dir/file` 改用 `libc::openat(parent_fd, name, ...)` | 单次 open 更便宜 |
| 多级路径逐级 `openat` | `open_path_dir` 用**一次 `openat2`**（`RESOLVE_IN_ROOT｜NO_SYMLINKS｜NO_MAGICLINKS`），`ENOSYS` 自动回退 | 多级走查合成 1 次 |
| 子项存在性检查要全路径 stat | 新增 `DirectoryHandle::child_present`，`LocalDirectory` 用 `fstatat(AT_SYMLINK_NOFOLLOW)` | 少一次全路径解析 |

`openat` 9.7 → 8.7 次/文件，新增 `openat2` 1.0 次；close 13.9 → 12.9；吞吐 **+7.5%**，且冷读尖峰显著减少。

### 提交 2 `7a6a6f52` —— OwnerFs 本地 create / flush 主路径

| 问题 | 改动 | 效果 |
|---|---|---|
| **每次 close 都 `sync_data()`（fdatasync 1.86 ms）** | `flush` 仅在「`write_sync == None` **且** 私有 local root **且** 未设 `AFS_OWNERFS_CLOSE_SYNC=1`」时跳过；共享 root、远端 handle、`O_SYNC/O_DSYNC`、显式 fsync 语义不变 | **569 → ~6,700 files/s（10.3×）**；close 尾延迟 8.8–22 ms → 0.06–0.15 ms |
| create 里父目录 stat + 子项全路径 stat + 两次按路径 open | 保留父目录句柄复用，子项探测走 `child_present`，两次 open 走 `openat` | path_missing 8.2 → 4.7 µs；create_open 32.2 → 24.6 µs（p99 528 → 40 µs） |
| create 结束还要「关掉重开 + identity 比对」防替换 | **私有 root 下省掉**（无 peer 写者无法替换）；共享 root 保留原校验 | 每文件少 1 次 open + 1 次 metadata + 1 次 close |
| 挂载根是合成 inode、不在 `state.inodes` 里，TTL 被判为 0 | `with_fuse_cache_policy` 对 `OWNERFS_ROOT_INODE` 给 1 s TTL（与 workspace 根一致） | 单文件回调 20 → 12；根 getattr 6 → 1（吞吐影响在噪声内） |
| 本地写走 worker 队列（拷贝 + 跨线程） | 覆盖 `write_admission_mode`，对齐 DFS：私有 root 本地写 → `Borrowed`（接收线程就地执行，**保留 per-fh 轮次记账**） | write 排队 38.7 → 19.4 µs |

### 提交 3 `4540101c` —— 诊断与就地执行

| 改动 | 作用 |
|---|---|
| `AFS_FUSE_TRACE=1` 门控的逐回调打点（op/ino/fh/耗时/排队等待）+ create 六段计时 | 默认零开销；正是靠它把 create 的 106 µs 拆到 `inode_for_path` 29.7 µs 等具体行 |
| `try_run_keyed`：轮次空闲就地执行、忙则回队列，**绝不阻塞接收线程** | getattr 少一次跨线程交接（先做过阻塞版，实测退化 1244/1280 files/s，已改） |
| `AFS_OWNERFS_INLINE=1` | 仅对照用；它会丢 per-fh 串行化，**不可转正** |

### 另有 4 个前置修复（本轮之前已提交）

`5b7f4fdb` 非 `user.*` xattr 探测回 `ENODATA`；`860a2f17` 启动打 build.info（版本可校验）；`fdd0bc24` / `1250b839` 把预期的 xattr/NotFound 日志降级 —— 500 文件场景 ERRO 502 → 1 条。

## 四、效果拆解（单进程 4 KiB 写，~150 µs/文件）

```
后端执行   ≈ 73 µs   create 37 + lookup 20 + getattr 4 + write 5 + flush 0.4 + release 7
排队       ≈ 21 µs   write 5 + flush 8 + release 8
FUSE 收发  ≈ 56 µs   6.6 次回调 × 8.5 µs（bindfs 约 10–12 µs/次，同级）
```

系统调用（每次 create）：**129 → 120 次/文件**，其中
- 可优化部分：`openat 9.7→8.7`、`openat2 1.0`、`close 13.9→12.0`、`statx 12.2→10.2`、`fcntl 4.1→3.2`
- 固有部分：`read 38 + writev 10`（FUSE 收发）、`pwrite64 1`（真正的数据写）

## 五、逐项归因（重要：别把这些当收益）

| 改动 | release 实测收益 |
|---|---|
| **O1：私有 root close 不同步** | **10.3×（决定性）** |
| openat + openat2 | +7.5% |
| create 精简 / statx 合并 | +5.5% |
| 根 TTL、child_present、父目录复用、write 借用、getattr try_run | **合计 < 2%（噪声内）** |

证据：`wrong close` 从 8.8–22.0 ms 降到 0.06–0.15 ms。**除 O1 外的其余改动必须在 O1 之后才有意义**（此前被 fdatasync 完全淹没）。

## 六、被否定的做法（避免重复试错）

| 尝试 | 结论 |
|---|---|
| 调 `fuse_threads/fuse_workers`（1/4 比 4/32 快 25%） | **不采纳**：单线程客户端标定不了并发参数，且该默认值 DFS 与 ownerfs 共用、已由 DFS benchmark 验证 |
| 搬 DFS 的 async read pipeline（ASYNC_DIO + deferred completion） | **不搬**：ownerfs 本地读已是内存级并已 inline，4 并发扩展 3.3×，再叠一层异步管线为负收益 |
| chown+chmod 占 55 µs | 实测仅 5.3 µs，旧结论错误 |
| 「TTL 一直生效」 | 错：根 inode TTL 一直为 0，已修 |
| 每文件 12 次回调 | 错：那是 shell 客户端造成的，ownerfs 自身 6.0–6.6 次 |
| 排队成本 160 µs/文件 | 高估：实测 56 µs/文件（write 39 + flush 9 + release 8） |

## 七、未落地与后续建议

| 项 | 状态 | 建议 |
|---|---|---|
| **close 持久性三态**（`always`/`group`/`never`） | `never` 已实现并默认；`group` 设计完成但**未落地** | 先把三态做成挂载选项（语义显式化），再实现 `group` |
| `group` 提交队列（序列号 + leader `syncfs` + condvar 放行） | 设计完成，代码被 rustc 1.95 **ICE** 阻断后已回退 | 按「取角色→动作」写法逐段加、逐段编译定位 |
| 单回调用户态开销（release 17–24 µs，本应 2–3 µs） | 未动 | 唯一还剩的优化象限：单进程小文件 |
| futex 33.7 次/文件 | 已定位（part 为线程 park/unpark，part 为每请求固定同步） | 属框架层通用成本，DFS 同样存在 |

**语义提醒**：当前默认等价于 ext4 默认（`data=ordered`）——进程崩溃不丢，**掉电可能丢最近若干秒**。若 close 必须意味着"掉电不丢"，请使用 `AFS_OWNERFS_CLOSE_SYNC=1`（性能回落至 569 files/s），或等 `group` 模式落地。

## 八、复现

| 工具 | 用途 |
|---|---|
| `/tmp/iobench3` | C 客户端，支持 `w`/`r`，输出 open/io/close 分段最差延迟 |
| `/root/aligned-bench.sh <host> <rounds> <closesync>` | 部署 + 版本校验 + 冒烟 + 多轮基准（`AFS_BIN=` 选二进制） |
| `/root/syscall-profile.sh <host> <files>` | strace -c 系统调用画像 |
| `/root/scenario-ac.sh` | 场景 A（`cp -r` 多层）+ 场景 C（256 MiB 大文件） |
| `/root/write-concurrent5.sh` / `/root/read-concurrent.sh` | 场景 B（4 进程并发读写） |
| `/root/futex-bisect.sh` | futex 与线程数二分 |

**构建**：`cargo build --release --offline --features ownerfs --bin afs-node`（**必须 release**，debug 数据不可用于比较）。
**部署**：务必从 `afs/target/release/` 或工作区根 `target/release/` 取二进制，两者是不同产物（曾因此误部署旧二进制，导致多项优化"看似无效"）。

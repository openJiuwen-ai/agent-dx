# AFS DFS 的 POSIX 支持边界

本文列出 DFS 挂载（`fs = "dfs"`）对 POSIX 与 Linux 文件操作的实际行为。每一项都有测试断言
应用看到的结果；多数行为由内核决定（ENOSYS 缓存、本机锁表、拷贝回退），所以以真实挂载的
用例为准：`afs/tests/suites/dfs/test_posix_boundary.py`（suite `posix-boundary`，两个 DFS Node、
各自一个 FUSE 挂载，在 adx-vm 的 Linux 6.8 上实测）。OwnerFs 只在行为不同处注明。

归类：**支持**；**仅本机**（同一 Node 上的进程之间有效，跨 Node 无效）；**不支持**（返回明确的 errno）。

## 已知限制（先读）

本节只列 POSIX 相关的限制；完整的已知限制（含 local-file Meta 目录必须在本机独占盘、打开着反复 fsync 的文件使 Meta 内存增长等更严重的项）与支持矩阵见[支持边界与已知限制](afs-support-boundary.md)。

1. **文件锁只在本机有效**（决策 11）。`flock`、`fcntl` 字节范围锁与 OFD 锁由每个 Node 的内核本地锁表处理：
   同一 Node 上的两个进程互斥，不同 Node 上的进程之间**没有互斥**。依赖文件锁协调多台机器的程序
   （数据库、`lockfile` 类工具）不能在 DFS 上跨 Node 使用。用例：`file_locks_exclude_on_one_node_only`。
2. **`statfs`／`df` 显示的是本 Node 自己的 chunk 存储盘**，不是集群容量或配额；`f_files`／`f_ffree` 对 DFS 无意义。
   用例：`statfs_reports_the_local_disk`。
3. 跨 Node 的一致性是 close-to-open：另一 Node 要在写入方 `fsync` 或 `close` 之后**重新打开**才看到新内容；
   已打开的读者固定在打开时的版本（决策 4a）。`mmap MAP_SHARED` 同理。

## 清单

| # | 操作 | 行为 | 归类 | 用例（`dfs.posix.*`，另注者除外） |
|---|---|---|---|---|
| 1 | `fallocate`（所有 mode） | `EOPNOTSUPP`；glibc `posix_fallocate` 会回退为逐块写零 | 不支持 | `fallocate_is_not_supported` |
| 2 | `lseek(SEEK_DATA/SEEK_HOLE)` | 内核回退：整个文件都是数据，`SEEK_HOLE` 返回文件大小，越过末尾 `ENXIO` | 支持（无洞信息） | `seek_data_hole_treat_the_file_as_data` |
| 3 | `readdirplus` | 不协商；`readdir` 返回全部条目和类型，属性靠后续 lookup | 支持 | `readdir_lists_every_entry_with_its_type` |
| 4 | `flock`、`fcntl` 锁、OFD 锁 | 同 Node 互斥（`EAGAIN`），跨 Node 不互斥 | 仅本机 | `file_locks_exclude_on_one_node_only`；`dfs.semantics.locks_local_only` |
| 5 | xattr `user.*` | 读写列删、`XATTR_CREATE`/`REPLACE`（`EEXIST`/`ENODATA`）、值最大 64KiB、名字超 255 字节 `ERANGE`（内核） | 支持 | `user_xattrs_round_trip` |
| 6 | xattr `security.*` | root 写或读 `security.capability` 均 `EOPNOTSUPP`；非 root 写 `EPERM`（内核） | 不支持 | `other_xattr_namespaces_are_not_supported` |
| 7 | xattr `trusted.*` | root `EOPNOTSUPP`；非 root `EPERM`（内核） | 不支持 | 同上 |
| 8 | POSIX ACL（`system.posix_acl_*`） | 读写均 `EOPNOTSUPP`；权限只按 mode／uid／gid | 不支持 | 同上 |
| 9 | `mknod` 普通文件 | 建成可写的普通文件 | 支持 | `mknod_regular_and_device_nodes` |
| 10 | `mknod` FIFO／Unix socket | inode 共享，管道与 socket 对象各 Node 独立：同 Node 可通信；跨 Node 写端 `ENXIO`、`connect` `ECONNREFUSED` | 仅本机 | `fifos_and_sockets_are_node_local` |
| 11 | 设备节点 | 保留 rdev；挂载带 `nosuid,nodev`，打开设备 `EACCES` | 支持（不可打开） | `mknod_regular_and_device_nodes` |
| 12 | 硬链接 | `nlink` 与内容跨 Node 一致，删一个名字另一个仍在 | 支持 | `hard_links_and_symlinks` |
| 13 | 符号链接 | 目标最长 4095 字节（内核 `PATH_MAX` 含结尾 NUL，先于 AFS 的 4096），超长 `ENAMETOOLONG`，空目标 `ENOENT` | 支持 | 同上 |
| 14 | `rename`、`RENAME_NOREPLACE` | 支持；目标已存在时 `EEXIST` | 支持 | `rename_flags` |
| 15 | `RENAME_EXCHANGE`／`RENAME_WHITEOUT` | `EINVAL`，且不会让该挂载的 `RENAME_NOREPLACE` 失效（S4a） | 不支持 | 同上 |
| 16 | `O_DIRECT` | 不要求对齐 | 支持 | `direct_io_needs_no_alignment` |
| 17 | `O_APPEND`（单 Node） | 多个描述符追加不交错覆盖 | 支持 | `append_on_one_and_two_nodes` |
| 18 | `O_APPEND`（多 Node） | 两个 Node 交替追加，各自用同一描述符读回一致（S4a 实测不错位） | 支持 | 同上 |
| 19 | `mmap MAP_SHARED` 写 | 本机可见；另一 Node 在 `msync`／`close` 后新打开可见 | 支持（close-to-open） | `mmap_shared_write_is_close_to_open` |
| 20 | `copy_file_range` | 得到源内容（条件满足时元数据 COW，否则内核回退拷贝）；`flags≠0` `EINVAL` | 支持 | `copy_file_range_copies_or_falls_back` |
| 21 | `ioctl` | 只接受 AFS 私有命令；`FS_IOC_GETFLAGS`（`lsattr`）`ENOTTY` | 不支持 | `unsupported_ioctls` |
| 22 | `utimensat` | 毫秒精度（纳秒截断）；`UTIME_OMIT`／`UTIME_NOW`；1970 年以前 `EINVAL`（D-S4-2）；`UTIME_NOW` 取写入 Node 的时钟 | 支持 | `timestamps` |
| 23 | `chmod`／`chown` 权限 | 非属主改 mode、非 root 改属主、改到非所属组均 `EPERM` | 支持 | `ownership_and_privilege_bits` |
| 24 | 写入／截断清 suid、sgid | 非特权写或截断清 suid 与带组执行位的 sgid；root（`CAP_FSETID`）不清 | 支持 | 同上 |
| 25 | `FUSE_HANDLE_KILLPRIV_V2` | 不协商，清位由内核发起（第 24 项）；见下文 | 不支持（已评估） | `official_fuser_delivery_does_not_request_lock_or_killpriv_capabilities`（`node/fuse/dispatch_tests.rs`） |
| 26 | `statfs` | 本 Node chunk 存储盘的数值，`namemax` 255 | 支持（见已知限制 2） | `statfs_reports_the_local_disk` |
| 27 | 截断扩展 | 扩出部分读回 0，跨 Node 一致 | 支持 | `truncate_extends_with_zeros` |
| 28 | 名字与路径长度 | 名字 1–255 字节，256 `ENAMETOOLONG`；路径受内核 `PATH_MAX` 限制 | 支持 | `name_and_path_limits` |
| 29 | 最大文件 | 在 1TiB 与 2^50（1PiB）处写 1 字节，得到对应大小的稀疏文件、读回正确；AFS 只在 `offset+len` 溢出时 `EFBIG` | 支持（2^50 以上未测） | `large_sparse_offsets` |
| 30 | 目录 `fsync` | 成功；目录项变更本就是同步 Meta 事务 | 支持 | `directory_fsync_and_tmpfile_and_poll_and_access` |
| 31 | `O_TMPFILE` | `EOPNOTSUPP` | 不支持 | 同上 |
| 32 | `poll`／`select` | 普通文件立即可读可写 | 支持 | 同上 |
| 33 | `access(2)` | 由内核按 mode 判断（`default_permissions`） | 支持 | 同上 |
| 34 | `O_PATH` | 可打开、可 `fstat`，读 `EBADF` | 支持 | 同上 |
| 35 | 符号链接／特殊文件上的 xattr | `user.*` 写 `EPERM`（内核） | 支持（同 Linux） | `xattrs_on_special_files` |
| 36 | `bmap`、`FIEMAP`、`FICLONE` | `FIEMAP`、`FICLONE` `EOPNOTSUPP`；`cp --reflink=auto` 回退普通拷贝 | 不支持 | `unsupported_ioctls` |
| 37 | `readdir` 大目录 | 按页取（R72），百万项可列出 | 支持 | `readdir_lists_every_entry_with_its_type`；S4a R72 场景 |
| 38 | 打开中的文件被 `unlink`／`rename` 覆盖 | 旧描述符继续读打开时的版本 | 支持 | `open_file_survives_replacement`；`dfs.semantics.unlink_open_file` |
| 39 | `open(O_CREAT)` 遇到已存在的名字 | 带 `O_EXCL`：`EEXIST`；不带：打开已有文件。名字刚被另一节点建出、本节点内核还没看到时也一样（Node 让内核重新查找一次再打开） | 支持 | `exclusive_create_of_a_taken_name_is_eexist`、`plain_create_of_a_name_that_just_appeared_opens_it` |

OwnerFs 与 DFS 不同之处：`RENAME_EXCHANGE`／`WHITEOUT` 同样回 `EINVAL`（公共 FUSE 层）；远端 Home 的 xattr
回 `ENOSYS`，内核会让该挂载的 xattr 全部失效（已登记，未改）；没有 `copy_file_range`（内核回退）；
挂载根的 `statfs` 为 `ENOSYS`；本地 Home 的目录 `fsync` 真同步。

## `FUSE_HANDLE_KILLPRIV_V2`：已评估，本轮不支持

开启后内核把“写入／截断／chown 时清 suid、sgid”交给用户态，可以省掉每次写前的一次
`security.capability` 探测往返（S3b：16 次写 16 次探测，开启后 1 次）。但 AFS 依赖的 fuser 0.18
不把内核的 `FUSE_OPEN_KILL_SUIDGID`（open／create 的 `open_flags`）和 `FATTR_KILL_SUIDGID`
（setattr 的 `valid` 位）交给文件系统，用户态无法知道哪些操作需要清位。不改 fuser 就开启，
非特权用户截断或 chown 一个 suid 文件后 suid 不会被清除，是安全回归。R58 已经把那次探测改在
FUSE 接收线程本地回复，剩下的只是一次内核与用户态之间的往返。要支持它，须先让 fuser 暴露这两个字段，
另行立项。

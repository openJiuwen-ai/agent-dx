# AFS 测试用例与编写 Prompt

这份清单维护每个场景要守住的行为、怎么重跑、什么变化应使断言失败。用例 ID 和实际选集由代码登记。清单按 ID 补充测试意图，不另外定义运行集。一般的 UT／LLT 只列入口。复杂场景按同一 Prompt 加参数展开，例如 OwnerFs／DFS 或旧到新／新到旧。

新增场景或修改业务断言时，随测试改动更新对应 ID 的 Prompt。已有场景改变运行环境或选集时，只改入口与证据。用例实跑记录绑定测试 SHA、产品与 SDK 制品、拓扑和清理状态。

## 五步验证路径

| 步骤 | 验证目标 | 入口 |
| --- | --- | --- |
| 1. 基础 UT／LLT | 模块逻辑、编码、错误传播和接口合同 | Linux 上的 `make afs-test`、`cargo test --locked -p afs`。包、feature 和 ignored 范围见[测试总规范](test-strategy.md#step-1) |
| 2. 单 VM 基础集成 | 真实 Meta、Node、FUSE／bind 的数据和权限语义 | [组件入口](../../afs/tests/run.py) 的 `local-basic`、`local-extended` 等按能力选集 |
| 3. 单 VM 多 namespace 集成 | 隔离角色之间的 TCP／mTLS、跨节点访问和恢复 | 同一组件入口的 `cluster-basic`、`cluster-manyread` 等选集。拓扑与参数见[组件 README](../../afs/tests/README.md) |
| 4. Buildkite 门禁 | 对候选提交和制品执行共享回归 | [Buildkite](../../.buildkite/README.md) 的 Base、默认 Full 和显式专项通过现有分派调用用例 |
| 5. 固定包验收 | 用特定版本重跑完整路径、故障、长稳、性能及新旧兼容 | [Kubernetes E2E](../../build/e2e/kubernetes/README.md) 或 [Docker E2E](../../build/e2e/README.md)，用制品身份、断言和清理回执判定 |

步骤 2／3 描述组件验证拓扑，步骤 4／5 描述集成和固定包验收方式。写入一条测试逻辑后，本地和 Buildkite 应复用该条断言。不同拓扑分别留证，例如一台 Linux VM 内的 Docker 节点与跨 Kubernetes worker 节点。

## 运行选集与证据

| 入口 | AFS 范围 | 已有实跑 |
| --- | --- | --- |
| `agent-dx` Base，`ADX_WITH_AFS=1` | 公共 L0／auth 之后，5 项 AFS 组件冒烟、OwnerFs／DFS 各 6 个 SDK 断言 | [Base #294](https://buildkite.com/agent-dx/agent-dx/builds/294) |
| `agent-dx-full-test`，无定向参数 | 11 组公共 E2E，29 项 `ci-full` AFS 组件，OwnerFs／DFS 各 8 个 SDK 断言 | [Full #105](https://buildkite.com/agent-dx/agent-dx-full-test/builds/105) |
| `ADX_E2E_TARGET_CASE=reliability` | 由组件登记的 OwnerFs／DFS 重启、DFS 有界同步、必需副本恢复和 close 失败退役 | [可靠性 #104](https://buildkite.com/agent-dx/agent-dx-full-test/builds/104) |
| 构建完成后固定制品定向跑 | `ADX_E2E_ARTIFACT_BUILD`／`ADX_E2E_ARTIFACT_COMMIT` 固定产品。各测试入口分别列明确的专项或用例选择参数 | 见本页对应场景 |

上述 Base、默认 Full、可靠性实跑共用 `be16d724` 产品。选集成员以 [组件 smoke 与可靠性阶段配置](../../build/e2e/kubernetes/afs_component.py)、[ci-full 配置](../../afs/tests/suites/ci_full.py)和 [可靠性用例登记](../../build/e2e/afs_specials.py)为准。新用例的接线情况和实跑结果应分别留证。

### 公共 SDK 挂载用例

每个 ID 都分别用于 OwnerFs 和 DFS。Base 选前六项，默认 Full 再选后两项。共用 [SDK 探针](../../build/e2e/afs_sdk.py)和[挂载拓扑](../../build/e2e/kubernetes/afs_product.py)，组件创建 workspace 后通过公开 Python SDK 暴露给 Environment。

| 子项 ID | 编写测试的短 Prompt |
| --- | --- |
| `mount-identity` | 分别在 A、B 创建 Environment 并挂载对应 workspace，核对宿主与容器的设备号、inode 和文件系统类型。挂错目录即失败。 |
| `bidirectional-publish` | 在两端分别写不同正文、`fsync`、对端重新打开读全量。覆盖三个版本和 SDK Files API 二进制数据，错一个字节即失败。 |
| `permissions` | 用独立 UID 打开私有文件，要求 EACCES。工作进程有效 UID 和能力集须与测试预期一致。 |
| `readonly` | 把同一目录以只读挂载放进新 Environment，尝试创建文件，要求 EROFS。 |
| `remove` | 用 SDK 删除文件，对端重新打开应返回 ENOENT，并删除本轮其他测试文件。 |
| `environment-cleanup` | 记录本轮创建的 ID，逐一关闭和回收，等待目录状态消失，确认两端 backend 无遗留。 |
| `namespace-identity` | 保持旧 FD，依次 rename、复用原文件名和 unlink。已打开 FD 要保留旧 inode 与旧正文。 |
| `truncate-visibility` | 在 A 连续写不同长度含零的版本并 `fsync`，B 新打开核对长度和内容。截短后残留尾部即失败。 |

## 组件用例 Prompt（122 项）
核对基线是 GitCode `master` 的 `3904266542ab39bf5ca47658296a81932f6f52bd`。从该 Git 树解包 `afs/tests`，运行 `python3 -B afs/tests/run.py list`，枚举 122 个组件 ID（116 功能含故障和 POSIX，6 测量）。下面按业务主题归档，每个 ID 恰好一行。`本地定向` 表示组件 CLI 已登记可选择，运行仍须满足对应拓扑、权限、制品和工具要求。

共用 Prompt 前置条件是复用 `Cluster`、冻结产品和测试资产、真实执行并保留可观测结果。以下每行附具体动作和预期。版本、挂载、行为、错误码或清理偏离预期即失败。Linux 本机执行可用 `python3 afs/tests/run.py run --case ID --env /path/environment.json --binary-dir /path/frozen-binaries --work-root /path/new-evidence`，故障、性能另看套件前置条件。

表中 `Base／Full／可靠性` 来自实际选集。它们是执行集合，可以同含一个 ID。现有运行证据为 `be16d724` 的 [Base #294](https://buildkite.com/agent-dx/agent-dx/builds/294)、[可靠性 #104](https://buildkite.com/agent-dx/agent-dx-full-test/builds/104)、[默认 Full #105](https://buildkite.com/agent-dx/agent-dx-full-test/builds/105)。其他 ID 以静态登记为起点，有明确实跑再随版本和环境记录。

### 组件功能、恢复与资源（35）

正文代际或 inode 错误、异常成功、远端计数为零、FD 未回收或挂载残留均应红。

| 用例 ID | 现有 CI 选集 | 短 Prompt |
| --- | --- | --- |
| [`cluster.topology_cleanup`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/cluster/test_multivm.py#L4) | Base／Full | 启动声明的 Meta／Node 角色及挂载，核对实际拓扑，按次序回收进程和挂载，残留即失败。 |
| [`dfs.basic.one_writer_many_readers`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L39) | Full | A 分三代写入并 fsync，B/C 读回各版本并校验，核对真实远端 RPC 和删除后的可见性。 |
| [`dfs.basic.namespace`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L92) | Full | 对真实 DFS 挂载执行创建、列举、截断、重命名及删除，核对两端元数据、正文和 ENOENT。 |
| [`dfs.cluster.read_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L39) | Base／Full | A 发布并 fsync 三代数据，B 逐代核对大小和 SHA，确认 peer RPC 实际增加，删除后的路径最终不可见。 |
| [`dfs.cluster.visibility`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L171) | Base／Full | 从 A 写入不同长度的三代正文，在 B 新打开两次，核对大小、内容和权限变化后的拒绝与恢复。 |
| [`dfs.cluster.restart`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L200) | Full／可靠性 | 保留持久目录正常重启角色，核对跨节点读回和 inode/属主/权限不变。独立角色再检查单 Meta 重启及 R1 唯一 Home 停机时的冷读拒绝。 |
| [`dfs.cluster.many_readers`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L39) | Full | 在 M/A/B/C 拓扑中让 B/C 并发读 A 的多个已确认版本，各自核对内容和远端访问计数。 |
| [`dfs.recovery.normal_restart`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L200) | Full | 保留 DFS 持久状态正常停启，重挂载后读回旧数据，核对 inode、属主、权限和尺寸保持。 |
| [`dfs.resources.reader_fd_budget`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/fd_budget.py#L100) | Full | 逐个发布和读完超过缓存容量的 chunk，确认旧 FD 淘汰、最多 256 个缓存 FD，重复读同一对象不新增 FD。 |
| [`dfs.semantics.visible_after_commit`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L61) | 本地定向 | A 两次 fsync 并关闭后，B、C 新打开并逐次读到完整最新内容。 |
| [`dfs.semantics.open_reader_after_commit`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L70) | 本地定向 | 在 B 预先打开文件，A 提交新版本后让 B 的旧 FD 和新 FD 分别读回各自版本。 |
| [`dfs.semantics.unlink_open_file`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L92) | Full | B 先持有读 FD，A unlink。新按路径查询在 TTL 预算内 ENOENT，B 旧 FD 仍读回原正文。 |
| [`dfs.semantics.close_mtime`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L105) | 本地定向 | A append 后直接 close，B 新 OPEN 的 fstat 立即看到新大小/mtime，路径 stat 在属性缓存预算内收敛。 |
| [`dfs.semantics.uncached_path_visibility`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L180) | 本地定向 | 关闭内核路径缓存。A 创建、改名、chmod、unlink 后，B 立即按路径观察到对应状态。 |
| [`dfs.semantics.fsync`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L141) | 本地定向 | 在无故障环境执行 fsync 并从另一节点读回已确认内容，写成功和持久同步结果分别核对。 |
| [`dfs.semantics.locks_local_only`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L149) | 本地定向 | 同一文件在 A、B 各自取得排他锁，验证跨节点不承诺互斥。 |
| [`dfs.semantics.atomic_save_cross_node`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L178) | Full | A 关闭临时文件后用 rename 原子替换目标，B 每次新打开都读到完整对应版本。 |
| [`dfs.semantics.write_then_rename_before_close`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_semantics.py#L194) | Full | A 保持写 FD 时改名、继续写再关闭，B 读到两段完整内容并检查旧路径状态。 |
| [`dfs.visibility.new_open_permissions`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L171) | Full | B 新打开两次校验每代内容。A 改 mode 后验证其他用户及未授权打开被拒，恢复权限后正文可读。 |
| [`dfs.visibility.real_cross_handle_cache`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/real_cache.py#L50) | Full | 同 inode 发布 V2，分别用旧 FD、第一次新 FD 和后续打开验证版本与页缓存，并实测读路径证据。 |
| [`dfs.cache.same_version_reopen`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/real_cache.py#L54) | Full | 跨 FD 重开同一版本并逐段读全文，确认 warm reuse 保持同一版本和文件内容。 |
| [`dfs.cache.warm_permission_change`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/real_cache.py#L58) | Full | 先暖缓存再改权限，验证旧 FD 仍可读而未授权新 OPEN 返回拒绝，恢复权限后内容仍正确。 |
| [`fuse.contracts.ordering`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/fuse/test_contracts.py#L23) | Full | 用同源冻结内核测试 ELF 对真实 FUSE 验证写和 truncate 序列，错序、坏内容或零匹配都失败。 |
| [`fuse.contracts.real_dfs_cache`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/fuse/test_contracts.py#L35) | Full | 运行精确选择器验证实际 DFS FUSE 跨句柄缓存语义，并核对版本、权限与真实执行计数。 |
| [`fuse.contracts.busy_unmount`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/fuse/test_contracts.py#L39) | Full | 保留文件句柄发起普通卸载，确认 busy 拒绝且挂载未被强拆，释放后可正常卸载。 |
| [`ownerfs.basic.read_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L39) | Full | 在 Home 的 ext4 bind 与对端 FUSE 间双向写、fsync、读回和删除，比较完整正文。 |
| [`ownerfs.extended.namespace_identity`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L387) | Full | 让两个节点竞争同名根，确认唯一成功、唯一 serving Home 和唯一实体目录。 |
| [`ownerfs.basic.namespace`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L92) | Full | 在 OwnerFs 真实挂载上验证文件与目录的创建、列表、截断、重命名、删除和错误语义。 |
| [`ownerfs.basic.remote_dir_close_fd`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L310) | Full | 重复远端 opendir/readdir/releasedir，采样 Home 同目录 FD，稳定退出后不增加持有句柄。 |
| [`ownerfs.cluster.read_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L39) | Base／Full | 在隔离的 Home bind 和远端 FUSE 上交替发布与读回，核对正文、fsync 和删除。 |
| [`ownerfs.cluster.namespace`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L387) | Full | 在相互隔离的节点同时创建同名根，核对恰有一个成功及唯一 Meta Home 和原生目录。 |
| [`ownerfs.cluster.permissions`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L119) | Base／Full | 跨节点用 owner 与另一数字 UID/GID 访问同一目录和文件，核对授权路径与拒绝错误。 |
| [`ownerfs.cluster.restart`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L200) | Full／可靠性 | 保留持久目录并正常重启所有角色，核对正文和 inode/属主/权限保持。再单独重启 Meta，并核对 Home 停机和恢复时的冷读。 |
| [`ownerfs.permissions.deny_other_uid`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L119) | Full | 用两个数字身份访问同一路径，分别验证 owner 成功和其他用户被拒绝。 |
| [`ownerfs.recovery.normal_restart`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/operations.py#L200) | Full | 保留 OwnerFs 持久状态正常重启后重挂载，核对确认写入，以及 inode、属主、权限、尺寸保持。 |

### DFS POSIX 行为边界（28）

名字、句柄、可见性或具体 errno 偏离当前已记录语义应红。对声明不支持的操作，误成功也应红。

| 用例 ID | 现有 CI 选集 | 短 Prompt |
| --- | --- | --- |
| [`dfs.posix.fallocate_is_not_supported`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L51) | 本地定向 | 真实挂载逐个调用六种 fallocate 模式，所有调用均须 EOPNOTSUPP。 |
| [`dfs.posix.seek_data_hole_treat_the_file_as_data`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L63) | 本地定向 | SEEK_DATA 按数据处理文件内偏移，SEEK_HOLE 返回 EOF，超出末尾的 SEEK_DATA 返回 ENXIO。 |
| [`dfs.posix.readdir_lists_every_entry_with_its_type`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L76) | 本地定向 | 混合创建多类目录项并列举，逐项核对完整集合和类型。 |
| [`dfs.posix.file_locks_exclude_on_one_node_only`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L114) | 本地定向 | 验证同节点竞争锁互斥，另一节点仍可以获取本地锁。 |
| [`dfs.posix.user_xattrs_round_trip`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L134) | 本地定向 | 用真实 FUSE 写、读、列举、移除 user xattr，检查长度、正文和移除后的错误。 |
| [`dfs.posix.other_xattr_namespaces_are_not_supported`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L158) | 本地定向 | 尝试其他 xattr 命名空间，明确核对不支持行为。 |
| [`dfs.posix.mknod_regular_and_device_nodes`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L180) | 本地定向 | 经挂载创建常规和设备节点，核对类型、权限及设备号语义。 |
| [`dfs.posix.fifos_and_sockets_are_node_local`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L205) | 本地定向 | A 上 FIFO 与 socket 本地通信成功。B 的 FIFO 读端不能使 A 非阻塞写成功，A 得 ENXIO。B 跨节点 socket 连接得 ECONNREFUSED。 |
| [`dfs.posix.hard_links_and_symlinks`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L234) | 本地定向 | 硬链接共享 inode、远端删原名后仍读到正文。symlink 4095 字节成功，过长返回 ENAMETOOLONG，空目标返回 ENOENT。 |
| [`dfs.posix.rename_flags`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L253) | 本地定向 | NOREPLACE 冲突返回 EEXIST，EXCHANGE/WHITEOUT 返回 EINVAL，随后 NOREPLACE 和普通覆盖仍按预期执行。 |
| [`dfs.posix.direct_io_needs_no_alignment`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L271) | 本地定向 | 用非对齐地址、长度或偏移测试 direct I/O，校验字节及当前对齐契约。 |
| [`dfs.posix.append_on_one_and_two_nodes`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L281) | 本地定向 | 分别在同节点和两节点追加，检查各记录的实际顺序／集合和数据完整。 |
| [`dfs.posix.mmap_shared_write_is_close_to_open`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L299) | 本地定向 | 通过共享 mmap 改写，检查同步和另一端新打开后看到的内容。 |
| [`dfs.posix.copy_file_range_copies_or_falls_back`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L311) | 本地定向 | 尝试 copy_file_range 或明确走 fallback，核对最终目标长度与字节。 |
| [`dfs.posix.unsupported_ioctls`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L324) | 本地定向 | 调用不支持的 ioctl，按契约检查明确 errno。 |
| [`dfs.posix.timestamps`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L338) | 本地定向 | 经挂载修改时间戳，核对 stat 返回值和各操作影响的时间字段。 |
| [`dfs.posix.ownership_and_privilege_bits`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L359) | 本地定向 | 执行真实 chown/chmod 和非特权写入，核对 owner/group 和权限特殊位语义。 |
| [`dfs.posix.setgid_directory_gives_new_files_its_group`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L383) | 本地定向 | 在带 setgid 的目录创建文件，核对子文件 group 继承。 |
| [`dfs.posix.statfs_reports_the_local_disk`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L409) | 本地定向 | 从各节点测 statfs 并与本机独占设备参数对照。 |
| [`dfs.posix.supplementary_group_members_can_change_a_group_writable_directory`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L420) | 本地定向 | 用持有补充组的非 owner 更改组可写目录，与无组对照核对权限。 |
| [`dfs.posix.truncate_extends_with_zeros`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L446) | 本地定向 | 扩展文件并重开，检查原区间不变、扩展范围读到零。 |
| [`dfs.posix.name_and_path_limits`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L453) | 本地定向 | 创建边界长度的名字和路径，核对允许值、拒绝值及 errno。 |
| [`dfs.posix.large_sparse_offsets`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L464) | 本地定向 | 在大偏移写入稀疏文件并重开，核对大小、洞区与已写入的完整内容。 |
| [`dfs.posix.directory_fsync_and_tmpfile_and_poll_and_access`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L480) | 本地定向 | 目录 fsync 成功，O_TMPFILE 返回 EOPNOTSUPP，普通文件 poll 就绪，access 依身份返回，O_PATH 可 fstat 但读返回 EBADF。 |
| [`dfs.posix.xattrs_on_special_files`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L504) | 本地定向 | 覆盖特殊类型节点的 xattr 行为，使用实际挂载验证支持或拒绝结果。 |
| [`dfs.posix.open_file_survives_replacement`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L521) | 本地定向 | 打开旧对象后替换同名文件，证实旧 FD 与新路径分别读取原内容与新内容。 |
| [`dfs.posix.exclusive_create_of_a_taken_name_is_eexist`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L550) | 本地定向 | A/B 并发排他创建同一批 300 个名字，要求恰好 300 次成功和 300 次 EEXIST。 |
| [`dfs.posix.plain_create_of_a_name_that_just_appeared_opens_it`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_posix_boundary.py#L590) | 本地定向 | A/B 同时使用普通 O_CREAT 打开同一批 300 个名字，两边所有调用成功，最后只有 300 个唯一名字。 |

### DFS 读写专项（20）

用错 CRC 模式、预取未触发或关闭后驻留、吞掉 strict fsync 失败、丢保留字节应红。

| 用例 ID | 现有 CI 选集 | 短 Prompt |
| --- | --- | --- |
| [`dfs.read.local.ranges_crc_off`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L36) | 本地定向 | A 发布后 B 范围读，完整核对 SHA 和实际 peer RPC，发送／接收 CRC 扫描均为 0。 |
| [`dfs.read.local.ranges_crc_receiver`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L36) | 本地定向 | A 发布后 B 范围读，完整核对 SHA 和实际 peer RPC，接收校验扫描必须有正文。 |
| [`dfs.read.local.prefetch_release`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L59) | 本地定向 | B 按 direct I/O 顺序读并验证实际远端来源和全文，预取必须被触发，关闭后 active／total 归零。 |
| [`dfs.read.local.cached_reuse`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L83) | 本地定向 | B 首读、同 FD 重读和重新打开均核对全文，确认未触发 direct-only 预取。 |
| [`dfs.read.cluster.ranges_crc_off`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L36) | 本地定向 | A 发布后 B 范围读，完整核对 SHA 和实际 peer RPC，发送／接收 CRC 扫描均为 0。 |
| [`dfs.read.cluster.ranges_crc_receiver`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L36) | 本地定向 | A 发布后 B 范围读，完整核对 SHA 和实际 peer RPC，接收校验扫描必须有正文。 |
| [`dfs.read.cluster.prefetch_release`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L59) | 本地定向 | B 按 direct I/O 顺序读并验证实际远端来源和全文，预取必须被触发，关闭后 active／total 归零。 |
| [`dfs.read.cluster.cached_reuse`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_read_special.py#L83) | 本地定向 | B 首读、同 FD 重读和重新打开均核对全文，确认未触发 direct-only 预取。 |
| [`dfs.write.local.cross_handle_order`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L53) | 本地定向 | 在同一文件多 FD 重叠写并同步，跨节点对照完整最终正文以检测写入错序。 |
| [`dfs.write.local.sync_append_close`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L57) | 本地定向 | 连续执行同步、追加和关闭，另一节点核对发表内容与各阶段持久性判据。 |
| [`dfs.write.local.bounded_sync_progress`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L61) | 本地定向 | 在 64 MiB 写队列限制下持续写和同步，检查有界等待、推进与正文。 |
| [`dfs.write.local.required_replica_recovery`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L104) | 本地定向 | strict 两副本下停 B，确认 A 收到数据但 fsync 失败。B 恢复后重试，验证两份物理副本完全一致。 |
| [`dfs.write.local.failed_close_retirement`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L126) | 本地定向 | strict 两副本下停 B 并在同步失败后关闭句柄。B 恢复后核对新写及先前保留数据最终可读。 |
| [`dfs.write.local.degraded_replica_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L108) | 本地定向 | 按降级策略停 B，A 仍能持久写并读回。B 恢复后验证物理副本一致和远端读回。 |
| [`dfs.write.cluster.cross_handle_order`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L53) | 本地定向 | 在同一文件多 FD 重叠写并同步，跨节点对照完整最终正文以检测写入错序。 |
| [`dfs.write.cluster.sync_append_close`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L57) | 本地定向 | 连续执行同步、追加和关闭，另一节点核对发表内容与各阶段持久性判据。 |
| [`dfs.write.cluster.bounded_sync_progress`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L61) | 可靠性 | 在 64 MiB 写队列限制下持续写和同步，检查有界等待、推进与正文。 |
| [`dfs.write.cluster.required_replica_recovery`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L104) | 可靠性 | strict 两副本下停 B，确认 A 收到数据但 fsync 失败。B 恢复后重试，验证两份物理副本完全一致。 |
| [`dfs.write.cluster.failed_close_retirement`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L126) | 可靠性 | strict 两副本下停 B 并在同步失败后关闭句柄。B 恢复后核对新写及先前保留数据最终可读。 |
| [`dfs.write.cluster.degraded_replica_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_write_special.py#L108) | 本地定向 | 按降级策略停 B，A 仍能持久写并读回。B 恢复后验证物理副本一致和远端读回。 |

### DFS 故障定向（33）

故障未发生、坏数据被交付、引用存活时被 GC 删除、应恢复的物理副本缺失，均应触发该用例判据。

| 用例 ID | 现有 CI 选集 | 短 Prompt |
| --- | --- | --- |
| [`dfs.fault.gc_lost_participant`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 隔离一个 GC 参与者，检查 GC 回执和保留安全，任何仍引用对象被删都失败。 |
| [`dfs.fault.s3_reclaim`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 在隔离 MinIO 写入再删除／覆盖，核对可回收对象真的删除，有引用的正文仍能读。 |
| [`dfs.fault.bad_replica_reported`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 破坏测试副本，让接收端 CRC 读触发坏副本上报，核对 Meta 的状态记录。 |
| [`dfs.fault.repair_wiped_node`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 只擦独占测试节点副本状态，触发修复，按内容检查恢复后的原副本目标。 |
| [`dfs.fault.repair_third_node`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 让旧副本不可用，核对新增第三节点获得完整目标副本。 |
| [`dfs.fault.bad_replica_never_delivered`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 破坏一个测试副本，从客户端读并验不能收到坏正文，保留错误或正确替代结果。 |
| [`dfs.fault.posix_rename_append`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 在真实挂载执行 rename 和 append 的 R20 组合，核对最终正文和 POSIX 错误。 |
| [`dfs.fault.front_write_admission`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 用原生写队列触发准入边界，核对接受量和拒绝或进度回执。 |
| [`dfs.fault.long_direct_reads`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 对真实挂载做持续 direct read，核对已知正文及异常回执。 |
| [`dfs.fault.big_directory_listing`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 生成固定大目录，逐项核对列举完整性并记录耗时。 |
| [`dfs.fault.disk_full_enospc`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 填满独占小 tmpfs，验证写路径返回 ENOSPC 和收尾。 |
| [`dfs.fault.rename_after_close`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 关闭写 FD 后立即 rename 并跨节点读，核对路径和最终正文。 |
| [`dfs.fault.fresh_owner_read`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 在 R1 冷态从真实 Owner 数据路径读确定内容，核对新鲜度和校验。 |
| [`dfs.fault.restart_during_patrol`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 在副本巡检时重启或失去测试节点，核对巡检及后续副本收敛。 |
| [`dfs.fault.degraded_write_killed_replica`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 终止 B 后按降级策略写入并 fsync，恢复 B 后核对修复副本。 |
| [`dfs.fault.degraded_write_hung_replica`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 冻结 B 后按降级策略写入并 fsync，恢复 B 后核对修复副本。 |
| [`dfs.fault.sdk_client_crash`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 杀死只属于本测试的原生 SDK 客户端，检查服务存活及后续可写读回。 |
| [`dfs.fault.s3_deleter_lost`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L135) | 本地定向 | 让 S3 删除方离线，确认仍被引用对象保持可读并记录回收状态。 |
| [`dfs.fault.s3_deleter_stuck`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L128) | 本地定向 | 阻塞 S3 删除方，确认仍被引用对象保持可读并记录超时和处置。 |
| [`dfs.fault.latent_bad_replica`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 制造潜伏坏副本并触发实际读取或巡检，记录探测与修复结果，禁止坏数据被读为成功。 |
| [`dfs.fault.pinned_reader_under_gc`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 跨 GC 保持打开的读句柄，确认被固定的旧版本在读取结束前仍正确可读。 |
| [`dfs.fault.scrub_restores_bad_replica`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L123) | 本地定向 | 启用后台 scrub，损坏独占副本并验证巡检标记和修复后的完整数据。 |
| [`dfs.fault.meta_backend_redis`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L144) | 本地定向 | 用隔离 Redis Meta 执行停止、重启、隔离和后端丢失阶段，核对拒绝或恢复与持久状态。 |
| [`dfs.fault.meta_backend_etcd`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L144) | 本地定向 | 用隔离 etcd Meta 执行停止、重启、隔离和后端丢失阶段，核对拒绝或恢复与持久状态。 |
| [`dfs.fault.local_commit_reader_under_gc`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 持有读 FD，跨本地及远端提交和 GC 后核对当前关联版本没有被回收。 |
| [`dfs.fault.lapsed_lease_reader_under_gc`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 让仍打开的写句柄租约过期，同时保留读 FD，核对 GC 未删仍须读回的版本。 |
| [`dfs.fault.r84_strict_upload_failure`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 注入严格副本上传失败，核对失败返回和已确认内容状态，查明恢复后是否可重试。 |
| [`dfs.fault.r84_local_upload_failure`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 在测试节点的本地写路径注入盘满，核对上传失败和已确认数据状态。 |
| [`dfs.fault.soft_reader_race`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 错开读登记和同步重写／GC，覆盖打开边界，核对固定读者的数据和版本。 |
| [`dfs.fault.reader_lease_lapse`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 冻结测试节点令读会话过期，再恢复，核对读者固定版本与回收安全。 |
| [`dfs.fault.meta_restart_reader`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 保持打开读 FD 后重启 Meta，核对读者可恢复且正文保持一致。 |
| [`dfs.fault.reader_partition`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 隔离读角色和 Meta 等待租约过期，再恢复网络，核对读者行为和 GC 安全。 |
| [`dfs.fault.unlinked_reopen_reader`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_faults.py#L111) | 本地定向 | 删除文件名后经 `/proc/self/fd` 重开，验证仍持有的正文不会被错误回收。 |

### 性能测量入口（6）

缺真实样本、冷热未佐证、角色未参与、完成字节不足或 OOM 的相位标记失败或不可用。测量入口以合格样本为验收，不包含默认数值 SLA。

| 用例 ID | 现有 CI 选集 | 短 Prompt |
| --- | --- | --- |
| [`bench.env.localfs`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/bench/test_bench.py#L222) | 本地定向 | 用 fio 在独占 ext4 上测 fsync 写、驱逐后冷读和驻留后热读。冷热标签必须由实际驻留样本支持。 |
| [`bench.env.network`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/bench/test_bench.py#L394) | 本地定向 | 用 A 的 iperf3 客户端连接 B 的一次性服务，核对阶段完成、实际流量与同核标记，保留原始样本。 |
| [`bench.dfs.sequential`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/bench/test_bench.py#L263) | 本地定向 | 用 fio 测 A 的 fsync 写、B 的首读与热重读，用真实缓存和后端观测判层。证据不足的热读标记未证实。 |
| [`bench.dfs.fanout.many_readers`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/bench/test_bench.py#L298) | 本地定向 | 让独立角色的 elbencho 服务并发整文件读。核对参与主机数、总字节等于文件大小乘读者数，并保留读路径和服务退出证据。 |
| [`dfs.performance.sequential_write`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_performance_assets.py#L31) | 本地定向 | 固定字节数和请求粒度，测量真实挂载顺序写、同步和正文校验，记录耗时及资源环境。 |
| [`dfs.performance.sequential_read`](https://gitcode.com/openJiuwen/agent-dx/blob/3904266542ab39bf5ca47658296a81932f6f52bd/afs/tests/suites/dfs/test_performance_assets.py#L35) | 本地定向 | 固定字节数和请求粒度，在真实挂载顺序读并校验摘要，记录耗时和缓存条件。 |

## 固定制品兼容专项

<a id="version-storage"></a>
### 落盘、滚动和回退

| 用例 ID | 编写测试的短 Prompt | 入口与证据 |
| --- | --- | --- |
| `{ownerfs,dfs}.version.upgrade_rollback` | 旧版生成并 `fsync` 独特正文，记录 SHA256、长度、属主和 mode，停写并正常停止 M/A/B 后冻结恢复点。启动新版读旧数据、写新区，再回退到旧快照。验证旧正文原样可读、快照后的写入消失、旧版还能再写。错用当前数据目录必须在恢复判定报红。 | [版本切换](../../afs/tests/tools/version_switch.py)，`afs-component-upgrade-rollback` 定向。[DFS 对照 #142](https://buildkite.com/agent-dx/agent-dx-full-test/builds/142) · [OwnerFs 对照 #143](https://buildkite.com/agent-dx/agent-dx-full-test/builds/143) · [错用当前目录 #121](https://buildkite.com/agent-dx/agent-dx-full-test/builds/121)／[#122](https://buildkite.com/agent-dx/agent-dx-full-test/builds/122) |
| `ownerfs.version.old_root_command` | 用旧版删除 workspace 留下一条未确认命令。换新版，用同名 workspace 连续建删三轮并核对 epoch 单调增加。重启新版 Meta 后检查旧命令残留。 | [版本切换](../../afs/tests/tools/version_switch.py)，[实跑 #110](https://buildkite.com/agent-dx/agent-dx-full-test/builds/110) 仍读到一条旧命令 |
| `dfs.version.rolling_e2e_preserve` | A 持续作为同一会话写入。M 已切新版、B 离线时，覆盖原 inode 并 `fsync`。新版 B 恢复副本后停 A，B 从新挂载冷读校验。完成滚动，再原地回退且保留已确认写入。 | 隔离测试分支 `test/afs-live-upgrade-e2e-20261122`，[#157](https://buildkite.com/agent-dx/agent-dx-full-test/builds/157) 通过 |
| `ownerfs.version.rolling_e2e_full_snapshot` | 新 B 纳管目录，旧 A 走 gRPC 写入并 `fsync`，核对 Home、epoch 和本地文件。换已有 workspace 的旧 Home A，验证 catalog 启动及持续业务。恢复旧快照后核对既有数据和快照后文件去向。 | 同上，[#158](https://buildkite.com/agent-dx/agent-dx-full-test/builds/158) 带业务混跑及快照恢复通过，完整滚动在旧 catalog 解码处失败 |
| `dfs.version.midway_preserve` | M、B 已换新版且离线期间已有确认覆盖写时中止。保留当前落盘状态，只回退 M、B 二进制。逐文件核对已确认正文，再写入和跨节点读回一笔新数据。 | 隔离测试分支 `test/afs-midway-rollback-e2e-20261122`，[#160](https://buildkite.com/agent-dx/agent-dx-full-test/builds/160) 通过 |
| `ownerfs.version.midway_ownerfs` | 新 B 已纳管且有后续确认写入，换回旧 B。旧版明确拒绝时，比较拒绝前后 catalog、正文、inode、权限和 Meta 根身份。再从旧快照恢复，检查数据回退范围。 | 同上，[#159](https://buildkite.com/agent-dx/agent-dx-full-test/builds/159) 核实旧版明确拒绝且快照回退通过 |

<a id="version-protocol"></a>
### Node–Meta 与 Node–Node

复用 [协议矩阵](../../afs/tests/tools/mixed_protocols.py)及[定向入口](../../build/e2e/kubernetes/afs_upgrade.py)。两侧独立固定 `AFS_E2E_BASELINE_BUILD`／`AFS_E2E_BASELINE_COMMIT` 和产品制品。每条使用独立 Meta 状态、M/A/B 和真实挂载。对已启动角色核对运行二进制 SHA256。

| 用例 ID | 编写测试的短 Prompt | 已有证据 |
| --- | --- | --- |
| `ownerfs.protocol.files.<layout>` | 确认 B 注册且 A 是 workspace Home。B 读 A 的文件后写入，由 A 回读。双方读取、写入 gRPC 字节计数应增足正文长度，任意一个字节错误应报红。 | [混跑 #123](https://buildkite.com/agent-dx/agent-dx-full-test/builds/123) · [改坏正文 #146](https://buildkite.com/agent-dx/agent-dx-full-test/builds/146) |
| `dfs.protocol.peer-read.<layout>` | 用单副本让 A、B 各写一份独立文件，由另一端第一次打开。同时验 SHA256 和 reader 的远端读计数。核对注册、会话与可适用的心跳，普通读写后重启 B 得到新会话。 | [混跑 #123](https://buildkite.com/agent-dx/agent-dx-full-test/builds/123) · [注入注册失败 #145](https://buildkite.com/agent-dx/agent-dx-full-test/builds/145) |
| `dfs.protocol.replica.<layout>` | 严格双副本，A 写入并 `fsync` 后停止 A，再从 B 第一次打开文件并验证正文。B 没有实际可用副本应报红。 | [混跑 #123](https://buildkite.com/agent-dx/agent-dx-full-test/builds/123) |
| `{ownerfs,dfs}.protocol.admission.{old-meta,new-meta}` | 只启动 M 和 A。换入异协议 B，核对 `50659329`（`FailedPrecondition`）、未挂载、无会话和无 DFS 副本落点。DFS 使用两副本且允许一份确认的降级策略。A 保持 PID，读回旧文件和 `fsync` 后的新文件。换回同协议 B 并确认就绪，再验证 OwnerFs gRPC 双向和停止 A 后的 DFS 三笔 B 副本冷读。 | `AFS_E2E_PROTOCOL_ADMISSION=old-meta\|new-meta`，[#163](https://buildkite.com/agent-dx/agent-dx-full-test/builds/163) · [#164](https://buildkite.com/agent-dx/agent-dx-full-test/builds/164) |

`<layout>` 的六个后缀为 `all-old`、`all-new`、`new-meta`、`old-meta`、`old-source`、`new-source`。18 项实跑配对是 #288 和 #294，同为协议号 10，覆盖 gRPC。#280／#294 的协议号是 7／10，实跑覆盖准入拒绝和恢复。带新能力的混合请求、RDMA 的版本扩展字段须另作验收。

<a id="version-posix-sdk"></a>
### POSIX 与独立 Python SDK

| 用例 ID 或选集 | 编写测试的短 Prompt | 入口与证据 |
| --- | --- | --- |
| `{ownerfs,dfs}.version.posix` | 在旧版、换新版和回到旧版三阶段执行同一组真实 FUSE 操作。验 rename 后旧名消失、截短精确、新打开权限错误 EACCES、unlink 和复用文件名后旧 FD 仍读旧字节。故意忽略截断应只在指定版本阶段报红。 | 历史隔离测试补丁 `AFS_E2E_VERSION_POSIX=1`，[对照 #125](https://buildkite.com/agent-dx/agent-dx-full-test/builds/125) · [实版本 #131](https://buildkite.com/agent-dx/agent-dx-full-test/builds/131) · [故障 #134](https://buildkite.com/agent-dx/agent-dx-full-test/builds/134) |
| `afs-sdk-{ownerfs,dfs}` 的 `common-sdk-and-afs` | 分别装固定 rc8、rc12 wheel，对冻结 #280、#294 服务做四组配对。核对 wheel 摘要和实际导入目录，验证 Environment create/get/reattach/delete、挂载身份、跨节点 `fsync` 后读回及 Files API。OwnerFs 查 Home/gRPC，DFS 停 A 后读 B 的独立副本。 | [共享兼容探针](../../build/e2e/afs_sdk_compat_probe.py)，`afs-sdk` 定向，[#170](https://buildkite.com/agent-dx/agent-dx-full-test/builds/170) 至 [#173](https://buildkite.com/agent-dx/agent-dx-full-test/builds/173) |
| `afs-sdk-{ownerfs,dfs}` 的 `recursive-false` | 在父目录缺失时发送 rc12 的 `recursive=False`。要求明确的不支持错误或非递归的 ENOENT，父、子目录均应不存在。超时、权限错误、意外成功或目录副作用都报红。原默认递归仍须工作。 | [新版对照 #173](https://buildkite.com/agent-dx/agent-dx-full-test/builds/173) · [旧端实际报红 #172](https://buildkite.com/agent-dx/agent-dx-full-test/builds/172) |

隔离分支和产品的提交独立保存。旧端明确拒绝、夹具缺条件、产品断言失败和清理错误分别记录。OwnerFs 的部分 POSIX 版本场景受旧 catalog 打开失败阻挡，不能用同版对照替代两版验证。

#### SDK 兼容用例复跑

选择一对已冻结的服务端与 SDK 构建。当前兼容试点允许 #280／#294 的四个组合。其他版本对接时明确记录制品、旧布局和可用能力。校验服务端 bundle、registry manifest 和对应镜像。SDK wheel 与 `sdk-candidate.json` 来自 SDK 构建，由该清单和固定摘要共同校验。

- Buildkite 使用 `agent-dx-full-test` 的 `ADX_E2E_TARGET_CASE=afs-sdk` 和 `ADX_WITH_AFS=1`。`ADX_E2E_ARTIFACT_BUILD`／`ADX_E2E_ARTIFACT_COMMIT` 指向服务端，`AFS_E2E_SDK_COMPAT_BUILD`／`AFS_E2E_SDK_COMPAT_COMMIT` 指向 SDK。Build 参数传 Buildkite 构建 UUID。
- Linux 直跑使用现有 Docker 固定包入口。SDK 目录只需对应的 wheel 和候选清单，服务端镜像归档用 [镜像导出工具](../../build/e2e/export_images.py) 生成并验原 registry digest 和 image ID。执行卷需要 Linux FUSE、ext4 和已有 Docker／cgroup 前置能力。输入以对应架构的冻结包为准。

```bash
AFS_E2E_SDK_COMPAT_BUILD="$sdk_build_uuid" \
AFS_E2E_SDK_COMPAT_COMMIT="$sdk_commit" \
AFS_E2E_SDK_COMPAT_DIR="$sdk_directory" \
python3 -B build/e2e/run.py --profile standalone --case afs-sdk \
  --bundle "$server_bundle" --registry-images "$server_registry_manifest" \
  --image-export "$server_export_manifest" --artifact-commit "$server_commit" \
  --cgroupns host --output "$new_output_directory"
```

对齐时逐组合比较 `case-results.json`、两模式的 `sdk-result.json`、DFS 的 `sdk-replica.json` 和各层清理。观察 AFS 与 SDK 摘要、阶段状态、正文／副本摘要和新增参数的实际副作用。Docker 同宿主与 K8s worker 的拓扑证据分别保留。已知 rc12→#280 的参数用例应保留原始失败结果。

2026-11-22 使用测试提交 `87ce77f2` 验证上述入口。独立 Linux x86_64/Docker 与 Buildkite 的四组 SDK 结果一致，共有操作 8/8 通过，rc12→#280 在两种文件系统的新增参数均报红。另用同一 #294 制品直接运行组件冒烟和可靠性，共 10 项与 Buildkite 一致，清理通过。 [rc8 接新版 #179](https://buildkite.com/agent-dx/agent-dx-full-test/builds/179) · [rc12 接旧端 #178](https://buildkite.com/agent-dx/agent-dx-full-test/builds/178) · [组件冒烟 #183](https://buildkite.com/agent-dx/agent-dx-full-test/builds/183) · [可靠性 #182](https://buildkite.com/agent-dx/agent-dx-full-test/builds/182)

## 固定包的其他验收入口

| 用例或能力 | 编写测试的短 Prompt | 运行范围 |
| --- | --- | --- |
| `afs-sdk-restart` | 两种 AFS 模式分别确认后台命令已启动，暂停 Adxlet 期间做同步写读。重启后确认新 PID／会话、相同 backend、下一笔新进展及原数据。 | [Docker 定向](../../build/e2e/afs_specials.py)，固定 Base236 有实跑 |
| `afs-sdk-pressure` | 固定 4 并发 × 8 写 × 64 KiB，验证对端全量内容、EEXIST 和准确计数。DFS 核对远端读证据，回收所有 Environment。 | 同上，固定 Base236 有实跑 |
| `afs-component-specials` | 按登记复跑重启、同步进度、副本恢复、失败 close 退役，核对被选中阶段实际执行且正常清理。 | [专项编排](../../build/e2e/afs_specials.py)。组成用例沿用组件 ID |
| 连续运行／长稳 | 固定拓扑及候选，持续写读、命令与 Environment churn，到时才能判通过。记录实际时长、内容、进展、FD／RSS／盘、错误和取消时回收。 | [固定包 soak](../../build/e2e/soak.py)。时长和后端以每次回执为准 |
| 性能 bench 与对照 | 固定硬件、拓扑、负载、预热与持久屏障。记录尾延迟、吞吐和资源。校验正文和实际路径，zero sample 必须失败。 | [DFS 和 Meta bench](../../build/e2e/afs/perf/README.md)。环境不同或只跑 smoke 时仅记录该范围 |
| 标准 POSIX／Linux 兼容 | 按选定的 pjdfstest／LTP／FSx／随机种子验返回码和内容，报告总数、预期跳过和真实执行。0 项或环境不符要明确。 | [标准套件入口](../../build/e2e/afs/acceptance/README.md)。旧适配器的 ARM64 和 Full 环境限制见入口 |

`afs-sdk-restart` 的阶段 `confirmed-before-restart`、`workload-during-control-pause`、`adxlet-session-changed`、`backend-identity-preserved`、`confirmed-after-restart`、`publish-after-restart` 依次核对确认正文、暂停期间进展、两端实际会话更新、原 backend 身份、恢复后正文和新写入。压力阶段 `bounded-concurrent-publish`、`exclusive-create-error`、`bounded-workload` 分别核对完整共 32 笔读写内容、errno 17 和实际并行数与预算。两套专项的挂载身份和清理沿用 SDK 挂载判据，DFS 还核对实际 peer read 计数。 [阶段登记](../../build/e2e/afs_specials.py)

历史固定包 Docker Full 验证了当时 24 项组件和 16 项 SDK 断言。当前 29 项组件 Full 的已验收证据来自 Buildkite #105。OwnerFs v4 有 7,200 秒的固定包运行证据。DFS 同条件正式长稳尚无通过证据。Pod 替换、物理宿主故障、外部 Meta／对象存储和 RDMA 分别需要环境与证据。 [固定包验收与长稳记录](test-strategy.md#step-5)

标准套件登记包含历史计划和已有 driver。下面七项有 driver，完成实际运行才更新结果。其余 `TODO` 以及不适用项保留在原登记中。 [标准场景登记](../../build/e2e/afs/acceptance/cases.json)

| 标准套件 ID | 编写测试的短 Prompt |
| --- | --- |
| `STD-01` | 在实际挂载跑 pjdfstest，按用例留 TAP 结果与预期失败清单。断言返回码变化和实际新增失败。 |
| `STD-02` | 在实际挂载执行选定 LTP 文件系统子集，记录每组计数、失败和跳过，核对执行量和白名单。 |
| `STD-03` | 固定 FSx 参数与种子，比较数据与长度。保留失败前最后一段操作序列用于复放。 |
| `STD-04` | 固定种子生成同一组文件操作，对照本地基准与 AFS 的结果、errno 和最后文件状态。 |
| `STD-05` | 对总数、实际执行量、跳过和错误状况做审计，空集和未知结果必须判失败。 |
| `FUN-13` | 检查挂载在声明的 namespace 中可见，隔离角色不能看到未授权挂载，并核对回收。 |
| `OPS-01` | 轮询真实部署的健康端点，验角色身份、就绪及依赖状态，保留失败响应。 |

版本相关设计约束见 [AFS 前后版本兼容设计规则](afs-version-compatibility.md)。

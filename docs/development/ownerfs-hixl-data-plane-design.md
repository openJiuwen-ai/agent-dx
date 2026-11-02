# OwnerFs HIXL 只读预读窗口设计

状态：第 7 版（2026-10-31）。本版取代第 1–6 版的“跨句柄在途读聚合”方案，并已按本文
落地软件实现；第 9 节记录已执行的验证与仍缺失的真机证据。第 2 节是工作树事实，
第 3 节之后是设计。阅读时以符号名为准，行号会随 rebase 漂移。

## 1 结论先行

OwnerFs 远端读接入 HIXL 的单位改为**每次 open 独立的只读预读窗口**：

- 只对远端只读打开（`O_RDONLY`，非 `O_PATH`/`O_DIRECT`）的句柄生效；可写句柄、
  `data_mode = rdma/auto` 和 `ownerfs_read_mode = off` 的读路径保持原样。
- READ 未命中窗口时，reader 发一次 `PrepareReadHixl`，请 Home 以**该 open 句柄**
  读取 `[offset, offset + W)`，校验与普通 `OwnerFiles.Read` 相同；Home 把读到的字节
  切成若干个 HIXL 导出槽，逐槽计算 BLAKE3。reader 用一次 native batch READ 拉回全部
  槽，逐槽校验后释放，再把整段放进该句柄的窗口，随后的 FUSE READ 在窗口内本地复制。
- 窗口大小随顺序访问翻倍（一个 HIXL 槽起步，上限 `ownerfs_window_bytes`）；
  `ownerfs_max_inflight = 2` 时在消费当前窗口后半段时后台预取下一窗口。
- `preferred` 下小文件、非顺序读和 dispatch 前的 `Unsupported`/槽位不足仍走原 gRPC
  单区间读；`required` 不回退。

## 2 已核对的现状

### 2.1 远端 gRPC 读链路

| 步骤 | 行为 | 位置 |
| --- | --- | --- |
| 1 | `AfsFuse::read` → `Backend::read`；OwnerFs 不覆盖 `supports_parallel_reads`，同一 FUSE 句柄的 READ 走独占队列 | `fuse.rs`、`vfs.rs` |
| 2 | `LocalOwnerFs::read` 持 per-handle `Mutex`，`OpenFileHandle::Remote` 分支调用 `RemoteFiles::read`，使用 open 时保存的 `RootGrant` | `ownerfs.rs` |
| 3 | `OwnerPeerClient::read` 先查 per-open `prefetched_reads`，未命中时发一次 unary `OwnerFiles.Read`；单次长度上限 1 MiB（`MAX_TRANSFER_BYTES`） | `rpc/peer.rs` |
| 4 | Home 的 `OwnerFilesService::read` 认证 peer，`OwnerFsPeerHandler::read` → `OwnerFsPeerExecutor::read` → `LocalOwnerFs::peer_read`（`validate_peer` + 句柄归属检查）→ 本地 `read_at` | `rpc/data.rs`、`ownerfs.rs` |

gRPC Home 回复显式清空 `data_checksum`，客户端只在非空时校验；现有 gRPC 读不做内容校验。

### 2.2 已存在的 per-open 快照先例

Home 在只读、非 `O_PATH`/`O_DIRECT`、不超过 4096 字节的 OPEN 回复中附带全部内容；
`OwnerPeerClient` 把它保存在以 opaque handle 为键的 `prefetched_reads`，release 时删除，
从不跨 open 复用。该只读 open 在其生命周期内读到的是 OPEN 时刻的快照。

### 2.3 DFS HIXL 的收益来源

DFS 实测中，HIXL 只有在单次控制往返搬运多 MiB 时才明显优于 TCP：组读
（128 MiB 组）加组预取（`dfs_read_group_max_inflight = 2`）在 1 GiB 顺序读上带来约
+50–66%，收益来自把源端 Prepare 与 FUSE 消费重叠；8 MiB 级非组批次只有约 +25%。
每个 HIXL 批至少有 Prepare 与 Release 两次控制往返，外加一次 native READ。

### 2.4 可复用的 HIXL 设施

- `HixlRuntimeGroup`：固定 Host 槽、代际、quarantine、`prepare_export_shared`、
  `read_batch_timed_into`（一次 native 调用多个窗口，每窗口占一个接收槽，单窗口
  ≤ `window_bytes`）。
- `rpc/data/hixl.rs` 的 `ExportProvider` / `PendingExport`（RAII 回滚）。
- `rpc/peer/hixl.rs` 的 `ReadRuntime`/`ReadLane` native 边界、`engine_transport_parameters`、
  并行校验池 `validation_pool`。
- `HixlMetrics`、`runtime_error`、`HIXL_PROTOCOL_VERSION`。

## 3 为什么放弃第 6 版的跨句柄在途聚合

| 第 6 版设定 | 问题 |
| --- | --- |
| 合并不同句柄已发起的 READ，等待 1 ms 组批 | 单流顺序读（并发 1）没有可聚合成员，每次 READ 反而多出 Prepare + Release 两次往返和 1 ms 等待，必然慢于 gRPC；并发 8 时批也只有 8 × ≤1 MiB，处在 DFS 收益最小的区间 |
| 逐 range 授权、逐 range 失败结果、整批 Fallback 广播 | 需要聚合线程、跨句柄锁顺序、广播回退和非 `'static` 缓冲交接；复杂度高，且换不来大块传输 |
| N2：不做预取与 Node 侧内容缓存 | 恰好排除了 DFS 已证明的唯一大杠杆（大窗口 + 预取） |

第 7 版把“一次控制往返搬运多少字节”作为首要指标：顺序流的窗口可达 64 MiB，
单流也能获得收益；并发度不再是前提。

## 4 设计决策

### D1 配置：`[hixl]` 下的 OwnerFs 字段

```toml
[hixl]
ownerfs_read_mode = "off"          # off | preferred | required
ownerfs_window_bytes = 16777216    # 单次 Prepare 的最大窗口
ownerfs_max_inflight = 1           # 1 串行；2 预取下一窗口
ownerfs_host_budget_bytes = 268435456
ownerfs_min_file_bytes = 1048576   # preferred 下 open 时文件不小于此值才启用
```

| 字段 | 约束 |
| --- | --- |
| `ownerfs_read_mode` | 非 off 时要求 Node、OwnerFs、`data_mode = "grpc"`、具体单播 `engine_id`、mTLS 与 `trusted_node_certs`；`required` 还要求编译 `hixl` feature。HIXL runtime 位于 `dfs` feature 内，未编译 `dfs` 时拒绝启用 |
| `ownerfs_window_bytes` | `window_bytes` 的整数倍，`window_bytes..=64 MiB`，且不超过 `min(receive_slots, export_slots) × window_bytes` |
| `ownerfs_max_inflight` | 1 或 2，默认 1 |
| `ownerfs_host_budget_bytes` | ≥ `ownerfs_window_bytes × ownerfs_max_inflight`，≤ 4 GiB |
| `ownerfs_min_file_bytes` | ≤ 1 TiB；`required` 忽略该阈值 |

`data_mode` 与 `ownerfs_read_mode` 是不同的轴：RDMA/auto 与 OwnerFs HIXL 互斥。
DFS 的 `dfs_read_mode` 与 OwnerFs 模式各自独立；任一非 off 时启动共享 HIXL runtime。

### D2 资格与一致性

- 资格在 OPEN 回复时确定：只读、非 `O_PATH`/`O_DIRECT`，且 `required` 或文件大小
  ≥ `ownerfs_min_file_bytes`。资格状态以 opaque handle 为键保存在 `OwnerPeerClient`，
  release 时删除，与 `prefetched_reads` 的生命周期一致。
- 语义：**窗口内容是 Home 执行该次 Prepare 读取时的结果**，在该 open 的生命周期内
  可被复用。这与 OPEN 预取一致，满足 close-to-open：open 之前已关闭的写入一定可见；
  open 之后其他节点的写入不保证对已缓存窗口可见。
- 同一 `OwnerPeerClient` 上任一句柄成功写入或 setattr 某文件后，失效同一
  `FileIdentity` 的全部窗口与预取，保持本节点的读己之写。
- 跨窗口不承诺原子一致；并发截断时返回窗口读取时的短读/EOF。
- 只有窗口记录 EOF 时才向调用方返回短读；跨窗口边界的 READ 会继续取下一窗口，
  不会把窗口边界误报为 EOF。

### D3 窗口大小与预取

- 句柄记录下一顺序偏移（初值 0）与上次窗口大小。命中顺序偏移的未命中按
  `max(window_bytes, 2 × 上次)` 翻倍，上限为 `ownerfs_window_bytes`、协商后的远端 lane
  导出槽容量与本地接收槽容量中的最小值；请求本身更长时至少覆盖请求。
- 非顺序未命中：`preferred` 交给原 gRPC 读并重置翻倍；`required` 只取请求长度。
- `ownerfs_max_inflight = 2` 时，消费越过当前窗口一半且未到 EOF，后台预取紧接的下一
  窗口。下一次未命中恰好落在预取起点时等待它；seek、写入失效或 release 会丢弃预取结果
  （后台任务仍自行完成 Release，不留下 Home 槽）。预取失败只丢弃，前台按普通路径重取。
- 进程级 `ownerfs_host_budget_bytes` 计入所有已缓存窗口和在途预取；超额时不做预读：
  `preferred` 回退 gRPC，`required` 只取请求长度（不计入预算，最多 1 MiB × FUSE worker）。

### D4 回退边界

| 情况 | `preferred` | `required` |
| --- | --- | --- |
| 本地 runtime 缺失、Home 未启用 OwnerFs HIXL（dispatch 前 `NODE_TRANSFER_UNSUPPORTED`） | 回退 gRPC | 失败 |
| Prepare 返回槽位/预算不足，未提交 native READ | 回退 gRPC，不关闭会话 | 失败 |
| 授权、句柄失效、权限错误 | 原样返回，不回退 | 同左 |
| checksum 不符、协议违例 | 返回错误并关闭会话 | 同左 |
| native READ 结果未知、Release 失败 | 返回错误，关闭会话；本地结果未知时隔离本地 runtime | 同左 |

回退发生在 `OwnerPeerClient::read` 内部：已从窗口复制的前缀保留，余下部分调用原
gRPC 读完成，不重入 `LocalOwnerFs::read`。

### D5 授权与会话

- `NegotiateHixl` 携带一个 `RootAccess`，Home 用 `OwnerFsPeerExecutor::authorize_transport`
  核验该 peer 当前持有有效 grant；会话只是传输租约，不授予文件访问。
- 每个 `PrepareReadHixl` 携带自己的 `RootAccess` 与 `OwnerHandle`，Home 走与
  `peer_read` 相同的 `validate_peer`、RemoteFile scope 与句柄归属检查后才读文件。
- 会话绑定 mTLS 认证的 peer Node ID、Home 的 `home_session_id` 与 reader engine 身份；
  Home 重启后旧会话不存在，reader 发现 `home_session_id` 变化或会话未知即重新协商。
- 每个 Home 一个 reader 会话，同一时刻只准入一个窗口获取（与 DFS 每 peer 单读一致）。

### D6 资源共享（D7-A）

OwnerFs 与 DFS 共享同一个 `HixlRuntimeGroup`、Host 槽池与 quarantine 失败域；一方占满
导出/接收槽会使另一方 Prepare 失败，任一方的未知结果会使整个 runtime 停止准入。
性能对照须在 OwnerFs-only 或暂停 DFS 流量时进行。

## 5 协议（`node_data.proto`，`OwnerFiles` service）

Owner HIXL 与 DFS 共用 `HIXL_PROTOCOL_VERSION = 1` 的 native 窗口语义，复用
`DfsHixlTransport`、`DfsHixlLane`、`DfsHixlReleaseOutcome` 三个传输层类型。

```proto
rpc NegotiateHixl(OwnerNegotiateHixlRequest) returns (OwnerNegotiateHixlReply);
rpc PrepareReadHixl(OwnerPrepareReadHixlRequest) returns (OwnerPrepareReadHixlReply);
rpc ReleaseHixlRead(OwnerReleaseHixlReadRequest) returns (OwnerReleaseHixlReadReply);
rpc CloseHixl(OwnerCloseHixlRequest) returns (OwnerCloseHixlReply);
```

- Negotiate：reader 报告 engine 身份、协议版本、transport 参数与 lane 数；Home 回复
  `session_id`、`home_session_id` 与有序 lanes（engine、代际、窗口、导出槽）。
- Prepare：`session_id`、`read_id`（reader 生成、同一 attempt 的重试保持不变）、
  `RootAccess`、`OwnerHandle`、`offset`、`length`、`lane_id`。回复 `read`（实际字节，
  `< length` 即 EOF）、`eof` 与按文件顺序连续的槽列表（槽 ID、代际、地址、长度、
  BLAKE3）。`read = 0` 时没有槽，也不需要 Release。
- 同一 `(session, read_id)` 的重复 Prepare 先重新授权，再返回原回复；请求不同则拒绝。
  reader 只对 Unavailable/DeadlineExceeded 重试同一请求，最多 3 次。
- Release：`session_id`、`read_id`、`engine_generation`、结果（Completed/NotSubmitted
  回收全部槽；Unknown/Failed 保留并 quarantine）与 `content_validated`。重复 Release
  由有界 tombstone 应答 `already_released`。
- Close：停止该会话准入，回复是否已无待释放槽；不声称 DMA 已排空。

错误分类以 AFS business code 为准：`NODE_TRANSFER_UNSUPPORTED`（能力缺失）、
`NODE_TRANSFER_RESOURCE_EXHAUSTED`（槽位/预算，读源数据前判定）、`NODE_OWNER_*`
（授权/句柄）、`NODE_TRANSFER_UNAVAILABLE`（会话未知或已冻结）。

## 6 Home 侧服务

`rpc/data/owner_hixl.rs` 的 `OwnerHixlService`：

- Prepare 在阻塞线程内执行：先在注册表锁内校验会话与容量，锁外以 executor 循环读取
  到 staging（进程级 staging 预算 256 MiB，超额返回 resource exhausted），并行计算逐槽
  BLAKE3，再回到锁内逐槽 `prepare_export_shared`，任一槽失败用 `PendingExport` 回滚；
  回滚失败即 quarantine。staging 在导出后立即释放，槽内字节在 Release 前不变。
- 会话 30 s 无活动关闭；关闭时仍有未释放槽、Release 报告未知结果、或停机 drain 超时，
  都使共享 runtime 停止准入并保留资源。
- 每 5 s 过期扫描；Node 停机时与 DFS 源端一起 drain（5 s 内接受 Release）。

## 7 Reader 侧

- `rpc/peer/owner_hixl.rs` 的 `OwnerHixlReader` 进程级共享：每 Home 一个控制会话槽、
  engine 地址绑定、peer→lane 绑定、窗口预算与指标。
- `OwnerPeerClient::with_hixl` 注入 reader；只有注入且 `data_mode = grpc` 时，open
  记录资格、read 走窗口逻辑、write/setattr 做失效、release 清理状态。未注入时这些
  分支全部跳过，现有路径不变。
- 窗口获取在独立 Tokio 任务中执行并拥有全部缓冲与租约；调用方超时只返回错误，
  任务继续完成 Release/隔离，并标记会话需重建。
- Release 在后台执行并持有该 Home 的租约，下一次获取在其完成后才准入。

## 8 指标与健康

- `afs_ownerfiles_payload_bytes_total{plane="hixl"}`：client 侧为从 HIXL 窗口交付给
  调用方的字节，server 侧为 Prepare 读取并导出的字节。
- `afs_ownerfs_hixl_reads_total{outcome}`：`window_hit`、`fetch`、`prefetch_hit`、
  `fallback`，用于证明窗口命中与回退比例。
- 共享 `afs_hixl_operation_duration_seconds` 等 runtime 指标（D6）。
- `/health` 的 `checks.hixl` 增加 `ownerfs_mode`。

## 9 验证状态

已执行（本地 WSL Ubuntu 22.04，默认 feature，fake native 边界，未编译 `hixl` feature）：

- Home 服务（`rpc/data/owner_hixl/tests.rs`）：协商授权与 lane/协议校验、Prepare 分槽与
  BLAKE3、EOF 与空读、陈旧句柄/他人会话在读源数据前拒绝、相同请求 replay、导出池与在用槽
  容量拒绝、导出失败回滚、Release 与 tombstone、Unknown 结果与 native Release 失败的
  quarantine、带未释放槽 close 的 quarantine、drain 超时与随后的释放。会话 30 s 过期路径
  沿用与 DFS 相同的注册表逻辑，未单独测试。
- Reader（`rpc/peer/owner_hixl/tests.rs`，真实 tonic OwnerFiles 服务回环）：顺序读窗口
  4→8→16 KiB 翻倍、跨窗口边界续读、EOF 短读、窗口命中计数、`preferred` 非顺序读走 gRPC
  后转入 HIXL、小文件与可写打开保持 gRPC、Home 不支持时 `preferred` 回退与 `required`
  失败、checksum 不符不交付、窗口在本次打开内为快照且失效入口（写入/setattr 调用的同一
  `invalidate_hixl`）后重新获取、`ownerfs_max_inflight = 2` 的预取命中。
- 配置校验（含 off 模式不受 OwnerFs 约束）、`/health` 字段，以及 afs 全量 lib 与集成测试回归。

A3 真机（2026-10-31，二进制 `df24475d`，`afs-node` sha256 `a58afc65…`）：

- 216 上 `cargo build --release -p afs --bins --features hixl`、
  `cargo clippy -p afs --all-targets --features hixl -- -D warnings` 通过，`--features hixl`
  下 owner_hixl/hixl/crc32c/配置/健康相关 167 个测试通过。
- 216 运行 Meta 与 node-a（Home），217 运行 node-b（reader），两端 `hccs-buffer-pool`、
  单设备、4 MiB 槽、16 个收发槽、`ownerfs_window_bytes = 16 MiB`；驱动为
  `out/validation/owner_hixl_func.py`（仓外验证工具）。

| 运行 | 结果 | 关键计数（reader） |
| --- | --- | --- |
| `required`，inflight 1 | 顺序读、1 MiB `dd`、64 次随机 `pread`、close-to-open 改写均与 Home 一致 | HIXL 交付 566.6 MB，gRPC 读 0；fetch 123，window_hit 8613；native 647.0 MB |
| `preferred`，inflight 1 | 同上 | fallback 64，gRPC 读 23.4 MB（随机读与 200 KiB 小文件），HIXL 543.2 MB |
| `required`，inflight 2 | 同上 | prefetch_hit 34，fetch 89 |

三轮结束时 Home `afs_hixl_export_slots_in_use = 0`，两端 `afs_hixl_quarantined_bytes = 0`，
停止后无遗留 AFS 进程与 FUSE 挂载。观察到 native 拉取量比交付量多约 80 MB：随机 `pread`
之后的内核 readahead 被视为顺序续读并打开 4 MiB 窗口，这部分预读未被消费，留待性能阶段评估
（例如要求连续两次顺序读才开窗）。

### 9.1 吞吐与分阶段对照（2026-11-02）

环境同第 9 节，HCCS 中转、单设备、4 MiB 槽、16 收发槽。数据集为 1 GiB 随机内容，写入一次；
每轮按轮转顺序重启两端节点，先做 1 MiB 预热读（不计时），再在 reader 上 `dd bs=1M` 读一遍。
OwnerFs 不在 open 之间保留页缓存，因此每轮都是冷读；指标在计时读后立即采样，每轮数据面恰为
1025 MiB。单流 4 轮、并发 4 路 3 轮，配置在同一轮内交替以抵消共享宿主机漂移。

| 配置 | 单流（MiB/s，中位[min..max]） | 并发 4 路 |
| --- | --- | --- |
| gRPC（`ownerfs_read_mode = "off"`） | 404.5 [363.5..498.2] | 665.0 [660.5..724.6] |
| HIXL，16 MiB 窗口，`max_inflight = 1` | 666.6 [652.1..697.8] | 759.8 [720.6..866.3] |
| HIXL，16 MiB 窗口，`max_inflight = 2` | 742.1 [691.9..892.7] | 743.9 [733.0..763.0] |
| HIXL，64 MiB 窗口，`max_inflight = 2` | 900.9 [835.4..1140.9] | 863.0 [816.7..871.9] |

分阶段（每窗口平均，单流；64 MiB 一列同为单流，最后一列为并发 4 路）：

| 阶段 | gRPC / 1 MiB | HIXL 16 MiB | HIXL 64 MiB | HIXL 64 MiB，4 路 |
| --- | --- | --- | --- | --- |
| reader 请求/窗口耗时 | 2.144 ms | 22.320 ms | 55.821 ms | 103.382 ms |
| Home 处理 | 0.529 ms | 12.332 ms | 34.849 ms | 21.775 ms |
| 其中 Home 读源文件 | 未单列 | 7.839 ms | 23.143 ms | 14.631 ms |
| native 传输 | — | 2.918 ms | 5.920 ms | 4.149 ms |
| BLAKE3 校验（reader） | — | 3.270 ms | 5.604 ms | 4.387 ms |
| Release | — | 0.473 ms | 0.694 ms | 0.604 ms |

结论：

- gRPC 的单流上限由每 1 MiB 一次往返决定（2.14 ms ≈ 468 MiB/s），Home 处理只占 0.53 ms；
  4 路并发把在途请求提到 4，聚合约 700 MiB/s。
- HIXL 的 native 传输只占每窗口 3–6 ms（5.5–11 GB/s）；瓶颈在 Home 的读源与 BLAKE3/导出，
  以及每窗口一次的 prepare 往返。窗口从 16 MiB 提到 64 MiB 把单流从 742 提到 901 MiB/s。
- 并发 4 路时收益收窄：reader 对同一 Home 只持有一个会话租约，窗口获取在 Home 串行，
  每窗口墙钟从 22 ms 涨到 67 ms（16 MiB）与 103 ms（64 MiB），多出的是排队。gRPC 随并发
  线性获益，相对优势因此从 2.2× 降到 1.3×。
- 限制：单机对、单设备、单 reader，Home 页缓存命中（数据刚写入）；共享宿主机轮间 max/min
  达 1.37，只能比较同轮交替的中位数。64 MiB 窗口需显式配置。

未执行：多设备 lane、Home 冷盘读、随机/小文件吞吐、故障注入与资源泄漏复测。
在这些证据出现前，不得把上述数字外推到其他拓扑。

建议验收：

| 用例 | 内容 |
| --- | --- |
| PERF-10 | 1 GiB 单文件顺序读，`ownerfs_read_mode = required` 对 gRPC，`max_inflight` 1/2 分档，三轮 `max/min ≤ 1.10` |
| PERF-11 | 1,000 × 256 KiB 与 10,000 × 4 KiB：证明小文件留在 gRPC（`fallback`/资格计数）且无回归 |
| RDMA-04 | `afs_ownerfiles_payload_bytes_total{plane="hixl"}` 与 `afs_hixl_reader_native_batch_bytes_total` 随读取增长，gRPC 只承载控制 |
| RDMA-05 | 成功、取消、超时、断链、退出后槽位与会话清理，重复运行无增长 |

## 10 后续工作

- 每 Home 多会话（允许 Home 侧 Prepare 与另一窗口的 native READ 重叠）。
- Home 侧 file→slot 直填，去掉 staging 复制。
- 依据 PERF-11 结果调整 `ownerfs_min_file_bytes` 默认值。

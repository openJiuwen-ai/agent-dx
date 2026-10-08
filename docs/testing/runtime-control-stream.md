# Execd 主动控制连接验证

本次将运行时协作接入 Execd 主动建立的双向 gRPC 控制流。Ready、工作负载 checkpoint 请求与 handoff 状态通过事件发送，adxlet 不再通过 HTTP 轮询它们。配置、启用条件与网络限制见[部署说明](../deployment/runtime-control-stream.md)，消息及应答规则见[协议契约](../../platform/api/proto/runtime-control.md)。周期监控仍负责健康、资源和空闲检查，并可从缓存补处理被有界通知队列遗漏的 checkpoint 请求。

## 测试范围

| 用例 | 验证目标 |
|---|---|
| Ready 早于等待者 | Start 返回前注册的完整状态仍可被就绪等待者读取，不丢通知 |
| checkpoint 状态通知 | 无 HTTP 控制查询、无周期 monitor tick 即可接收请求并发送 Prepare／Finish |
| 断流重连 | 重连发送完整 pending 状态，完成应答沿用 operation ID，重复 Finish 收敛 |
| 凭证与归属 | 错误凭证、未授权执行不能注册；执行 token 绑定 Environment／Runtime／generation |
| 无效握手隔离 | revision 为零的 Hello 被拒绝，不替换当前健康连接 |
| 执行清理 | 删除后移除节点控制槽位，迟到连接不能复活旧执行 |
| 真实 Execd 进程 | 实际 gRPC、Unix `/checkpoint` 和 FIFO handoff 交互；成功应答等待 Finish |
| adxlet 创建与 checkpoint | 真实 Execd 在 Start 返回前主动 Ready；原生命周期串行执行，状态发布阻塞时 Unix 请求不提前成功，删除释放资源 |
| 恢复身份切换 | 真实 Execd 读取恢复环境后，按目标 generation／token 重新注册，旧执行退役 |

adxlet 和组件检查还覆盖：控制器执行时拒绝旧 generation 的排队通知，旧 stream 的迟到应答及断开不覆盖新 stream，请求取消释放待应答槽位，节点凭据文件权限及 sandboxd 环境不泄露节点 secret。暂停确认后端停止后清理旧控制槽位；后端停止结果未知时保留该状态，后续删除对账再清理。HTTP 控制回归仍保留。

## 复跑命令

```sh
cargo test -p adx-execd --test control_stream --test control --test control_http -j 2
cargo test -p adxlet -p adx-protocol -j 2
cargo test -p adx-deployment --test config -j 2
cargo clippy --workspace --all-targets --all-features -j 2 -- -D warnings
cargo fmt --all -- --check
python3 build/docs/check.py
```

日志：`out/ci/runtime-control-stream/`。新增接口不存在时的编译失败见 `red.log`；最终结果见 `green-complete.log`／`stream-complete.log`、`platform-complete.log`、`deployment-final.log`、`clippy-complete.log`。无效 Hello 和暂停槽位残留的独立 red 证据见 `invalid-hello-red.log`、`pause-retire-red.log`。

## 本次结果

- 双向控制流：9 项通过，包含 3 项真实 Execd 进程协作测试。
- adxlet／协议：174 项通过；原 Execd 控制／HTTP：14 项通过。
- 部署配置：27 项通过，包含示例解析和最终组件配置生成。
- workspace Clippy（所有 targets/features，警告即失败）、fmt、文档链接及 diff 检查通过。

## 验证边界

上述 Rust 测试使用真实 Execd 进程和网络连接，checkpoint 后端用 FIFO 与制品夹具模拟，Coordinator 持久化由可控 StateSink 模拟。另以当前工作树构建的 Linux ARM64 发布包执行真实 Standalone 与 Firecracker 回归，记录见下节。该本地验证不代表集群升级，也不是启动性能测量。未修改 sandboxd 协议或公开 SDK 接口；本轮修正 SDK 命令订阅的连续断流预算，Unix checkpoint 空 body 仍默认 600 秒。

## 真实后端 E2E 回归（2026-10-07）

运行环境为本机 `adx-fc` Lima ARM64 VM，Linux 6.8.0-142-generic，4 vCPU／6 GiB；系统盘初始 40 GiB，本轮因 MinIO 预留空间不足扩为 64 GiB；实际 KVM API 12、创建 VM 和 vCPU 均通过。源码基线 `aa697427bcc658106fa2fca5a7b1b57c830cba7f` 加本次未提交改动，manifest 的 dirty 为 true。源码快照、测试镜像、发布包及 kit 摘要保存于 `out/ci/runtime-control-stream/e2e/`；产品 Rust 构建使用 1.95，实际 SDK 为新构建的 0.1.0 wheel。

显式设置 `ADX_E2E_RUNTIME_CONTROL=1`，部署独立控制端口并使用重建的静态 Execd。复跑准备见 [FC 驱动说明](../../build/e2e/firecracker/README.md)。

- 两节点 Docker Standalone：最终统一包在 `standalone-006` 通过 11/11 组，包括 sdk、data-plane、lifecycle、auth、capacity、placement、local-first、node-failure、sandboxd-restart、restart、stop；缺失检查和清理错误均为空。该路径使用 API Server／Ingress 合进程，FC 路径使用独立 Ingress，两者的发布二进制均基于本次源码重新构建，最终结论以这份统一包为准。
- Firecracker：`firecracker-008` 使用最终修复包通过主动控制流专项 5/5、原 checkpoint 功能 19/19、节点生命周期 7/7，共 31 项。专项覆盖主动 Ready、真实 Unix checkpoint 的最终成功 ACK 与元数据持久化、源进程继续运行、reload 后身份切换、暂停后仅重启 adxlet 再恢复及删除清理。原矩阵还验证 S3 rootfs／挂载、独立资源 limit、镜像 entrypoint、受限网络、内存／PID／文件恢复、快照双克隆与回收、运行时重启、failover、Coordinator 失联降级与过期会话对账。最终 Environment 目录及物理后端为空，快照为 Deleted 且无引用，checkpoint 和 S3 制品清理检查通过。
- 网桥绑定：`firecracker-008/export/evidence/runtime-control-listener.json` 确认控制流监听和回连地址为 `10.231.16.1:19003`，`bound=true`。这是 adxlet 与 sandboxd 共享网络命名空间的实测结果，不代表可以绑定其他命名空间的网桥。
- 驱动检查：61 项通过。首次 FC 因夹具缺少管理员 Key 无法启动，新增配置回归先失败、修复后转绿；原租户仍为非管理员，测试凭据不导出。

本轮复用已有 sandboxd kit `efc201531d7e2e9d69505da151eb66084b61eebf`，全部 11 个制品摘要与 kit manifest 一致。该已有 kit 包含历史 `0001-use-nydus-s3-backend.patch`；本轮没有修改或重新构建 sandboxd，不能将它表述为无补丁上游版本的验证。

本地 FC 回归还发现命令订阅在 checkpoint 暂停期间收到临时 503 后直接失败。Ingress 现在将临时路由／下游连接失败转换为重连通知，权限和身份冲突保持拒绝；SDK 仅在收到真实命令状态后重置 30 秒连续断流预算。新增 Gateway 分类和 SDK 预算回归均先失败再修复；SDK unit 163 项及 56 个 subtests 通过。该修复不会重新提交或重新启动用户命令。

FC 重跑保留失败证据：`firecracker-003` 为临时订阅 503；`firecracker-004` 在 checkpoint 之前读到后台计数文件的空值，夹具改用原子 rename 发布计数；`firecracker-005` 对象上传触发 MinIO 的 `507 XMinioStorageFull`，Unix 接口返回失败，不能据此判控制流完成；`firecracker-006` 在 reload 后首个文件请求遇到旧绑定的 Relay 409。本轮只扩容选定测试 VM，不清理其他工作树或公共编译缓存；扩容后重新验证 KVM 和网络前置条件。前述场景在 `firecracker-008` 均通过。

扩容重启后，`standalone-004` 因宿主未加载 EROFS 模块，sandboxd 禁用 runc 后端，平台准备超时且 0/11 组执行。为选定测试 VM 加载 EROFS 并配置开机加载后，同一包在 `standalone-005` 通过；随后补齐链接 Gateway 的 API Server 等发布二进制重建，最终统一包在 `standalone-006` 再次通过。不能将该前置条件缺失计为控制流故障，也不能用前一份包的结果替代最终包验收。

另补充 reload 后首个数据请求的真实 H2／Relay 回归：旧执行 CONNECT 被拒绝后仅等待本实例路由增量（最多 50 ms），再打开一次新执行连接；没有增量则保持失败，租户或安全策略变化不能复用旧授权。该重试发生在应用请求字节发送之前。Coordinator 本次源码已经使用提交事件和 10 ms 合并窗口，200 ms 是恢复检查周期，不能将本轮 409 归因于旧的 200 ms 定时发布实现。

Gateway 最终 lib 检查 108 项通过、2 项忽略，包含上述 4 项真实 H2／Relay 路由恢复回归；workspace Clippy 和 fmt 通过。最终日志位于 `out/ci/runtime-control-stream/e2e/coherent-final-e2e.log`，证据目录为 `out/ci/runtime-control-stream/e2e/evidence/`：FC 导出在 `firecracker-008/export/`，Standalone 结果在 `standalone-006/`。源码与包身份见该目录的 `build-final-identity.json`、`bundle/bundle.json`，所有 29 个包文件校验通过。SDK wheel SHA256 为 `07bcfa92b97aa821e4ea0fcdfd9310732c0c69aa175b8c88636cba842d1310dd`。本次源码归档 SHA256 为 `037049cd5a3c0e6e7a8968eb3198e954f6be7bb6b75868d024e22d0f0ca7db0d`；文档补充发生在归档之后，产品代码与最终测试包一致。

## 全网段封禁回归（2026-10-08）

复用上述 Lima、KVM 和 sandboxd kit，在真实 SDK 创建请求中配置双向、任意协议、`0.0.0.0/0` 拒绝规则，优先级为用户可用最大值 `4294967294`；入站、出站及 DNS 默认动作均为拒绝。先验证 stateful，再在线切换 stateless。受控 TCP 目标先确认被阻断，临时添加窄范围允许规则后确认可访问，再恢复全拒绝并确认阻断，避免把目标服务不可达误判为策略生效。

首次 `firecracker-009` 在专项第 4 项失败：切换 stateless 后，SDK 提交出站探测命令发生读取超时，尚未进入第二次 checkpoint。原因是平台只允许 Execd 服务端口的入站流量，stateless 无连接跟踪，TCP 回包被用户全拒绝规则拦截。修复后，Execd 50090 和声明的公开 TCP 端口在 stateless 模式下使用按沙箱本地端口匹配的双向系统规则；stateful 仍使用入站规则和连接跟踪。主动控制连接继续只放行本节点 IPv4／控制端口的系统规则。规则及管理访问例外见[部署契约](../deployment/runtime-control-stream.md)。本轮未修改或重建 sandboxd。

新增 `stateless_policy_allows_execd_and_published_port_replies` 先在真实 gRPC／UDS 适配器测试中失败，修复后转绿，覆盖 Start 和运行期策略更新；adxlet 全套 162 项、workspace 全 targets/features Clippy 和 fmt 均通过。随后重建 adxlet，统一包的 29 个文件全部校验通过，与前一份包相比仅 `bin/adxlet` 内容变化。

- `firecracker-010`：主动控制流专项 **7/7**、原 checkpoint **19/19**、节点生命周期 **7/7**，共 **33/33**。专项验证全拒绝下 Ready、命令／文件访问、stateful 与 stateless Unix checkpoint 最终应答、reload、暂停后仅重启 adxlet 再恢复；恢复后普通出站仍被阻断。控制监听 `10.231.16.1:19003`，`bound=true`。
- `standalone-008`：同一统一包完成双节点 Docker Standalone **11/11 组**，包含数据面、生命周期、认证、调度、local-first、节点故障、sandboxd 重启、服务重启和停机；`missing_checks`、`cleanup_errors` 均为空。
- 最终清理：Environment 目录为空，sandboxd 库存仅表头；快照全部 Deleted 且无引用，checkpoint 文件剩余为零，S3 制品清理检查通过。

证据位于 `out/ci/runtime-control-stream/deny-all-20261008/`：失败日志为 `e2e.log`，测试 red／green 为 `stateless-red.log` 和 `stateless-green-final.log`，最终日志为 `final-e2e.log`；导出结果在 `evidence/firecracker-010/export/` 和 `evidence/standalone-008/`。源码归档 SHA256 为 `4ea45bba904da57386ba46fd7b93968198f5af198097269b5fb5273b453d95c7`，基线仍为上述提交加未提交改动；SDK wheel 摘要保持不变。报告补充发生在源码归档后，产品代码和测试驱动与验收包一致。

本次封禁实测范围是 IPv4／TCP；DNS 默认拒绝为配置事实，未单独执行 DNS 或 IPv6 拒绝矩阵。平台管理端口保留例外，因此“全网段封禁”不意味着关闭管理访问。该结果为本地真实后端回归，不代表 cn-north-4 集群验收。

发布前同步社区 `refactor` 至 `56b3b48`，保留新增可观测性改动，再对合并后的源码执行验证：adxlet 162、protocol 14、Execd 控制／HTTP／控制流 23、部署配置 27、Gateway 全特性 lib 110，共 336 项通过、0 失败，Gateway 另有 2 项忽略；workspace 全 targets/features Clippy 和 fmt 通过，日志为同一目录的 `release-check.log`。上面的真实后端 E2E 使用同步前验收包；这次合并后检查为源码组件验证，没有重新生产发布包或升级集群。

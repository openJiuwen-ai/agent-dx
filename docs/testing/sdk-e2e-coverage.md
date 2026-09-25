# Sandbox SDK 公开能力与端到端覆盖

本清单从 `adx_sandbox` 的公开导出反向检查端到端用例。当前 `__all__` 有 41 个名字：6 个
操作入口类、12 个错误类型、21 个数据／策略类型，以及 `resources()` 和
`SDK_CAPABILITIES`。`Commands` 与 `Filesystem` 不单独顶层导出，但由 `Sandbox` 属性返回，
也是用户接口。

操作数的统计口径是上述操作入口及其返回对象的每个公开方法／属性，加顶层
`resources()`；数据对象的本地校验和序列化不算独立 E2E 操作。构造参数和错误类型另外
统计。一个业务用例可以同时覆盖多个操作，但 JUnit 必须展示稳定的子用例 ID，不能只显示
`data-plane` 这一组名称。

`build/e2e/sdk_surface.py` 保存 66 个操作的处置结果，并把全部 66 个已支持操作逐项关联到稳定的
E2E case ID；静态测试会在 SDK 新增、删除或漏登记公开成员时失败。该映射只说明用例归属，
仍以对应 Standalone／Firecracker 实跑结果作为通过证据。

## 公开操作

| 公开对象 | 操作数 | E2E 覆盖 | 运行层级 | 说明 |
|---|---:|---:|---|---|
| `Sandbox` | 26 | 26 | Standalone + KVM FC | 普通生命周期、访问操作和 reverse tunnel 由 Standalone 验证；checkpoint、S3 rootfs/mount、入口继承、reload、运行期网络策略和 failover 由 FC 验证 |
| `CommandHandle` | 8 | 8 | Standalone | ID、归属、poll、同步／异步 wait、kill、stdin、EOF |
| `Commands` | 6 | 6 | Standalone | run/get/list/kill/send_stdin/close_stdin |
| `Filesystem` | 10 | 10 | Standalone | 文本／二进制、CRUD、深度目录、双向目录复制 |
| `Pty` / `PtySession` | 9 | 9 | Standalone | 创建、会话 ID、输入、EOF、resize、wait、终态和关闭 |
| `Shells` / `Shell` | 6 | 6 | Standalone | 创建／集合关闭、会话 ID、状态保持、退出码、同步／异步关闭 |
| `resources()` | 1 | 1 | Standalone | 两节点状态、CPU／内存／磁盘 capacity 与 allocatable、标签 |
| **合计** | **66** | **66** |  | 每个公开操作均有关联的真实 E2E case ID |

普通 runc Standalone 负责其中 55 个操作；KVM Firecracker profile 负责其余 11 个操作，包括
checkpoint/snapshot、入口继承、reload 和运行期网络策略。这里的“覆盖”要求
请求真实经过 API Server、Gateway、adxlet、sandboxd 和 Execd；SDK 单测不计入此表。

文件传输故障另有两个定向 Full 用例：`upload-response-cut` 在 Execd 已写入首块后切断应答，
SDK 查询实际偏移并沿同一上传 ID 继续；`download-response-cut` 在首个文件响应只返回部分字节后
断开，SDK 保留 `.part` 文件并用 Range 续传。两者都以最终 SHA256 校验完整性；
2026-09-25 在 cn-north-4 的双物理 worker 定向部署中通过。

### Reverse tunnel 集成

`Sandbox(upstream=...)` 创建请求携带 tunnel 配置，API Server 写入 Execd tunnel 端口，Ingress 将
`/tunnel/{instance}` 解析为 8765，Relay 复核实例绑定后连接 Execd，SDK `TunnelClient` 再把
请求转给本机 upstream。adxlet 不得给生产 Execd 注入隔离测试专用的
`EXECD_HTTP_ONLY=1`，否则 Execd 只启动命令／文件 HTTP listener，tunnel 握手会在 Ingress 返回 502。

Standalone r11 的 `reverse-tunnel.sdk-upstream-roundtrip` 从 Sandbox 内访问
`get_tunnel_url()`，经过 Execd、Relay、Ingress 和 SDK TunnelClient 到达 SDK 进程中的真实 HTTP
upstream，并校验响应体和路径。该项不是客户端序列化单测。

## 构造能力

公开构造参数比方法数更多，必须按语义验证，不能只证明 JSON 能发送。

| 能力 | 当前 E2E | 后续缺口 |
|---|---|---|
| image/runtime、CPU/内存、env/cwd、name、node_id、labels、affinity | 已覆盖 | 增加调度超时终态和 API 错误字段断言 |
| idle timeout、detached、close/kill/context manager | 已覆盖 | 增加活动续期防误回收 |
| port forwarding + 默认 `tls-token` | 已覆盖；Standalone r4 同时通过默认 TLS+Token 和每实例纯 TLS | 增加显式拒绝错误字段断言 |
| snapshot_id、restart_policy | FC 覆盖 | 快照分页、过期和被引用时延迟删除 |
| xpu | 仅校验和调度 UT；独立真实设备用例见 `build/e2e/device/`，尚无 GPU/NPU 实跑结果 | 需要真实 GPU/NPU worker |
| storage_mb | FC 用例验证独立 request/limit 可下发并启动 | 仍需写满边界、超限及回收 E2E |
| S3 rootfs、S3 EROFS mount、failover、inherit_entrypoint、network、独立 request/limit | 本地 KVM 已逐项实跑；r16 SDK 19/19，r17/r18 在已更新 package 上连续通过前 16 项后才触发已知双克隆网络故障 | 继续修复 ARM FC 双克隆网络问题并取得同一次 26/26 严格验收 |
| data_plane_security | Standalone r4 已通过每实例纯 TLS 与默认 TLS+Token 对照 | 增加非法安全模式和证书轮换场景 |
| extra_config | API→Environment→sandboxd 请求契约测试已覆盖 | 具体键的含义由 sandboxd/runtime 定义；按实际 runtime 增加语义 E2E |
| upstream | Standalone r11 实跑通过 | 增加断线期间在途请求续传和大响应的部署形态 E2E |

## 错误类型

| 错误 | 当前 E2E |
|---|---|
| `SandboxNotFound`、`PermissionDenied` | 已覆盖 |
| `CommandConflict`、`CommandNotFound`、等待超时返回 `RUNNING`／`WAIT_TIMEOUT`、重复 kill 返回 `False` | SDK 单测覆盖；端到端覆盖以对应运行记录为准 |
| `CommandSubmissionError` | `command-response-cut` 在 cn-north-4 双 worker 定向 Full 实跑通过：真实 `process.start` 应答被 TLS 代理切断，新 SDK 客户端以稳定命令 ID 查询结果并检查副作用仅一次；见本轮测试报告 |
| `CommandUnavailable` | `command-watch-unavailable` 和 `command-watch-query-unavailable` 均在 cn-north-4 双 worker 定向部署通过；后一项同时拒绝 Watch 与 `process.get`，验证结构化结果未知 |
| `CommandExpired` | `command-expiry` 先在旧 Execd 上复现过期记录错误地返回 `COMMAND_NOT_FOUND`；修复后在 cn-north-4 双 worker 定向部署通过，从未存在与已过期的命令 ID 分别返回 `CommandNotFound` 和 `CommandExpired` |
| `UnsupportedFeature` | `command-unsupported-feature` 在本轮双 worker 定向 Full 通过：真实 Execd 的 capability 应答由 TLS 代理删去 Watch 能力，SDK 在 `process.start` 前拒绝；健康客户端再以相同命令 ID 成功执行。真实旧版 Execd 的兼容部署另行验证 |
| `ResourceExhausted` | `command-registry-capacity` 在本轮双 worker 定向 Full 通过：node1 Execd 的 registry 上限设为 1，运行中的命令占满后第二个稳定命令 ID 被拒，释放后以原 ID 成功且副作用一次。实例容量排队不替代此用例 |

## 当前可执行子用例

`data-plane` 组现有 17 个稳定子用例：资源目录、创建查询、两种 stdin/EOF、同步与异步等待、
命令恢复、幂等重放／冲突、not-found／wait-timeout、两种 kill、Shell 状态与会话退出终态、文件文本／二进制／
目录复制、PTY 输出／交互、端口转发和 reverse tunnel 往返。`lifecycle` 有 4 个子用例：detached 显式删除、close
保留远端、context manager 删除和空闲回收。`placement` 的 6 条规则也以独立 JUnit 用例展示。

`sdk` 组至少展示创建查询、命令结果、二进制文件和删除清理 4 条；带本地 Runtime
Environment 的 Standalone 再展示 default、runtime-only 和 custom-image 3 条。完整
Standalone 的 JUnit 数量因此由运行环境决定，不能继续把“10 组”写成“只有 10 条功能用例”。

2026-09-20 的本地 runc Standalone r4 完整回归实际展开为 40 条 JUnit 用例，0 失败、0 跳过：
`sdk` 7 条、`data-plane` 16 条、`lifecycle` 4 条、`placement` 6 条，认证、容量、本地优先、
节点故障、adxlet 重启、停机清理各 1 条，再加 1 条全局残留清理检查。结构化结果位于
`out/ci/sdk-capability-fc-20260920/standalone-run-r4/result.json`，
JUnit 位于同目录 `junit.xml`。

40 是 r4 当时的 Standalone 可观察用例数，不是 SDK 接口数。该批次的 54 个 Standalone 公共操作由
稳定功能 case ID 组合覆盖，一个 case 会连续验证多个相关方法的协作语义；11 个 Firecracker 操作由
严格 KVM case ID 负责。r11 新增 reverse tunnel 实跑后，Standalone 公共操作增加为 55。静态映射
只能防止公共方法漏登记，实际通过仍分别以 Standalone 和 KVM 运行证据为准。

2026-09-21 的定向 Standalone r11 将 `data-plane` 扩展到 17/17，通过新增的
`reverse-tunnel.sdk-upstream-roundtrip`，并完成实例、容器和网络清理；结果位于
`out/ci/sdk-capability-fc-20260920/standalone-tunnel-r11/run/result.json`。该批次只重跑
`data-plane`，因此不把它与 r4 的其余组拼成一次新的完整 Standalone 通过记录。加入 tunnel 后，
Standalone 负责的公开操作数为 55。

当前 Firecracker 严格集合已由 18 项扩展为 26 项。新增 S3 rootfs、S3 mount、独立资源 limit、
入口继承、reload、创建/运行期网络策略以及有/无 checkpoint 的 failover 已有真实 Firecracker
逐项运行证据：r16 的 SDK 组 19/19；r17、r18 使用更新后的组件包，均在通过前 16 项后于双克隆
写入阶段遇到 ARM FC 网络 504。该拆分证据证明新增能力链路，但不等于同一次 26/26 严格验收；
完整门禁仍需修复双克隆网络问题后重跑。

生命周期隔离套件 r24 另有 7/7 通过及完整清理证据，覆盖有／无 checkpoint 的 failover、
Coordinator 不可用时 SQLite 降级、adxlet 重启等待、心跳过期后的旧会话隔离清理，以及资源
观测过期门禁；证据位于 `out/ci/sdk-capability-fc-20260920/fc-lifecycle-r24/`。该套件没有运行
双克隆，不能用来覆盖上述 26/26 缺口。

2026-09-25 的 cn-north-4 定向部署使用基础包
[Buildkite #95](https://buildkite.com/agent-dx/agent-dx/builds/95) 的同一不可变产品提交
`16298a4612f58edaf85bbcfcbd4c647c6287373b`，11 项场景全部通过，JUnit 14/14、无缺失检查和清理错误；
两个测试 Pod 分别落在 `192.168.10.48`、`192.168.10.192`。包含创建结果未知、中心调度
deadline、命令提交／Watch／查询故障、registry 容量、能力协商、命令过期、文件上传／下载
断流和 SQLite 降级期间 Adxlet 重启。具体场景与证据见
[定向部署记录](2026-09-25-cn-north-4-targeted-reliability.md)。此结果不代表同次运行了完整 Full 十一组。

仍需补充：真实旧版 Execd 能力协商、快照分页／过期／引用删除、真实 storage/XPU profile，
以及活动期间不触发空闲回收的独立故障注入。Firecracker 双克隆问题和多 VM 实机验收继续单列。

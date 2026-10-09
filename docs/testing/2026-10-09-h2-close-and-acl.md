# H2 CONNECT 收尾与运行期网络策略回归

## 实现契约

Ingress 的 HTTP 调用方可能在读完响应正文后释放 CONNECT，尚未读取 Relay 发出的 H2 END_STREAM。旧路径直接发送 CANCEL；对端结束帧迟到后，h2 可能将其计为内部 STREAM_CLOSED reset。持续累积达到库的保护上限会关闭共享物理连接，影响其他逻辑流。

正常释放现在先发送请求侧 END_STREAM，并保留传输对象和连接池租约，异步消费对端收尾。等待上限为 1 秒、未读数据上限为 64 KiB；超过任一上限、读错误或路由主动取消时发送 CANCEL。正常会话结束导致取消通知发送端被释放，不视为路由失效；活跃流失去通知发送端仍按取消处理。保留 h2 内部 reset 保护上限。

运行期网络策略转换为 Execd HTTP TCP 50090 保留双向平台规则。该规则按沙箱本地端口匹配，适用于 stateful/stateless 策略；节点控制流使用独立节点地址和端口的既有双向规则。用户发布端口在 stateful 模式仍只显式允许入站。默认拒绝策略仍限制普通网络流量。

## 本地检查

`gateway/tests/connect_shutdown.rs` 使用真实 TCP 和 h2 对端，覆盖：

- 调用方读完正文、对端结束帧尚未到达时正常释放。
- 1,100 次短连接流复用同一物理连接，正常收尾不发送 reset。
- 对端不结束时有界回收。
- 对端持续发送未读数据时有界回收。
- 收尾期间路由取消立即生效。
- 正常会话通知发送端释放不取消收尾。
- 活跃流丢失通知发送端仍中止。

七项通过（包括活跃流丢失取消发送端仍中止的补充测试）。Gateway 全 features 基线共 134 项通过、2 项忽略；补充测试单独通过。RED 阶段正常收尾两项因收到 CANCEL 失败；修复后通过。日志位于 `out/ci/h2-shutdown-20261009/`。

## 真实运行验证

验证主机为远端 x86 Linux/KVM standalone，使用 task 本地镜像 `akernel-h2-acl:20261009`，只覆盖构建后的 API Server、adxlet 二进制。它尚不是发布或集群升级证据。运行矩阵为：

1. runsc＋bpfnat：先建立到 Execd 的持久 TCP，更新到默认拒绝策略；检查原 TCP、新 TCP和普通出站限制。
2. runsc＋iptables：相同检查。
3. 原报告混合负载：32 个常驻实例、20 次/秒创建流转、客户端最多 128 个在途事务，持续 120 秒；每个常驻事务执行命令及 36 字节文件读写。

两种网络后端均通过：更新前可访问的用户测试 TCP 在更新后超时，节点侧测试服务仍存活；既有 Execd TCP 源端口保持不变且返回 200，新 TCP 同样返回 200。

混合负载提交的 22,458 次常驻操作、2,212 次 churn 全部成功，创建、命令、清理错误均为 0，未出现文件截断。2,401 个 churn 到达槽中有 189 个被客户端 128 在途限制拒绝提交，实际 churn 约 18.43 次/秒，未完全达到目标 20 次/秒。包含准备及收尾的总耗时为 150.007 秒。本轮 churn 完整生命周期 P99 桶上界 15 秒，不能把它当作单独 Create 延迟。

日志包含 457,037 条 H2 DATA 帧记录，未出现 STREAM_CLOSED、library-reset、内部 reset 上限或 H2 connection error。另有 3,011 条 CANCEL 帧记录；日志同时包含 gRPC 和 CONNECT 流，不将所有 CANCEL 当作异常，也不宣称整条链路完全没有 reset。原故障对应的库内部 STREAM_CLOSED 累积未复现。

三阶段结束 Redis/backend 均为空，测试容器全部移除。测试入口位于 AKernel 工作树的 `out/ci/report-regression-20261009/h2-acl-verify.sh`，结果目录为 `h2-acl/`；本地汇总为 `h2-acl-evidence/`。首两轮探针分别因嵌入 Python 缩进及 guest 缺少 curl 失败，修正测试驱动后完成上述有效验证，原错误日志保留。

本地测试镜像 ID 为 `sha256:af672ddb11a91609adfc18011c26ace5100ce243bf546b164fc26b1333e01d18`。API Server 二进制 SHA256 为 `aa40996e23ab58987e703c6e1cae4d11073996b0bd6dd4ad4e27ed2c979003f5`，adxlet 为 `0eea73b5f9d785e6e25c5be0e5417a184b521b04ad83f53012af0b0a6bdf5452`。

## Kata 旧镜像对照

旧镜像 `akernel-all-in-one:main-cd4e891-yr0.10.2rc9-github-20260920`，使用对应 `cd4e891` 的部署脚本与 SDK；当前镜像为 `pr78-operation-cache-v2-20261009`。两者在同一主机运行真实 Kata，分别使用各自默认的运行时 rootfs 与运行服务，因而用于比较完整旧链路与 ADX 链路，不是只替换单个二进制的实验。

| 检查 | 旧链路 | 当前 ADX 链路 |
|---|---|---|
| 普通命令 | 成功 | 成功 |
| PTY 输出及 exit 7 | 成功 | WebSocket 建立后断开 |
| PTY close 后立即检查进程 | 仍存活 | PTY 无法建立 |
| CLI exec | 30 秒超时 | WebSocket 错误 |

旧镜像证明 Kata PTY 曾可工作，不能仅凭 `/dev/pts` 缺失或 OCI spec 静态比较认定旧链路同样不可用。两边普通命令观察到的文件系统均没有 `/dev/pts`；对应旧提交的 FunctionSystem `f2befc8a` 中，`ExecSessionActor` 在节点创建 PTY，再以 `sbox exec -t` 执行后端命令；当前 Execd 的 `/pty` 在 guest 内调用 `portable_pty::openpty`。两条执行路径不同，guest 普通命令看到的 `/dev/pts` 不能证明节点 PTY 路径也不可用。应围绕 Execd guest 的终端设备准备继续定位。close 检查仅为立即探测，不证明进程永久残留。当前 Redis/backend 审计为空，两个阶段的 sandbox 已删除、测试容器已清理。

证据在 AKernel 工作树 `out/ci/report-regression-20261009/kata-ab/{old,current}/`，包括 result、镜像身份、daemon 与启动/停止日志。该 A/B 阶段没有修改 Kata 执行后端。

### Execd 终端环境定位与修复

进一步检查实际 Kata guest 和生成的 OCI 配置：`/dev/ptmx` 指向 `pts/ptmx`，但 OCI mounts 未包含 devpts，Execd 与普通命令使用同一 mount namespace。Execd 的 `openpty` 返回 ENOENT。guest capabilities 仅有 `AUDIT_WRITE`、`KILL`、`NET_BIND_SERVICE`，自行挂载 devpts 返回 permission denied（exit 32），因此不能要求用户或 Execd 在 guest 中补挂载。

旧 sandboxd 提交 `f7ab6d4a` 的 `cmd/sbox/kata_exec.go` 把 `Terminal=true` 写入 OCI Process 和发给 Kata shim 的 `ExecProcessRequest`，通过宿主机 FIFO 转发输入输出，通过 `ResizePty` 更新窗口。这与普通 guest 进程自行打开 ptmx 的权限和挂载环境不同。旧 FunctionSystem 的宿主机 PTY 是连接 `sbox` 的外层终端，不应将整个 Kata 执行路径描述成仅在宿主机分配一个 PTY。

修复位于 ADX 的 `platform/adxlet/src/sandboxd.rs`：Kata 创建请求在没有显式 `/dev/pts` 挂载时添加 devpts，选项为 `nosuid,noexec,newinstance,ptmxmode=0666,mode=0620,gid=5`，通过既有 sandboxd Mount 协议交给运行时创建。保留显式挂载，不修改其他 runtime，不增加 guest 权限，也未修改 sandboxd 二进制或源码。

TDD 的 RED 日志确认缺少挂载时新用例失败；修复后完整 sandboxd RPC suite 30 项通过，覆盖 Kata 默认终端准备、显式挂载保留及其他 runtime 的行为保持。Rust workspace 全 targets/features Clippy 和 fmt 均通过。日志位于 ADX 工作树 `out/ci/kata-devpts-20261009/`。

真实 x86 Linux/KVM standalone 使用本地镜像 `akernel-kata-devpts:20261009`，只在此前 `akernel-h2-acl:20261009` 基础上覆盖 adxlet。镜像 ID 为 `sha256:33a9b0858e99931c077a8fb8845a1f4f0310c87683fe0bd4e51af5e10af81932`，adxlet SHA256 为 `18f0d46bb7308477cc3140f6b3b2d6f2d1827f06723c92c97a074700d5120a62`。

- Kata：普通命令、PTY 输出与 exit 7、close 后进程清理、CLI 探针均通过；已有 PTY/CLI suite 3 项通过，包括连续 100 次 PTY 会话及会话记录回收。
- runsc 对照：同一套 PTY/CLI suite 3 项通过，包含连续 100 次 PTY 会话。
- 修复后的 OCI 配置和 guest mountinfo 均包含 devpts；Execd 仍无 `CAP_SYS_ADMIN`，guest 手工 mount 仍被拒绝，原生 PTY 正常。
- 最终 Redis/backend 审计为空，沙箱和测试容器均已清理。

结果位于 AKernel 工作树 `out/ci/report-regression-20261009/kata-devpts/`；本地摘录位于 `kata-devpts-evidence/`。这是选定 Kata/runsc PTY 与 CLI 的真实 standalone 回归，不代表所有 Kata 功能或集群升级验收，也尚未发布该测试镜像。

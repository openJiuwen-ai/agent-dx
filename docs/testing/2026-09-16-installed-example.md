# 完整安装示例验收

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

2026-09-16，在独立 Lima ARM64 KVM 主机上，用 package-v22 按[单机安装指南](../deployment/standalone.md)安装并运行整份发布包示例。Master、Node Manager、Node Proxy、Sandbox API、Edge 分进程启动；Redis 和 sandboxd 由测试环境独立托管。节点使用自动资源探测，业务通过 HTTPS Edge 和包内 Sandbox SDK 访问真实 Firecracker 实例。

## 结果

r2 的 6/6 项验收通过：

| 用例 | 证据 |
| --- | --- |
| CLI validate/render | 发布示例与 `/etc/adx/deployment.json` SHA256 相同，未改写配置字段 |
| 五角色就绪 | 五个 PID 存活、失败标记为 false、重启次数为 0；节点注册、可路由、可准入 |
| 管理员创建租户密钥 | HTTPS Edge 管理入口返回密钥，后续 SDK 使用租户身份 |
| SDK 命令与文件 | stdout=`ready`、stderr=`diagnostic`、退出码 7；二进制文件往返一致 |
| 显式删除 | 第一个实例经 SDK 删除；Redis 为 Deleted，资源释放 |
| 停机清理 | 第二个实例保留到 `adxctl stop`；停止后两个实例均 Deleted、资源释放、sandboxd 清单为空 |

停止 ADX 后，外部 Redis 和 sandboxd 仍存活，再由测试环境关闭。`cleanup_errors=[]`；复核四个安装目录、socket 和本轮进程均无残留。自动探测结果为 CPU 4000 millicores、内存 6197436416 bytes、可用磁盘 6089003008 bytes，数值属于当次测试主机。

日志保留了创建后路由尚未到达 Edge 的短暂 HTTP 503。SDK 按现有重试契约完成请求；本轮没有修改重试或超时参数，也不以此次验收声称创建响应后路由立即可见。

## 首轮失败与修复

r1 完成 CLI 校验/渲染后，Sandbox API 四次启动均报 `discovery namespace and positive intervals required`。CLI 只注入发现地址和 namespace，遗漏 `poll_seconds`；先前专用 E2E 配置器显式填写该值，所以未暴露整份安装示例的问题。

新增读取真实部署示例的配置回归测试，先复现 `null != 5`，再修复 CLI：Sandbox API 缺省轮询间隔为 5 秒，保留显式覆盖，拒绝非整数、零、负数和超过 86400 秒的配置。Node Manager 的发现协议保持独立。配置测试 7/7、Clippy、ARM64 release 编译通过；验收驱动回归 53/53 通过。驱动保留失败调用栈、进程状态和组件日志。

## 制品与证据

- 包：`out/ci/pause-resume/package-v22/`；`aarch64-unknown-linux-gnu / release`，基准 `0dde79ad57583e998389101a763e4d2d825be63e` 加当前修改，`dirty=true`。
- 包清单 SHA256：`bfd7adfc21b3d22335c1b404df4ea9da69fe522b3ef7fbb3814843e20684a04b`。
- 对比校验过的 v21，仅 `bin/adxctl` 改变，SHA256=`76a284892d2fa85c8494d75592e06a3323855cc2df81d989382eb5fc640759f3`；其余组件、SDK、Redis、RRT 均相同。
- 发布示例与安装配置 SHA256：`9eaaef1350316250a25bf46ce02a770b213e1484fe1dfb076966ae229752feee`。
- RRT 镜像：`sha256:30d4cc37e91a9ed21eca33bd23d95c0c5f7977e49dadb8ffed9d1c77b4c5abab`。
- `out/ci/stage-6/example/r2/evidence/`：`result.json`、`status-running.json`、`node-ready.json`、`sdk.json`、`catalog-final.json`、包清单及组件日志。
- 同目录上层的 `cli-red.log`、`cli-green-2.log`、`driver-regression.log`、`package-v22.log`、`r2.log` 保存编译与测试输出。r1 的失败证据保留。
- 入库驱动及准备方式：[example/README.md](../../build/e2e/example/README.md)。

结合此前身份管理、共进程/分进程及 SDK 接线验收，阶段 6 的本地验收完成。此轮是单机进程部署，正式 Kubernetes Buildkite、新增功能的集群验收仍属阶段 7。

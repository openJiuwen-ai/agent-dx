# 2026-09-25 定向 Full E2E 记录

本轮在隔离的 Kubernetes 命名空间执行真实进程、Redis、sandboxd/runc 和公开 Python SDK。两个 Node Pod 分别落在物理 worker `10.244.128.124` 和 `10.244.128.160`。产品基础包取自 Buildkite Base #91 的提交 `0b936fd8b0822fb757e18fba794f4a0cb61b13b2`；测试镜像和 SDK 制品按各次 Full 构建参数固定。驱动提交与产品提交分别记录，避免把测试脚本变更误写成产品修复。

| 验收 | 结果与证据边界 |
|---|---|
| [Full #48](https://buildkite.com/agent-dx/agent-dx-full-test/builds/48) | Redis Pod/PVC 重建与 AOF 恢复通过。PVC UID 不变、Pod UID 变化；双节点原实例保持归属，公开 SDK 读写、删除和最终清理通过。 |
| [Full #50](https://buildkite.com/agent-dx/agent-dx-full-test/builds/50) | 异构运行时亲和通过。node1 仅有 runc，node2 有 runc/runsc；公开 SDK 创建 runsc 实例后实际归属 node2，执行命令、删除及双节点 backend 清空。runsc 二进制由区域镜像摘要固定，并在构建时校验 SHA512。 |
| [Full #53](https://buildkite.com/agent-dx/agent-dx-full-test/builds/53) | 330.321 秒混合负载通过：每类命令和文件往返各 1515 次，创建/删除各 267 次；逐实例读回归属 node1=134、node2=133，业务错误及清理错误为零。日志出现 293 次最终恢复的首次路由 503，路由发布时效仍是独立待办。 |
| [Full #55](https://buildkite.com/agent-dx/agent-dx-full-test/builds/55) | Coordinator 与 node2 Adxlet 同时重启通过。Coordinator PID 114→295、epoch 1→2；Adxlet PID 110→202 且 session 改变。两个实例归属和物理 backend 不变，公开 SDK 查询、命令、文件及最终清理通过。此场景是进程联合故障，不等同于物理节点故障。 |
| [Full #56](https://buildkite.com/agent-dx/agent-dx-full-test/builds/56) | Never 策略下真实 sandboxd daemon/runtime 丢失后，Redis 进入 Failed 且公开 Ingress 路由在删除前返回 503（路由不存在），此子项通过。Restart 子项在 Redis 已重新 Running 后立即查询 API Server，仍读到旧 Failed，整组失败；物理 backend 和命名空间清理通过。驱动随后加入最多 5 秒的同实例状态可见性等待，保留超过上限时的失败判定。 |
| [Full #57](https://buildkite.com/agent-dx/agent-dx-full-test/builds/57) | 修正测试时序后，Never 和 Restart 两项均通过。Never 删除前路由确认为已撤销（HTTP 503、路由不在同步缓存）；Restart 创建了不同的新 backend，`restart_attempts=1`，公开 SDK 命令通过。Redis Running 后的 API Server 状态可见延迟为 0.023 秒；两个物理 backend 目录为空、命名空间删除、`cleanup_errors=[]`。 |
| [Full #58](https://buildkite.com/agent-dx/agent-dx-full-test/builds/58) | 数据面 18/18 子项通过，包括 Host 子域名端口转发的匿名拒绝与带令牌访问。该版本的测试 HTTP 服务对任意路径返回相同内容，所以此结果仅证明 Host 路由到达后端；后续驱动增加独立的上游路径回显断言。 |
| [Full #59](https://buildkite.com/agent-dx/agent-dx-full-test/builds/59) | 放置 8/8 子项通过：节点/实例亲和与反亲和、加权及有序偏好、`node_id` 对 OR 分支的约束，与预期节点逐项一致；不可用运行时未分配。公开 SDK 命令、终态资源释放、双节点 backend 和命名空间清理通过。 |
| [Full #60](https://buildkite.com/agent-dx/agent-dx-full-test/builds/60) | 前台 SDK 命令跨越 6 秒空闲阈值仍继续运行，完成后实例才被空闲删除；结果为 29.661 秒通过，资源释放和最终清理完成。后台命令在客户端退出后的空闲回收另由生命周期组验证。 |
| [Full #61](https://buildkite.com/agent-dx/agent-dx-full-test/builds/61) | 加严 Host 转发的上游路径断言后，数据面 18/18 再次通过。匿名访问被拒绝；带令牌请求经 Host 子域名到达后端，后端仅在收到 `/functional/host?x=1` 时返回独立标记 `ADX-HOST-PATH-OK`。两端 backend 清空且命名空间删除。 |
| [Full #62](https://buildkite.com/agent-dx/agent-dx-full-test/builds/62) | 生命周期 5/5 子项通过。独立 SDK 客户端启动 120 秒后台命令后退出，实例在命令自然结束前约 14.802 秒被空闲回收；Redis 终态 Deleted、资源释放、双节点 backend 和命名空间清理均通过。与 #60 合起来覆盖“活动请求不能误回收”和“客户端退出后应回收”。 |
| [Base #92](https://buildkite.com/agent-dx/agent-dx/builds/92) | 提交触发路由发布的新包完成构建和 K8s L0；L0/认证 2/2 通过、`cleanup_errors=0`、命名空间删除。产品提交为 `0338b2c8362b476ade3408ae6430e05edc118a60`。 |
| [Full #63](https://buildkite.com/agent-dx/agent-dx-full-test/builds/63) | 新包的混合负载用例尚未运行：镜像构建通过，但双节点限定生成了 Kubernetes 不接受的单个 `metadata.name In` 多值字段选择器，Pod 创建被拒绝。没有实际节点放置；清理正常。已在测试部署脚本中把每个候选节点改为单值 OR 条件，并用回归用例验证，仍需重新跑 Full。 |

修正后的两个单值 OR 条件还通过了 cn-north-4 Kubernetes API 的 server dry-run（`out/ci/route-event-full63-selector-apiserver.log`），没有创建 Pod。此结果只证明该 API Server 接受清单语法，不证明 Full 测试集群已完成部署或混合负载验收。

上述证据的构建判定、原始日志和验收产物位于 `out/ci/multivm-coverage-0925/`。该目录不随 Git 提交；Buildkite 构建页面保存相应运行产物。

尚待完成：独立一控两工作节点的三 VM 控制面验收；本地 Firecracker 双克隆网络定位；真实 GPU/NPU 设备验收。路由提交到 Ingress 的发布延迟已记录为独立待办，本轮混合负载通过不能将其视为完成。

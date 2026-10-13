# Standalone OOM 到 SDK 验收

## 功能契约

只使用 sandboxd 已有的 OOM 信息。Execd 不采集 cgroup 或 guest 内存事件。

- sandboxd 后端退出时，Adxlet 在清理前调用既有 `Wait`，保存 `failure.reason`、`exit_code` 和说明；该记录经过 Coordinator 持久化和目录订阅到达 API Server。
- SDK `get_info().failure.reason == "OomKilled"` 表示 sandboxd 提供了后端 OOM 信息。数据调用失败后的恢复查询只在认证目录已确认 Failed/OomKilled 时返回 `SandboxError.code == "SANDBOX_OOM"`，`retry=never`、`outcome=terminal`；后台提交也保留该终态分类。
- 早于状态发布的调用仍可能返回 `OUTCOME_UNKNOWN`；调用方可随后查询 `get_info()`。终态描述的是后端执行，不能据此断言此前命令的副作用未发生。
- 用户进程被 OOM 杀死、后端仍运行时，现有 sandboxd API 没有单独的进程 OOM 原因。SDK 保留命令失败及实际 `exit_signal`，Environment 保持 Running。普通 SIGKILL 同样产生信号 9，不猜测为 OOM。
- 原因附着于同一个 Environment 执行记录，成功新建、恢复或重启清除旧原因。后端缺失且无法读取退出信息记为 Missing，普通退出为 Exited。

完整错误语义见 [错误契约](../../platform/api/http/error-contract.md#oom-原因与-sdk)。

## 环境与运行

此用例属于独立 Linux standalone 专项，不是基础门禁，不依赖 Kubernetes。

需要一个测试专用、无其他 Sandbox 的 AKernel standalone 容器，Docker exec 权限、cgroup v2、runc/runsc；Firecracker 还需要可用 KVM 和 guest runtime。sandboxd 须支持现有 `WaitResponse.Message` 的 OOM 说明。SDK 和安装的 ADX/Execd 必须来自待验收源码；不要用旧的已发布 wheel 验收新行为。

内存限制为 256 MiB，压力程序逐页触碰，单次最多申请 1 GiB。执行前检查实际沙箱限额，避免压力落到无上限宿主进程。测试不会停止已有业务容器；容器准备与最后停机由外层专用 fixture 管理。

```bash
python build/e2e/oom_sdk.py \
  --container akernel-oom-sdk-test \
  --api 127.0.0.1:38443 \
  --gateway http://127.0.0.1:38080 \
  --key-file /path/to/private/data/token \
  --output out/ci/oom-sdk-e2e
```

默认 sandboxd OCI 配置目录为 `/home/akernel/sandboxd/root/containers`，可用 `--backend-root` 设置。资源释放检查使用该 standalone 的 `adx:{akernel}:control:v1` 命名空间；这不是任意集群的通用压力入口。

## 覆盖和通过标准

| 场景 | 内核 OOM 证据 | SDK / 状态断言 |
|---|---|---|
| runc 用户进程压力 | 专属 cgroup oom_kill 增量 | 前台与后台命令保留信号 9；无推断 OOM 错误码；Environment Running，后续命令可用 |
| runsc 后端压力 | 宿主 cgroup OOM 杀 sentry | Failed/OomKilled 及资源释放；后续 SDK 调用为 SANDBOX_OOM |
| FC guest 用户进程压力 | guest 内核 OOM 日志 | 前台与后台命令保留信号 9；VM 仍 Running，后续命令可用 |
| runc 整组压力 | 测试单独启用该 sandbox 叶子 cgroup memory.oom.group=1 后触发 OOM | Failed/OomKilled 及资源释放；后续 SDK 调用为 SANDBOX_OOM |

两个进程场景同时验证普通 kill、超时和 exit 7，不产生 OOM 推断码。测试结束必须没有 backend 与测试 Environment 残留，外层容器 PID 和启动时间不变。Docker 的 OOMKilled 标志可能由子 cgroup 事件置位，不能单凭此标志断言父容器退出。

输出包含逐场景 SDK 结果、前后台命令、状态采样、Redis 记录、cgroup 计数和清理摘要。`OOM_SDK_ACCEPTANCE_PASSED` 仅在四场景及清理全部通过后打印。guest / 宿主内核日志用于确认测试确实构造了 OOM，不属于产品的 OOM 采集实现。

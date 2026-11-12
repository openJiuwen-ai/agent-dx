# 沙箱实例挂载教程

本文说明在创建沙箱实例（Environment）时，如何把节点目录或受管 Volume 挂载到实例内部。内容覆盖 Python SDK、HTTP API 和 Agent 模板三条入口，并给出验证、排错和生命周期边界。

> 状态说明：Local bind（`host_path`）入口已接入公共 API/SDK。受管 Volume 已完成本地实现和受控运行时验证，尚未在集群部署，跨主机共享存储、网络分区和 Snapshot/checkpoint 真实验收仍待完成。生产使用前请按 [Platform Volume 设计](../development/platform-volume-design.md)核对当前边界。

最短路径：

1. 在节点准备目录，或配置存储域后用 `ensure_volume` 创建 Volume。
2. 创建沙箱实例时传入一个 `Mount`，来源选择 `host_path` 或 `volume_id`。
3. 进入实例检查 `/proc/self/mounts`，并做一次写入和读取。
4. 重建实例并挂载同一路径或 Volume，确认数据持久化。

## 1 挂载方式与来源

一个 `mounts` 条目只能选择一个来源：`host_path`、`volume_id`、`image_url` 或 `s3_config`。同时为空或同时出现多个来源会返回 `InvalidArgument`（HTTP 400）。`target` 必须是沙箱内的绝对路径，且不能包含 `..` 路径分量。

| 方式 | 关键字段 | 数据身份 | 适用场景 |
| --- | --- | --- | --- |
| Local bind | `host_path` | 只是节点上的绝对路径，平台不登记目录身份 | 节点已挂载的共享文件系统、受信静态目录、临时目录 |
| Volume reference | `volume_id`、可选 `volume_uid` | 租户内稳定 Volume ID 加不可复用 UID，包含访问模式和初始化规则 | 用户工作区、需要跨版本或跨重建复用、需要独占或目录初始化 |
| 镜像 | `image_url` | 镜像制品，只读 | 工具、模型、只读依赖 |
| 对象存储 | `s3_config` | S3 兼容对象，只读 | 只读数据集、模型文件 |

选择建议：

- 目录由部署静态准备、调用方完全可信、不需要平台登记目录身份时，使用 Local bind。
- 目录需要稳定身份、访问模式、初始化规则、引用保护或存储域调度时，使用 Volume。
- 只需要把只读制品暴露给沙箱时，使用 `image_url` 或 `s3_config`，不要把这类来源声明为 `rw`。

Agent 模板同样支持挂载。模板在 `volumes` 中声明 Volume 定义，在 `mounts` 中通过 `host_path` 或 `volume_id` 引用；Activator 在激活时渲染并确保 Volume 存在。见第 5 节。

## 2 前置条件

### 2.1 节点目录（Local bind）

Local bind 不会替调用方挂载文件系统。源目录必须已经存在于执行节点的文件系统中，并且在 sandboxd 执行挂载的 mount namespace 中可见：

```text
SFS / MooseFS / 其他共享文件系统
  → Worker 固定挂载点（例如 /mnt/sfs）
    → sandboxd 所在 mount namespace 可见
      → 沙箱内 target（例如 /workspace）
```

需要确认：

- 所有可能被调度到的 Worker 都挂载同一个后端，并使用相同逻辑路径。只挂载部分节点时，Local 路径本身不会表达共享存储身份，也不会自动限制调度范围；应使用 `node_id` 固定节点，或改用 Volume 的存储域拓扑。
- 节点启动脚本若使用私有 mount namespace 和 `rprivate`，应在 sandboxd 启动前完成共享文件系统挂载。启动后再在其他 namespace 添加挂载，不保证传播到 sandboxd。
- 源路径必须是绝对路径，不含控制字符和 `.`、`..` 路径分量。节点会逐级打开路径且不跟随符号链接。
- 沙箱实际使用的 UID/GID 必须能读写目标目录。ADX 不会递归 chown 已存在的目录；需要特定属主时，使用 `create_options` 或由部署提前设置。
- `create_if_missing=true` 只创建最后一层源目录，父目录必须由部署预先准备。已有目录会被复用，属主和权限保持不变。
- 删除沙箱只移除实例挂载，不会删除源目录数据。

### 2.2 受管 Volume（可选）

Volume 使用 Coordinator 的受信存储域配置和 adxlet 的本地挂载配置。以下片段是部署配置，不是创建请求字段：

```json
{
  "storage_domains": {
    "sfs-main": {
      "backend_id": "server:/export",
      "dynamic_nodes": true,
      "node_roots": {},
      "tenant_paths": {"tenant-a": "workspaces/tenant-a"}
    }
  }
}
```

```json
{
  "storage_mounts": {
    "sfs-main": {"backend_id": "server:/export", "root": "/mnt/sfs"}
  }
}
```

要点：

- `storage_domains` 由 Coordinator 的可信部署配置提供，包含后端身份、可访问节点、挂载根和租户路径前缀。
- adxlet 通过 `storage_mounts` 声明本地逻辑根。只有通过后端身份和目录校验的挂载才会在节点注册和心跳中上报。
- `dynamic_nodes=true` 适用于弹性共享存储节点；固定拓扑继续使用 `node_roots`。两种策略都要求不同节点上的 root 指向同一后端的同一逻辑目录。
- 节点上实际挂载失效时，执行仍可能失败；上报结果只表示当前观测能力，不是持续存储健康检查。
- 删除 Volume 定义不删除目录数据；删除前必须没有引用、生命周期关联和占用。

### 2.3 认证与调用方边界

公共 Environment API 使用租户 API Key：

```http
Authorization: Bearer <ADX tenant API key>
```

Local bind 的 `host_path` 没有租户级目录授权校验。部署应将能够提交任意 host_path 的 Environment 管理接口限制给受信调用方；普通租户直接调用前，必须先补齐部署侧路径授权。

## 3 使用 Python SDK

### 3.1 连接配置

使用发布包中的 SDK，并通过环境变量或 `ConnectionConfig` 连接平台：

```sh
export ADX_SERVER_ADDRESS=adx.example.com:8443
export ADX_TOKEN="$(cat /secure/path/tenant-api-key)"
export ADX_TLS=1
```

```python
from adx_environment import ConnectionConfig

connection = ConnectionConfig(
    server_address="adx.example.com:8443",
    token="<tenant-api-key>",
    use_tls=True,
)
```

### 3.2 挂载节点目录

```python
from adx_environment import CreateOptions, Environment, Mount

workspace = "/mnt/sfs/workspaces/tenant-a/alice"
mount = Mount(
    target="/workspace",
    host_path=workspace,
    options=("rw",),
    create_if_missing=True,
    create_options=CreateOptions(uid=10001, gid=10001, mode="0700"),
)

environment = Environment(
    image="registry.example/app:v1",
    name="mount-demo",
    mounts=[mount],
)
try:
    environment.files.write("/workspace/hello.txt", "hello from sandbox\n")
    print(environment.commands.run("cat /workspace/hello.txt").stdout)
finally:
    environment.kill()
```

说明：

- `options` 默认是只读。目录 bind 需要读写时显式传 `("rw",)`；还可以追加 `nosuid`、`nodev`、`noexec`。
- `create_if_missing` 只对 Local bind 生效，默认 `false`。
- `create_options` 必须与 `create_if_missing=True` 同时使用，`uid`、`gid`、`mode` 三个字段都必填；`uid`/`gid` 范围是 0–4294967294，`mode` 是 `0000`–`0777` 的四位八进制字符串。
- 只有本次真正创建目录时才会设置属主和权限。已有目录不会被递归修改，也不会自动修复。
- Local 路径只在部分节点可见时，可用 `Environment(node_id=...)` 固定节点；这会降低调度弹性，能使用共享存储时应优先让所有候选节点看到同一路径。
- 用户沙箱不需要 `CAP_SYS_ADMIN`；挂载由节点运行时完成。

### 3.3 挂载受管 Volume

```python
from adx_environment import (
    CreateOptions,
    DirectoryVolumeSource,
    Environment,
    Mount,
    VolumeSpec,
)

volume = Environment.ensure_volume(
    VolumeSpec(
        id="workspace-alice",
        source=DirectoryVolumeSource(
            storage_domain="sfs-main",
            path="workspaces/tenant-a/alice",
        ),
        access_mode="exclusive",
        create_if_missing=True,
        create_options=CreateOptions(uid=10001, gid=10001, mode="0700"),
    )
)
print(volume.uid, volume.initialization)

mount = Mount(
    target="/workspace",
    volume_id=volume.spec.id,
    volume_uid=volume.uid,
    options=("rw",),
)

environment = Environment(
    image="registry.example/app:v1",
    name="volume-demo",
    mounts=[mount],
)
try:
    environment.files.write("/workspace/hello.txt", "hello from volume\n")
    print(environment.commands.run("cat /workspace/hello.txt").stdout)
finally:
    environment.kill()
```

说明：

- `ensure_volume` 是幂等 ensure，不创建目录、不移动文件、不修改权限；目录准备发生在节点执行准入之后。
- 已存在的 Volume 按原定义返回，不会因为本次请求修改来源、访问模式或初始化规则。同名删除后重建会得到新的 UID。
- Volume ID 长度为 1–256，只允许 ASCII 字母、数字、点、下划线和短横线；`.` 和 `..` 无效。
- `DirectoryVolumeSource.path` 是存储域内相对路径，不能是绝对路径，不能包含空分量、`.` 或 `..`；`storage_domain` 必须是 Coordinator 已配置的存储域。
- `access_mode` 默认 `shared`；需要单执行者时显式设为 `exclusive`。一个 Environment 最多引用一个不同的 exclusive Volume，同一 Volume 的多个挂载目标合并占用。
- exclusive 是控制面占用，不是存储侧锁。节点失联释放占用后，旧执行仍可能访问目录；业务仍需保证同一目录只有一个写者。
- Volume 引用挂载不能携带 `create_if_missing` 或 `create_options`，这两个字段属于 `VolumeSpec`。
- `volume_uid` 对直接调用方可选；建议传入 `ensure_volume` 返回的 UID，固定本次引用的具体 Volume。
- 创建可能因 exclusive 占用等待而排队，受调度和创建超时约束。可通过 `Environment.get_volume(id).initialization` 查看 `not_requested`、`uninitialized`、`pending`、`ready` 或 `needs_repair` 状态。

### 3.4 验证挂载与持久化

先在实例内确认挂载和目标路径可写：

```python
print(environment.commands.run("cat /proc/self/mounts").stdout)
environment.files.write("/workspace/hello.txt", "hello\n")
print(environment.commands.run("cat /workspace/hello.txt").stdout)
```

`/proc/self/mounts` 中应出现 target 为 `/workspace` 的挂载项。然后重建实例，验证数据来自共享目录或 Volume，而不是实例临时层：

```python
first = Environment(
    image="registry.example/app:v1",
    name="mount-first",
    mounts=[mount],
)
try:
    first.files.write("/workspace/persist.txt", "persist\n")
finally:
    first.kill()

second = Environment(
    image="registry.example/app:v1",
    name="mount-second",
    mounts=[mount],
)
try:
    print(second.commands.run("cat /workspace/persist.txt").stdout)
finally:
    second.kill()
```

第二次创建可能调度到另一个节点。使用 Local bind 时，这要求所有候选节点看到同一后端和同一路径；使用 Volume 时，由存储域拓扑约束节点选择。`kill()` 只删除实例，不删除挂载目录。

## 4 使用 HTTP API

### 4.1 确保 Volume 存在

如果不使用 Volume，跳过本节。先创建或复用 Volume 定义：

```sh
export ADX_BASE_URL="https://adx.example.com:8443"
export ADX_TOKEN="$(cat /secure/path/tenant-api-key)"
```

`volume.json`：

```json
{
  "id": "workspace-alice",
  "source": {
    "type": "directory",
    "storage_domain": "sfs-main",
    "path": "workspaces/tenant-a/alice"
  },
  "access_mode": "exclusive",
  "create_if_missing": true,
  "create_options": {"uid": 10001, "gid": 10001, "mode": "0700"}
}
```

```sh
curl --fail-with-body -X POST "$ADX_BASE_URL/api/environment/v1/volumes" \
  -H "Authorization: Bearer $ADX_TOKEN" \
  -H "Content-Type: application/json" \
  --data @volume.json
```

普通响应使用统一 envelope，`data` 是 base64 编码的 JSON：

```json
{"code": 200, "message": "", "data": "<base64-encoded JSON>"}
```

解码后的 `data` 形如：

```json
{"uid":"vol-uid-example","spec":{"id":"workspace-alice","source":{"type":"directory","storage_domain":"sfs-main","path":"workspaces/tenant-a/alice"},"access_mode":"exclusive","create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"}},"revision":1}
```

后续创建挂载时使用返回的 `uid` 作为 `volume_uid`。

### 4.2 创建带挂载的沙箱实例

Local bind 请求示例（保存为 `local-mount.json`）：

```json
{
  "name": "mount-demo",
  "image": "registry.example/app:v1",
  "cpu": 1000,
  "memory": 2048,
  "mounts": [
    {
      "type": "bind",
      "host_path": "/mnt/sfs/workspaces/tenant-a/alice",
      "target": "/workspace",
      "options": ["rw"],
      "create_if_missing": true,
      "create_options": {"uid": 10001, "gid": 10001, "mode": "0700"}
    }
  ]
}
```

Volume 引用请求示例（保存为 `volume-mount.json`）：

```json
{
  "name": "volume-demo",
  "image": "registry.example/app:v1",
  "mounts": [
    {
      "type": "bind",
      "target": "/workspace",
      "options": ["rw"],
      "volume_id": "workspace-alice",
      "volume_uid": "vol-uid-example"
    }
  ]
}
```

```sh
curl --fail-with-body -X POST "$ADX_BASE_URL/api/environment/v1/environments" \
  -H "Authorization: Bearer $ADX_TOKEN" \
  -H "Content-Type: application/json" \
  --data @volume-mount.json
```

挂载 Local bind 时，把 `--data @volume-mount.json` 换成 `--data @local-mount.json`。

普通响应的 `data` 解码后形如：

```json
{"environmentId":"volume-demo","status":"running","requestId":"create-request","newlyCreated":true}
```

如果请求头 `Accept` 包含 `text/event-stream`，创建结果通过 SSE 的 `final` 事件返回。`newlyCreated` 只在确认创建了新实例时为 `true`；复用时为 `false`。

### 4.3 响应与错误

- 普通响应使用 `{"code":200,"message":"","data":"..."}` envelope。
- 错误响应在 envelope 之外包含稳定的 `error` 对象，调用方应按 `error.code`、`error.retry` 和 `error.outcome` 处理，不要仅依赖 HTTP 状态码。完整语义见 [错误契约](../../platform/api/http/error-contract.md)。
- Volume 定义、引用和查询的完整字段见 [Volume API](../../platform/api/http/volumes.md)。
- Environment 创建字段和 SSE 语义见 [Environment 生命周期 API](../../gateway/apiserver/docs/environment-lifecycle-api.md)。

## 5 通过 Agent 模板挂载

Agent 模板版本可以声明 `volumes` 和 `mounts`，并使用 `${parameter}` 占位符。首次激活创建 AgentBinding 时，参数与 generation 一起持久化；后续请求沿用已保存的参数。以下片段省略镜像等字段：

```json
{
  "volumes": [
    {
      "id": "workspace-${workspace}",
      "source": {
        "type": "directory",
        "storage_domain": "sfs-main",
        "path": "workspaces/${workspace}"
      },
      "access_mode": "exclusive",
      "create_if_missing": true
    }
  ],
  "mounts": [
    {
      "type": "bind",
      "volume_id": "workspace-${workspace}",
      "target": "/workspace",
      "options": ["rw"]
    }
  ]
}
```

要点：

- Activator 在激活时渲染参数、确保 Volume 存在，并通过公共 Sandbox 能力创建实例。
- `host_path` 中引用的路径参数必须存在且非空，不能是绝对路径，不能包含空路径分量、`.`、`..`、反斜杠或控制字符。
- `volume_id` 的渲染结果必须符合 Volume ID 规则，因此示例中的 `workspace` 参数应使用不含斜杠的稳定标识，例如 `w-0123456789abcdef`。
- 模板版本不可变。已有 Binding 忽略新请求的初始化参数；删除 Binding 后再次访问才使用新参数。
- Local bind 模板示例见 [Jiuwen SFS 模板](../../agent/activator/examples/jiuwen-sfs-template.json)；参数和目录映射契约见 [用户工作区持久化挂载设计](../development/user-workspace-volumes-design.md)。

## 6 常见问题与排错

| 现象 | 常见原因 | 处理 |
| --- | --- | --- |
| HTTP 400，提示 source 数量错误 | 同一个 mount 同时写了多个来源，或一个来源都没写 | 确保 `host_path`、`volume_id`、`image_url`、`s3_config` 恰好一个非空 |
| HTTP 400，`create_options` 相关错误 | 没有同时设置 `create_if_missing=true`，或 `uid`/`gid`/`mode` 缺失、越界 | 补齐三个字段，并确认目录创建开关 |
| HTTP 400，`rw` 不被允许 | 对 image/S3 来源请求读写 | image/S3 保持只读，只有目录 bind 可以 `rw` |
| HTTP 400，Volume 引用带初始化字段 | 在 mount 上写了 `create_if_missing` 或 `create_options` | 把这两个字段移到 `VolumeSpec` |
| 创建失败，提示源目录不存在 | 节点没有该路径，或路径不在 sandboxd 的 mount namespace 中 | 在所有候选节点准备并校验挂载；必要时固定 `node_id`，或在父目录已存在时启用 `create_if_missing` |
| 沙箱内读写报权限错误 | 沙箱 UID/GID 对目录没有权限 | 调整目录属主/权限，或用 `create_options` 创建新目录；不要依赖自动 chown |
| Volume 长时间等待或创建超时 | exclusive Volume 正被其他执行占用，或没有满足存储域拓扑的节点 | 等待占用释放，确认旧实例已停止，并检查调度、存储域和创建超时 |
| Volume 状态为 `needs_repair` | 目录初始化中断，或已有目录属性与创建属性不匹配 | 停止使用该目录，按设计文档人工核对并修复；当前版本没有自动修复接口 |
| 重建后文件不存在 | 两次使用了不同路径或不同 Volume；或写入落在实例临时层 | 复用同一 `host_path`/`volume_id`，并在实例内确认 `/proc/self/mounts` 和真实落盘 |
| 从 Snapshot 创建的实例没有挂载数据 | Snapshot/clone 不继承源实例的 mounts | 在新实例创建请求中显式声明相同挂载 |

## 7 生命周期与边界

- 删除 Environment 只移除实例和挂载关系，不删除源目录或 Volume 数据。
- 删除 Volume 需要没有 Environment、Snapshot、生命周期关联、占用、等待者或未知的在途执行；删除只移除定义，保留底层目录。
- Snapshot 保护 Volume 定义，但不复制或冻结外部目录数据；从 Snapshot 创建的新实例不会继承 mounts。
- Local bind 没有平台级目录身份、访问控制或独占协调。多个实例挂载同一路径时，需要业务自行保证互斥。
- Volume 的 exclusive 占用只约束控制面执行，不提供存储侧 fencing；不能把它当作跨主机文件锁。
- `create_if_missing` 是“缺失即创建”的许可，不是数据丢失检测器。需要禁止误创建的场景应提前准备好目录并设为 `false`。
- 暂停/恢复使用同一 Environment 身份和已保存规格，挂载配置随规格保留；挂载只覆盖目录数据，运行中进程、临时目录和未落盘内容不由挂载本身恢复。

## 8 参考

- [Environment SDK 指南](../../platform/sdk/environment/python/README.md)
- [Volume API](../../platform/api/http/volumes.md)
- [Environment 生命周期 API](../../gateway/apiserver/docs/environment-lifecycle-api.md)
- [Platform Volume 设计](../development/platform-volume-design.md)
- [用户工作区持久化挂载设计](../development/user-workspace-volumes-design.md)
- [Jiuwen SFS 模板示例](../../agent/activator/examples/jiuwen-sfs-template.json)

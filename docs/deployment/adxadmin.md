# `adxadmin` 集群管理工具

`adxadmin` 是运行在管理员工作站、运维机或 CI 中的 Python 客户端。它只通过 Edge/API
Server 的公开 HTTPS API 管理 ADX，不读取部署 YAML，不连接 Redis，也不调用 Master
内部 gRPC。主机进程部署仍由 [`adxctl`](adxctl.md) 负责，Sandbox 业务操作仍由
`adx-sandbox` 或 Python SDK 负责。

## 安装

`adxadmin` 要求 Python 3.10 或更高版本。wheel 为平台无关的
`adxadmin-<version>-py3-none-any.whl`，Linux、macOS 和 Windows 使用同一产物：

```sh
pipx install ./adxadmin-0.1.0-py3-none-any.whl
# 或安装构建产物
python3 -m pip install ./adxadmin-0.1.0-py3-none-any.whl
```

从源码构建 wheel 和 sdist：

```sh
PYTHON=python3.12 bash tools/admin/build.sh out/wheels
```

`adxadmin` 独立发布，不进入 Linux 平台包；服务器安装 `adxctl` 不会额外安装 Python
或管理客户端。

正式发布后可直接从 PyPI 安装：

```sh
pipx install adxadmin==0.1.0
```

仓库中的 `agent-dx-admin` Buildkite 流水线始终构建并校验 wheel 与 sdist，但默认不上传。
只有版本标签与 `pyproject.toml` 完全一致，且构建显式设置
`ADX_ADMIN_PYPI_UPLOAD=1` 时才会执行 PyPI/TestPyPI 发布。具体变量、Secret 和制品回读
契约见 [Buildkite 说明](../../.buildkite/README.md#optional-pypi-publication)。

## 连接配置

生产入口必须使用 HTTPS。私有 CA、管理员 Key 和超时可以通过参数配置：

```sh
adxadmin \
  --endpoint https://adx.example.com:8443 \
  --ca ~/.config/adx/public-ca.pem \
  --token-file ~/.config/adx/admin.key \
  key list
```

对应环境变量为：

```sh
export ADX_ENDPOINT=https://adx.example.com:8443
export ADX_CA_FILE=$HOME/.config/adx/public-ca.pem
export ADX_ADMIN_TOKEN_FILE=$HOME/.config/adx/admin.key
export ADX_ADMIN_TIMEOUT_SECONDS=30
export ADX_ADMIN_OUTPUT=table       # 或 json
```

在 POSIX 系统中，管理员 Key 文件必须只允许当前用户访问，推荐使用 `0600`：

```sh
install -m 0600 /secure/source/admin-key ~/.config/adx/admin.key
```

Windows 不使用 POSIX mode；管理员需要通过用户 ACL 保护该文件。CLI 拒绝带用户名、
密码、路径、查询参数或 fragment 的 endpoint。仅本机测试可显式使用
`--allow-loopback-http` 访问 `http://localhost` 或回环 IP；远端明文 HTTP 始终被拒绝。

## 租户 Key

创建不过期的租户 Key。明文只在本次响应中出现：

```sh
adxadmin key create --tenant team-a
# 显式写法
adxadmin key create --tenant team-a --no-expiry
```

使用绝对 Unix 秒设置到期时间：

```sh
adxadmin key create --tenant batch-jobs --expires-at 1798761600
```

默认将明文写到标准输出。需要直接保存时使用可选的 `--output-file`：

```sh
adxadmin key create --tenant team-a --output-file ./team-a.key
```

文件以 `0600` 创建，父目录在新建时使用 `0700`，已存在文件不会被覆盖。同一租户可以
拥有多个 Key，每个 Key 有独立摘要 ID、到期时间和吊销状态。

创建请求不会自动重试。若响应在服务端提交后丢失，应先查询该租户的 Key 元数据并吊销
不需要的记录，再决定是否创建新 Key。

查询和分页：

```sh
adxadmin key list
adxadmin key list --tenant team-a --page-size 100
adxadmin key list --page-token <previous-next-page-token>
adxadmin --output json key list --tenant team-a
```

列表只包含摘要 ID、租户和到期时间，不包含 Key 明文。使用摘要 ID 吊销：

```sh
adxadmin key revoke <64-character-key-id>
```

服务端吊销后，API Server 与 Edge 已缓存的身份仍可能使用到各自认证缓存 TTL 到期。
CLI 不直接清理服务端缓存。

## 错误与自动化

非成功响应会保留服务端稳定错误码、`retry`、`outcome` 和 `request_id`，并以非零状态
退出。`--output json` 适用于成功结果的机器读取。创建与吊销等管理写操作不会自动
重试。

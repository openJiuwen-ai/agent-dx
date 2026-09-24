# `adx-inspect` 只读排障工具

发布包包含 `/opt/adx/current/bin/adx-inspect`。它读取与 `adxctl` 相同的**本机部署 YAML**，从其中的 `redis_url` 和 `namespace` 连接 ADX 状态库。部署 YAML 中可以使用环境变量展开；例如 AKernel Coordinator Pod 通过 `ADX_REDIS_URL` 注入连接信息。不需要在命令行参数中输入 Redis 密码。

```sh
# 在 ADX 主机上
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml summary
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml node list
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml node get NODE_ID
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml environment get ENVIRONMENT_ID
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml snapshot list

# 更新 AKernel 使用包含该工具的 ADX 发布包后，可直接进入现有 Coordinator Pod
kubectl -n akernel exec deployment/akernel-adx-coordinator -- \
  /opt/adx/current/bin/adx-inspect -c /etc/akernel/adx-coordinator.yaml summary
```

`summary` 显示控制目录的 schema、epoch、generation、revision，以及控制字段数和快照记录数。`node`、`environment`、`snapshot` 各支持 `list` 和 `get ID`。`list` 返回 `ids` 与 `next_cursor`；当游标非零时传入 `--cursor` 继续遍历。`--count` 是 Redis 扫描工作量提示，范围 1–100，不保证精确页大小。

```sh
/opt/adx/current/bin/adx-inspect -c /opt/adx/config/deployment.yaml \
  environment list --cursor 42 --count 50
```

`get` 输出经过筛选：不展示用户环境变量、快照制品位置和 Redis 凭证。尚未提交执行结果的 Environment 显示 `Pending`。命令仅发送 `HGET`、`HLEN`、`HSCAN`；不会修改 Redis，也不会通过全量 Hash 查询遍历主账本。输出是 JSON，适合配合 `jq` 排查。它用于诊断已持久化的状态，不代替用户 API 或实时运行状态探测。需要检查原始字段时，发布包另有 `bin/redis-cli`，应按部署环境的权限要求使用。

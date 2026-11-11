# Nydus Connector 与 lazy_fill

本实现供原生 Nydus `http-proxy` backend 读取 Registry 中已发布的 Nydus 镜像。镜像转换、bootstrap 获取和容器挂载由镜像工具／Nydus 负责。Connector 默认关闭，仅支持原生 DFS，不依赖 FUSE 挂载。

## 模块边界

```text
Node → ConnectorRuntime → 各 ConnectorModule
                           ├─ register_lazy_handlers：注册本机 kind → handler
                           ├─ http_routes：返回绑定 Service 的 Router，合入 Node REST
                           └─ stop：关闭接入、排空仍在执行的文件工作

Nydus → HTTP adapter → NydusService → vfs::Backend（DistributedFs）
                            │                    │ 缺少已提交范围
                            │                    └─ 当前 owner → LazyFillHandler
                            └─ 查询镜像描述                         │
                                 RegistryProvider ← NydusLazyFillHandler
                                      │ 返回完整范围或错误
                                 OCI Registry

DFS 收到范围正文后，沿既有写入、fsync、版本提交和 Peer 读取路径处理。
```

代码在 `src/node/connectors/{mod.rs,nydus/}`；通用合同在 `src/node/vfs/lazy_fill.rs`，DFS 实现在 `src/node/vfs/dfs/lazy_fill.rs`。DFS 不引用 Registry、Nydus 类型或凭据。

`Backend::prepare_lazy_file(ctx,parent,name,&LazyFileSpec)` 发布固定长度、不可普通写入的懒文件。`LazyFileSpec` 只保存长度及 `{kind,payload}`；绑定通过保留 xattr 持久化。临时文件完成长度和绑定后才以最终名称发布。`Backend::publish_prepared` 以 no-replace rename 发布；丢失回复时按 inode 身份确认成功，只在明确被同名文件占用时允许清理失败者。结果仍不确定时保留现场，不能按旧 inode 清空已发布目录。同名相同描述可重入，不同描述报冲突。普通 truncate、写打开、绑定修改和 COW 拷贝不得改变懒文件正文；普通稀疏文件仍读出零。

`LazyFillHandler::fill(LazyFillRequest)` 接收绑定、offset、length、deadline，返回恰好该长度的 Bytes 或错误；它不能提交文件。缺失范围直接由已提交布局判断，没有第二套持久化完成位图。owner 先在文件门闩内合并等待，再申请节点回源槽位；同文件等待者不占用其他文件的槽位。第一版仍按文件串行填充。排空计数独立覆盖等待者、回源及句柄释放。每次最多取 1 MiB，仅补请求覆盖的空缺。超时、短响应和旧 owner epoch 不作为成功提交。提交完成才供读者使用。

## 配置与调用

所有可能担任 owner 的 Node 必须配置一致的 source key、Registry 和 repository，注册同一 handler kind；凭据只在本机配置读取，不写入文件绑定。

在现有 Node TOML 中增加：

```toml
[connectors.nydus]
enabled = true
root_name = ".afs-nydus"
uid = 0
gid = 0
timeout_ms = 30000
max_requests = 16
# http_token_file = "/etc/afs/nydus-http-token"

[connectors.nydus.sources.example]
registry = "https://registry.example.com"
repository = "team/nydus-image"
# username = "reader"
# password_file = "/etc/afs/registry-password"
# bearer_token_file = "/etc/afs/registry-token"
# auth_realm = "https://auth.example.com/token"
# allow_insecure_http = true # 仅用于隔离测试的明文 Registry
```

配置身份必须有存储目录的创建／读取权限。Nydus 路由默认只接受来自回环连接的请求，适合 Nydus 与 AFS Node 同机运行。非回环请求需要在 Node 配置 `http_token_file`，并携带 `Authorization: Bearer …`，远程连接应由部署侧加密保护。原生 Nydus 的 `http-proxy` 配置没有自定义鉴权头，同机直接访问即可，跨机可由受信代理处理鉴权与 TLS。该令牌授予全部已配置来源的读取权限，应只交给受信客户端，且与 Registry 凭据分开。

Nydus backend 示例（替换实际地址与具体 manifest digest）：

```json
{
  "type": "http-proxy",
  "http-proxy": {
    "addr": "http://127.0.0.1:7401",
    "path": "/nydus/example/sha256:MANIFEST_DIGEST/blobs",
    "timeout": 30,
    "retry_limit": 0
  }
}
```

请求为 `HEAD/GET /nydus/{source}/{manifest}/blobs/{blob}`。GET 支持单一字节 Range（含开放结尾、后缀范围），返回 206；无 Range 返回 200；无法满足的范围返回 416。Nydus 自行追加 blob ID。来源信息必须包含 source key 和**具体平台镜像 manifest 的 sha256 digest**；仅有 blob ID 无法推导整张镜像。tag／多平台 index 应在调用方先解析，不接受可变 tag 作为此路径的文件身份。

## 核心流程和失败边界

1. 首次 HEAD 或直接 GET：Service 查询并校验固定 manifest，预建 `.afs-nydus/images/{source}/{manifest-hex}/blobs/{blob-hex}` 以及 `descriptor.json`；不展开容器内部文件树，不下载 blob 正文。每个 blob 是一个文件。不同镜像目录下的共享 blob 尚不合并为同一个 inode。
2. 并发准备：各请求使用私有 staging，既有 no-replace rename 决定最终目录。明确竞争失败的一方校验已发布描述并清理自己的 staging；目录 CAS 冲突的有界重试在 DFS 文件层完成，Service 不识别 Meta 错误码。进程崩溃或结果无法确认可能留下 staging，由运维确认无活动任务后清理，不影响最终目录的可读性。
3. 冷读：本节点已提交范围可读则直接读取；范围未填充则复用现有 owner 路由。owner 调本机 hook，Provider 校验 Range 响应的状态、Content-Range、长度和 deadline；DFS 提交后返回。Provider 不管理副本、选主或文件版本。转发 owner 读取时，沿既有 ReadScope 和 gRPC timeout 传递剩余预算；owner 在排队、回源及交付时使用同一截止时间。
4. 故障：失败的范围不读成零；已有范围仍可复用。回源中断后请求可重试。接管遵循现有租约与 fencing，不能承诺故障后立即切换；已提交数据的可用性取决于既有持久副本，不因绑定了 Registry 就自动重建丢失的已提交 chunk。
5. 停止：关闭 HTTP 接入，排空保留生命周期的文件工作和 lazy-fill，再执行已有 DFS 排空。删除复用 unlink/rmdir 和既有 GC；开放句柄与后台回收仍遵循 AFS 本身的合同。

默认只有按需读取。配置的 Registry 必须受信，默认使用 HTTPS，HTTPS 请求禁止降级到 HTTP。Provider 校验 manifest digest 与传输范围。首次填充的字节依赖受信 Registry 的正确性，完整 blob digest 无法独立校验单次 Range。首次验证不代表性能或跨物理机 HA 资格。

## 验证入口

通用单测过滤器 `distribution_`、`recovery_`；协议测试过滤器 `node::connectors`。真实 OCI Registry、Nydus 与三节点复现材料见 [E2E 脚本](../build/e2e/afs/nydus/README.md)。本次完整运行结果以该入口记录的证据为准。


## 通用副本读取

原生 DFS 节点可设置 `dfs_read_parallelism`（默认 1，最大 8）和
`dfs_read_stripe_bytes`（默认 256 KiB）。同一读请求跨来源时固定文件版本，
使用已有授权与 Peer 请求预算。启用 `dfs_read_cache_bytes`（默认 0）后，
同一版本的已验证范围可以在有界内存中合并。完整 chunk 在后台落入本地目录并登记为临时供数副本。
普通文件和懒文件均可使用。临时副本不计入写入持久化确认。
该配置与 `dfs_peer_cache` 分别选择，避免为同一正文保留两份磁盘缓存。

来源恢复仅重新安装与已提交 chunk 相同的字节。校验或身份冲突时返回读取错误。

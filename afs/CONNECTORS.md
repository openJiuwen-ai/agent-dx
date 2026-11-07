# Nydus Connector 与 lazy_fill

本实现供原生 Nydus `http-proxy` backend 读取 Registry 中已发布的 Nydus 镜像。镜像转换、bootstrap 获取和容器挂载由镜像工具／Nydus 负责。Connector 默认关闭，仅支持原生 DFS，不依赖 FUSE 挂载或后续 DFS 重构。

## 模块边界

```text
Node → ConnectorRuntime → 各 ConnectorModule
                           ├─ register_lazy_handlers：注册本机 kind → handler
                           ├─ http_routes：返回绑定 Service 的 Router，合入 Node REST
                           └─ stop：关闭接入、排空仍在执行的文件工作

Nydus → HTTP adapter → NydusService → vfs::Backend（DistributedFs）
                            │                    │ 缺失范围或已提交副本不可用
                            │                    └─ 当前 owner → LazyFillHandler
                            └─ 查询镜像描述                         │
                                 RegistryProvider ← NydusLazyFillHandler
                                      │ 返回完整范围或错误
                                 OCI Registry

未填充范围沿既有写入、fsync、版本提交处理；已有 Chunk 的来源恢复只重建副本，不改文件版本。
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

[connectors.nydus.sources.example]
registry = "https://registry.example.com"
repository = "team/nydus-image"
# username = "reader"
# password_file = "/etc/afs/registry-password"
# bearer_token_file = "/etc/afs/registry-token"
# auth_realm = "https://auth.example.com/token"
```

配置身份必须有存储目录的创建／读取权限。这些 HTTP 路由使用配置的服务身份，服务于受信任的 Nydus 客户端；本模块没有新增终端用户鉴权。部署方须限制 Node REST 的访问范围，不能把它当作公开的多租户 Registry 网关。

Nydus backend 示例（替换实际地址与具体 manifest digest）：

```json
{
  "type": "http-proxy",
  "http-proxy": {
    "addr": "http://afs-node:7401",
    "path": "/nydus/example/sha256:MANIFEST_DIGEST/blobs",
    "timeout": 30,
    "retry_limit": 0
  }
}
```

请求为 `HEAD/GET /nydus/{source}/{manifest}/blobs/{blob}`。GET 支持单一字节 Range（含开放结尾、后缀范围），返回 206；无 Range 返回 200；无法满足的范围返回 416。Nydus 自行追加 blob ID。来源信息必须包含 source key 和**具体平台镜像 manifest 的 sha256 digest**；仅有 blob ID 无法推导整张镜像。tag／多平台 index 应在调用方先解析，不接受可变 tag 作为此路径的文件身份。

## 核心流程和失败边界

1. 首次 HEAD 或直接 GET：Service 查询并校验固定 manifest，预建 `.afs-nydus/images/{source}/{manifest-hex}/blobs/{blob-hex}` 以及 `descriptor.json`；不展开容器内部文件树，不下载 blob 正文。每个 blob 是一个文件。新准备的描述文件也绑定 `nydus.registry-plan.v1`：hook 按固定 manifest 校验并重建同一序列化描述，避免描述文件丢失先阻断 blob 恢复；这个 kind 的字节编码必须保持稳定，修改编码需新 kind。不同镜像目录下的共享 blob 尚不合并为同一个 inode。
2. 并发准备：各请求使用私有 staging，既有 no-replace rename 决定最终目录。明确竞争失败的一方校验已发布描述并清理自己的 staging；目录 CAS 冲突的有界重试在 DFS 文件层完成，Service 不识别 Meta 错误码。进程崩溃或结果无法确认可能留下 staging，由运维确认无活动任务后清理，不影响最终目录的可读性。
3. 冷读：本节点已提交范围可读则直接读取；范围未填充则复用现有 owner 路由。owner 调本机 hook，Provider 校验 Range 响应的状态、Content-Range、长度和 deadline；DFS 提交后返回。Provider 不管理副本、选主或文件版本。转发 owner 读取时，沿既有 ReadScope 和 gRPC timeout 传递剩余预算；owner 在排队、回源及交付时使用同一截止时间。
4. 故障：失败的范围不读成零；已有范围仍可复用。回源中断后请求可重试。接管遵循现有租约与 fencing，不能承诺故障后立即切换。懒文件的已提交副本不可用时，当前 owner 可再次调用同一 hook，按原布局重建完整 Chunk（至多 4 MiB），核对原描述，沿既有 GC publication pin 和持久副本写入取得 receipt；`RestoreFileChunk` 校验当前 owner、head、Chunk、复制策略和 GC 状态后恢复 Copy/Placement，不改文件版本。未绑定来源的普通文件不获得凭空回源能力。
5. 停止：关闭 HTTP 接入，排空保留生命周期的文件工作和 lazy-fill，再执行已有 DFS 排空。删除复用 unlink/rmdir 和既有 GC；开放句柄与后台回收仍遵循 AFS 本身的合同。

默认只有按需读取，没有全量后台下载或新增回源预取。没有必选分段校验清单：Provider 校验 manifest digest 与传输范围，Nydus 校验内容；完整但错误的源站正文可能先进入缓存，自动修复此类内容错误不在本版范围。首次验证不代表性能或跨物理机 HA 资格。

## 验证入口

通用单测过滤器 `lazy_fill_`、`recovery_`；协议测试过滤器 `node::connectors`。真实 OCI Registry、Nydus 与三节点复现材料见 [E2E 脚本](../build/e2e/afs/nydus/README.md)。本次完整运行结果以该入口记录的证据为准。

已提交副本的来源恢复及普通缓存持久对账见[恢复 E2E](../build/e2e/afs/recovery/README.md)。传输／提交失败保留已校验的本地字节，后续读取按现有布局及本地 catalog 对照 Meta 来源，补齐未确认的副本发布；旧 owner 的迟到回调不能发布。

旧版本已经准备的普通 `descriptor.json` 不会被静默改写为懒文件；需重新准备镜像目录才能获得描述文件来源恢复。本变更不提供旧目录在线迁移。

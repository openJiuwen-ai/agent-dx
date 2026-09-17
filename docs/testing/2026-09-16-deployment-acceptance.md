# 部署接线与公开密钥管理验收

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

后续已完成 package-v22 的整份安装示例启动复验，见[完整安装示例验收](2026-09-16-installed-example.md)。以下保留 package-v18 当次证据及范围。

2026-09-16，修复统一部署示例的三处不匹配：Edge 控制路由使用可解析的 exact/prefix 格式并包含管理接口；Edge 上游对应本机 HTTP Frontend；Edge 与 Node Proxy 两侧配置完整 mTLS。示例的内部服务地址采用单机回环地址，资源源采用 auto，并配置 SQLite 降级日志。

同时修复 Edge 默认路由遗漏 `/api/admin/v1/keys`。原先真实 HTTPS 管理接口测试直连 Frontend，未覆盖 Edge 入口。新用例通过 HTTPS Edge 执行管理员创建、查询、吊销和重复吊销；验证租户管理请求被拒绝、新密钥可使用、列表不暴露明文、吊销在缓存期限内生效。该用例加入本地和 Kubernetes 共用的 auth 组；K8s Secret 同步投影管理员测试密钥。

## 验证

| 检查 | 结果 |
| --- | --- |
| 新路由/部署接线回归 | 红灯3项，修复后3/3通过 |
| 既有静态路由回归 | 1/1通过 |
| K8s 管理密钥投影 | 缺项红灯；修复后全部48项驱动契约通过 |
| 当前配置示例与真实 adxctl | validate/render通过，0700目录/0600文件、发现参数注入、重复服务ID拒绝 |
| Linux ARM64 workspace release binaries | 构建通过 |
| package-v18 本地双节点 E2E | 六组全过，清理零残留 |

- `sdk`：passed，22.504秒。
- `auth`：passed，20.857秒。
- `capacity`：passed，33.394秒。
- `placement`：passed，94.855秒。
- `restart`：passed，34.984秒。
- `stop`：passed，21.062秒。

单机安装说明见 [部署指南](../deployment/standalone.md)，说明了证书和叶证书 DER 配套更新、启动读取配置、公共 SDK 就绪检查及 stop 删除本机实例的契约。CLI 渲染检查没有启动整份示例；真实双节点启动使用入库 E2E 配置器，二者的验证范围分开记录。

## 制品与证据

- `out/ci/pause-resume/package-v18/manifest.json`：`aarch64-unknown-linux-gnu / release`，基准 `0dde79ad57583e998389101a763e4d2d825be63e` 加未提交修改，`dirty=true`。
- Rust bins 从当前源码重新构建；未变更的 Go API、SDK 和 Redis 复用已校验 package-v17 文件，SHA256保持一致。当前部署示例随新包组装并校验。
- 节点镜像：`sha256:7b5d0232d043b151464ecb73646e3793d7d592a42055b6792e32782be06a4a8f`。
- RRT 镜像：`sha256:b0ae1641a392ff1d4cf22c35b9e60147953419de6af0dbaaa92bb114e669ca81`。
- sandboxd：`efc201531d7e2e9d69505da151eb66084b61eebf`，本轮 runc。
- `out/ci/stage-6/deployment/{red,green,route-regression,secret-red,driver-final,render,build-linux,package-e2e}.log`：回归、CLI、编译与E2E日志。
- 同目录 `render-result.json`、`bundle/bundle.json`、`local/{result,auth-result,case-results}.json`：结构化结果与制品身份。
- 本轮资源 ID：`adx-e2e-5334af671f69`；两个测试容器、网络和后端实例清理成功。

本轮为同主机两个逻辑节点的本地验收，未运行正式 Buildkite/K8s。配置/证书热重载与 x86 双克隆对照继续待办；本轮未修复 FC 双克隆网络问题或实现故障接管。

# 发布流水线与软件包规划

## 目标

ADX 使用三条职责单一的 Buildkite 流水线：基础出包、Python Sandbox SDK 出包、
Full 端到端验收。基础出包流水线同时生成相互独立的 Platform 包、RRT 包和 Runtime
Pack；RRT 不进入 Platform 包，但不单独占用一条流水线。构建只发生在前两条流水线；
Full 流水线消费不可变制品，不得从源码重新编译或替换二进制。

最终发布由一个机器可读的 `release.json` 串联平台包、RRT、SDK、运行后端包、OCI
镜像和部署包。进程部署与 Kubernetes 部署使用相同的二进制和版本关系。RRT 是
独立的数据面运行时产品，不是 Platform Package 内的目录或附属二进制。

## 当前实现与调整边界

当前 `.buildkite/pipeline.yml` 依次完成平台构建、镜像发布和 Kubernetes E2E；
`build/release/build.sh` 同时编译 Rust 平台、RRT 和 Python SDK，统一
`adx-release.tar.gz` 内还包含 SDK wheel。它已经提供了干净提交检查、Cargo 缓存、
逐文件 SHA256、镜像 digest 和 K8s 验收基础，但各类制品的生命周期耦合在一起。

调整后：

- Platform 包不再包含 RRT 或 Python SDK，也不把 RRT payload 或 wheel 当作平台包
  完整性的必需文件。
- 同一条基础出包流水线独立组装 RRT 静态二进制、EROFS payload 和 OCI 镜像，输出
  单独的 RRT 版本、归档与候选清单。
- Python SDK 独立测试、定版和发布。
- sandboxd、runc、Firecracker 等执行后端进入独立 Runtime Pack；它们仍由部署环境
  托管，不并入 `adxctl` 的控制面进程监督范围。
- Full 流水线下载平台、RRT、SDK、Runtime Pack 和镜像清单，核对关联关系后部署测试。
- 面向用户的一键离线包在 Full 流水线开始前组合，并使用同一份包完成安装验收；
  Full 通过后只提升已有字节，不重新打包。

## 流水线一：ADX Base Packages

建议 Buildkite pipeline 名称为 `adx-base-package`，配置入口为
`.buildkite/pipeline-package.yml`。

### 触发

- Pull Request：影响控制面、Gateway、部署、内部协议、Runtime Pack 或打包逻辑时触发。
- 主分支提交：总是触发，保留候选制品。
- `vX.Y.Z` 标签：构建正式候选，待 Full 流水线通过后提升至正式下载位置。
- 手动构建：允许指定提交和目标架构，禁止从 dirty tree 发布。

### 步骤

1. `source-gate`
   - 校验提交身份和干净工作树。
   - 执行格式、Clippy、Rust 单元/契约测试、协议生成一致性、文档与打包器测试。
   - 不执行长时间的集群、故障注入和 checkpoint E2E。
2. `platform-build-{amd64,arm64}`
   - 使用固定 Rust/Go/Redis 工具链和持久 Cargo/sccache 缓存。
   - 每个架构只编译一次 ADX 控制面、Gateway 和 `adxctl`。
3. `rrt-build-{amd64,arm64}`
   - 独立构建静态 `rrt-runtime`、本地 EROFS payload 和 RRT OCI 镜像。
   - RRT 镜像只复制已验证二进制，不在 Dockerfile 中重新编译。
   - 输出 RRT 协议版本、二进制/payload SHA256 和镜像 digest。
4. `runtime-pack-{backend}-{arch}`
   - 从 `third_party/*/source.json` 指定的提交和补丁构建 sandboxd 与后端依赖。
   - 首期提供 `runc`；Firecracker 使用独立 KVM Runtime Pack。
5. `platform-assemble-{arch}`
   - 组装平台包、配置模板、安装工具、许可证、SBOM 和逐文件清单。
   - 在全新目录执行 `package.py verify`，并做解包与 `adxctl --version` 冒烟。
6. `platform-images-{arch}`
   - 只从已经验证的平台包制作控制面、Node 和 Gateway 镜像，不在 Dockerfile 中重新编译。
   - 输出不可变 digest、基础镜像 digest 和平台包 SHA256 的关联清单。
7. `package-smoke-{arch}`
   - 只使用前面步骤上传的 Platform、RRT 和 runc Runtime Pack，不读取 Cargo target
     或工作区内的临时二进制。
   - 在单节点真实 Linux 环境启动 Redis、Master、API Server、Edge、Node Manager、
     Node Proxy、sandboxd 和包内 RRT。
   - 验证服务就绪、API Key 鉴权、Instance 创建/查询、RRT 命令、文件读写、显式删除、
     资源释放和 sandboxd inventory 清空。
   - 用例必须有界，不包含 checkpoint、节点失联、进程重启、日志滚动等待和多节点放置。
   - 使用仓库内固定的轻量 HTTP/数据面 smoke client，不临时构建或发布 Python SDK；
     公共 SDK 与平台制品的组合由 Full 流水线验收。目标总耗时不超过 10 分钟。
   - 输出 JUnit、逐用例日志、组件日志、包/镜像身份、最终 inventory 和清理结果；
     缺失用例、跳过或清理失败均阻止候选生成。
8. `candidate-index`
   - 分别输出 `platform-candidate.json`、`rrt-candidate.json` 和 Runtime Pack 清单。
   - 汇总清单记录提交、版本、架构、全部 SHA256/digest、兼容范围和同一个 Buildkite
     build ID，但不把三个归档重新合并。
9. `obs-publish-{arch}`
   - 仅消费本次构建已验证的候选归档和清单，不重新编译或打包。
   - 日常候选写入 `adx/daily/<timestamp>-<commit12>/<platform>/<arch>/`；正式候选
     写入 `adx/release/<version>/<platform>/<arch>/`。
   - 上传后回读对象元数据，并生成包含提交、Buildkite build ID、大小、SHA256 和 URL
     的 `manifest.json`。OBS 凭据仅通过 Kubernetes Secret 注入。

### 产物

```text
adx-platform-<version>-linux-<arch>.tar.zst
adx-platform-<version>-linux-<arch>.tar.zst.sha256
adx-rrt-<rrt-version>-linux-<arch>.tar.zst
adx-rrt-<rrt-version>-rootfs-<arch>.erofs
adx-runtime-runc-<runtime-version>-linux-<arch>.tar.zst
adx-runtime-firecracker-<runtime-version>-linux-<arch>.tar.zst   # KVM 候选
platform-candidate.json
rrt-candidate.json
platform-sbom.spdx.json
rrt-sbom.spdx.json
platform-images.json                                            # 全部使用 digest
rrt-images.json
logs/、junit/
```

平台包对所有角色保持一致，通过每台主机自己的 YAML profile 启动 Master、Node、
API Server、Edge 或 Standalone，避免按角色维护多套二进制包。

基础出包的 `source-gate` 负责单元、契约和静态检查，`package-smoke` 负责验证刚生成
制品的最小真实闭环。它证明基础包可安装、可启动和可完成一次 Instance 生命周期，
但不替代 Full 流水线中的公共 SDK、多节点、故障和恢复验收。只有前述门禁通过的
候选才允许进入 OBS；上传步骤失败时，该候选不具备可发布状态。

## 流水线二：ADX Python SDK Package

建议 Buildkite pipeline 名称为 `adx-python-sdk`，配置入口为
`.buildkite/pipeline-sdk.yml`。

### 触发

- SDK、公开 OpenAPI、数据面协议或兼容性测试发生变化时，由 Pull Request 触发。
- 主分支提交生成开发候选。
- SDK 标签 `sdk-vX.Y.Z` 生成正式候选；SDK 继续使用自己的版本号。

### 步骤

1. `sdk-contract`
   - 检查 SDK 与 `sandbox.yaml`、`data-plane.yaml` 的能力和错误映射。
2. `sdk-test-py{310,311,312,313}`
   - 单元、传输、重试、PTY、reverse tunnel 和类型契约测试。
3. `sdk-build`
   - 在隔离环境构建 wheel 和 sdist；执行 `twine check` 或等价元数据检查。
4. `sdk-install-smoke`
   - 在无源码 `PYTHONPATH` 的新虚拟环境安装 wheel，验证导入、CLI 和版本。
5. `sdk-candidate-index`
   - 输出版本、提交、Python 范围、公开协议版本、文件 SHA256 和 build ID。
6. `sdk-publish`
   - 仅正式标签且发布凭据可用时执行；先上传暂存索引，通过回读安装后再提升。

### 产物

```text
adx_sandbox-<sdk-version>-py3-none-any.whl
adx_sandbox-<sdk-version>.tar.gz
sdk-candidate.json
sdk-sbom.spdx.json
logs/、junit/
```

SDK wheel 不再隐式跟随平台版本。一个 ADX release 通过 `release.json` 明确选择已经
验证过的 SDK 版本，允许 SDK 修订版本独立发布。

## 流水线三：ADX Full Acceptance

建议 Buildkite pipeline 名称为 `adx-full-test`，配置入口为
`.buildkite/pipeline-full.yml`。

### 输入与触发

输入必须是：

- `ADX_BASE_PACKAGE_BUILD_ID`（同一构建中的 Platform、RRT 和 Runtime Pack）
- `ADX_SDK_BUILD_ID`
- 目标架构和目标部署环境
- 可选的 `ADX_FIRECRACKER_RUNTIME_BUILD_ID`

流水线只允许通过 Buildkite artifact API 或正式制品仓读取这些 build ID 对应的内容。
主分支候选可手动或串联触发；每日定时运行最新同提交候选；正式标签必须通过后才能
发布。Full 流水线自身不接受源码变化，也不调用 Cargo、Go 或 Python build。

### 步骤

1. `resolve-candidate`
   - 从基础出包 build 下载相互独立的 Platform、RRT、Runtime Pack 清单和归档，
     再下载 SDK 候选，核对提交、协议版本、兼容范围、架构和 SHA256。
   - 生成唯一 `release.json`，不允许用“最新”标签补齐缺失输入。
2. `compose-offline-bundle`
   - 将平台包、RRT 包、SDK、默认 runc Runtime Pack、部署模板和安装器组合成离线包。
   - 立即执行完整解包校验；后续用例使用这份离线包。
3. `fresh-install`
   - 在干净 Linux 环境执行一键安装，验证目录权限、systemd/前台启动入口、
     `adxctl config init/validate/render/status/stop` 和卸载边界。
4. `k8s-full`
   - 复用现有两 Pod/两 worker 部署，运行完整十组公共 SDK 用例。
   - 覆盖 SDK、认证、容量、放置、本地优先、数据面、生命周期、节点故障、重启和停止。
   - `full` 要求两个 ADX Node Pod 位于不同物理 worker；保存实际 placement。
5. `standalone-full`
   - 使用同一离线包完成真实 sandboxd/RRT 的进程部署验收。
   - 长耗时、日志滚动、Collector 重启和完整停机放在这里，不进入基础出包门禁。
6. `firecracker-full`（条件步骤）
   - 只在明确标记的 KVM worker 上运行 pause/resume、snapshot、clone 和故障恢复。
   - 无 KVM 时标记为不适用，不能伪装成 runc 通过。
7. `release-verdict`
   - 汇总 JUnit、`result.json`、清理结果、后端 inventory、日志、镜像 digest 和安装包摘要。
   - 所选用例缺失、跳过、清理失败、制品身份不一致均阻止发布。

Full 流水线建议每日和发版候选执行。每次提交的快速门禁只保留有界的 L0 创建、执行、
文件和删除闭环，避免把等待心跳、日志滚动、节点故障等固定耗时塞入基础流水线。

## 软件包组织

### 1. 平台包

```text
adx-platform-<version>-linux-<arch>/
├── bin/                         # 所有角色和 adxctl
├── etc/
│   ├── profiles/                # standalone/master/node/edge-api
│   ├── examples/
│   └── systemd/
├── install/
│   ├── install.sh
│   └── uninstall.sh
├── third_party/                 # 版本、许可证和补丁信息，不混入后端二进制
├── LICENSE
├── VERSION
├── manifest.json
└── sbom.spdx.json
```

安装到版本化目录 `/opt/adx/releases/<version>`，原子更新
`/opt/adx/current`；`/etc/adx` 和 `/var/lib/adx` 永远不放进 release 目录，升级不得
覆盖用户配置和状态。回滚只切换 `current` 并重启对应服务。

### 2. RRT 包

```text
adx-rrt-<rrt-version>-linux-<arch>/
├── bin/rrt-runtime
├── payload/adx-runtime-rootfs.img
├── config/capabilities.json
├── manifest.json
├── LICENSE
└── sbom.spdx.json
```

RRT 包安装到 `/opt/adx/rrt/<rrt-version>`，由 `release.json` 选择当前受测版本。
Platform 升级不隐式替换 RRT；安装器在切换版本前校验 Platform/RRT 协议兼容范围。

### 3. Runtime Pack

```text
adx-runtime-<backend>-<runtime-version>-linux-<arch>/
├── bin/sandboxd
├── bin/<runc|firecracker|helper>
├── guest/                       # Firecracker 时存在
├── config/
├── manifest.json
├── compatibility.json
├── LICENSES/
└── sbom.spdx.json
```

Runtime Pack 与平台包分开，是因为 runc 与 Firecracker 的宿主权限、内核、guest 和
更新频率不同。安装器可以安装并生成 sandboxd 的 systemd unit，但 ADX supervisor
仍不托管 sandboxd。

### 4. Python SDK 包

标准 wheel/sdist 是唯一 SDK 发布物。平台包不再复制一份可能过期的 wheel；离线包
可以携带 `packages/python/` 下由 `release.json` 指定的 wheel。

### 5. 部署与离线包

```text
adx-offline-<release-version>-linux-<arch>.tar.zst
├── release.json
├── packages/platform.tar.zst
├── packages/rrt.tar.zst
├── packages/runtime-runc.tar.zst
├── packages/python/adx_sandbox-*.whl
├── deploy/process/              # YAML profiles、systemd 模板
├── deploy/kubernetes/           # Helm chart，仅渲染现有进程/Pod，不增加 Operator
├── install.sh
└── SHA256SUMS
```

在线安装器根据 `release.json` 下载相同的文件；离线安装器从本地 `packages/` 读取。
二者共享安装逻辑，不能维护两套目录规则。

`release.json` 是机器生成的制品锁文件，不是用户部署配置。用户仍只编辑 YAML。
示例：

```json
{
  "schemaVersion": 1,
  "release": "0.2.0",
  "commit": "<40-hex>",
  "platform": {"version": "0.2.0", "target": "linux-amd64", "sha256": "..."},
  "rrt": {"version": "0.2.3", "protocol": 1, "sha256": "..."},
  "sdk": {"name": "adx-sandbox", "version": "0.2.1", "sha256": "..."},
  "runtime": {"backend": "runc", "version": "sandboxd-efc2015.1", "sha256": "..."},
  "images": {"node": "registry/adx-node@sha256:...", "rrt": "registry/adx-rrt@sha256:..."}
}
```

## 一键安装与部署体验

安装和部署分为两个可重复步骤，便于生产环境先审查配置：

```sh
# 在线或离线执行相同安装逻辑
sudo ./install.sh --prefix /opt/adx --runtime runc

# 单机默认配置；生成后可审查 YAML
sudo /opt/adx/current/bin/adxctl config init --profile standalone
sudo /opt/adx/current/bin/adxctl deploy --config /etc/adx/deployment.yaml
```

规划新增的 `adxctl deploy` 应完成 `validate`、`render`、systemd unit 安装、按依赖顺序
启动和业务就绪检查；失败时保留日志并回滚本轮启动的服务。现有 `run` 继续作为前台
容器/Pod 入口。

开发快速体验可增加显式的 `--dev-bootstrap`，生成仅用于回环/测试的本地证书和随机
API Key。生产 profile 必须由部署环境提供证书、密钥、网络、持久目录和外部依赖，
不能在无提示的情况下生成生产凭据。

Kubernetes 一键部署使用 chart：

```sh
helm upgrade --install adx deploy/kubernetes/adx \
  --namespace adx --create-namespace -f deployment-values.yaml
```

chart 只组织现有进程、Secret、Service、持久卷和外部 sandboxd Runtime Pack，保持
当前“进程运行在 Pod 内”的设计，不引入新的控制器。

## 版本与发布规则

- Platform 使用 `vX.Y.Z`，RRT 使用独立版本字段，SDK 使用独立 `sdk-vX.Y.Z`，
  Runtime Pack 使用后端提交和补丁集派生的不可变版本。Platform 与 RRT 可在同一次
  基础出包构建中生成，但必须是两个独立归档和两个独立 manifest。
- `release.json` 才定义一次 ADX 产品发布中 Platform、RRT、SDK 和 Runtime Pack 的
  受测组合。
- 候选构建、Full 测试和正式发布使用同一份字节；正式发布只复制或提升制品。
- 所有归档提供 SHA256、SBOM、来源提交和 Buildkite build ID；后续可增加签名，校验
  流程预留签名字段。
- Platform OCI 镜像由已验证平台包构建，RRT OCI 镜像由已验证 RRT 包构建；各自必须
  在清单中反向记录来源包 SHA256。
- `latest` 只作为人类便利标签，不进入安装、测试或发布清单。

## 实施顺序

1. 先把 `build/release/build.sh` 拆成 Platform 与 RRT 两个独立组装结果，同时拆出
   SDK build，并升级 package manifest schema。
2. 新增三份 pipeline YAML；保留现有 `.buildkite/pipeline.yml` 作为过渡入口，通过
   动态上传选择 package、sdk 或 full。
3. 增加 `release.json` 和候选清单校验器，Full 流水线禁止源码构建。
4. 完成版本化安装目录、`install.sh` 和 fresh-install 测试。
5. 增加 `adxctl deploy` 与 systemd 模板，再提供离线包。
6. 最后增加 Helm chart；复用经过 Full 验证的镜像 digest 和同一份 release 清单。

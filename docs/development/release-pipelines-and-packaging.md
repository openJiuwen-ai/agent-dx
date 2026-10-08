# 发布流水线与软件包规划

## 目标

ADX 使用四条 Buildkite 流水线：基础出包（含 SDK 和 adxadmin）、SDK 独立出包、adxadmin 独立出包和 Full 端到端验收。基础出包流水线同时生成相互独立的 Platform 包、Execd 包和 Runtime
Pack；Execd 不进入 Platform 包，但不单独占用一条流水线。产品构建发生在前三条流水线；
Full 流水线消费不可变制品，不得从源码重新编译或替换二进制。

最终发布由一个机器可读的 `release.json` 串联平台包、Execd、SDK、运行后端包、OCI
镜像和部署包。进程部署与 Kubernetes 部署使用相同的二进制和版本关系。Execd 是
独立的数据面运行时产品，不是 Platform Package 内的目录或附属二进制。

## 当前实现与调整边界

当前仓库提供四个配置入口：基础出包使用 `.buildkite/pipeline-package.yml`，
Python SDK 使用 `.buildkite/pipeline-sdk.yml`，adxadmin 使用 `.buildkite/pipeline-admin.yml`，Full 验收使用 `.buildkite/pipeline-full.yml`。
入口 `.buildkite/pipeline.yml` 按 pipeline slug 分派。Full 显式指定基础包与 SDK build UUID，
核对提交、候选清单和 SHA256，只消费制品，不重编译产品。

基础出包包含六个并行门禁：Platform、Gateway、Execd 各自先运行 UT 再编译出包，
SDK 与 adxadmin 分别运行 UT、wheel/sdist 构建及安装检查，source gate 检查 fmt/Clippy 和构建脚本。
公共 Rust crate 测试归 Platform，Agent crate 测试归 Gateway，分组测试检查整个 workspace
没有漏项或重复。被显式忽略的环境依赖测试仍由专项验收负责，不计为本轮 UT 已通过。
adxadmin 复用 SDK 的 Python 3.12 容器执行器，不使用 Rust 构建镜像的 Python 3.9。

组件中间产物默认经 OBS `adx/ci/<build UUID>/<commit>/<component>/` 传递，校验身份和
SHA256 后组装。`ADX_ARTIFACT_TRANSPORT=buildkite` 可切回 Buildkite 传递。
组装、临时目录安装检查和默认 OBS 上传都在 `platform-build` 内完成，上传直接使用本地
字节，不再下载整包。最终基础包、独立 `adx-execd.tar.gz`、backend、SDK 和 adxadmin 仍存为 Buildkite artifact。
独立的 `artifact-manifest` 步骤校验 OBS manifest 的提交与 build ID，并将包含全部制品链接、
大小和 SHA256 的 `out/buildkite/index.html` 上传到 Buildkite Artifacts。
基础线随后复用镜像组合与 K8s 驱动，执行 `l0` SDK/auth 及清理门禁；Full 仍单独运行全量用例。
基础线的可选 PyPI 发布必须等待 L0 通过。OBS 上传是组装后的候选产物发布，不能仅凭 URL 判断 L0 已通过。
`build-manifest.json` 绑定基础包各组成；`admin-candidate.json` 单独绑定同提交、同 build 的 Python 包。

基础包默认正式上传 OBS，可设 `ADX_OBS_UPLOAD=0` 关闭；独立 SDK 流水线默认不上传，
需要 `ADX_OBS_UPLOAD=1` 显式开启。
`ADX_ADMIN_PYPI_UPLOAD=1` 与 `ADX_SDK_PYPI_UPLOAD=1` 分别控制管理工具和 SDK 的 PyPI
发布，还需对应版本标签、凭据。仓库选择由各自 `*_PYPI_REPOSITORY=pypi|testpypi` 指定。
OBS 中间传递与正式上传是两个独立控制；完全不用 OBS 时选择 `buildkite` 且关闭上传。
参数默认值、Secret 和路径见 [Buildkite 说明](../../.buildkite/README.md#pipeline-controls)。

现阶段兼容的一体化 `adx-release.tar.gz` 仍包含 Execd 和 SDK wheel。独立 SDK 流水线输出的 wheel、sdist 与
`sdk-candidate.json` 是 Full 验收的 SDK 输入；Full 不使用基础包内的 wheel。继续拆分
Platform 与 Runtime Pack 的安装器结构属于后续包结构改造。Execd 当前已同时提供独立归档；
不能把当前一体包描述成已经完成全部拆分。

目标边界：

- Platform 包不再包含 Execd 或 Python SDK，也不把 Execd payload 或 wheel 当作平台包
  完整性的必需文件。
- 同一条基础出包流水线独立组装 Execd 静态二进制、EROFS payload 和 OCI 镜像，输出
  单独的 Execd 版本、归档与候选清单。
- Python SDK 独立测试、定版和发布。
- sandboxd、runc、Firecracker 等执行后端进入独立 Runtime Pack；它们仍由部署环境
  托管，不并入 `adxctl` 的控制面进程监督范围。
- Full 流水线下载平台、Execd、SDK、Runtime Pack 和镜像清单，核对关联关系后部署测试。
- 面向用户的一键离线包在 Full 流水线开始前组合，并使用同一份包完成安装验收；
  Full 通过后只提升已有字节，不重新打包。

## 流水线一：ADX Base Packages

Buildkite pipeline slug 为 `agent-dx`，配置入口为
`.buildkite/pipeline-package.yml`。

### 触发

- Pull Request：影响控制面、Gateway、部署、内部协议、Runtime Pack 或打包逻辑时触发。
- 主分支提交：总是触发，保留候选制品。
- `vX.Y.Z` 标签：构建正式候选，待 Full 流水线通过后提升至正式下载位置。
- 手动构建：允许指定提交和目标架构，禁止从 dirty tree 发布。

### 步骤

当前已落地的是 Linux AMD64 的 `build-platform`、`build-gateway`、`build-execd`、
`admin-package`、`sdk-package`、`source-gate`、`platform-build` 及 K8s L0 链路。下面列出的多架构独立候选、package smoke、
SBOM 和最终 `release.json` 是目标状态，不能作为当前流水线已经提供的能力。

1. `source-gate`
   - 校验提交身份和干净工作树。
   - 执行格式、Clippy、Rust 单元/契约测试、协议生成一致性、文档与打包器测试。
   - 不执行长时间的集群、故障注入和 checkpoint E2E。
2. `platform-build-{amd64,arm64}`
   - 使用固定 Rust/Go/Redis 工具链和持久 Cargo/sccache 缓存。
   - 每个架构只编译一次 ADX 控制面、Gateway 和 `adxctl`；平台无关的 `adxadmin` wheel 不进入原生编译。
3. `execd-build-{amd64,arm64}`
   - 独立构建静态 `adx-execd`、本地 EROFS payload 和 Execd OCI 镜像。
   - Execd 镜像只复制已验证二进制，不在 Dockerfile 中重新编译。
   - 输出 Execd 协议版本、二进制/payload SHA256 和镜像 digest。
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
   - 只使用前面步骤上传的 Platform、Execd 和 runc Runtime Pack，不读取 Cargo target
     或工作区内的临时二进制。
   - 在单节点真实 Linux 环境启动 Redis、Coordinator、内嵌 Ingress 的 API Server、内嵌
     Relay 的 adxlet、sandboxd 和包内 Execd。
   - 验证服务就绪、API Key 鉴权、Environment 创建/查询、Execd 命令、文件读写、显式删除、
     资源释放和 sandboxd inventory 清空。
   - 用例必须有界，不包含 checkpoint、节点失联、进程重启、日志滚动等待和多节点放置。
   - 使用仓库内固定的轻量 HTTP/数据面 smoke client，不临时构建或发布 Python SDK；
     公共 SDK 与平台制品的组合由 Full 流水线验收。目标总耗时不超过 10 分钟。
   - 输出 JUnit、逐用例日志、组件日志、包/镜像身份、最终 inventory 和清理结果；
     缺失用例、跳过或清理失败均阻止候选生成。
8. `candidate-index`
   - 分别输出 `platform-candidate.json`、`execd-candidate.json` 和 Runtime Pack 清单。
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
adx-execd-<execd-version>-linux-<arch>.tar.zst
adx-execd-<execd-version>-rootfs-<arch>.erofs
adx-runtime-runc-<runtime-version>-linux-<arch>.tar.zst
adx-runtime-firecracker-<runtime-version>-linux-<arch>.tar.zst   # KVM 候选
platform-candidate.json
execd-candidate.json
platform-sbom.spdx.json
execd-sbom.spdx.json
platform-images.json                                            # 全部使用 digest
execd-images.json
logs/、junit/
```

当前发布包默认共进程部署：`adx-apiserver` 内嵌 Ingress，`adxlet` 内嵌 Relay；同时提供 `adx-ingress` 和 `adx-relay`，供显式分进程部署。调试用 `adx-data-plane-forward` 不随包发布。

平台包的 `bin/` 为 `adxctl`、`adx-coordinator`、`adxlet`、`adx-apiserver` 和
`redis-server`；一体化归档继续包含 `runtime/adx-execd`、EROFS payload 与 SDK。

基础出包的 `source-gate` 负责单元、契约和静态检查，`package-smoke` 负责验证刚生成
制品的最小真实闭环。它证明基础包可安装、可启动和可完成一次 Environment 生命周期，
但不替代 Full 流水线中的公共 SDK、多节点、故障和恢复验收。只有前述门禁通过的
候选才允许进入 OBS；上传步骤失败时，该候选不具备可发布状态。`source-gate` 同时
运行 `adxadmin` 的快速 HTTP/CLI 契约测试，但管理工具 wheel 仍按独立制品构建。

## 流水线二：ADX Python SDK Package

Buildkite pipeline slug 为 `agent-dx-python-sdk`，配置入口为
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
   - `ADX_SDK_PYPI_UPLOAD=1` 且标签严格匹配 `sdk-v<version>` 时才执行；
     `ADX_SDK_PYPI_REPOSITORY` 可选择 PyPI 或 TestPyPI。
   - 只上传 `sdk-package` 生成并校验的 wheel/sdist，上传后按文件名和 SHA256 回读索引；
     OBS 发布仍由独立的 `ADX_OBS_UPLOAD` 控制。

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

## 流水线三：adxadmin 独立出包

入口为 `agent-dx-admin` 的 `.buildkite/pipeline-admin.yml`，复用基础线的测试出包脚本。`admin-package` 在每次构建中运行 Python 单测和发布契约
测试，使用 `python -m build` 生成一个通用 wheel 和一个 sdist，执行 `twine check`，并
在无源码路径的新虚拟环境中安装 wheel。`admin-candidate.json` 记录版本、提交、build ID
及两个文件的 SHA256。

上传是可选动作。默认没有发布步骤；只有 `ADX_ADMIN_PYPI_UPLOAD=1` 时才运行
`admin-pypi`。`ADX_ADMIN_PYPI_REPOSITORY` 可设为 `testpypi` 或 `pypi`，省略时选择
正式 PyPI。发布还要求标签严格为 `adxadmin-v<version>`，从 Kubernetes Secret
`adx-pypi-credentials` 读取对应 API Token。发布步骤只消费 `admin-package` 的不可变
候选，不重新构建，也不跳过已存在版本；上传后通过索引 JSON API 核对完整文件集合和
SHA256。这样普通提交、PR 和未显式开启上传的标签构建都只留下可审查制品，不修改包
索引。

## 流水线四：ADX Full Acceptance

Buildkite pipeline slug 为 `agent-dx-full-test`，配置入口为
`.buildkite/pipeline-full.yml`。

### 输入与触发

输入必须是：

- `ADX_BASE_PACKAGE_BUILD_ID`（同一构建中的 Platform、Execd 和 Runtime Pack）
- `ADX_SDK_BUILD_ID`
- 目标架构和目标部署环境
- 可选的 `ADX_FIRECRACKER_RUNTIME_BUILD_ID`

流水线只允许通过 Buildkite artifact API 或正式制品仓读取这些 build ID 对应的内容。
主分支候选可手动或串联触发；每日定时运行最新同提交候选；正式标签必须通过后才能
发布。Full 流水线自身不接受源码变化，也不调用 Cargo、Go 或 Python build。

### 步骤

1. `resolve-candidate`
   - 从基础出包 build 下载相互独立的 Platform、Execd、Runtime Pack 清单和归档，
     再下载 SDK 候选，核对提交、协议版本、兼容范围、架构和 SHA256。
   - 生成唯一 `release.json`，不允许用“最新”标签补齐缺失输入。
2. `compose-offline-bundle`
   - 将平台包、Execd 包、SDK、默认 runc Runtime Pack、部署模板和安装器组合成离线包。
   - 立即执行完整解包校验；后续用例使用这份离线包。
3. `fresh-install`
   - 在干净 Linux 环境执行一键安装，验证目录权限、systemd/前台启动入口、
     `adxctl config init/validate/render/status/stop` 和卸载边界。
4. `k8s-full`
   - 复用现有两 Pod/两 worker 部署，运行完整十组公共 SDK 用例。
   - 覆盖 SDK、认证、容量、放置、本地优先、数据面、生命周期、节点故障、重启和停止。
   - `full` 要求两个 ADX Node Pod 位于不同物理 worker；保存实际 placement。
5. `standalone-full`
   - 使用同一离线包完成真实 sandboxd/Execd 的进程部署验收。
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
├── bin/                         # 共进程入口、adxctl 和 Redis
├── etc/
│   ├── profiles/                # standalone/coordinator/node/ingress-api
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

安装到版本化目录 `/opt/adx/releases/<commit>`，原子更新
`/opt/adx/current`；`/opt/adx/config`、`/opt/adx/data` 和 `/opt/adx/run` 永远不放进
release 目录，升级不得覆盖用户配置和状态。回滚只切换 `current` 并重启对应服务。

### 2. Execd 包

```text
adx-execd-<execd-version>-linux-<arch>/
├── bin/adx-execd
├── payload/adx-runtime-rootfs.img
├── config/capabilities.json
├── manifest.json
├── LICENSE
└── sbom.spdx.json
```

Execd 包安装到 `/opt/adx/execd/<execd-version>`，由 `release.json` 选择当前受测版本。
Platform 升级不隐式替换 Execd；安装器在切换版本前校验 Platform/Execd 协议兼容范围。

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

### 5. 管理工具包

`adxadmin` 使用独立版本的纯 Python wheel/sdist，面向管理员工作站而非集群节点。源码
入口为 `tools/admin/`，`tools/admin/build.sh` 生成
`adxadmin-<version>-py3-none-any.whl`。平台原生包不包含该工具；离线包可以携带选定
版本的 wheel。

### 6. 部署与离线包

```text
adx-offline-<release-version>-linux-<arch>.tar.zst
├── release.json
├── packages/platform.tar.zst
├── packages/execd.tar.zst
├── packages/runtime-runc.tar.zst
├── packages/python/adx_sandbox-*.whl
├── packages/python/adxadmin-*.whl
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
  "execd": {"version": "0.2.3", "protocol": 1, "sha256": "..."},
  "sdk": {"name": "adx-sandbox", "version": "0.2.1", "sha256": "..."},
  "runtime": {"backend": "runc", "version": "sandboxd-efc2015.1", "sha256": "..."},
  "images": {"node": "registry/adx-node@sha256:...", "execd": "registry/adx-execd@sha256:..."}
}
```

## 一键安装与部署体验

安装和部署分为两个可重复步骤，便于生产环境先审查配置：

```sh
# 在线或离线执行相同安装逻辑
sudo ./install.sh --prefix /opt/adx --runtime runc

# 单机默认配置；生成后可审查 YAML
sudo adxctl config init --profile standalone
sudo adxctl deploy --config /opt/adx/config/deployment.yaml
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

- Platform 使用 `vX.Y.Z`，Execd 使用独立版本字段，SDK 使用独立 `sdk-vX.Y.Z`，
  Runtime Pack 使用后端提交和补丁集派生的不可变版本。Platform 与 Execd 可在同一次
  基础出包构建中生成，但必须是两个独立归档和两个独立 manifest。
- `release.json` 才定义一次 ADX 产品发布中 Platform、Execd、SDK 和 Runtime Pack 的
  受测组合。
- 候选构建、Full 测试和正式发布使用同一份字节；正式发布只复制或提升制品。
- 所有归档提供 SHA256、SBOM、来源提交和 Buildkite build ID；后续可增加签名，校验
  流程预留签名字段。
- Platform OCI 镜像由已验证平台包构建，Execd OCI 镜像由已验证 Execd 包构建；各自必须
  在清单中反向记录来源包 SHA256。
- `latest` 只作为人类便利标签，不进入安装、测试或发布清单。

## 实施顺序

1. 已把基础构建拆为 Platform、Gateway、Execd 三个组件归档，并由独立组装步骤生成
   当前兼容基础包和 `build-manifest.json`；下一步拆成 Platform 与 Execd 两个正式候选，
   同时升级 package manifest schema。
2. 已新增三份 pipeline YAML；`.buildkite/pipeline.yml` 只负责按 pipeline slug 动态
   上传 package、sdk 或 full 配置。
3. 已增加 SDK 候选清单校验器，并让 Full 显式消费基础包和 SDK build UUID；继续增加
   完整 `release.json`，并拆分 Platform、Execd 与 Runtime Pack 候选清单。
4. 完成版本化安装目录、`install.sh` 和 fresh-install 测试。
5. 增加 `adxctl deploy` 与 systemd 模板，再提供离线包。
6. 最后增加 Helm chart；复用经过 Full 验证的镜像 digest 和同一份 release 清单。

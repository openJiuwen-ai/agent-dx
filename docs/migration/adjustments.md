# 导入适配

> 历史记录：命令、组件和产物名称对应当时版本；当前命名见 [组件命名](../architecture/naming.md)。

> 首次导入阶段的历史记录。后续 Rust 控制面、HTTP RRT、统一部署和 K8s 已实施；当前目录和状态见 [架构](../architecture/repository-layout.md) 与 [实施总览](../testing/control-plane-implementation.md)。以下来源、数量和当时边界保留。

- Agent 包与测试目录移动；版本文件的相对路径/源码包包含规则同步调整。
- Sandbox SDK 版本由自身 `pyproject.toml` 的 `[project].version` 单一声明，不从仓库标签或发布环境变量推导。
- Gateway、RRT 加入根 Cargo workspace。根锁文件以 Gateway 原锁为基础补充 RRT 依赖。
- 协议文件集中到 platform/api/proto/legacy，build.rs/codegen 指向新路径。Frontend go_package 和所有本地 Go import 重写到单模块 internal 目录。
- Go 按实际调用裁剪：保留 Sandbox HTTP handler、必要编码与实例摘要；删除整个 internal/legacy 和 runtimeapi，通过显式 backend 依赖接入后端。保留 9 条 Agent 路由，认证后透传到宿主注入的 Agent 层处理器。生成代码按脚本重建。
- RRT initgroups 第二参数使用平台推导的 C 类型转换，修复 macOS 签名差异。
- build.sh、Makefile、静态 Linux 构建脚本与源码文档调整路径；build/ 保留源码脚本，out/ 保存临时打包产物。
- 原整个集群 AIO 测试 harness 未迁入；源路径及固定提交见 sources.json。单组件和 mock 测试已迁入。

Sandbox SDK 命名已按决定改为 adx-sandbox / adx_sandbox / ADX_*，测试、示例与联调引用同步更新。HTTP 字段和实例生命周期规则保持不变；配置前缀、品牌协议头及路由键已同步采用 ADX，需同版本组件配套运行。

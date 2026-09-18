# 内置运行环境

统一部署配置的 `runtime_environment` 定义实例默认根文件系统和 bootstrap。运行环境可以来自本地 EROFS 文件，也可以来自同一个不可变 OCI image。两种来源都包含静态 RRT、基础命令、目录和 CA 证书，不包含 Python。

```json
{
  "runtime_environment": {
    "rootfs": {
      "runtime": "runsc",
      "type": "local",
      "path": "/opt/adx/runtime/adx-runtime-rootfs.img",
      "readonly": false
    },
    "bootstrap": {
      "type": "erofs",
      "root": "/opt/adx/runtime/adx-runtime-rootfs.img",
      "target": "/__adx",
      "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"]
    },
    "env": {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
  }
}
```

OCI 配置使用同一个 digest 引用作为默认 rootfs 和自定义镜像的 bootstrap：

```json
{
  "runtime_environment": {
    "rootfs": {
      "runtime": "runc",
      "type": "image",
      "image": "registry.example/adx-runtime@sha256:...",
      "readonly": false
    },
    "bootstrap": {
      "type": "image",
      "image": "registry.example/adx-runtime@sha256:...",
      "target": "/__adx",
      "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"]
    },
    "env": {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
  }
}
```

`adxctl` 校验配置结构并向 API Server 和 Node Manager 下发同一配置。OCI 运行环境必须使用完整 `@sha256:` 摘要，避免不同节点或重启前后解析到不同内容。EROFS 模式下，Node Manager 启动时检查文件存在且具有 EROFS 格式标识；OCI 模式由 sandboxd 按 image digest 拉取、校验并缓存。配置修改后重启组件生效。启动入口使用 argv 数组，不进行隐式 shell 拼接。

| 请求 | 行为 |
|---|---|
| 不传 image | 选择的内置环境直接作为 rootfs；内置 `/__adx/usr -> /usr` 等软链接保证统一入口有效。 |
| 指定 image | 用户镜像作为根文件系统；同一个 EROFS 或 OCI 环境只读挂载到 `/__adx`。用户镜像不需要预装 RRT。 |
| 仅指定 runtime | 只覆盖 runsc/runc/firecracker 选择，保留内置环境，不添加 bootstrap 挂载。 |

EROFS 本身不会被修改；`rootfs.readonly=false` 表示实例获得自己的可写文件系统。执行后端支持范围仍由 sandboxd 决定。

API Server 把选定配置放入 InstanceSpec，由 Master 与实例一起持久化。快照创建继承源实例配置；Node Manager 拒绝与本机配置不一致的环境。部署包 manifest 记录制品摘要；不要原地替换正在使用的制品，升级时应使用新的版本路径。

RRT 在 Linux PID 1 场景内置进程回收：单线程启动阶段 fork 实际服务进程，PID 1 转发信号并回收已退出的孤儿；HTTP/命令/PTY 服务子进程保留自身的子进程等待逻辑。主服务退出后返回对应退出码（信号退出为 `128+signal`）。非 PID 1 启动维持原行为，孤儿由所在环境的 init 管理。

发布构建需要原生 musl Rust target、musl C 工具链、`busybox-static`、`erofs-utils` 和 `readelf`。`build/runtime/rootfs.py` 拒绝带动态解释器的 RRT/BusyBox，再生成并检查 EROFS。构建示例与进程部署见 [standalone](standalone.md)。

使用 EROFS 配置的节点必须支持从普通文件创建只读 loop 设备并实际挂载 EROFS。`/proc/filesystems` 中出现 `erofs` 只表示驱动已登记，不足以证明该内核构建和设备路径可用；对应 preflight 会对发布包内制品执行一次真实挂载和卸载。OCI 配置不要求 EROFS，仍要求 sandboxd 能访问并解析配置的 digest 引用。Buildkite K8s 验收使用 OCI 模式；本地与 standalone 验证继续覆盖 EROFS 模式。

本轮真实 runc 启动、双节点 SDK 和 PID 1 回收结果见 [验证记录](../testing/2026-09-17-runtime-environment.md)。

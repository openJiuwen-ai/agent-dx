# 本地运行环境

统一部署配置的 `runtime_environment` 定义实例默认根文件系统和 bootstrap。Linux release 包携带 `runtime/adx-runtime-rootfs.img`，由静态 RRT、静态 BusyBox、基础目录及 CA 证书构成，不包含 Python。文件路径按 sandboxd 所在环境解释，各节点必须部署匹配制品。

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

`adxctl` 校验配置结构并向 API Server 和 Node Manager 下发同一配置；Node Manager 启动时检查文件存在且具有 EROFS 格式标识。配置修改后重启组件生效。启动入口使用 argv 数组，不进行隐式 shell 拼接。

| 请求 | 行为 |
|---|---|
| 不传 image | 使用本地 rootfs。内置 `/__adx/usr -> /usr` 等软链接保证启动入口有效。 |
| 指定 image | 用户镜像作为根文件系统；bootstrap 的本地 EROFS 只读挂载到 `/__adx`。用户镜像不需要预装 RRT。 |
| 仅指定 runtime | 只覆盖 runsc/runc/firecracker 选择，保留本地 rootfs，不添加 bootstrap 挂载。 |

EROFS 本身不会被修改；`rootfs.readonly=false` 表示实例获得自己的可写文件系统。执行后端支持范围仍由 sandboxd 决定。

API Server 把选定配置放入 InstanceSpec，由 Master 与实例一起持久化。快照创建继承源实例配置；Node Manager 拒绝与本机配置不一致的环境。部署包 manifest 记录制品摘要；不要原地替换正在使用的制品，升级时应使用新的版本路径。

RRT 在 Linux PID 1 场景内置进程回收：单线程启动阶段 fork 实际服务进程，PID 1 转发信号并回收已退出的孤儿；HTTP/命令/PTY 服务子进程保留自身的子进程等待逻辑。主服务退出后返回对应退出码（信号退出为 `128+signal`）。非 PID 1 启动维持原行为，孤儿由所在环境的 init 管理。

发布构建需要原生 musl Rust target、musl C 工具链、`busybox-static`、`erofs-utils` 和 `readelf`。`build/runtime/rootfs.py` 拒绝带动态解释器的 RRT/BusyBox，再生成并检查 EROFS。构建示例与进程部署见 [standalone](standalone.md)。

本轮真实 runc 启动、双节点 SDK 和 PID 1 回收结果见 [验证记录](../testing/2026-09-17-runtime-environment.md)。

# 本地开发进度页

进度服务仅监听 `127.0.0.1`，页面每 3 秒读取 JSON 状态。命令包装器记录开始、结束、退出码和日志路径；阶段完成需要明确更新，命令通过不会自动代表阶段通过。多个构建任务使用文件锁与原子替换写入同一状态文件。

在仓库根目录执行一次初始化，再启动页面：

```bash
python3 build/dev/progress.py init --roadmap docs/testing/control-plane-roadmap.md
python3 build/dev/progress.py stage 1 --status active --note '正在完成同节点暂停恢复验收'
python3 build/dev/progress.py serve
```

服务输出访问地址，地址与 PID 同时写入 `out/dev/progress/server.json`。停止该服务后页面会保留最近一次状态，显示断线并继续尝试连接。

把实际任务放在 `run` 后，标准输出和错误会同时写入日志并继续输出到调用方，退出码原样传回：

```bash
python3 build/dev/progress.py run --stage 1 --job node-tests \
  --label 'Node Manager 单测' --log out/dev/progress/node-tests.log \
  -- cargo test -p adx-node-manager -j 2
```

同一 job ID 展示最近一次运行，日志追加保留。不同任务使用不同 ID 和日志文件。命令参数、环境变量和日志正文不会被放入页面状态；测试本身的输出仍应遵守项目凭证脱敏要求。页面只提供首页与状态接口，不提供任意文件浏览或执行命令接口。

阶段状态由验收结论驱动：

```bash
python3 build/dev/progress.py stage 1 --status blocked --note 'OCI 根文件系统需要启用 virtio-fs'
python3 build/dev/progress.py stage 1 --status complete --note '真实公共 SDK 暂停恢复用例通过'
python3 build/dev/progress.py stage 2 --status active
```

`--state /绝对路径/state.json` 可在子命令前指定独立任务状态文件。`serve --port 端口` 可固定本机地址，默认自动选取可用端口。服务显示包装器进程已消失但没有结束记录的任务为“结果待核对”；不会据此判通过。重启系统后需重新启动服务。

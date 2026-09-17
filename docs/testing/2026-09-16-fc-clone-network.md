# Firecracker 双克隆 CONNECT 超时取证

> 当次验收/调查记录：版本、数字及未覆盖范围仅适用于文中批次；当前实现与状态见 [实施总览](control-plane-implementation.md) 和 [阶段路线图](control-plane-roadmap.md)。

统一 package-v17 在 Lima `adx-fc` 上运行 r21、r22，均在第一个克隆的文件写入失败；每次已通过10/18项，包括新增的SDK实例标签、亲和条件组及偏好参数接线。r22补充网络取证，产品二进制、SDK、RRT与超时设置保持相同。两轮完整验收均失败，不能用其中已通过的用例代替完整通过。

## 已确认的故障边界

r22的第一个克隆位于 `10.231.16.178`，预期guest MAC `02:fc:0a:e7:10:b2`，TAP为`tap.0ae710b2`；第二个克隆位于`10.231.16.92`，预期MAC `02:fc:0a:e7:10:5c`，TAP为`tap.0ae7105c`。

两个克隆启动后均曾通过命令执行和文件读取。第二个克隆启动后，再向第一个克隆写入文件，Node Proxy的3秒TCP连接超时触发504。失败现场：

- 邻居表中两个IP对应的MAC正确。
- 网桥FDB把两个guest MAC都学到了第二个克隆的`tap.0ae7105c`，第一个克隆的MAC条目指向错误端口。
- 绕过Edge和Node Proxy直接连接第一个克隆的50090端口，前两次各2秒超时，第三次约1秒后成功；第二个克隆三次均成功。
- 因此不是仅有HTTP路由缓存失配；同时存在可观测的二层转发错误。网络随后自行恢复，不能视为首次操作已成功。

sandboxd日志表明生成可复用快照时，源实例使用`.178`/`...:b2`；第二个克隆恢复到`.92`/`...:5c`。当前假设是恢复的旧网络状态或在途报文重新发出了源MAC，影响了同MAC的现存克隆。FDB与连接探测支持这个调查方向，但尚未抓到触发学习的报文，不能据此断言具体内核或VMM代码已定位。

固定sandboxd的`configureNetwork`通过netlink设置MAC、替换IPv4与默认路由；Firecracker snapshot load替换宿主TAP，之后经guest agent重新配置网络。恢复网络与旧报文处理属于该执行后端边界；本轮没有通过增加Frontend重试、延长Node Proxy超时或改变用例顺序消除失败。

## 制品与证据

- ADX基础提交`0dde79ad57583e998389101a763e4d2d825be63e`及未提交工作树；`out/ci/pause-resume/package-v17/manifest.json`记录发布制品摘要。
- sandboxd PR #56固定提交`efc201531d7e2e9d69505da151eb66084b61eebf`。
- Firecracker `v1.16.1-akernel.3`，提交`b9a362d1070ee17991e7388d3b53a9ccce25ecca`，原生ARM64/KVM。
- RRT OCI digest `sha256:23d2266203257711e1403bde3ddc62499d31480b4f66a5d3af0bbb27a55a7d0a`。
- `out/ci/stage-5/groups/fc-r21.log`与`fc-r22.log`保存完整执行输出。
- `out/ci/pause-resume/fc-r22/evidence/network-failure.json`记录FDB、邻居表、网卡、路由及TCP探测；`sandboxd-service.log`与`state/logs/`用于对应实例身份和时间。
- 两轮最后均返回平台停止成功；没有运行到完整制品/资源清空验收，不能声明本轮18项通过。既有package-v16/r20的17项通过是之前版本和用例顺序的历史证据。

下一步在后端恢复路径抓取只含网络头的报文，验证源MAC重放时序；修复后保留本次用例顺序，重新运行完整18项及清理验收。正式Kubernetes Buildkite尚未运行本轮功能。

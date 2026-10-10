# AFS 标准套件准备

为 STD-01 至 STD-05 固定上游版本、发现测试清单、记录适用性及运行短 ext4 对照。工具输出不是产品验收结果。

仅在 Linux 上运行；状态和数据必须位于真实 ext4，不能使用宿主共享挂载。默认 `STATE_ROOT=/var/lib/afs-acceptance/suites-reference`，可显式覆盖；不依赖特定 VM 名称或磁盘标签。正式运行前先检查 Git、编译器、套件依赖和容量。不得将 smoke 结果改标为完整 STD PASS，不得在失败后追加排除项。

- `prepare_ctl_reference.sh`：下载固定版本套件、记录身份、生成 inventory/applicability/accounting，执行短 ext4 smoke。
- `random_model.py`：STD-04 随机操作模型。
- `suite-contract.json`：上游 pins 和套件范围。
- `ltp-filesystem-selectors.txt`：LTP filesystem 子集选择器。

每轮证据默认写入 `$STATE_ROOT/evidence/runs/$RUN_ID`；通过 `EVIDENCE_ROOT` 可显式指定仓外目录。保留 summary、upstream identities、inventory、applicability、accounting、logs 和 short-ext4 原始输出，失败记录不得覆盖。

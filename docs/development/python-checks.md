# Python 提交前检查

从仓库根目录运行，先安装 `uv`，并更新 PR 目标分支对应的本地 remote ref。例如目标分支为 `github/master`：

```sh
git fetch github master
make python-check PYTHON_CHECK_BASE=github/master
```

入口固定使用 Ruff 0.11.13 和 Pylint 3.3.8，首次运行由 `uv tool run` 安装工具。
它从目标分支与 HEAD 的共同祖先开始，检查 PR 中所有新增或修改的 Python 文件，
同时包含暂存、未暂存和未被忽略的新文件。扫描完整文件；删除的文件不扫描。
基线必须显式提供，无法解析时检查失败，避免漏掉之前提交里的文件。

当前本地规则覆盖：120 字符行宽、语法和常见名称错误、函数/参数/局部变量命名、
外层变量遮蔽、重复异常，以及直接用相对命令名启动子进程。
规则配置在 `build/ci/ruff-codecheck.toml`，Pylint 规则与版本在 `build/ci/python-check.sh`。
该入口是独立本地预检，现有 Buildkite 流水线尚未调用它。

静态检查通过后，还要运行改动组件的回归测试。测试通过只能证明已覆盖的行为；
CodeCheck 的平台规则另行检查。处理 Python 代码时尤其注意：

- URL 使用 `urllib.parse` 构建和编码；动态字典查找显式处理键缺失。
- 外部命令先通过 `shutil.which` 查找、处理缺失，再解析为绝对路径。
- 第三方回调命名等例外需核对平台支持情况；本地 `pylint: disable` 不代表平台认可。
- 每次修复后重新检查整个 PR 的 Python 文件，避免改名等修复引入超长行。
- 推送后核对 CodeCheck 报告的 `head_sha` 与 PR 最新提交完全相同，并确认最终结果。

本地检查仅覆盖上述规则，不能等同于完整的远端 CodeCheck。后续平台发现的新规则，
应在修复时同步补入可自动检查的本地规则，或补入有明确适用场景的审查要求。

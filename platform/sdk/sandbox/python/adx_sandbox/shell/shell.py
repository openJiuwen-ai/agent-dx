"""Persistent shell sessions backed by HTTP submit and poll actions.

State such as cwd, environment variables, and shell functions is preserved
across ``run()`` calls because the same bash process lives inside the sandbox.
Communication uses short-lived ``shell.run`` and ``shell.poll`` requests, which
avoids gateway idle-connection timeouts.
"""

import asyncio
import logging
import os
import re
from typing import Optional

from .._transport import SandboxClient
from ..types import CommandResult

logger = logging.getLogger(__name__)

# Long-poll wait inside the container (seconds).
_POLL_WAIT = int(os.environ.get("ADX_POLL_WAIT", 10))


class Shell:
    """A persistent shell session. Created via :meth:`Shells.create`."""

    def __init__(self, client: SandboxClient, sandbox_id: str, session_id: str):
        self._client = client
        self._sid = sandbox_id
        self._session_id = session_id
        self._lock = asyncio.Lock()

    @property
    def session_id(self) -> str:
        return self._session_id

    async def run(
        self,
        cmd: str,
        envs: Optional[dict] = None,
        cwd: Optional[str] = None,
        timeout: int = 60,
    ) -> CommandResult:
        """Execute *cmd* in the persistent shell and return the result."""
        async with self._lock:
            effective_cmd = self._build_cmd(cmd, envs=envs, cwd=cwd)

            submit = await asyncio.to_thread(
                self._client.invoke,
                self._sid,
                "shell.run",
                {
                    "session_id": self._session_id,
                    "command": effective_cmd,
                    "timeout": timeout,
                },
            )
            if submit.get("error"):
                logger.warning(
                    "Shell.run submit failed: sandbox=%s session=%s error=%s",
                    self._sid,
                    self._session_id,
                    submit["error"],
                )
                return CommandResult("", submit["error"], -1)

            deadline = asyncio.get_event_loop().time() + timeout + _POLL_WAIT + 5
            while asyncio.get_event_loop().time() < deadline:
                poll = await asyncio.to_thread(
                    self._client.invoke,
                    self._sid,
                    "shell.poll",
                    {"session_id": self._session_id, "wait_timeout": _POLL_WAIT},
                )
                if poll["status"] == "done":
                    return CommandResult(
                        stdout=self._clean_output(poll.get("stdout", "")),
                        stderr=poll.get("stderr", ""),
                        exit_code=poll.get("exit_code", 0),
                    )
                if poll["status"] == "error":
                    return CommandResult("", poll.get("error", "unknown error"), -1)
                # status == "running" → keep polling

            return CommandResult("", f"Command timed out after {timeout}s", -1)

    async def kill(self) -> None:
        """Destroy this shell session."""
        try:
            await asyncio.to_thread(
                self._client.invoke,
                self._sid,
                "shell.close",
                {"session_id": self._session_id},
            )
        except Exception as e:
            logger.warning("Shell.kill failed: session=%s: %s", self._session_id, e)

    def close(self) -> None:
        """Synchronously destroy the session (safe from non-async contexts)."""
        try:
            self._client.invoke(
                self._sid, "shell.close", {"session_id": self._session_id}
            )
        except Exception as e:
            logger.warning("Shell.close failed: session=%s: %s", self._session_id, e)

    # EXECD's bash_poll returns raw pty output up to the ``__EXECD_DONE_<rc>__``
    # sentinel (command echo + output + prompt + the echoed sentinel command).
    # Clean it here so callers receive command output only.
    # TODO(execd): ideally EXECD's bash_poll should strip this server-side.
    _ANSI_RE = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    _EXECD_PROMPT = "__EXECD_PROMPT__ "

    @classmethod
    def _clean_output(cls, raw: str) -> str:
        text = cls._ANSI_RE.sub("", raw)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = text.split("\n")
        # Drop the leading command echo. EXECD reserves a deterministic prompt
        # marker so output without a trailing newline can be retained while
        # the prompt and sentinel command are removed.
        body = []
        for line in lines[1:]:
            if cls._EXECD_PROMPT in line:
                output, _prompt = line.split(cls._EXECD_PROMPT, 1)
                if output:
                    body.append(output)
                continue
            if "__EXECD_DONE_" not in line:
                body.append(line)
        return "\n".join(body).strip("\n")

    @staticmethod
    def _build_cmd(
        cmd: str, envs: Optional[dict] = None, cwd: Optional[str] = None
    ) -> str:
        """Wrap *cmd* with one-shot cd/export prefixes in a subshell."""
        if not cwd and not envs:
            return cmd
        parts = []
        if cwd:
            parts.append(f"cd {cwd}")
        if envs:
            for k, v in envs.items():
                parts.append(f"export {k}='{v}'")
        parts.append(cmd)
        return "( " + " && ".join(parts) + " )"

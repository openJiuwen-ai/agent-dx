"""Recoverable command execution for sandbox v1.

The public identity is ``command_id``.  A pid is exposed for diagnostics only
and is never required to recover a handle after the SDK process restarts.
"""

import asyncio
import logging
import random
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from ._http_pool import SandboxClientClosedError
from ._transport import SandboxClient, SandboxHTTPError
from ._command_metrics import increment, observe_wait
from .types import CommandInfo, CommandResult, CommandStatus

logger = logging.getLogger(__name__)

_POLL_THRESHOLD = 30
_POLL_INTERVAL = 10
_POLL_RETRY_DELAY = 1  # seconds between failed wait calls
_COMMAND_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class CommandSubmissionError(RuntimeError):
    """Submission outcome is unknown, but the stable ID remains recoverable."""

    def __init__(
        self,
        message: str,
        command_id: str,
        may_have_started: bool,
        *,
        sandbox_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.command_id = command_id
        self.may_have_started = may_have_started
        self.sandbox_id = sandbox_id
        self.request_id = request_id


class CommandNotFound(LookupError):
    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str = "",
        command_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.request_id = request_id


class CommandConflict(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str = "",
        command_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.request_id = request_id


class CommandExpired(CommandNotFound):
    pass


class CommandWaitTimeout(TimeoutError):
    def __init__(self, sandbox_id: str, command_id: str, timeout: Optional[float]):
        super().__init__(f"command {command_id} did not finish within {timeout} seconds")
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.timeout = timeout


class CommandUnavailable(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str = "",
        command_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.request_id = request_id


class UnsupportedFeature(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str = "",
        command_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.request_id = request_id


class ResourceExhausted(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str = "",
        command_id: str = "",
        request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.sandbox_id = sandbox_id
        self.command_id = command_id
        self.request_id = request_id


def _validate_command_id(command_id: str) -> str:
    if not isinstance(command_id, str) or _COMMAND_ID.fullmatch(command_id) is None:
        raise ValueError(
            "command_id must contain 1..128 ASCII letters, digits, '.', '_', ':', or '-'"
        )
    return command_id


def _result(snapshot: dict) -> CommandResult:
    raw_status = snapshot.get("status")
    if raw_status is None:
        raw_status = "SUCCEEDED" if int(snapshot.get("exit_code", -1)) == 0 else "FAILED"
    status_text = str(raw_status).upper()
    if status_text in ("DONE", "FINISHED"):
        status_text = "SUCCEEDED" if int(snapshot.get("exit_code", 0)) == 0 else "FAILED"
    status = CommandStatus(status_text)
    stdout_truncated = bool(snapshot.get("stdout_truncated", False))
    stderr_truncated = bool(snapshot.get("stderr_truncated", False))
    exit_code = snapshot.get("exit_code")
    return CommandResult(
        stdout=str(snapshot.get("stdout", "")),
        stderr=str(snapshot.get("stderr", "")),
        exit_code=int(exit_code) if exit_code is not None else None,
        status=status,
        truncated=bool(snapshot.get("truncated", stdout_truncated or stderr_truncated)),
        error_code=snapshot.get("error_code"),
        error_message=snapshot.get("error_message"),
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
    )


def _info(snapshot: dict) -> CommandInfo:
    if "status" in snapshot:
        status_text = str(snapshot["status"]).upper()
    else:
        status_text = "RUNNING" if bool(snapshot.get("running", False)) else "SUCCEEDED"
    if status_text == "DONE":
        status_text = "SUCCEEDED" if int(snapshot.get("exit_code", 0)) == 0 else "FAILED"
    status = CommandStatus(status_text)
    finished_at = snapshot.get("finished_at_ms")
    created_at = snapshot.get("created_at_ms")
    started_at = snapshot.get("started_at_ms")
    pid = snapshot.get("pid")
    return CommandInfo(
        pid=int(pid) if pid not in (None, -1) else None,
        command=str(snapshot.get("cmd", "")),
        running=status in (CommandStatus.PENDING, CommandStatus.RUNNING),
        id=str(snapshot.get("command_id", "")),
        status=status,
        exit_code=int(snapshot["exit_code"]) if snapshot.get("exit_code") is not None else None,
        created_at=datetime.fromtimestamp(int(created_at) / 1000, timezone.utc) if created_at is not None else None,
        started_at=datetime.fromtimestamp(int(started_at) / 1000, timezone.utc) if started_at is not None else None,
        finished_at=datetime.fromtimestamp(int(finished_at) / 1000, timezone.utc) if finished_at is not None else None,
    )


class CommandHandle:
    """A recoverable reference to one command in one sandbox."""

    def __init__(
        self,
        command_id: str,
        client: SandboxClient,
        sandbox_id: str,
        pid: int = 0,
    ):
        self.command_id = _validate_command_id(command_id)
        self.pid = pid
        self._client = client
        self._sid = sandbox_id

    @property
    def id(self) -> str:
        return self.command_id

    @property
    def sandbox_id(self) -> str:
        return self._sid

    def _snapshot(self) -> CommandInfo:
        return _info(self._raw_snapshot())

    def _raw_snapshot(self) -> dict:
        try:
            snapshot = self._client.invoke(
                self._sid, "process.get", {"command_id": self.command_id}
            )
        except SandboxHTTPError as error:
            if error.status_code == 404:
                raise CommandNotFound(
                    str(error.payload.get("error", error)),
                    sandbox_id=self._sid,
                    command_id=self.command_id,
                    request_id=error.request_id,
                ) from error
            raise
        status = str(snapshot.get("status", "")).upper()
        if status in ("NOT_FOUND", "EXPIRED"):
            error_type = CommandExpired if status == "EXPIRED" else CommandNotFound
            raise error_type(
                snapshot.get("error", f"command {self.command_id} not found"),
                sandbox_id=self._sid,
                command_id=self.command_id,
                request_id=getattr(snapshot, "request_id", None),
            )
        info = _info(snapshot)
        if info.pid:
            self.pid = info.pid
        return snapshot

    def poll(self) -> CommandStatus:
        return self._snapshot().status

    def wait(self, timeout: Optional[float] = None) -> CommandResult:
        increment("command_wait_total")
        started = time.monotonic()
        try:
            snapshot = self._raw_snapshot()
            if str(snapshot.get("status", "")).upper() in ("PENDING", "RUNNING"):
                connection = getattr(self._client, "_connection", None)
                if connection is None:
                    snapshot = self._client.invoke(
                        self._sid,
                        "process.wait",
                        {"command_id": self.command_id, "timeout": timeout},
                        timeout=-1 if timeout is None else max(1, int(timeout) + 1),
                    )
                else:
                    from ._command_watch import manager_for

                    manager_for(connection).wait(self._sid, self.command_id, timeout)
                    snapshot = self._raw_snapshot()
            return _result(snapshot)
        finally:
            observe_wait(time.monotonic() - started)

    async def wait_async(self, timeout: Optional[float] = None) -> CommandResult:
        """Wait without blocking the caller's event loop.

        The shared command watch transport can wake this wait; the authoritative
        terminal result is always fetched through ``process.wait/get``.
        """
        connection = getattr(self._client, "_connection", None)
        if connection is None:
            return await asyncio.to_thread(self.wait, timeout)

        increment("command_wait_total")
        started = time.monotonic()
        try:
            snapshot = await asyncio.to_thread(self._raw_snapshot)
            if str(snapshot.get("status", "")).upper() in ("PENDING", "RUNNING"):
                from ._command_watch import manager_for

                await manager_for(connection).wait_async(
                    self._sid, self.command_id, timeout
                )
                snapshot = await asyncio.to_thread(self._raw_snapshot)
            return _result(snapshot)
        finally:
            observe_wait(time.monotonic() - started)

    def kill(self) -> bool:
        return bool(
            self._client.invoke(
                self._sid, "process.kill", {"command_id": self.command_id}
            )["killed"]
        )

    def send_stdin(self, data: str, eof: bool = False) -> None:
        response = self._client.invoke(
            self._sid,
            "process.send_stdin",
            {"command_id": self.command_id, "data": data, "eof": eof},
        )
        if response.get("error"):
            raise RuntimeError(f"Failed to send stdin: {response['error']}")

    def close_stdin(self) -> None:
        self.send_stdin("", eof=True)


class Commands:
    """Command collection belonging to one sandbox."""

    def __init__(self, client: SandboxClient, sandbox_id: str, default_cwd: Optional[str] = None):
        self._client = client
        self._sid = sandbox_id
        self._default_cwd = default_cwd
        self._capabilities_checked = False

    def _require_recovery_capability(self, command_id: str = "") -> None:
        if self._capabilities_checked:
            return
        try:
            response = self._client.invoke(self._sid, "process.capabilities", {})
        except Exception as error:
            raise UnsupportedFeature(
                "sandbox runtime does not expose the recoverable command capability",
                sandbox_id=self._sid,
                command_id=command_id,
                request_id=getattr(error, "request_id", None),
            ) from error
        capabilities = set(response.get("capabilities", ()))
        required = {"stable-command-id", "recoverable-command-result", "multiplexed-command-watch"}
        if response.get("protocol_version") != 1 or not required.issubset(capabilities):
            raise UnsupportedFeature(
                "sandbox runtime does not support command recovery protocol v1",
                sandbox_id=self._sid,
                command_id=command_id,
                request_id=getattr(response, "request_id", None),
            )
        self._capabilities_checked = True

    def run(
        self,
        cmd: str,
        background: bool = False,
        envs: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        timeout: Optional[int] = None,
        stdin: bool = False,
        *,
        command_id: Optional[str] = None,
    ) -> Union[CommandResult, CommandHandle]:
        """Run a command, with a default 60-second foreground deadline.

        Background commands have no execution deadline unless ``timeout`` is
        supplied explicitly.
        """
        if stdin and not background:
            raise ValueError("stdin is only supported when background=True")
        effective_cwd = cwd if cwd is not None else self._default_cwd
        if background:
            increment("command_submit_total")
            stable_id = _validate_command_id(command_id or f"cmd-{uuid.uuid4()}")
            self._require_recovery_capability(stable_id)
            request = {
                "command_id": stable_id,
                "command": cmd,
                "envs": envs,
                "cwd": effective_cwd,
                "want_stdin": stdin,
            }
            if timeout is not None:
                request["timeout"] = timeout
            try:
                response = self._client.invoke(self._sid, "process.start", request)
            except SandboxHTTPError as error:
                if error.status_code == 409:
                    raise CommandConflict(
                        str(error.payload.get("error", error)),
                        sandbox_id=self._sid,
                        command_id=stable_id,
                        request_id=error.request_id,
                    ) from error
                if error.status_code in (429, 503):
                    raise ResourceExhausted(
                        str(error.payload.get("error", error)),
                        sandbox_id=self._sid,
                        command_id=stable_id,
                        request_id=error.request_id,
                    ) from error
                if error.status_code == 501:
                    raise UnsupportedFeature(
                        str(error.payload.get("error", error)),
                        sandbox_id=self._sid,
                        command_id=stable_id,
                        request_id=error.request_id,
                    ) from error
                raise RuntimeError(str(error.payload.get("error", error))) from error
            except Exception as error:
                raise CommandSubmissionError(
                    f"command submission outcome is unknown: {error}",
                    command_id=stable_id,
                    may_have_started=True,
                    sandbox_id=self._sid,
                    request_id=getattr(error, "request_id", None),
                ) from error
            if response.get("error"):
                error_code = str(response.get("error_code", ""))
                if error_code == "COMMAND_CONFLICT":
                    raise CommandConflict(
                        str(response["error"]),
                        sandbox_id=self._sid,
                        command_id=stable_id,
                        request_id=getattr(response, "request_id", None),
                    )
                if error_code == "RESOURCE_EXHAUSTED":
                    raise ResourceExhausted(
                        str(response["error"]),
                        sandbox_id=self._sid,
                        command_id=stable_id,
                        request_id=getattr(response, "request_id", None),
                    )
                raise RuntimeError(f"Failed to start command: {response['error']}")
            return CommandHandle(stable_id, self._client, self._sid, int(response.get("pid", 0)))

        timeout = 60 if timeout is None else timeout
        if timeout > _POLL_THRESHOLD:
            return self._run_with_poll(cmd, envs, effective_cwd, timeout)
        response = self._client.invoke(
            self._sid,
            "process.exec",
            {"cmd": cmd, "envs": envs, "cwd": effective_cwd, "timeout": timeout},
            timeout=timeout,
        )
        return _result(response)

    def _run_with_poll(
        self, cmd: str, envs: Optional[Dict[str, str]], cwd: Optional[str], timeout: int
    ) -> CommandResult:
        handle = self.run(
            cmd,
            background=True,
            envs=envs,
            cwd=cwd,
            timeout=timeout,
        )
        assert isinstance(handle, CommandHandle)
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                try:
                    handle.kill()
                except SandboxHTTPError as error:
                    if error.status_code != 400 or error.payload.get("error_code") != "COMMAND_NOT_RUNNING":
                        raise
                    # RRT may reach its execution deadline before the local
                    # wait expires. Return that authoritative terminal result.
                    return handle.wait(0)
                return CommandResult(
                    "",
                    f"Command timed out after {timeout} seconds",
                    None,
                    status=CommandStatus.TIMED_OUT,
                )
            wait = min(_POLL_INTERVAL * (0.7 + random.random() * 0.6), remaining)
            try:
                return handle.wait(wait)
            except SandboxClientClosedError:
                raise
            except TimeoutError:
                continue
            except Exception as error:
                logger.warning("command wait failed (command_id=%s): %s", handle.id, error)
                retry_delay = min(_POLL_RETRY_DELAY, deadline - time.monotonic())
                if retry_delay > 0:
                    time.sleep(retry_delay)

    def get(self, command_id: str) -> CommandHandle:
        """Read an existing RRT command record and return its handle."""
        increment("command_get_total")
        stable_id = _validate_command_id(command_id)
        self._require_recovery_capability(stable_id)
        handle = CommandHandle(stable_id, self._client, self._sid)
        handle._snapshot()
        return handle

    def list(self) -> List[CommandInfo]:
        processes = self._client.invoke(self._sid, "process.list", {}).get("processes")
        if not isinstance(processes, list):
            return []
        return [_info(item) for item in processes if isinstance(item, dict)]

    def kill(self, command: Union[str, int]) -> bool:
        key = {"command_id": command} if isinstance(command, str) else {"pid": command}
        return bool(self._client.invoke(self._sid, "process.kill", key)["killed"])

    def send_stdin(self, command: Union[str, int], data: str, eof: bool = False) -> None:
        key = {"command_id": command} if isinstance(command, str) else {"pid": command}
        response = self._client.invoke(
            self._sid, "process.send_stdin", {**key, "data": data, "eof": eof}
        )
        if response.get("error"):
            raise RuntimeError(f"Failed to send stdin: {response['error']}")

    def close_stdin(self, command: Union[str, int]) -> None:
        self.send_stdin(command, "", eof=True)

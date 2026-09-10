#!/usr/bin/env python3
# coding=UTF-8
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sandbox implementation for isolated code execution."""

import argparse
import logging
import re
import subprocess
import sys
import tempfile
import os
import json
import time
import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Union, Callable

import yr
from yr.config import InvokeOptions, PortForwarding
from yr.runtime_holder import global_runtime
from yr.config_manager import ConfigManager
from yr.agentexecutor.sandbox.filesystem import CpDirection, SandboxFilesystem, _get_gateway_host

logger = logging.getLogger(__name__)

_TRACE_ID_MAX_LEN = 128
_TRACE_ID_RE = re.compile(r"^[A-Za-z0-9_\-.:]+$")


def _validate_trace_id(trace_id: str) -> None:
    """Validate trace_id format to prevent injection and log corruption.

    Allows alphanumerics, ``_ - . :`` (covers UUID, hex, dotted names).
    Max 128 characters.
    """
    if not trace_id or len(trace_id) > _TRACE_ID_MAX_LEN:
        raise ValueError(f"trace_id length must be 1-{_TRACE_ID_MAX_LEN}")
    if not _TRACE_ID_RE.match(trace_id):
        raise ValueError(
            "trace_id contains invalid characters "
            "(allowed: A-Z a-z 0-9 _ - . :)"
        )


def _with_trace(trace_id: Optional[str]) -> InvokeOptions:
    """Build per-call InvokeOptions carrying the given trace_id.

    When *trace_id* is empty/None, returns a default InvokeOptions so the
    backend mints its own trace id (existing behavior).
    """
    opts = InvokeOptions()
    if trace_id:
        _validate_trace_id(trace_id)
        opts.trace_id = trace_id
    return opts


def _local_request_id() -> str:
    """Generate a local request id for SDK-side trace correlation."""
    return str(uuid.uuid4())


def _sandbox_trace_enter(op: str, trace_id: Optional[str], request_id: str,
                         **extra) -> None:
    """Emit ``[sandbox.<op>.enter]`` log line for signal-channel operations."""
    if trace_id:
        _validate_trace_id(trace_id)
    fields = [f"yr.trace_id={trace_id}" if trace_id else None,
              f"request_id={request_id}"]
    fields.extend(f"{k}={v}" for k, v in extra.items() if v is not None)
    logger.info("[sandbox.%s.enter] %s", op, " ".join(f for f in fields if f))


def _sandbox_trace_exit(op: str, trace_id: Optional[str], request_id: str,
                        result: str = "", cost_ms: int = 0, **extra) -> None:
    """Emit ``[sandbox.<op>.exit]`` log line for signal-channel operations."""
    if trace_id:
        _validate_trace_id(trace_id)
    fields = [f"yr.trace_id={trace_id}" if trace_id else None,
              f"request_id={request_id}"]
    fields.extend(f"{k}={v}" for k, v in extra.items() if v is not None)
    fields.append(f"result={result}")
    fields.append(f"cost_ms={cost_ms}")
    logger.info("[sandbox.%s.exit] %s", op, " ".join(f for f in fields if f))


def _sanitize_instance_id(instance_id: str) -> str:
    """Sanitize instance ID to match TraefikRegistry::SanitizeID (C++).

    Rules: @ -> -at-, / . _ -> -, truncate to 200 chars.
    """
    result = instance_id
    pos = 0
    while True:
        pos = result.find("@", pos)
        if pos == -1:
            break
        result = result[:pos] + "-at-" + result[pos + 1:]
        pos += 4
    result = result.replace("/", "-").replace(".", "-").replace("_", "-")
    if len(result) > 200:
        result = result[:200]
    return result


def _build_gateway_url(instance_id: str, sandbox_port: int, gateway_host: str, path: str = "") -> str:
    """Build Gateway HTTP path URL: http://{gateway_host}/{safeID}/{sandbox_port}{path}.

    URL format must match TraefikRegistry::RegisterInstance in function_proxy:
    - {safeID} is sanitized instance ID (SanitizeID logic)
    - {sandbox_port} is the original sandbox port
    - Full path format: /{safeID}/{sandbox_port}

    See: functionsystem/src/function_proxy/local_scheduler/traefik_registry/traefik_registry.cpp
    """
    safe_id = _sanitize_instance_id(instance_id)
    base = f"http://{gateway_host}/{safe_id}/{sandbox_port}"
    if path:
        path = path if path.startswith("/") else f"/{path}"
        return f"{base}{path}"
    return base


def _print_gateway_urls(instance_id: str, port_forwardings: List["PortForwarding"]) -> None:
    """Print Gateway URLs for port forwardings after sandbox creation."""
    if not port_forwardings:
        return
    gateway_host = _get_gateway_host()
    if not gateway_host:
        logger.warning("cannot print port forwarding URLs: YR_GATEWAY_ADDRESS or YR_SERVER_ADDRESS not set")
        return
    logger.info("sandbox created, port forwarding URLs:")
    for pf in port_forwardings:
        url = _build_gateway_url(instance_id, pf.port, gateway_host)
        logger.info("  port %s: %s", pf.port, url)


@yr.instance
class SandboxInstance:
    """
    SandboxInstance class provides isolated environment for code execution.

    This class creates a sandboxed environment where code can be executed
    with limited permissions and resource constraints.

    This is the underlying instance class decorated with @yr.instance.
    Users should typically use the Sandbox wrapper class instead.
    """

    def __init__(
        self, working_dir: Optional[str] = None, env: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the Sandbox instance.

        Args:
            working_dir (Optional[str]): The working directory for sandbox execution.
                If None, a temporary directory will be created.
            env (Optional[Dict[str, str]]): Environment variables for the sandbox.
                If None, inherits from parent process.
        """
        if working_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="yr_sandbox_")
            self.working_dir = self._temp_dir.name
            self._temp_dir_created = True
        else:
            self._temp_dir = None
            self.working_dir = working_dir
            self._temp_dir_created = False

        self.env = env if env is not None else os.environ.copy()
        self._initialized = True
        self.__yr_before_snapshot__ = None
        self.__yr_after_snapstart__ = None

    def __del__(self):
        """Destructor to ensure cleanup on object deletion."""
        self.cleanup()

    @staticmethod
    def get_internal_urls() -> Dict[int, str]:
        """Return internal cluster URLs for port-forwarded services.

        Reads environment variables injected by Runtime Manager to build
        direct-access URLs that bypass the Traefik gateway. Other sandbox
        instances can call this method via RPC to discover how to reach
        this sandbox's forwarded ports on the internal network.

        Returns:
            Dict[int, str]: Mapping from container port to internal URL.
                e.g. {8080: "https://192.0.2.1:40001", 9090: "https://192.0.2.1:40002"}.
                Returns an empty dict if no port forwarding is configured.
        """
        host_ip = os.environ.get("YR_INTERNAL_HOST_IP", "")
        pf_str = os.environ.get("YR_PORT_FORWARDINGS", "")
        if not host_ip or not pf_str:
            return {}

        result = {}
        for mapping in pf_str.split(";"):
            parts = mapping.split(":")
            if len(parts) >= 3:
                protocol = parts[0].lower()
                host_port = parts[1]
                container_port = int(parts[2])
                scheme = "https" if protocol == "https" else "http"
                result[container_port] = f"{scheme}://{host_ip}:{host_port}"
        return result

    @staticmethod
    def start_tunnel_server(ws_port: int = 8765, http_port: int = 8766) -> None:
        """Start TunnelServer in a background thread within this sandbox instance."""
        import asyncio
        import threading

        from yr.agentexecutor.sandbox.tunnel_server import TunnelServer

        ready = threading.Event()
        error = [None]

        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            server = TunnelServer(ws_port=ws_port, http_port=http_port)
            try:
                loop.run_until_complete(server.start())
            except Exception as e:
                error[0] = e
                ready.set()
                return
            ready.set()
            loop.run_forever()

        t = threading.Thread(target=_run, name="tunnel-server", daemon=True)
        t.start()
        if not ready.wait(timeout=5.0):
            logger.warning("TunnelServer not ready within 5s, continuing anyway")
        if error[0] is not None:
            raise RuntimeError(f"TunnelServer failed to start: {error[0]}") from error[0]

    @staticmethod
    def get_name():
        """
        Get the name of the sandbox instance.

        Returns:
            str: The name of the sandbox instance.
        """
        return os.environ.get("INSTANCE_ID", "")

    @staticmethod
    def read_file(path: str, mode: str = "rb"):
        """
        Read a file inside the sandbox using native Python I/O.

        This method does NOT depend on container commands like tar/cat/sh,
        making it suitable for Docker sandboxes with minimal images.

        Args:
            path (str): Absolute path of the file to read inside the sandbox.
            mode (str): File open mode. "rb" for binary (default), "r" for text.

        Returns:
            bytes or str: File content (bytes for "rb", str for "r").

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: If the file cannot be read.

        Examples:
            >>> content = yr.get(sandbox.read_file.invoke("/sandbox/data.txt", mode="r"))
            >>> print(content)
        """
        from yr.agentexecutor.sandbox.filesystem import _read_file_impl
        return _read_file_impl(path, mode)

    @staticmethod
    def write_file(path: str, data, mode: str = "wb") -> None:
        """
        Write data to a file inside the sandbox using native Python I/O.

        This method does NOT depend on container commands like tar/tee/sh,
        making it suitable for Docker sandboxes with minimal images.
        Parent directories are created automatically.

        Args:
            path (str): Absolute path of the file to write inside the sandbox.
            data (bytes or str): Data to write to the file.
            mode (str): File open mode. "wb" for binary (default), "w" for text,
                        "a"/"ab" for append.

        Raises:
            OSError: If the file cannot be written.

        Examples:
            >>> yr.get(sandbox.write_file.invoke("/sandbox/output.txt", "hello", mode="w"))
        """
        from yr.agentexecutor.sandbox.filesystem import _write_file_impl
        return _write_file_impl(path, data, mode)

    @staticmethod
    def list_files(
            path: str,
            recursive: bool = False,
            max_depth: Optional[int] = None,
            include_files: bool = True,
            include_dirs: bool = True,
    ) -> List[Dict]:
        """
        List files and directories inside the sandbox using native Python I/O.

        Args:
            path (str): Absolute path of the directory to list.
            recursive (bool): Whether to list recursively. Default False.
            max_depth (Optional[int]): Maximum recursion depth. None = unlimited.
            include_files (bool): Include files in result. Default True.
            include_dirs (bool): Include directories in result. Default True.

        Returns:
            List[Dict]: List of item dicts with keys: name, path, size,
                        is_directory, modified_time, type.

        Raises:
            FileNotFoundError: If the directory does not exist.
            OSError: If the path cannot be accessed.
        """
        from yr.agentexecutor.sandbox.filesystem import _list_files_impl
        return _list_files_impl(path, recursive=recursive, max_depth=max_depth,
                                include_files=include_files, include_dirs=include_dirs)

    @staticmethod
    def search_files(
            path: str,
            pattern: str,
            exclude_patterns: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Search files inside the sandbox by glob pattern using native Python I/O.

        Recursively searches under ``path`` for files matching ``pattern``,
        excluding any path matching ``exclude_patterns``.

        Args:
            path (str): Absolute path of the search root directory.
            pattern (str): Glob pattern to match file names (e.g. "*.txt").
            exclude_patterns (Optional[List[str]]): Glob patterns to exclude.

        Returns:
            List[Dict]: List of item dicts with keys: name, path, size,
                        is_directory, modified_time, type.

        Raises:
            FileNotFoundError: If the search root does not exist.
            OSError: If the path cannot be accessed.
        """
        from yr.agentexecutor.sandbox.filesystem import _search_files_impl
        return _search_files_impl(path, pattern, exclude_patterns=exclude_patterns)

    def execute(
        self,
        command: Union[str, List[str]],
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute a command in the sandbox environment.

        Args:
            command (Union[str, List[str]]): The command to execute.
            working_dir (Optional[str]): Working dir for command execution.
            env (Optional[Dict[str, str]]): Environment variables for command execution.
            timeout (Optional[int]): Timeout in seconds for command execution.
                If None, no timeout is set.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - returncode (int): The return code of the command.
                - stdout (str): Standard output of the command.
                - stderr (str): Standard error of the command.

        Raises:
            RuntimeError: If the sandbox is not initialized.
            subprocess.TimeoutExpired: If the command execution times out.

        Examples:
            >>> sandbox = yr.agentexecutor.sandbox.Sandbox.invoke()
            >>> result = yr.get(sandbox.execute.invoke("ls -la"))
            >>> print(result['stdout'])
        """
        if not self._initialized:
            raise RuntimeError("Sandbox is not initialized")

        if isinstance(command, str):
            cmd_args = ["/bin/sh", "-c", command]
        elif isinstance(command, list):
            if len(command) == 0:
                return {
                    "returncode": -1,
                    "stdout": "",
                    "stderr": "Error: cmd list cannot be empty",
                }
            if not all(isinstance(arg, str) for arg in command):
                return {
                    "returncode": -1,
                    "stdout": "",
                    "stderr": "Error: All elements in command list must be strings",
                }
            cmd_args = command
        else:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": f"Error: cmd must be a string or a list of strings, got {type(command).__name__}",
            }
        if not cmd_args:
            return {
                "returncode": -1,
                "stdout": "",
                "stderr": "Error: cmd list cannot be empty",
            }

        try:
            subprocess_cwd = self.working_dir
            if working_dir is not None:
                subprocess_cwd = working_dir
            subprocess_env = self.env
            if env is not None:
                subprocess_env = env
            result = subprocess.run(
                args=cmd_args,
                shell=False,
                cwd=subprocess_cwd,
                env=subprocess_env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except subprocess.TimeoutExpired as e:
            return {
                "returncode": -1,
                "stdout": e.stdout if e.stdout else "",
                "stderr": f"Command timed out after {timeout} seconds",
            }
        except Exception as e:
            return {"returncode": -1, "stdout": "", "stderr": str(e)}

    def get_working_dir(self) -> str:
        """
        Get the working directory of the sandbox.

        Returns:
            str: The path to the working directory.
        """
        return self.working_dir

    def cleanup(self) -> None:
        """
        Cleanup the sandbox environment.

        This method removes temporary files and directories created by the sandbox.
        """
        # Use getattr to safely handle proxy objects that may not have these attributes
        temp_dir_created = getattr(self, "_temp_dir_created", False)
        working_dir = getattr(self, "working_dir", None)
        temp_dir = getattr(self, "_temp_dir", None)

        if temp_dir_created and working_dir and os.path.exists(working_dir):
            import shutil

            try:
                if temp_dir is not None:
                    temp_dir.cleanup()
                else:
                    shutil.rmtree(working_dir)
            except Exception as e:
                # Log the error but don't raise
                logger.warning("Failed to cleanup sandbox directory %s: %s", working_dir, e)

    def register_before_snapshot_hook(self, hook_func: Callable[..., Any]):
        """Register a hook to be called before snapshot."""
        self.__yr_before_snapshot__ = hook_func

    def register_after_snapstart_hook(self, hook_func: Callable[..., Any]):
        """Register a hook to be called after snapstart."""
        self.__yr_after_snapstart__ = hook_func


@dataclass
class SandboxCreateOptions:
    name: Optional[str] = None
    rootfs: Optional[str] = None
    image: Optional[str] = None
    host_dir: Optional[str] = None
    workdir: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    idle_timeout: int = 300
    working_dir: Optional[str] = None
    cpu: Optional[int] = None
    memory: Optional[int] = None
    extra_config: Optional[Dict[str, Any]] = None
    ports: Optional[List[str]] = None
    upstream: Optional[str] = None
    proxy_port: int = 8766
    sandbox_type: str = ""
    # Docker stop grace period (seconds) before SIGKILL. None -> docker default "0".
    graceful_shutdown_time: Optional[int] = None
    before_checkpoint_func: Optional[Callable[..., Any]] = None
    after_restore_func: Optional[Callable[..., Any]] = None
    user: Optional[str] = None
    trace_id: str = ""


def create(
    *args: str,
    name: Optional[str] = None,
    rootfs: Optional[str] = None,
    image: Optional[str] = None,
    host_dir: Optional[str] = None,
    workdir: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    idle_timeout: int = 300,
    working_dir: Optional[str] = None,
    cpu: Optional[int] = None,
    memory: Optional[int] = None,
    extra_config: Optional[Dict[str, Any]] = None,
    ports: Optional[str] = None,
    upstream: Optional[str] = None,
    proxy_port: int = 8766,
    sandbox_type: str = "",
    before_checkpoint_func: Optional[Callable[..., Any]] = None,
    after_restore_func: Optional[Callable[..., Any]] = None,
    graceful_shutdown_time: Optional[int] = None,
    user: Optional[str] = None,
    trace_id: str = "",
):
    """
    Create a new Sandbox instance.

    Args:
        working_dir: Working directory for sandbox execution.
        env: Environment variables for the sandbox.
        port: Additional port forwarding rules.
        upstream: Local service address to tunnel to, e.g. "upstream.example.com:8000".
            When set, starts a reverse tunnel so sandbox code can reach the
            local service via http://127.0.0.1:{proxy_port}.
        proxy_port: Port B — the HTTP proxy port inside the sandbox
            (default 8766). Port A (WS tunnel) = proxy_port - 1.
        sandbox_type (str): Type of sandbox executor.
            Supported values:
            - "": Use default executor (RUNTIME)
            - "supervisor": Uses SUPERVISOR executor
            - "docker": Uses DOCKER executor
        image (Optional[str]): Docker image name (e.g., "python:3.12-slim").
            Only effective when sandbox_type="docker".
        host_dir (Optional[str]): Host directory to mount into the Docker sandbox.
            Only effective when sandbox_type="docker".
        workdir (Optional[str]): Working directory inside the Docker sandbox.
            Only effective when sandbox_type="docker".
        graceful_shutdown_time (Optional[int]): Graceful shutdown timeout in seconds
            passed to the backend as the GRACEFUL_SHUTDOWN_TIME create-option.
            Controls how long the executor waits before force-killing the
            instance (e.g. Docker stop?t= for docker). None (default) uses "0"
            for docker sandboxes so terminate is immediate.
        user (Optional[str]): Container run-as user, passed to the backend as
            the ``host_user`` deploy option and ultimately set as Docker's
            ``Config.User``. Supports any format Docker accepts, e.g. ``"1000"``,
            ``"1000:1000"``, ``"root"``, ``"root:root"``.
            Only effective when sandbox_type="docker".
        trace_id (str): Optional trace id propagated through the whole invoke
            chain for link tracing. Empty (default) keeps the existing
            behavior: the backend mints one.

    Returns:
        Sandbox wrapper instance.

    Examples:
        >>> import yr
        >>> yr.init()
        >>>
        >>> sandbox = yr.agentexecutor.sandbox.create()
        >>> # Create sandbox with supervisor executor
        >>> sandbox = yr.agentexecutor.sandbox.create(sandbox_type="supervisor")
        >>> result = yr.get(sandbox.exec("pwd"))
        >>> print(result['stdout'])
        >>>
        >>> # Create sandbox with docker executor
        >>> sandbox = yr.agentexecutor.sandbox.create(sandbox_type="docker", image="python:3.12-slim")
        >>> result = yr.get(sandbox.exec("python --version"))
        >>> print(result['stdout'])
        >>>
        >>> sandbox.terminate()
        >>> yr.finalize()
    """
    try:
        return Sandbox(
            name=name,
            rootfs=rootfs,
            image=image,
            host_dir=host_dir,
            workdir=workdir,
            cpu=cpu,
            memory=memory,
            idle_timeout=idle_timeout,
            working_dir=working_dir,
            env=env,
            extra_config=extra_config,
            ports=ports,
            upstream=upstream,
            proxy_port=proxy_port,
            sandbox_type=sandbox_type,
            before_checkpoint_func=before_checkpoint_func,
            after_restore_func=after_restore_func,
            graceful_shutdown_time=graceful_shutdown_time,
            user=user,
            trace_id=trace_id,
        )
    except Exception as e:
        logger.warning("failed to create sandbox, exception: %s", e)
        return None


def restore(
    checkpoint_id: str,
    before_checkpoint_func: Optional[Callable[..., Any]] = None,
    after_restore_func: Optional[Callable[..., Any]] = None,
    trace_id: str = "",
):
    """
    Restore a Sandbox from a previously created checkpoint.

    Args:
        checkpoint_id (str): The checkpoint ID returned by a previous checkpoint() call.
        before_checkpoint_func (Optional[Callable]): Hook called before future checkpoints.
        after_restore_func (Optional[Callable]): Hook called after restore completes.
        trace_id (str): Optional trace id for link tracing of the restore chain.
            Empty (default) keeps the existing behavior.

    Returns:
        Sandbox wrapper instance restored from the checkpoint.

    Examples:
        >>> import yr
        >>> yr.init()
        >>>
        >>> sandbox = yr.agentexecutor.sandbox.create()
        >>> checkpoint_id = sandbox.checkpoint()
        >>> restored = yr.agentexecutor.sandbox.restore(checkpoint_id)
        >>> result = restored.exec("echo hello")
        >>> restored.terminate()
        >>> yr.finalize()
    """
    return Sandbox(
        checkpoint_id=checkpoint_id,
        before_checkpoint_func=before_checkpoint_func,
        after_restore_func=after_restore_func,
        trace_id=trace_id,
    )


class Sandbox:
    """
    Sandbox wrapper class for convenient sandbox operations.

    When upstream is provided, starts a reverse tunnel:
    - Port B (proxy_port, loopback): sandbox code calls http://127.0.0.1:{proxy_port}
    - Port A (proxy_port-1, 0.0.0.0): WebSocket tunnel endpoint registered with Traefik

    Examples:
        >>> import yr
        >>> yr.init()
        >>>
        >>> # Basic sandbox
        >>> sb = yr.agentexecutor.sandbox.Sandbox()
        >>> result = yr.get(sb.exec("echo hello"))
        >>> print(result['stdout'])
        >>>
        >>> # Sandbox with reverse tunnel to local service
        >>> sb = yr.agentexecutor.sandbox.create(upstream="upstream.example.com:8000")
        >>> url = sb.get_tunnel_url()   # "http://127.0.0.1:8766"
        >>> result = yr.get(sb.exec(f"curl {url}/api/data"))
        >>>
        >>> sb.terminate()
        >>> yr.finalize()
    """

    def __init__(
        self,
        checkpoint_id: Optional[str] = None,
        name: Optional[str] = None,
        rootfs: Optional[str] = None,
        image: Optional[str] = None,
        host_dir: Optional[str] = None,
        workdir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        idle_timeout: int = 300,
        working_dir: Optional[str] = None,
        cpu: Optional[int] = None,
        memory: Optional[int] = None,
        extra_config: Optional[Dict[str, Any]] = None,
        ports: Optional[List[str]] = None,
        upstream: Optional[str] = None,
        proxy_port: int = 8766,
        sandbox_type: str = "",
        before_checkpoint_func: Optional[Callable[..., Any]] = None,
        after_restore_func: Optional[Callable[..., Any]] = None,
        graceful_shutdown_time: Optional[int] = None,
        user: Optional[str] = None,
        trace_id: str = "",
    ):
        """
        Initialize the Sandbox wrapper.

        Args:
            checkpoint_id (Optional[str]): If provided, restore from this checkpoint
                instead of creating a new instance.
            name (Optional[str]): Name of the sandbox instance.
            rootfs (Optional[str]): Root filesystem image for the sandbox.
            env (Optional[Dict[str, str]]): Environment variables for the sandbox.
            idle_timeout (int): Idle timeout in seconds. Default 300.
            working_dir (Optional[str]): Working directory inside the sandbox.
            cpu (Optional[int]): CPU resource limit in millicores.
            memory (Optional[int]): Memory resource limit in MB.
            extra_config (Optional[Dict[str, Any]]): Additional configuration.
            ports (Optional[List[str]]): Port forwarding configurations.
            upstream (Optional[str]): Local service address to tunnel to.
            proxy_port (int): HTTP proxy port inside the sandbox. Default 8766.
            sandbox_type (str): Type of sandbox executor.
                Supported values:
                - "": Use default executor (RUNTIME)
                - "supervisor": Uses SUPERVISOR executor
                - "docker": Uses DOCKER executor
            image (Optional[str]): Docker image name (e.g., "python:3.12-slim").
                Only effective when sandbox_type="docker".
            host_dir (Optional[str]): Host directory to mount into the Docker sandbox.
                Only effective when sandbox_type="docker".
            workdir (Optional[str]): Working directory inside the Docker sandbox.
                Only effective when sandbox_type="docker".
            before_checkpoint_func (Optional[Callable]): Hook called before checkpoint.
            after_restore_func (Optional[Callable]): Hook called after restore.
            graceful_shutdown_time (Optional[int]): Graceful shutdown timeout in
                seconds passed to the backend as the GRACEFUL_SHUTDOWN_TIME
                create-option. Controls how long the executor waits before
                force-killing the instance (e.g. Docker stop?t= for docker).
                None (default) uses "0" for docker sandboxes so terminate is
                immediate.
            user (Optional[str]): Container run-as user, passed to the backend
                as the ``host_user`` deploy option and ultimately set as Docker's
                ``Config.User``. Supports any format Docker accepts, e.g.
                ``"1000"``, ``"1000:1000"``, ``"root"``, ``"root:root"``.
                Only effective when sandbox_type="docker".
            trace_id (str): Optional trace id propagated through the whole invoke
                chain for link tracing. Empty (default) keeps the existing
                behavior: the backend mints one.
        """
        self._forwarded_ports = set()
        self._tunnel_client = None
        self._proxy_port = proxy_port
        self._filesystem: Optional["SandboxFilesystem"] = None
        self._trace_id = trace_id

        if checkpoint_id is None:
            self.create_new_instance(SandboxCreateOptions(
                name=name,
                rootfs=rootfs,
                image=image,
                host_dir=host_dir,
                workdir=workdir,
                env=env,
                idle_timeout=idle_timeout,
                working_dir=working_dir,
                cpu=cpu,
                memory=memory,
                extra_config=extra_config,
                ports=ports,
                upstream=upstream,
                proxy_port=proxy_port,
                sandbox_type=sandbox_type,
                before_checkpoint_func=before_checkpoint_func,
                after_restore_func=after_restore_func,
                graceful_shutdown_time=graceful_shutdown_time,
                user=user,
                trace_id=trace_id,
            ))
        else:
            self.restore_instance(checkpoint_id=checkpoint_id, trace_id=trace_id)

    def __del__(self):
        """
        Destructor to ensure cleanup and termination on object deletion.

        Automatically calls cleanup() and terminate() when the Sandbox object is deleted.
        """
        try:
            if hasattr(self, "_instance") and self._instance is not None:
                yr.get(self.cleanup())
                self.terminate()
        except Exception:
            logger.debug("ignore sandbox cleanup failure during destructor", exc_info=True)

    @property
    def filesystem(self) -> "SandboxFilesystem":
        """Namespace for sandbox filesystem copy operations.

        Example::

            sb.filesystem.copy_from_local("/local/data.csv", "/sandbox/data.csv")
            sb.filesystem.copy_to_local("/sandbox/output.txt", "/local/output.txt")
        """
        if self._filesystem is None:
            self._filesystem = SandboxFilesystem(self)
        return self._filesystem

    @staticmethod
    def get_exec_result(exec_ref):
        return yr.get(exec_ref)

    def create_new_instance(self, options: SandboxCreateOptions):
        """
        Initialize the Sandbox wrapper.

        Args:
            working_dir (Optional[str]): The working directory for sandbox execution.
                If None, a temporary directory will be created.
            env (Optional[Dict[str, str]]): Environment variables for the sandbox.
                If None, inherits from parent process.
            ports (Optional[str]): List of port forwarding
                configurations. Each string specifies a port to be forwarded
                inside the sandbox environment with format protocol:port.
            sandbox_type (str): Type of sandbox executor.
                Supported values:
                - "supervisor": Uses SUPERVISOR executor
                - "docker": Uses DOCKER executor
                - "": Use default executor (RUNTIME)
        """
        # Create InvokeOptions with skip_serialize=True for cross-version compatibility
        name = options.name
        rootfs = options.rootfs
        image = options.image
        host_dir = options.host_dir
        workdir = options.workdir
        env = options.env
        idle_timeout = options.idle_timeout
        working_dir = options.working_dir
        cpu = options.cpu
        memory = options.memory
        extra_config = options.extra_config
        ports = options.ports
        upstream = options.upstream
        proxy_port = options.proxy_port
        sandbox_type = options.sandbox_type
        before_checkpoint_func = options.before_checkpoint_func
        after_restore_func = options.after_restore_func
        self._proxy_port = proxy_port
        self._tunnel_client = None
        opt = yr.InvokeOptions()
        opt.skip_serialize = True
        opt.idle_timeout = idle_timeout
        if sandbox_type:
            opt.custom_extensions["sandbox_type"] = sandbox_type

        if options.graceful_shutdown_time is not None:
            opt.custom_extensions["GRACEFUL_SHUTDOWN_TIME"] = str(options.graceful_shutdown_time)

        if options.user is not None:
            opt.custom_extensions["host_user"] = options.user

        if options.trace_id:
            _validate_trace_id(options.trace_id)
            opt.trace_id = options.trace_id

        opt.recover_retry_times = 3

        if name is not None:
            opt.name = name
        if cpu is not None:
            opt.cpu = cpu
        if memory is not None:
            opt.memory = memory
        if env is not None:
            opt.env_vars = env
        if working_dir is not None:
            opt.runtime_env["working_dir"] = working_dir
        if extra_config is not None:
            opt.custom_extensions["extra_config"] = json.dumps(extra_config)
        if rootfs is not None:
            opt.custom_extensions["rootfs"] = rootfs
        elif image is not None:
            rootfs_config = {"type": "image", "imageurl": image}
            if workdir is not None:
                rootfs_config["workdir"] = workdir
            if host_dir is not None:
                rootfs_config["mounts"] = [{
                    "source": host_dir,
                    "target": workdir if workdir is not None else "/mnt/host",
                    "readonly": True,
                }]
            opt.custom_extensions["rootfs"] = json.dumps(rootfs_config)
        if ports is not None:
            yr_port_forwardings = []
            for port_forward in ports:
                parts = port_forward.split(":")
                if len(parts) == 2:
                    protocol, port_str = parts
                    try:
                        port = int(port_str)
                    except ValueError as e:
                        raise ValueError(
                            f"Invalid port number: '{port_str}' in '{port_forward}'. "
                            "Port must be a valid integer."
                        ) from e
                    yr_port_forwardings.append(PortForwarding(port=port, protocol=protocol.upper()))
                elif len(parts) == 1:
                    # Default to TCP if only port is provided
                    try:
                        port = int(parts[0])
                    except ValueError as e:
                        raise ValueError(
                            f"Invalid port number: '{parts[0]}'. "
                            "Port must be a valid integer."
                        ) from e
                    yr_port_forwardings.append(PortForwarding(port=port, protocol="TCP"))
                else:
                    raise ValueError(
                        f"Invalid port_forwarding format: {port_forward}. "
                        "Expected format: protocol:port (e.g., tcp:8080)"
                    )
            opt.port_forwardings = yr_port_forwardings

        # Store the forwarded ports for later use in get_tunnel
        self._forwarded_ports = set()
        if ports is not None:
            for port_forward in ports:
                parts = port_forward.split(":")
                if len(parts) >= 1:
                    port_str = parts[-1]
                    try:
                        port = int(port_str)
                        self._forwarded_ports.add(port)
                    except ValueError:
                        # Skip invalid ports, will be handled elsewhere
                        pass

        if upstream is not None:
            tunnel_port = proxy_port - 1
            tunnel_pf = yr.PortForwarding(port=tunnel_port)
            opt.port_forwardings = (list(opt.port_forwardings) if opt.port_forwardings else []) + [tunnel_pf]
            self._instance = SandboxInstance.options(opt).invoke(working_dir, env)
            if opt.port_forwardings:
                instance_id = yr.get(self._instance.get_name.invoke())
                _print_gateway_urls(instance_id, opt.port_forwardings)
            # Start tunnel server inside sandbox as a background thread
            yr.get(self._instance.start_tunnel_server.invoke(tunnel_port, proxy_port))
            # Build WSS URL for tunnel Port A via Traefik
            instance_id = yr.get(self._instance.get_name.invoke())
            gateway_host = _get_gateway_host()
            tunnel_url = _build_gateway_url(instance_id, tunnel_port, gateway_host)
            tunnel_ws_url = tunnel_url.replace("https://", "wss://").replace("http://", "ws://")
            # Start local TunnelClient in background thread and wait for connection
            from yr.agentexecutor.sandbox.tunnel_client import TunnelClient
            self._tunnel_client = TunnelClient(upstream)
            logger.info("Connecting to tunnel: %s", tunnel_ws_url)
            if self._tunnel_client.start(tunnel_ws_url, timeout=10.0):
                logger.info("TunnelClient connected successfully")
            else:
                logger.warning("TunnelClient connection timeout, will retry in background")
            return # Return early since instance is already created and tunnel is set up

        self._instance = SandboxInstance.options(opt).invoke(working_dir, env)
        # Wait for the instance to be fully initialized by calling a simple method
        # This ensures the sandbox is ready before we return
        try:
            # Use get_name as a lightweight verification method
            instance_id = yr.get(self._instance.get_name.invoke())
            if opt.port_forwardings:
                _print_gateway_urls(instance_id, opt.port_forwardings)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize sandbox instance: {e}") from e

        hook_opts = _with_trace(options.trace_id)
        if before_checkpoint_func is not None:
            yr.get(self._instance.register_before_snapshot_hook.options(hook_opts).invoke(before_checkpoint_func))
        if after_restore_func is not None:
            yr.get(self._instance.register_after_snapstart_hook.options(hook_opts).invoke(after_restore_func))

    def exec(
        self,
        command: str,
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        trace_id: Optional[str] = None,
    ):
        """
        Execute a command in the sandbox environment.

        Args:
            *args (str): The command to execute.
            working_dir (Optional[str]): Working dir for command execution.
            env (Optional[Dict[str, str]]): Environment variables for command execution.
            timeout (Optional[int]): Timeout in seconds for command execution.
                If None, no timeout is set.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Returns:
            A dictionary containing:
                - returncode (int): The return code of the command.
                - stdout (str): Standard output of the command.
                - stderr (str): Standard error of the command.
        """
        exec_ref = self._exec(
            command=command,
            working_dir=working_dir,
            env=env,
            timeout=timeout,
            trace_id=trace_id,
        )
        return self.get_exec_result(exec_ref)

    def exec_async(
        self,
        command: str,
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        trace_id: Optional[str] = None,
    ):
        """
        Execute a command in the sandbox environment asynchronously.

        Args:
            command (str): The command to execute.
            working_dir (Optional[str]): Working dir for command execution.
            env (Optional[Dict[str, str]]): Environment variables for command execution.
            timeout (Optional[int]): Timeout in seconds for command execution.
                If None, no timeout is set.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Returns:
            ObjectRef: Reference to the execution result that can be used with self.get_exec_result().
        """
        return self._exec(
            command=command,
            working_dir=working_dir,
            env=env,
            timeout=timeout,
            trace_id=trace_id,
        )

    def read_file(self, path: str, mode: str = "rb",
                  trace_id: Optional[str] = None):
        """
        Read a file from the sandbox via RPC.

        Uses native Python I/O inside the sandbox instance, so it works
        even with minimal Docker images that lack tar/cat/sh.

        Args:
            path (str): Absolute path of the file inside the sandbox.
            mode (str): Python open mode. "rb" for binary (default), "r" for text.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Returns:
            bytes or str: File content.

        Examples:
            >>> content = sb.read_file("/sandbox/data.txt", mode="r")
            >>> print(content)
        """
        method = self._instance.read_file
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return yr.get(method.invoke(path, mode=mode))

    def write_file(self, path: str, data, mode: str = "wb",
                   trace_id: Optional[str] = None) -> None:
        """
        Write data to a file in the sandbox via RPC.

        Uses native Python I/O inside the sandbox instance, so it works
        even with minimal Docker images that lack tar/tee/sh.
        Parent directories are created automatically.

        Args:
            path (str): Absolute path of the file inside the sandbox.
            data (bytes or str): Data to write.
            mode (str): Python open mode. "wb" for binary (default), "w" for
                        text, "a"/"ab" for append.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Examples:
            >>> sb.write_file("/sandbox/output.txt", "hello world", mode="w")
        """
        method = self._instance.write_file
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return yr.get(method.invoke(path, data, mode=mode))

    def list_files(self, path: str, **options: Any) -> Dict:
        """
        List files and directories in the sandbox via RPC.

        Keyword options (names and semantics identical to ``yr.sandbox``
        ``Sandbox.list_files``; declared as ``**options`` because the upstream
        signature carries six parameters, above the G.FNM.03 limit):

            recursive (Union[bool, int]): ``False`` (default) = no recursion,
                ``True`` = unlimited depth, ``int`` = max N levels deep.
            max_depth (Optional[int]): Explicit recursion depth limit; takes
                precedence over the ``int`` form of ``recursive``. ``None``
                (default) uses the depth implied by ``recursive``.
            include_files (bool): Include files in result. Default True.
            include_dirs (bool): Include directories in result. Default True.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Returns:
            Dict: ``{"items": [{name, path, size, is_directory, modified_time, type}, ...]}``

        Examples:
            >>> result = sb.list_files("/tmp", recursive=2)
            >>> for item in result["items"]:
            ...     print(item["name"], item["size"])
        """
        recursive = options.get("recursive", False)
        max_depth: Optional[int] = options.get("max_depth")
        include_files: bool = options.get("include_files", True)
        include_dirs: bool = options.get("include_dirs", True)
        trace_id: Optional[str] = options.get("trace_id")
        if isinstance(recursive, int) and not isinstance(recursive, bool):
            _recursive = True
            _max_depth = recursive
        else:
            _recursive = bool(recursive)
            _max_depth = None
        if max_depth is not None:
            _max_depth = max_depth
        method = self._instance.list_files
        if trace_id:
            method = method.options(_with_trace(trace_id))
        items = yr.get(method.invoke(
            path,
            recursive=_recursive,
            max_depth=_max_depth,
            include_files=include_files,
            include_dirs=include_dirs,
        ))
        return {"items": items}

    def search_files(
        self,
        path: str,
        pattern: str,
        exclude_patterns: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
    ) -> Dict:
        """
        Search files in the sandbox by glob pattern via RPC.

        Args:
            path (str): Absolute path of the search root directory.
            pattern (str): Glob pattern to match file names (e.g. "*.txt").
            exclude_patterns (Optional[List[str]]): Glob patterns to exclude.
            trace_id (Optional[str]): Trace id for link tracing of this invoke.
                None (default) keeps the existing behavior.

        Returns:
            Dict: ``{"items": [{name, path, size, is_directory, modified_time, type}, ...]}``

        Examples:
            >>> result = sb.search_files("/tmp", "*.py", exclude_patterns=["*.pyc"])
        """
        method = self._instance.search_files
        if trace_id:
            method = method.options(_with_trace(trace_id))
        items = yr.get(method.invoke(
            path,
            pattern,
            exclude_patterns=exclude_patterns,
        ))
        return {"items": items}

    def get_working_dir(self, trace_id: Optional[str] = None):
        """Get the working directory of the sandbox."""
        method = self._instance.get_working_dir
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return method.invoke()

    def cleanup(self, trace_id: Optional[str] = None):
        """Cleanup temp files in the sandbox."""
        method = self._instance.cleanup
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return method.invoke()

    def terminate(self, trace_id: Optional[str] = None):
        """
        Terminate the sandbox instance.
        Stop tunnel client (if any) and terminate the sandbox instance.

        Args:
            trace_id (Optional[str]): Trace id for link tracing. Emitted in the
                [sandbox.terminate] log line alongside the request id used by
                the signal chain. None (default, e.g. implicit calls from
                ``__del__``) omits the key.
        """
        request_id = _local_request_id()
        start = time.monotonic()
        _sandbox_trace_enter("terminate", trace_id, request_id)
        try:
            if self._tunnel_client is not None:
                self._tunnel_client.stop()
                self._tunnel_client = None
            self._instance.terminate()
            _sandbox_trace_exit("terminate", trace_id, request_id,
                                result="OK",
                                cost_ms=int((time.monotonic() - start) * 1000))
        except Exception as e:
            _sandbox_trace_exit("terminate", trace_id, request_id,
                                result=f"ERR:{e}",
                                cost_ms=int((time.monotonic() - start) * 1000))
            raise

    def get_instance_id(self) -> str:
        """Return the platform instance id of this sandbox.

        Delegates to ``SandboxInstance.get_name`` via RPC; the returned id is
        used by ``SandboxManager`` as the key for multi-instance routing and is
        the ``instance_id`` exposed by the create endpoint.
        """
        return yr.get(self._instance.get_name.invoke())

    def get_tunnel_url(self) -> str:
        """Return the internal HTTP proxy URL for sandbox code to call.

        Returns:
            str: e.g. "http://127.0.0.1:8766"
        Raises:
            RuntimeError: if no upstream was configured.
        """
        if self._tunnel_client is None:
            raise RuntimeError("No upstream configured. Pass upstream= to create().")
        return f"http://127.0.0.1:{self._proxy_port}"

    def get_internal_urls(self, trace_id: Optional[str] = None) -> Dict[int, str]:
        """Return internal cluster URLs for port-forwarded services.

        Other sandbox instances can use these URLs to reach this sandbox's
        forwarded ports on the internal network.

        Args:
            trace_id (Optional[str]): Trace id propagated through the whole
                invoke chain. None (default) keeps the existing behavior.

        Returns:
            Dict[int, str]: Mapping from container port to internal URL.
                e.g. {8080: "https://192.0.2.1:40001", 9090: "https://192.0.2.1:40002"}
                Returns an empty dict if no port forwarding is configured.
        """
        method = self._instance.get_internal_urls
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return yr.get(method.invoke())

    def get_tunnel(self, port: int) -> str:
        """
        Get the tunnel URL for a forwarded port.

        Args:
            port (int): The port number to get the tunnel for.

        Returns:
            str: The tunnel URL in format http://{TraefikAddress}/{real_id}/{port}

        Raises:
            ValueError: If the port is not in the list of forwarded ports.
        """
        # Check if port is in the forwarded ports list
        if port not in self._forwarded_ports:
            raise ValueError(f"Invalid port: {port}. Port is not in the list of forwarded ports.")

        # Get the logical instance id from the instance proxy
        logical_id = self._instance.instance_id

        # Get the real instance id from global_runtime
        real_id = global_runtime.get_runtime().get_real_instance_id(logical_id)

        # Get the Traefik address from environment variable
        traefik_address = os.environ.get("YR_GATEWAY_ADDRESS", "")
        if traefik_address == "":
            raise ValueError(f"YR_GATEWAY_ADDRESS is not set.")

        # Construct and return the tunnel URL
        return f"http://{traefik_address}/{real_id}/{port}"

    def checkpoint(
        self,
        ttl: int = -1,
        leave_running: bool = False,
        trace_id: Optional[str] = None,
    ) -> str:
        """
        Create a checkpoint of the current sandbox state.

        This triggers a snapshot of the underlying instance. The returned checkpoint ID
        can be used with ``yr.agentexecutor.sandbox.restore()`` or ``Sandbox(checkpoint_id=...)`` to
        create a new sandbox with the same state.

        Args:
            ttl (int): Time-to-live for the checkpoint in seconds. -1 means no expiration.
                Default -1.
            leave_running (bool): If True, the sandbox continues running after
                checkpointing. If False, the sandbox is terminated after checkpoint.
                Default False.
            trace_id (Optional[str]): Trace id for link tracing. Emitted in the
                [sandbox.checkpoint] log line alongside the request id used by
                the signal chain. None (default) omits the key.

        Returns:
            str: The checkpoint ID that uniquely identifies this snapshot.

        Raises:
            RuntimeError: If the sandbox instance is not active.

        Examples:
            >>> sandbox = yr.agentexecutor.sandbox.create(rootfs="python:3.12-slim")
            >>> sandbox.exec("echo setup done")
            >>> checkpoint_id = sandbox.checkpoint(leave_running=True)
            >>> print(f"Checkpoint: {checkpoint_id}")
        """
        request_id = _local_request_id()
        start = time.monotonic()
        _sandbox_trace_enter("checkpoint", trace_id, request_id)
        try:
            checkpoint_id = self._instance.snapshot(
                ttl=ttl,
                leave_running=leave_running,
            )
            _sandbox_trace_exit("checkpoint", trace_id, request_id,
                                result="OK",
                                cost_ms=int((time.monotonic() - start) * 1000))
            return checkpoint_id
        except Exception as e:
            _sandbox_trace_exit("checkpoint", trace_id, request_id,
                                result=f"ERR:{e}",
                                cost_ms=int((time.monotonic() - start) * 1000))
            raise

    def restore_instance(self, checkpoint_id: str,
                         trace_id: Optional[str] = None):
        """
        Restore the sandbox from a checkpoint.

        Uses ``InstanceCreator.snapstart()`` to create a new instance from the
        checkpoint, then verifies the instance is ready.

        Args:
            checkpoint_id (str): The checkpoint ID returned by a previous
                ``checkpoint()`` call.
            trace_id (Optional[str]): Trace id for link tracing. Emitted in the
                [sandbox.restore] log line alongside the request id used by
                the signal chain. None (default) omits the key.

        Raises:
            RuntimeError: If the restore or readiness check fails.
        """
        request_id = _local_request_id()
        start = time.monotonic()
        _sandbox_trace_enter("restore", trace_id, request_id)
        try:
            self._instance = SandboxInstance.snapstart(checkpoint_id=checkpoint_id)

            # Wait for the restored instance to be fully ready
            try:
                ref = self._instance.get_working_dir.invoke()
                yr.get(ref)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to restore sandbox from checkpoint {checkpoint_id}: {e}"
                ) from e
            _sandbox_trace_exit("restore", trace_id, request_id,
                                result="OK",
                                cost_ms=int((time.monotonic() - start) * 1000))
        except Exception as e:
            _sandbox_trace_exit("restore", trace_id, request_id,
                                result=f"ERR:{e}",
                                cost_ms=int((time.monotonic() - start) * 1000))
            raise

    def cp(
        self,
        src: str,
        dst: str,
        direction: CpDirection = CpDirection.UPLOAD,
        streaming: Optional[bool] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        """Copy a file or directory to or from the sandbox.

        The *direction* parameter controls which side is local and which is the
        sandbox:

        * ``CpDirection.UPLOAD`` *(default)* – ``src`` is a **local** path,
          ``dst`` is the destination path **inside the sandbox**.
        * ``CpDirection.DOWNLOAD`` – ``src`` is a path **inside the sandbox**,
          ``dst`` is the **local** destination path.

        Prefer :attr:`filesystem` for explicit, self-documenting code::

            # Equivalent to sb.cp("/local/file", "/remote/file")
            sb.filesystem.copy_from_local("/local/file", "/remote/file")

            # Equivalent to sb.cp("/remote/file", "/local/file", direction=CpDirection.DOWNLOAD)
            sb.filesystem.copy_to_local("/remote/file", "/local/file")

        Args:
            src: Source path (local when uploading, sandbox path when downloading).
            dst: Destination path (sandbox path when uploading, local when downloading).
            direction: Transfer direction. Defaults to :attr:`CpDirection.UPLOAD`.
            streaming: Transfer mode override.

                * ``True``  – always use gzip streaming (best for large compressible data).
                * ``False`` – always use non-streaming (best for small or binary data).
                * ``None``  – auto-select based on file size and compressibility (default).

        Raises:
            FileNotFoundError: *src* does not exist on the local machine (upload only).
            RuntimeError: Server address is not configured.

        Examples:
            >>> sb = yr.agentexecutor.sandbox.create()
            >>> # Upload a local file to the sandbox (default direction)
            >>> sb.cp("/local/data.csv", "/sandbox/data.csv")
            >>> # Download a file from the sandbox
            >>> from yr.agentexecutor.sandbox.filesystem import CpDirection
            >>> sb.cp("/sandbox/output.txt", "/local/output.txt", direction=CpDirection.DOWNLOAD)
            >>> # Upload a directory
            >>> sb.cp("/local/project/", "/sandbox/project/")
        """
        getattr(self.filesystem, "_cp")(src, dst, direction=direction, streaming=streaming,
                                        trace_id=trace_id)

    def _exec(
        self,
        command: str,
        working_dir: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        trace_id: Optional[str] = None,
    ):
        method = self._instance.execute
        if trace_id:
            method = method.options(_with_trace(trace_id))
        return method.invoke(
            command=command,
            working_dir=working_dir,
            env=env,
            timeout=timeout,
        )


def main():
    parser = argparse.ArgumentParser(description="Create a detached sandbox instance")
    parser.add_argument(
        "--name", type=str, default=None, help="Name of the sandbox instance"
    )
    parser.add_argument(
        "--namespace",
        type=str,
        default="detached.sandbox",
        help="Namespace for the sandbox instance",
    )
    args = parser.parse_args()
    os.environ.pop("YR_WORKING_DIR", None)

    cfg = yr.Config()
    cfg.in_cluster = True
    yr.init(cfg)
    try:
        opt = yr.InvokeOptions()
        opt.custom_extensions["lifecycle"] = "detached"
        opt.idle_timeout = 60 * 60 * 24 * 7
        opt.cpu = 1000
        opt.memory = 2048
        opt.name = args.name
        opt.namespace = args.namespace
        opt.skip_serialize = True  # Skip serialization for pre-deployed SDK class
        if not opt.name:
            opt.name = str(uuid.uuid4())

        sandbox = SandboxInstance.options(opt).invoke()
        try:
            name = yr.get(sandbox.get_name.invoke())
            logger.info("sandbox created, instance_name=%s", name)
        except Exception as e:
            logger.error("sandbox create failed, name=%s, error=%s", opt.name, e)
    finally:
        yr.finalize()


if __name__ == "__main__":
    main()

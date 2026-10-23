#!/usr/bin/env python3
"""Persistent public SDK client; JSON-lines commands are test orchestration only."""

import dataclasses
import hashlib
import importlib.metadata
import json
import logging
import os
from pathlib import Path, PurePosixPath
import sys
import traceback


class ProtocolHandler(logging.StreamHandler):
    """Keep protocol failures visible so the client releases its Environments."""

    def handleError(self, record):
        stream_error = sys.exc_info()[1]
        if stream_error is not None:
            raise stream_error
        super().handleError(record)


protocol_logger = logging.getLogger("adx.jiuwenbox.sdk_protocol")
for previous_handler in list(protocol_logger.handlers):
    protocol_logger.removeHandler(previous_handler)
    previous_handler.close()
protocol_handler = ProtocolHandler(sys.stdout)
protocol_handler.setFormatter(logging.Formatter("%(message)s"))
protocol_logger.addHandler(protocol_handler)
protocol_logger.setLevel(logging.INFO)
protocol_logger.propagate = False


def send_message(message):
    protocol_logger.info("%s", json.dumps(message, ensure_ascii=False))


config = json.loads(Path(sys.argv[1]).read_text())
os.environ["SSL_CERT_FILE"] = config["ca"]
sdk_module = importlib.import_module("adx_environment")
ConnectionConfig = sdk_module.ConnectionConfig
Environment = sdk_module.Environment
EnvironmentNotFound = sdk_module.EnvironmentNotFound
AdxEnvironmentError = sdk_module.EnvironmentError
RestartPolicy = sdk_module.RestartPolicy

connection = ConnectionConfig(
    server_address=config["endpoint"],
    token=Path(config["token_file"]).read_text().strip(),
    use_tls=True,
    verify_tls=True,
)
environments = {}
payload = b"adx-sdk-binary\x00\xff\n" * 10000


def check(runtime_class):
    environment = environments[runtime_class]
    if not (environment.get_info().state == "running"):
        raise AssertionError("acceptance condition failed")
    result = environment.commands.run("printf 'adx-sdk'; printf 'stderr-sdk' >&2; exit 7")
    if not ((result.stdout, result.stderr, result.exit_code) == ("adx-sdk", "stderr-sdk", 7)):
        raise AssertionError(repr(result))
    if not (
        environment.files.read(str(PurePosixPath("/workspace") / f"{runtime_class}.bin"), format="bytes")
        == payload
    ):
        raise AssertionError("acceptance condition failed")
    environment.files.write(str(PurePosixPath("/workspace") / f"{runtime_class}.txt"), "SDK 文件读写\n")
    if not (
        environment.files.read(str(PurePosixPath("/workspace") / f"{runtime_class}.txt")) == "SDK 文件读写\n"
    ):
        raise AssertionError("acceptance condition failed")
    probe = environment.commands.run(
        (
            "sha256sum /__adx/usr/local/bin/adx-execd; grep ' /__adx ' /proc/mounts; "
            "if touch /__adx/SHOULD_NOT_WRITE 2>/dev/null; then exit 9; fi; echo EROFS_READONLY_OK"
        )
    )
    if not (
        probe.exit_code == 0 and config["execd_sha"] in probe.stdout and "EROFS_READONLY_OK" in probe.stdout
    ):
        raise AssertionError(repr(probe))
    # Exercise the installed static payload too: musl's O_ACCMODE includes O_PATH,
    # so a host-libc unit test alone cannot catch flags masked by OpenOptions.
    fifo = str(PurePosixPath("/workspace") / f"{runtime_class}.fifo")
    created = environment.commands.run(f"mkfifo {fifo}")
    if created.exit_code != 0:
        raise AssertionError(repr(created))
    previous_timeout = os.environ.get("ADX_DIRECT_UPLOAD_TIMEOUT")
    try:
        os.environ["ADX_DIRECT_UPLOAD_TIMEOUT"] = "3"
        try:
            environment.files.read(fifo)
        except AdxEnvironmentError as fifo_error:
            if getattr(fifo_error, "status_code", None) != 404:
                raise AssertionError(f"FIFO download was not rejected: {fifo_error}") from fifo_error
        else:
            raise AssertionError("FIFO download unexpectedly succeeded")
    finally:
        if previous_timeout is None:
            os.environ.pop("ADX_DIRECT_UPLOAD_TIMEOUT", None)
        else:
            os.environ["ADX_DIRECT_UPLOAD_TIMEOUT"] = previous_timeout
        environment.files.remove(fifo)
    return {
        "id": environment.id,
        "info": dataclasses.asdict(environment.get_info()),
        "binary_bytes": len(payload),
        "binary_sha256": hashlib.sha256(payload).hexdigest(),
        "guest_erofs": probe.stdout,
        "fifo_download_status": 404,
        "command": {"stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
    }


send_message(
    {
        "ready": True,
        "sdk_version": importlib.metadata.version("adx-environment"),
        "sdk_location": str(Path(sys.modules["adx_environment"].__file__).resolve()),
    }
)
try:
    for line in sys.stdin:
        try:
            request = json.loads(line)
            action, backend = request["action"], request.get("backend")
            if action == "create":
                s = Environment(
                    image="acceptance:v1",
                    runtime=backend,
                    cpu=1000,
                    memory=512,
                    idle_timeout=0,
                    connection=connection,
                    create_timeout=150,
                    inherit_entrypoint=True,
                    restart_policy=RestartPolicy(
                        max_attempts=5, initial_backoff_seconds=1, max_backoff_seconds=2
                    ),
                )
                environments[backend] = s
                s.files.write(str(PurePosixPath("/workspace") / f"{backend}.bin"), payload)
                value = check(backend)
            elif action == "attach":
                for runtime, environment_id in request["ids"].items():
                    environments[runtime] = Environment.from_id(environment_id, connection=connection)
                value = {runtime: check(runtime) for runtime in environments}
            elif action == "delete_ids":
                for environment_id in request["ids"].values():
                    Environment.delete(environment_id, connection=connection)
                value = True
            elif action == "detach":
                value = {runtime: s.id for runtime, s in environments.items()}
                for s in environments.values():
                    s.close()
                environments.clear()
            elif action == "check":
                value = check(backend)
            elif action == "delete":
                s = environments[backend]
                Environment.delete(s.id, connection=connection)
                s.close()
                try:
                    remaining = Environment.from_id(s.id, connection=connection)
                except EnvironmentNotFound:
                    pass
                else:
                    remaining.close()
                    raise AssertionError("deleted Environment still exists")
                s.close()
                del environments[backend]
                value = {"id": s.id, "absent": True}
            elif action == "close":
                for s in environments.values():
                    s.kill()
                    s.close()
                environments.clear()
                value = True
            else:
                raise ValueError(action)
            send_message({"ok": True, "value": value})
        except Exception as error:
            traceback.print_exc(file=sys.stderr)
            send_message({"ok": False, "error": f"{type(error).__name__}: {error}"})
finally:
    for s in environments.values():
        try:
            s.kill()
        except Exception:
            traceback.print_exc(file=sys.stderr)
        s.close()

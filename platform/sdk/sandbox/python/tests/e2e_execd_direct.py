"""Live adx-sandbox -> execd direct-connection e2e.

Requires a deployed sandbox cluster. Wires the adx-sandbox to the frontend
(``ADX_SERVER_ADDRESS``). EXECD direct invoke uses ``/direct`` on that endpoint,
creates an EXECD-backed sandbox using the deployment's default runtime image,
exercises filesystem + command ops over the frontend /direct path, then asserts the
direct path stayed healthy -- i.e. the ops reached the sandbox's execd daemon
through the frontend /direct route and never sticky-fell-back to the frontend tunnel.

This is the LIVE counterpart to the offline ``test_transport_direct.py`` contract
test: together they cover adx-sandbox -> execd direct verification (contract + real cluster).

Env:
  ADX_SERVER_ADDRESS   frontend host:port (required)
  ADX_TOKEN            auth token (any value when JWT disabled; default 'ci')
  ADX_TLS              '0' for plain HTTP (default here)
  ADX_SANDBOX_MEMORY   scheduling memory in MB (default 4096)
"""

import os
import sys

os.environ.setdefault("ADX_TLS", "0")
os.environ.setdefault("ADX_TOKEN", "ci")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(
            f"SKIP: {name} not set; the live execd-direct e2e needs a deployed cluster",
            file=sys.stderr,
        )
        sys.exit(0)
    return value


server = _require("ADX_SERVER_ADDRESS")
memory = int(os.environ.get("ADX_SANDBOX_MEMORY", "4096"))

from adx_sandbox import Sandbox

passed: list = []
failed: list = []


def chk(name: str, cond: bool, detail: str = "") -> None:
    (passed if cond else failed).append(name)
    suffix = f"  {detail}" if detail else ""
    print(f"[{'PASS' if cond else 'FAIL'}] {name}{suffix}")


# Path lives inside the isolated sandbox container, not on the host.
REMOTE_PATH = "/tmp/direct.txt"

print(f"frontend={server} direct=/direct")
sb = Sandbox(
    name="execd-direct-e2e",
    memory=memory,
    create_timeout=180,
    schedule_timeout=120,
)
client = sb._client
chk("create execd sandbox", bool(sb.id), f"id={sb.id}")
chk("direct path configured", client.direct_enabled is True, "path=/direct")
try:
    info = sb.files.write(REMOTE_PATH, "hello execd direct")
    chk("files.write (direct)", info.size == 16, f"size={info.size}")
    chk(
        "files.read (direct)",
        sb.files.read(REMOTE_PATH) == "hello execd direct",
    )
    result = sb.commands.run("echo execd-ok")
    chk(
        "commands.run (direct)",
        result.exit_code == 0 and "execd-ok" in result.stdout,
        f"rc={result.exit_code} out={result.stdout!r}",
    )
    # The crucial direct-path assertion: every op above went through /direct
    # and none of them sticky-disabled it (no fallback to the frontend tunnel).
    chk(
        "frontend /direct stayed healthy (no fallback)",
        client._direct_disabled is False,
    )
finally:
    sb.kill()
    chk("kill", sb.is_running() is False)

print(f"\n==== {len(passed)} PASS, {len(failed)} FAIL ====")
if failed:
    print("FAILED:", failed)
    sys.exit(1)

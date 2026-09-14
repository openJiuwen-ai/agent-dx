"""Submit and recover a durable background command.

The caller owns persistence of sandbox_id and command_id. This example accepts
both values from the environment to make the recovery step explicit.
"""

import os
import uuid

from adx_sandbox import Sandbox


sandbox_id = os.environ["SANDBOX_ID"]
command_id = os.environ.get("COMMAND_ID", f"cmd-{uuid.uuid4()}")
sandbox = Sandbox.from_id(sandbox_id)

if os.environ.get("RECOVER", "0") == "1":
    command = sandbox.commands.get(command_id)
else:
    # Persist this pair in the caller's own database before submission. If the
    # response is lost, retrying the same request with this ID is idempotent.
    print(f"persist before submit sandbox_id={sandbox_id} command_id={command_id}")
    command = sandbox.commands.run(
        "sleep 2; printf durable-result",
        background=True,
        command_id=command_id,
    )

result = command.wait()
print(result.stdout)

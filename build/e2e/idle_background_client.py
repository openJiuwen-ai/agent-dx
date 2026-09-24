#!/usr/bin/env python3
"""Exit a separate SDK client while its detached command remains active."""
import json
import os
from pathlib import Path
import sys

from adx_sandbox import CommandStatus, ConnectionConfig, Sandbox


def main(image, output):
    secrets = Path('/secrets')
    connection = ConnectionConfig(
        server_address='127.0.0.1:8443',
        token=(secrets / 'api-key').read_text().strip(),
        use_tls=True,
        verify_tls=True,
    )
    sandbox = Sandbox(
        image=image,
        runtime='runc',
        cpu=500,
        memory=512,
        idle_timeout=6,
        detached=True,
        node_id='node1',
        connection=connection,
        create_timeout=150,
    )
    try:
        command = sandbox.commands.run(
            'sleep 120', background=True, command_id='idle-client-exit'
        )
        assert command.poll() == CommandStatus.RUNNING
        output.write_text(json.dumps({
            'instance_id': sandbox.id,
            'command_id': command.id,
            'command_running_before_exit': True,
            'client_pid': os.getpid(),
        }) + '\n')
    except Exception:
        try:
            sandbox.kill()
        except Exception:
            pass
        raise
    finally:
        sandbox.close()


if __name__ == '__main__':
    main(sys.argv[1], Path(sys.argv[2]))

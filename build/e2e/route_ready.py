"""Wait for the subscribed data-plane route before injecting a file fault."""

import time

from adx_sandbox import SandboxError


def wait_for_route(sandbox, *, timeout=10, clock=time.monotonic, sleep=time.sleep):
    """Use a read-only command query; retry only a missing synchronized route."""
    deadline = clock() + timeout
    while True:
        try:
            sandbox.commands.list()
            return
        except SandboxError as error:
            if 'route absent from synchronized cache' not in str(error):
                raise
            if clock() >= deadline:
                raise TimeoutError('instance route did not synchronize') from error
            sleep(.1)

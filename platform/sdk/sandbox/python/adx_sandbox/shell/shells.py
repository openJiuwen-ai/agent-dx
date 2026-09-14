"""Shells — factory for creating persistent Shell sessions via HTTP invoke."""

import asyncio
from typing import Dict, List, Optional

from .._transport import SandboxClient
from .shell import Shell


class Shells:
    """Factory for :class:`Shell` instances. Accessible as ``sandbox.shells``."""

    def __init__(
        self,
        client: SandboxClient,
        sandbox_id: str,
        default_cwd: Optional[str] = None,
    ):
        self._client = client
        self._sid = sandbox_id
        self._default_cwd = default_cwd
        self._shells: List[Shell] = []
        self._counter = 0

    async def create(
        self,
        cwd: Optional[str] = None,
        envs: Optional[Dict[str, str]] = None,
        shell: str = "/bin/bash",
        timeout: int = 60,
    ) -> Shell:
        """Create a new persistent shell session (RRT ``bash_init``)."""
        self._counter += 1
        session_id = f"sh_{self._counter}"

        result = await asyncio.to_thread(
            self._client.invoke,
            self._sid,
            "shell.create",
            {"session_id": session_id, "shell": shell},
        )
        if result.get("error"):
            raise RuntimeError(f"Failed to create shell session: {result['error']}")

        sh = Shell(self._client, self._sid, session_id)
        self._shells.append(sh)

        effective_cwd = cwd if cwd is not None else self._default_cwd
        if effective_cwd or envs:
            init_parts = []
            if effective_cwd:
                init_parts.append(f"cd {effective_cwd}")
            if envs:
                for k, v in envs.items():
                    init_parts.append(f"export {k}='{v}'")
            await sh.run(" && ".join(init_parts), timeout=timeout)

        return sh

    def close(self) -> None:
        """Synchronously close all shell sessions."""
        for sh in self._shells:
            sh.close()
        self._shells.clear()

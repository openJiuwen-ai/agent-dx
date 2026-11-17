"""Installed public DigitalMate caller against the real task-owned Rust issuer.

The Rust driver requires one exact success marker on stdout. Validation failures
propagate within the async caller; only the process entry logs the bounded reason
and exits non-zero. Client cleanup runs on either path.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

from dm_composio_mail.api import ConnectorClient, OwnerTarget

RESULT_MARKER = "shared-workload-identity-ok"
RESULT_LOG = logging.Logger(__name__, level=logging.INFO)
# The Rust driver compares stdout exactly, so the handler must emit the bare message
# with no logger name, level or timestamp decoration.
_RESULT_HANDLER = logging.StreamHandler(sys.stdout)
_RESULT_HANDLER.setFormatter(logging.Formatter("%(message)s"))
RESULT_LOG.addHandler(_RESULT_HANDLER)
ERROR_LOG = logging.Logger(__name__ + ".error", level=logging.ERROR)
_ERROR_HANDLER = logging.StreamHandler(sys.stderr)
_ERROR_HANDLER.setFormatter(logging.Formatter("%(message)s"))
ERROR_LOG.addHandler(_ERROR_HANDLER)


class WorkloadIdentityError(ValueError):
    """A bounded fixture invariant failure, separate from process exit."""


async def main():
    """Renew an expired normal privacy identity and prove its exact target."""
    packet = json.load(sys.stdin)
    client = ConnectorClient(
        packet["origin"], OwnerTarget(**packet["target"]), ca_file=Path(packet["ca_file"]),
    )
    try:
        credential = await client.renew(packet["token"])
        if not credential.token.startswith("dm-privacy-v1."):
            raise WorkloadIdentityError("renewed credential is not a normal privacy workload identity")
        if credential.recovery_expires_at <= credential.expires_at:
            raise WorkloadIdentityError("recovery deadline does not extend beyond the access deadline")
        RESULT_LOG.info("%s", RESULT_MARKER)
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except WorkloadIdentityError as error:
        ERROR_LOG.error("%s", error)
        raise SystemExit(1) from None

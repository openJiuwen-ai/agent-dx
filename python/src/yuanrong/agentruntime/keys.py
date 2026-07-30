"""Deterministic DataSystem keys."""

import hashlib
import json


def _hash_parts(*parts: str) -> str:
    canonical = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    # Go encoding/json always escapes the JavaScript line/paragraph separators,
    # even with HTML escaping disabled. Mirror that behavior without escaping
    # ordinary non-ASCII text.
    canonical = canonical.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    encoded = canonical.encode("utf-8")
    # Keep DataSystem keys compact while retaining 128 bits of collision resistance.
    return hashlib.sha256(encoded).hexdigest()[:32]


class SessionKeys:
    def __init__(
        self,
        tenant_id: str,
        function_name: str,
        function_version: str,
        session_context_id: str,
    ):
        self._prefix = "ar:s:{}:{}".format(
            _hash_parts(tenant_id, function_name, function_version),
            _hash_parts(session_context_id),
        )

    def turn(self, index: int) -> str:
        return f"{self._prefix}:t{index}"

    def event(self, seq: int) -> str:
        return f"{self._prefix}:e{seq}"

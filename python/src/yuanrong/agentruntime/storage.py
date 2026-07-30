"""Thin asynchronous KV adapter over the public DataSystem Python SDK."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, Iterable, List, Mapping, Optional, Protocol

from .errors import DataSystemError

_LOG = logging.getLogger(__name__)
_DEFAULT_WRITE_MODE_ENV = "YR_DATASYSTEM_DEFAULT_WRITE_MODE"


class KVStore(Protocol):
    async def get(self, key: str) -> Optional[bytes]:
        ...

    async def mget(self, keys: Iterable[str]) -> List[Optional[bytes]]:
        ...

    async def set(self, key: str, value: bytes) -> None:
        ...

    async def delete(self, keys: Iterable[str]) -> None:
        ...


class MemoryKVStore:
    """Deterministic in-memory adapter for tests."""

    def __init__(self, initial: Optional[Dict[str, bytes]] = None):
        self.values: Dict[str, bytes] = dict(initial or {})

    async def get(self, key: str) -> Optional[bytes]:
        return self.values.get(key)

    async def mget(self, keys: Iterable[str]) -> List[Optional[bytes]]:
        return [self.values.get(key) for key in keys]

    async def set(self, key: str, value: bytes) -> None:
        self.values[key] = value

    async def delete(self, keys: Iterable[str]) -> None:
        for key in keys:
            self.values.pop(key, None)


class DataSystemKVStore:
    """KV adapter using ``yr.datasystem.DsClient`` only."""

    def __init__(self, client: object, kv_client: object, write_mode: object):
        self._client = client
        self._kv = kv_client
        self._write_mode = write_mode

    @classmethod
    async def create(cls, tenant_id: str) -> "DataSystemKVStore":
        try:
            from yr.datasystem import DsClient, WriteMode

            host, port = _datasystem_address()
            client = DsClient(host, port, tenant_id=tenant_id)
            await asyncio.to_thread(client.init)
            return cls(client, client.kv(), _default_write_mode(WriteMode))
        except Exception as exc:
            raise DataSystemError(f"failed to initialize DataSystem client: {exc}") from exc

    async def get(self, key: str) -> Optional[bytes]:
        values = await self.mget([key])
        return values[0]

    async def mget(self, keys: Iterable[str]) -> List[Optional[bytes]]:
        key_list = list(keys)
        if not key_list:
            return []
        try:
            exists = await asyncio.to_thread(self._kv.exist, key_list)
            if len(exists) != len(key_list):
                raise DataSystemError(
                    "DataSystem returned an unexpected number of existence results"
                )
            present_keys = [key for key, present in zip(key_list, exists) if present]
            if not present_keys:
                return [None] * len(key_list)
            raw_values = await asyncio.to_thread(self._kv.get, present_keys)
            if len(raw_values) != len(present_keys):
                raise DataSystemError(
                    "DataSystem returned an unexpected number of EventLog values"
                )
            present_values = iter(raw_values)
            result: List[Optional[bytes]] = []
            for present in exists:
                if not present:
                    result.append(None)
                    continue
                value = next(present_values)
                result.append(None if value is None else bytes(value))
            return result
        except DataSystemError:
            raise
        except Exception as exc:
            raise DataSystemError(f"failed to read EventLog data: {exc}") from exc

    async def set(self, key: str, value: bytes) -> None:
        try:
            await asyncio.to_thread(self._kv.set, key, value, self._write_mode)
        except Exception as exc:
            raise DataSystemError(f"failed to persist EventLog data: {exc}") from exc

    async def delete(self, keys: Iterable[str]) -> None:
        key_list = list(keys)
        if not key_list:
            return
        try:
            failed = await asyncio.to_thread(self._kv.delete, key_list)
        except Exception as exc:
            raise DataSystemError(f"failed to delete EventLog data: {exc}") from exc
        if failed:
            raise DataSystemError(f"failed to delete {len(failed)} EventLog key(s)")


def _datasystem_address() -> tuple[str, int]:
    address = os.getenv("DATASYSTEM_ADDR", "")
    host, separator, port_text = address.rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError("DATASYSTEM_ADDR must use host:port format")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError("DATASYSTEM_ADDR port must be an integer") from exc
    if port <= 0 or port > 65535:
        raise ValueError("DATASYSTEM_ADDR port is out of range")
    return host, port


def _default_write_mode(
    write_mode: object,
    environ: Optional[Mapping[str, str]] = None,
) -> object:
    source = os.environ if environ is None else environ
    configured = source.get(_DEFAULT_WRITE_MODE_ENV, "").upper()
    modes = {
        "": "NONE_L2_CACHE",
        "0": "NONE_L2_CACHE",
        "NONE_L2_CACHE": "NONE_L2_CACHE",
        "1": "WRITE_THROUGH_L2_CACHE",
        "WRITE_THROUGH_L2_CACHE": "WRITE_THROUGH_L2_CACHE",
        "2": "WRITE_BACK_L2_CACHE",
        "WRITE_BACK_L2_CACHE": "WRITE_BACK_L2_CACHE",
        "3": "NONE_L2_CACHE_EVICT",
        "NONE_L2_CACHE_EVICT": "NONE_L2_CACHE_EVICT",
    }
    mode_name = modes.get(configured)
    if mode_name is None:
        _LOG.warning(
            "invalid %s=%r; using NONE_L2_CACHE",
            _DEFAULT_WRITE_MODE_ENV,
            configured,
        )
        mode_name = "NONE_L2_CACHE"
    return getattr(write_mode, mode_name)

import unittest
from unittest.mock import patch

from yuanrong.agentruntime.keys import SessionKeys
from yuanrong.agentruntime.storage import (
    DataSystemKVStore,
    _datasystem_address,
    _default_write_mode,
)


class FakeKV:
    def __init__(self):
        self.values = {}
        self.calls = []

    def set(self, key, value, write_mode):
        self.calls.append(("set", key, value, write_mode))
        self.values[key] = value

    def get(self, keys):
        self.calls.append(("get", keys))
        return [self.values.get(key) for key in keys]

    def exist(self, keys):
        self.calls.append(("exist", keys))
        return [key in self.values for key in keys]

    def delete(self, keys):
        self.calls.append(("delete", keys))
        for key in keys:
            self.values.pop(key, None)
        return []


class DataSystemAdapterContractTest(unittest.IsolatedAsyncioTestCase):
    def test_reuses_libruntime_default_write_mode_config(self):
        class FakeWriteMode:
            NONE_L2_CACHE = object()
            WRITE_THROUGH_L2_CACHE = object()
            WRITE_BACK_L2_CACHE = object()
            NONE_L2_CACHE_EVICT = object()

        vectors = {
            "": FakeWriteMode.NONE_L2_CACHE,
            "0": FakeWriteMode.NONE_L2_CACHE,
            "none_l2_cache": FakeWriteMode.NONE_L2_CACHE,
            "1": FakeWriteMode.WRITE_THROUGH_L2_CACHE,
            "write_through_l2_cache": FakeWriteMode.WRITE_THROUGH_L2_CACHE,
            "2": FakeWriteMode.WRITE_BACK_L2_CACHE,
            "write_back_l2_cache": FakeWriteMode.WRITE_BACK_L2_CACHE,
            "3": FakeWriteMode.NONE_L2_CACHE_EVICT,
            "none_l2_cache_evict": FakeWriteMode.NONE_L2_CACHE_EVICT,
            "invalid": FakeWriteMode.NONE_L2_CACHE,
        }
        for configured, expected in vectors.items():
            with self.subTest(configured=configured):
                self.assertIs(
                    _default_write_mode(
                        FakeWriteMode,
                        {"YR_DATASYSTEM_DEFAULT_WRITE_MODE": configured},
                    ),
                    expected,
                )

    async def test_maps_public_kv_methods_and_write_mode(self):
        kv = FakeKV()
        mode = object()
        store = DataSystemKVStore(object(), kv, mode)
        await store.set("key", b"value")
        self.assertEqual(await store.get("key"), b"value")
        self.assertEqual(await store.mget(["key", "missing"]), [b"value", None])
        await store.delete(["key"])
        self.assertEqual(
            [call[0] for call in kv.calls],
            ["set", "exist", "get", "exist", "get", "delete"],
        )
        self.assertIs(kv.calls[0][3], mode)

    async def test_rejects_a_short_get_result_as_datasystem_error(self):
        class ShortReadKV(FakeKV):
            def exist(self, keys):
                return [True for _ in keys]

            def get(self, keys):
                return []

        store = DataSystemKVStore(object(), ShortReadKV(), object())
        with self.assertRaisesRegex(
            RuntimeError, "unexpected number of EventLog values"
        ):
            await store.mget(["key"])

    def test_reads_faas_datasystem_address(self):
        with patch.dict("os.environ", {"DATASYSTEM_ADDR": "172.17.0.3:31501"}):
            self.assertEqual(_datasystem_address(), ("172.17.0.3", 31501))

    def test_reads_bracketed_ipv6_datasystem_address(self):
        with patch.dict("os.environ", {"DATASYSTEM_ADDR": "[::1]:31501"}):
            self.assertEqual(_datasystem_address(), ("::1", 31501))

    def test_rejects_invalid_datasystem_address(self):
        for address in ("", "localhost", "localhost:not-a-port", "localhost:70000"):
            with self.subTest(address=address):
                with patch.dict("os.environ", {"DATASYSTEM_ADDR": address}):
                    with self.assertRaises(ValueError):
                        _datasystem_address()

    def test_session_keys_use_datasystem_safe_separator(self):
        keys = SessionKeys("tenant", "function", "latest", "session")
        self.assertRegex(keys.turn(1), r"^ar:s:[0-9a-f]{32}:[0-9a-f]{32}:t1$")
        self.assertRegex(keys.event(1), r"^ar:s:[0-9a-f]{32}:[0-9a-f]{32}:e1$")

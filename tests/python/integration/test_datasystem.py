import os
import unittest
from uuid import uuid4

from yuanrong.agentruntime.storage import DataSystemKVStore


@unittest.skipUnless(
    os.getenv("YR_RUN_DATASYSTEM_INTEGRATION") == "1",
    "set YR_RUN_DATASYSTEM_INTEGRATION=1 in a configured FaaS/DataSystem environment",
)
class DataSystemIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_public_sdk_round_trip(self):
        store = await DataSystemKVStore.create(os.getenv("DATASYSTEM_TENANT_ID", "default"))
        key = f"ar:integration:{uuid4().hex}"
        try:
            await store.set(key, b"value")
            self.assertEqual(await store.get(key), b"value")
        finally:
            await store.delete([key])

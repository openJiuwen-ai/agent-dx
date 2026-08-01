import unittest

from yuanrong.agentruntime.errors import AgentRuntimeNotConfigured
from yuanrong.agentruntime.metadata import RuntimeMetadata

from .helpers import FakeFunctionContext


class MetadataTest(unittest.TestCase):
    def test_reads_function_context_and_environment(self):
        metadata = RuntimeMetadata.from_function_context(
            FakeFunctionContext(),
            {"YR_SESSION_CTX_ID": "session"},
        )
        self.assertEqual(metadata.tenant_id, "tenant")
        self.assertEqual(metadata.function_name, "agent")
        self.assertEqual(metadata.function_version, "v1")
        self.assertEqual(metadata.session_context_id, "session")

    def test_rejects_missing_and_empty_session_context(self):
        for environ in ({}, {"YR_SESSION_CTX_ID": ""}, {"YR_SESSION_CTX_ID": " "}):
            with self.subTest(environ=environ):
                with self.assertRaises(AgentRuntimeNotConfigured):
                    RuntimeMetadata.from_function_context(
                        FakeFunctionContext(), environ
                    )

    def test_rejects_none_or_empty_function_identity(self):
        for method, value in (
            ("getTenantID", None),
            ("getFunctionName", ""),
            ("getVersion", " "),
        ):
            with self.subTest(method=method):
                context = FakeFunctionContext()
                setattr(context, method, lambda value=value: value)
                with self.assertRaises(AgentRuntimeNotConfigured):
                    RuntimeMetadata.from_function_context(
                        context, {"YR_SESSION_CTX_ID": "session"}
                    )

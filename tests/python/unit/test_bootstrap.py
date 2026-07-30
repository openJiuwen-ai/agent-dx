import concurrent.futures
import asyncio
import sys
import types
import unittest
from unittest.mock import patch

from yuanrong.agentruntime import AgentExecutor, Complete, InputRequired
from yuanrong.agentruntime.bootstrap import AgentRuntime, RuntimeState, _load_executor
from yuanrong.agentruntime.errors import (
    AgentExecutorLoadFailed,
    DataSystemError,
    SessionContextBindingMismatch,
)
from yuanrong.agentruntime.storage import MemoryKVStore

from .helpers import ConcurrentAgent, FakeFunctionContext, FakeStream, SequenceAgent


class BootstrapTest(unittest.TestCase):
    def _initialize(self, agent, store=None, session_id="session"):
        runtime = AgentRuntime()
        self.addCleanup(runtime.close)
        context = FakeFunctionContext()
        with patch.dict("os.environ", {"YR_SESSION_CTX_ID": session_id}, clear=True):
            runtime.initialize(
                context,
                store=store or MemoryKVStore(),
                executor_factory=lambda: agent,
            )
        return runtime, context

    @staticmethod
    def _invoke(runtime, message, context, session_id="session"):
        with patch.dict("os.environ", {"YR_SESSION_CTX_ID": session_id}, clear=True):
            return runtime.invoke(message, context)

    def test_initialize_and_invoke(self):
        agent = SequenceAgent([Complete({"ok": True})])
        runtime, context = self._initialize(agent)
        result = self._invoke(runtime, {"any": ["json"]}, context)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(runtime.state, RuntimeState.READY)
        self.assertEqual(agent.init_calls, 1)
        self.assertEqual(context.stream.values, [])

    def test_initializer_failure_is_cached_and_does_not_escape(self):
        class FailingAgent(AgentExecutor):
            def __init__(self):
                self.calls = 0

            async def init(self, session_context):
                self.calls += 1
                raise ValueError("private path /tmp/secret")

            async def execute(self, request_context):
                raise AssertionError("must not execute")

        agent = FailingAgent()
        runtime, context = self._initialize(agent)
        self.assertEqual(runtime.state, RuntimeState.START_FAILED)
        self.assertEqual(agent.calls, 1)
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "AGENT_INIT_FAILED") as raised:
                self._invoke(runtime, {}, context)
            self.assertNotIn("/tmp/secret", str(raised.exception))
        self.assertEqual(agent.calls, 1)

    def test_missing_or_empty_session_context_id_is_cached(self):
        runtime = AgentRuntime()
        self.addCleanup(runtime.close)
        runtime.initialize(
            FakeFunctionContext(),
            store=MemoryKVStore(),
            executor_factory=lambda: SequenceAgent([Complete(None)]),
        )
        self.assertEqual(runtime.state, RuntimeState.START_FAILED)

        empty, _ = self._initialize(SequenceAgent([Complete(None)]), session_id="")
        self.assertEqual(empty.state, RuntimeState.START_FAILED)

    def test_cached_datasystem_error_is_sanitized(self):
        runtime = AgentRuntime()
        self.addCleanup(runtime.close)

        def fail():
            raise DataSystemError("native loader failed at /private/runtime/lib.so")

        with patch.dict("os.environ", {"YR_SESSION_CTX_ID": "session"}, clear=True):
            runtime.initialize(
                FakeFunctionContext(),
                store=MemoryKVStore(),
                executor_factory=fail,
            )
        with self.assertRaisesRegex(RuntimeError, "DATASYSTEM_ERROR") as raised:
            self._invoke(runtime, {}, FakeFunctionContext())
        self.assertNotIn("/private/runtime", str(raised.exception))

    def test_missing_stream_is_rejected(self):
        runtime, _ = self._initialize(SequenceAgent([Complete(None)]))

        class ContextWithoutStream(FakeFunctionContext):
            def get_stream(self):
                return None

        with self.assertRaisesRegex(RuntimeError, "SSE_STREAM_REQUIRED"):
            self._invoke(runtime, {}, ContextWithoutStream())

    def test_runtime_does_not_write_eof_and_returns_final_value(self):
        async def execute(request):
            await request.output.write("chunk")
            return Complete("final")

        runtime, context = self._initialize(SequenceAgent([execute]))
        result = self._invoke(runtime, {}, context)
        self.assertEqual(result, "final")
        self.assertEqual(context.stream.values, ["chunk"])

    def test_recovery_happens_before_agent_init(self):
        store = MemoryKVStore()
        first, first_context = self._initialize(
            SequenceAgent([InputRequired("confirm")]),
            store=store,
        )
        self._invoke(first, {"step": 1}, first_context)

        class RecoveringAgent(AgentExecutor):
            def __init__(self):
                self.events_at_init = []
                self.executed_turn_id = None

            async def init(self, session_context):
                self.events_at_init = await session_context.event_log.get()

            async def execute(self, request_context):
                self.executed_turn_id = request_context.turn_id
                return Complete("done")

        agent = RecoveringAgent()
        second, second_context = self._initialize(agent, store=store)
        self.assertEqual(
            [event.type for event in agent.events_at_init],
            ["input.message", "turn.input_required"],
        )
        self._invoke(second, {"confirmed": True}, second_context)
        self.assertEqual(agent.executed_turn_id, "turn-000001")

    def test_init_and_invoke_share_one_persistent_event_loop(self):
        class AsyncResourceAgent(AgentExecutor):
            async def init(self, session_context):
                self.init_loop = asyncio.get_running_loop()
                self.queue = asyncio.Queue()
                self.waiter = asyncio.create_task(self.queue.get())
                await asyncio.sleep(0)

            async def execute(self, request_context):
                self.execute_loop = asyncio.get_running_loop()
                self.queue.put_nowait("ready")
                return Complete(await self.waiter)

        agent = AsyncResourceAgent()
        runtime, context = self._initialize(agent)

        async def invoke_from_running_loop():
            return self._invoke(runtime, {}, context)

        self.assertEqual(asyncio.run(invoke_from_running_loop()), "ready")
        self.assertIs(agent.init_loop, agent.execute_loop)

    def test_repeated_initialize_rejects_a_different_session_binding(self):
        runtime, context = self._initialize(
            SequenceAgent([Complete("unused")]),
            session_id="session-a",
        )
        with patch.dict(
            "os.environ", {"YR_SESSION_CTX_ID": "session-b"}, clear=True
        ):
            runtime.initialize(
                context,
                store=MemoryKVStore(),
                executor_factory=lambda: SequenceAgent([Complete("unused")]),
            )
        self.assertEqual(runtime.state, RuntimeState.START_FAILED)
        with self.assertRaises(SessionContextBindingMismatch):
            self._invoke(runtime, {}, context, session_id="session-a")

    def test_invoke_rejects_a_different_session_without_poisoning_runtime(self):
        runtime, context = self._initialize(
            SequenceAgent([Complete("ok")]),
            session_id="session-a",
        )
        with self.assertRaises(SessionContextBindingMismatch):
            self._invoke(runtime, {}, context, session_id="session-b")
        self.assertEqual(
            self._invoke(runtime, {}, context, session_id="session-a"),
            "ok",
        )

    def test_concurrent_invocations_are_serialized(self):
        agent = ConcurrentAgent()
        runtime, context = self._initialize(agent)
        with patch.dict("os.environ", {"YR_SESSION_CTX_ID": "session"}, clear=True):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(lambda value: runtime.invoke(value, context), [1, 2])
                )
        self.assertEqual(sorted(results), [1, 2])
        self.assertEqual(agent.max_active, 1)

    def test_fixed_agent_entry_is_enforced(self):
        class ValidAgent(AgentExecutor):
            async def init(self, session_context):
                return None

            async def execute(self, request_context):
                return Complete(None)

        module = types.ModuleType("agent")
        module.Agent = ValidAgent
        with patch.dict(sys.modules, {"agent": module}):
            self.assertIsInstance(_load_executor(), ValidAgent)

        bad_module = types.ModuleType("agent")
        bad_module.Agent = object
        with patch.dict(sys.modules, {"agent": bad_module}):
            with self.assertRaises(AgentExecutorLoadFailed):
                _load_executor()

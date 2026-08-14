#!/usr/bin/env python3
# coding=UTF-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import unittest

from yuanrong.agentruntime import Complete, InputRequired
from yuanrong.agentruntime.context import SessionContext
from yuanrong.agentruntime.dispatcher import Dispatcher
from yuanrong.agentruntime.errors import (
    EventAppendNotActive,
    InvalidExecutionResult,
    OutputNotActive,
)
from yuanrong.agentruntime.event_log import EventLog
from yuanrong.agentruntime.keys import SessionKeys
from yuanrong.agentruntime.storage import MemoryKVStore
from yuanrong.agentruntime.turn_writer import TurnWriter

from .helpers import FakeStream, SequenceAgent


class DispatcherTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = MemoryKVStore()
        keys = SessionKeys("tenant", "function", "v1", "session")
        self.writer = TurnWriter(self.store, keys, "session")
        await self.writer.recover()
        self.event_log = EventLog(self.writer)
        self.session = SessionContext("session", self.event_log)

    async def test_complete_persists_single_terminal_event_and_returns_value(self):
        agent = SequenceAgent([Complete({"answer": 42})])
        result = await Dispatcher(agent, self.session, self.writer).dispatch(
            {"question": "x"}, FakeStream()
        )
        self.assertEqual(result, {"answer": 42})
        events = await self.event_log.get()
        self.assertEqual([event.type for event in events], ["input.message", "turn.completed"])
        self.assertEqual(events[-1].data, {"output": {"answer": 42}})

    async def test_input_required_does_not_allocate_new_turn(self):
        agent = SequenceAgent([InputRequired("confirm"), Complete("done")])
        dispatcher = Dispatcher(agent, self.session, self.writer)
        self.assertEqual(await dispatcher.dispatch("one", FakeStream()), "confirm")
        self.assertEqual(await dispatcher.dispatch("two", FakeStream()), "done")
        self.assertEqual(
            [request.turn_id for request in agent.requests],
            ["turn-000001", "turn-000001"],
        )

    async def test_output_is_persisted_before_stream_write(self):
        operations = []

        class RecordingStore(MemoryKVStore):
            async def set(inner_self, key, value):
                await super().set(key, value)
                if ":e" in key:
                    operations.append(("persist", key))

        store = RecordingStore()
        writer = TurnWriter(store, SessionKeys("t", "f", "v", "s"), "s")
        await writer.recover()
        event_log = EventLog(writer)
        session = SessionContext("s", event_log)

        async def execute(request):
            await request.output.write({"chunk": 1})
            return Complete("final")

        await Dispatcher(SequenceAgent([execute]), session, writer).dispatch(
            "input", FakeStream(operations)
        )
        stream_index = operations.index(("stream", '{"chunk":1}'))
        output_persist_index = next(
            index
            for index, operation in enumerate(operations)
            if operation[0] == "persist" and index > 0
        )
        self.assertLess(output_persist_index, stream_index)

    async def test_context_is_inactive_after_execute(self):
        holder = {}

        async def execute(request):
            holder["request"] = request
            await request.session_context.event_log.append(request, "agent.fact", {"x": 1})
            return Complete("done")

        agent = SequenceAgent([execute])
        await Dispatcher(agent, self.session, self.writer).dispatch("x", FakeStream())
        request = holder["request"]
        with self.assertRaises(EventAppendNotActive):
            await self.event_log.append(request, "agent.late", {})
        with self.assertRaises(OutputNotActive):
            await request.output.write("late")

    async def test_context_is_inactive_after_cancellation(self):
        holder = {}

        async def execute(request):
            holder["request"] = request
            raise asyncio.CancelledError()

        agent = SequenceAgent([execute])
        with self.assertRaises(asyncio.CancelledError):
            await Dispatcher(agent, self.session, self.writer).dispatch(
                "x", FakeStream()
            )
        request = holder["request"]
        self.assertFalse(request.is_active)
        with self.assertRaises(EventAppendNotActive):
            await self.event_log.append(request, "agent.late", {})
        with self.assertRaises(OutputNotActive):
            await request.output.write("late")

    async def test_turn_id_cannot_be_reassigned_or_forged_by_another_context(self):
        holder = {}

        async def execute(request):
            holder["request"] = request
            with self.assertRaises(AttributeError):
                request.turn_id = "forged"
            with self.assertRaises(AttributeError):
                request._turn_id = "forged"
            return Complete("done")

        await Dispatcher(
            SequenceAgent([execute]), self.session, self.writer
        ).dispatch("input", FakeStream())
        self.assertEqual(holder["request"].turn_id, "turn-000001")

    async def test_concurrent_writes_share_one_contiguous_sequence(self):
        async def execute(request):
            await asyncio.gather(
                request.output.write("one"),
                request.session_context.event_log.append(
                    request, "agent.concurrent", {"value": 2}
                ),
                request.output.write("three"),
            )
            return Complete("done")

        await Dispatcher(
            SequenceAgent([execute]), self.session, self.writer
        ).dispatch("input", FakeStream())
        events = await self.event_log.get()
        self.assertEqual([event.seq for event in events], list(range(1, 6)))
        self.assertEqual(events[-1].type, "turn.completed")

    async def test_invalid_result_marks_turn_failed(self):
        agent = SequenceAgent(["bare"])
        with self.assertRaises(InvalidExecutionResult):
            await Dispatcher(agent, self.session, self.writer).dispatch("x", FakeStream())
        events = await self.event_log.get()
        self.assertEqual(events[-1].type, "turn.failed")
        self.assertEqual(self.writer.state_for("turn-000001"), "FAILED")

    async def test_user_exception_marks_failed_and_next_call_uses_new_turn(self):
        agent = SequenceAgent([ValueError("secret"), Complete("ok")])
        dispatcher = Dispatcher(agent, self.session, self.writer)
        with self.assertRaisesRegex(RuntimeError, "AGENT_EXECUTION_FAILED"):
            await dispatcher.dispatch("x", FakeStream())
        await dispatcher.dispatch("y", FakeStream())
        self.assertEqual(agent.requests[-1].turn_id, "turn-000002")

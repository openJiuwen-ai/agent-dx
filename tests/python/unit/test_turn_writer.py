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

from yuanrong.agentruntime.keys import SessionKeys
from yuanrong.agentruntime.errors import DataSystemError
from yuanrong.agentruntime.serialization import to_json_bytes
from yuanrong.agentruntime.storage import MemoryKVStore
from yuanrong.agentruntime.turn_writer import TurnWriter


class TurnWriterTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = MemoryKVStore()
        self.keys = SessionKeys("tenant", "function", "v1", "session")
        self.writer = TurnWriter(self.store, self.keys, "session")
        await self.writer.recover()

    async def test_input_required_continues_and_terminal_allocates_next_turn(self):
        first = await self.writer.accept_input({"step": 1})
        await self.writer.append_platform(first, "turn.input_required", {"output": "confirm"})
        continued = await self.writer.accept_input({"confirm": True})
        self.assertEqual(continued, first)
        await self.writer.append_platform(first, "turn.completed", {"output": "done"})

        second = await self.writer.accept_input("next")
        self.assertEqual(first, "turn-000001")
        self.assertEqual(second, "turn-000002")

        events = await self.writer.get_events()
        self.assertEqual([event.seq for event in events], [1, 2, 3, 4, 5])
        self.assertEqual(
            [event.type for event in events],
            [
                "input.message",
                "turn.input_required",
                "input.message",
                "turn.completed",
                "input.message",
            ],
        )

    async def test_failed_turn_is_terminal(self):
        first = await self.writer.accept_input("bad")
        await self.writer.append_platform(first, "turn.failed", {"error": {}})
        second = await self.writer.accept_input("retry")
        self.assertEqual(second, "turn-000002")

    async def test_concurrent_inputs_share_one_turn_and_sequence_lock(self):
        turn_ids = await asyncio.gather(
            self.writer.accept_input("one"),
            self.writer.accept_input("two"),
        )
        self.assertEqual(turn_ids, ["turn-000001", "turn-000001"])
        self.assertIsNone(await self.store.get(self.keys.turn(2)))
        events = await self.writer.get_events()
        self.assertEqual([event.seq for event in events], [1, 2])

    async def test_recovery_stops_at_first_missing_event(self):
        turn = await self.writer.accept_input("one")
        await self.writer.append_sdk(turn, "agent.fact", {"value": 2})
        self.store.values.pop(self.keys.event(2))
        self.store.values[self.keys.event(3)] = self.store.values[self.keys.event(1)]

        recovered = TurnWriter(self.store, self.keys, "session")
        await recovered.recover()
        events = await recovered.get_events()
        self.assertEqual([event.seq for event in events], [1])

    async def test_empty_turn_record_is_reused_after_recovery(self):
        turn = await self.writer._create_turn()
        recovered = TurnWriter(self.store, self.keys, "session")
        await recovered.recover()
        accepted = await recovered.accept_input("resume")
        self.assertEqual(accepted, turn.turn_id)
        self.assertIsNone(await self.store.get(self.keys.turn(2)))

    async def test_get_after_seq_and_limit(self):
        turn = await self.writer.accept_input(1)
        await self.writer.append_sdk(turn, "agent.fact", 2)
        await self.writer.append_platform(turn, "turn.completed", {"output": 3})
        events = await self.writer.get_events(after_seq=1, limit=1)
        self.assertEqual([(event.seq, event.type) for event in events], [(2, "agent.fact")])

    async def test_recovery_rejects_an_unsupported_turn_schema(self):
        self.store.values[self.keys.turn(1)] = to_json_bytes(
            {"schemaVersion": 2}
        )
        with self.assertRaisesRegex(DataSystemError, "schemaVersion"):
            await self.writer.recover()

    async def test_recovery_rejects_a_malformed_event(self):
        turn = await self.writer.accept_input("one")
        self.assertEqual(turn, "turn-000001")
        self.store.values[self.keys.event(1)] = to_json_bytes(
            {"schemaVersion": 1}
        )
        recovered = TurnWriter(self.store, self.keys, "session")
        with self.assertRaisesRegex(DataSystemError, "stored Event field"):
            await recovered.recover()

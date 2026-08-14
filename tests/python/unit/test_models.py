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

import unittest

from yuanrong.agentruntime import Complete, InputRequired, RequestInput
from yuanrong.agentruntime.errors import EventSerializationFailed
from yuanrong.agentruntime.keys import SessionKeys, _hash_parts
from yuanrong.agentruntime.serialization import to_json_bytes, to_stream_value


class ModelTest(unittest.TestCase):
    def test_request_input_preserves_any_json_value(self):
        values = [
            {"a": 1},
            [1, "x"],
            "text",
            42,
            2.5,
            True,
            None,
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertIs(RequestInput(value).message, value)

    def test_execution_results_are_immutable(self):
        complete = Complete({"ok": True})
        required = InputRequired("answer")
        with self.assertRaises(AttributeError):
            complete.value = None
        with self.assertRaises(AttributeError):
            required.value = None

    def test_stream_serialization(self):
        self.assertEqual(to_stream_value("raw"), "raw")
        self.assertEqual(to_stream_value({"b": 2, "a": 1}), '{"a":1,"b":2}')
        with self.assertRaises(EventSerializationFailed):
            to_json_bytes({1, 2})

    def test_key_hashing_is_unambiguous_and_deterministic(self):
        first = SessionKeys("ab", "c", "v", "s").event(1)
        second = SessionKeys("a", "bc", "v", "s").event(1)
        self.assertNotEqual(first, second)
        self.assertEqual(first, SessionKeys("ab", "c", "v", "s").event(1))

    def test_key_hashing_matches_cross_language_special_character_vectors(self):
        self.assertEqual(_hash_parts("a&b"), "78ff3361117058d209f96a2334ce7045")
        self.assertEqual(_hash_parts("a<b"), "8c144a7602db11518476ebee4a99fe58")
        self.assertEqual(_hash_parts("中文"), "65bcb1a065d8b11ec83cecd06be6e3ae")
        self.assertEqual(_hash_parts("x\u2028y"), "011dd9da72bd83788f26f20d0b5b8fb2")

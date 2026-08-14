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

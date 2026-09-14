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

from ar_cli.utils import normalize_addr
from ar_cli.urn import function_urn_to_public_agent, public_agent_to_function_version_urn


def test_bare_host_port_gets_http():
    assert normalize_addr("127.0.0.1:31182") == "http://127.0.0.1:31182"


def test_existing_http_scheme_kept():
    assert normalize_addr("http://127.0.0.1:31182") == "http://127.0.0.1:31182"


def test_existing_https_scheme_kept():
    assert normalize_addr("https://host:443") == "https://host:443"


def test_surrounding_whitespace_stripped():
    assert normalize_addr("  host:8080  ") == "http://host:8080"


def test_public_agent_to_function_version_urn_defaults_latest():
    assert (
        public_agent_to_function_version_urn("0@default@demo")
        == "sn:cn:yrk:default:function:0@default@demo:latest"
    )


def test_public_agent_to_function_version_urn_keeps_namespace():
    assert (
        public_agent_to_function_version_urn("0@faaspy@demo")
        == "sn:cn:yrk:default:function:0@faaspy@demo:latest"
    )


def test_public_agent_to_function_version_urn_keeps_version():
    assert (
        public_agent_to_function_version_urn("0@default@demo:v2")
        == "sn:cn:yrk:default:function:0@default@demo:v2"
    )


def test_function_urn_to_public_agent_omits_latest():
    assert (
        function_urn_to_public_agent("sn:cn:yrk:default:function:0@default@demo:latest")
        == "0@default@demo"
    )


def test_function_urn_to_public_agent_keeps_namespace():
    assert (
        function_urn_to_public_agent("sn:cn:yrk:default:function:0@faaspy@demo:latest")
        == "0@faaspy@demo"
    )


def test_function_urn_to_public_agent_keeps_non_latest_version():
    assert (
        function_urn_to_public_agent("sn:cn:yrk:default:function:0@default@demo:v2")
        == "0@default@demo:v2"
    )


def test_function_urn_to_public_agent_rejects_empty_namespace():
    assert function_urn_to_public_agent("sn:cn:yrk:default:function:0@@demo:latest") is None


def test_function_urn_to_public_agent_rejects_non_numeric_tenant():
    assert function_urn_to_public_agent("sn:cn:yrk:default:function:tenant@faaspy@demo:latest") is None


def test_function_urn_to_public_agent_rejects_non_zero_prefix():
    assert function_urn_to_public_agent("sn:cn:yrk:default:function:123@faaspy@demo:latest") is None

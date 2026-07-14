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

import json
import importlib

import pytest
from click.testing import CliRunner

from ar_cli.const import ENABLE_SESSION_CTX_KEY
from ar_cli.main import cli
from ar_cli.utils import load_spec

deploy_module = importlib.import_module("ar_cli.commands.deploy")


def _capture_registered_spec(monkeypatch):
    """Patch register_function to capture the spec the command would send."""
    captured = {}

    def fake_register(self, meta_addr, spec):
        captured["addr"] = meta_addr
        captured["spec"] = spec
        return {"function": {"functionVersionUrn": "sn:cn:yrk:default:function:0@default@demo:latest"}}

    monkeypatch.setattr("ar_cli.client.AgentRuntimeClient.register_function", fake_register)
    return captured


def test_inject_default_true_when_absent(monkeypatch):
    captured = _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(cli, ["deploy", "-s", '{"name": "demo"}', "--server", "meta:31182"])
    assert result.exit_code == 0
    assert captured["spec"][ENABLE_SESSION_CTX_KEY] is True


def test_user_false_is_preserved(monkeypatch):
    captured = _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["deploy", "-s", '{"name": "demo", "enableSessionCtx": false}', "--server", "meta:31182"]
    )
    assert result.exit_code == 0
    assert captured["spec"][ENABLE_SESSION_CTX_KEY] is False


def test_user_true_is_preserved(monkeypatch):
    captured = _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["deploy", "-s", '{"enableSessionCtx": true}', "--server", "meta:31182"]
    )
    assert result.exit_code == 0
    assert captured["spec"][ENABLE_SESSION_CTX_KEY] is True


def test_invalid_json_spec_is_param_error():
    runner = CliRunner()
    result = runner.invoke(cli, ["deploy", "-s", "{not json}", "--server", "meta:31182"])
    assert result.exit_code == 2


def test_server_addr_gets_http_scheme(monkeypatch):
    captured = _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(cli, ["deploy", "-s", '{"name": "demo"}', "--server", "meta:31182"])
    assert result.exit_code == 0
    assert captured["addr"] == "http://meta:31182"


def test_deploy_server_without_port_is_rejected(monkeypatch):
    captured = _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(cli, ["deploy", "-s", '{"name": "demo"}', "--server", "meta"])
    assert result.exit_code == 2
    assert "host:port" in result.output
    assert "addr" not in captured  # registration not attempted


def test_deploy_nonexistent_spec_file_error_mentions_file(monkeypatch):
    _capture_registered_spec(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(cli, ["deploy", "-s", "./no_such_spec.json", "--server", "meta:31182"])
    assert result.exit_code == 2
    assert "existing file" in result.output


def test_deploy_prints_public_agent_urn(monkeypatch):
    _capture_registered_spec(monkeypatch)
    printed = []

    def fake_print(message, *args):
        printed.append(message % args)

    monkeypatch.setattr(deploy_module.print_logger, "info", fake_print)
    runner = CliRunner()

    result = runner.invoke(cli, ["deploy", "-s", '{"name": "demo"}', "--server", "meta:31182"])

    assert result.exit_code == 0
    assert printed == ["Deployed. agent: 0@default@demo"]
    assert "sn:cn:yrk" not in printed[0]


def test_deploy_prints_explicit_public_agent_version(monkeypatch):
    def fake_register(self, meta_addr, spec):
        return {"function": {"functionVersionUrn": "sn:cn:yrk:default:function:0@default@demo:v1"}}

    monkeypatch.setattr("ar_cli.client.AgentRuntimeClient.register_function", fake_register)
    printed = []
    monkeypatch.setattr(deploy_module.print_logger, "info", lambda message, *args: printed.append(message % args))
    runner = CliRunner()

    result = runner.invoke(cli, ["deploy", "-s", '{"name": "demo"}', "--server", "meta:31182"])

    assert result.exit_code == 0
    assert printed == ["Deployed. agent: 0@default@demo:v1"]


def test_load_spec_from_file(tmp_path):
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps({"name": "from-file"}), encoding="utf-8")
    assert load_spec(str(spec_file)) == {"name": "from-file"}


def test_load_spec_inline():
    assert load_spec('{"name": "inline"}') == {"name": "inline"}


def test_load_spec_invalid_raises():
    with pytest.raises(Exception):
        load_spec("{bad}")

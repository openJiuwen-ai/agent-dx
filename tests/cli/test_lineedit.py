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

from types import SimpleNamespace

import pytest

from ar_cli.interactive import lineedit
from ar_cli.interactive.lineedit import (
    _Line,
    _apply_completion,
    _completion_candidates,
    _move_completion_selection,
    _read_key,
    _width,
)


def _reader(byte_chunks):
    """Build a read_byte(timeout) that yields the given bytes one at a time.

    Each element is a bytes object of length 1; b"" marks a timeout/EOF.
    """
    it = iter(byte_chunks)

    def read_byte(timeout):
        try:
            return next(it)
        except StopIteration:
            return b""

    return read_byte


# --- _read_key: escape sequences -> logical keys ---------------------------

def test_arrow_keys():
    assert _read_key(_reader([b"\x1b", b"[", b"D"])) == "LEFT"
    assert _read_key(_reader([b"\x1b", b"[", b"C"])) == "RIGHT"
    assert _read_key(_reader([b"\x1b", b"[", b"A"])) == "UP"
    assert _read_key(_reader([b"\x1b", b"[", b"B"])) == "DOWN"


def test_home_end_delete():
    assert _read_key(_reader([b"\x1b", b"[", b"H"])) == "HOME"
    assert _read_key(_reader([b"\x1b", b"[", b"1", b"~"])) == "HOME"
    assert _read_key(_reader([b"\x1b", b"[", b"F"])) == "END"
    assert _read_key(_reader([b"\x1b", b"[", b"4", b"~"])) == "END"
    assert _read_key(_reader([b"\x1b", b"[", b"3", b"~"])) == "DELETE"


def test_control_keys():
    assert _read_key(_reader([b"\r"])) == "ENTER"
    assert _read_key(_reader([b"\n"])) == "ENTER"
    assert _read_key(_reader([b"\x7f"])) == "BACKSPACE"
    assert _read_key(_reader([b"\x04"])) == "EOF"
    assert _read_key(_reader([b"\x01"])) == "HOME"
    assert _read_key(_reader([b"\x05"])) == "END"
    assert _read_key(_reader([b"\t"])) == "TAB"


def test_lone_esc_is_exposed_to_the_selector():
    # Line editing still ignores this because ESC has no editing action; the
    # SessionCtx selector uses it as the cancel key.
    assert _read_key(_reader([b"\x1b"])) == "ESC"


def test_eof_when_no_bytes():
    assert _read_key(_reader([])) is None


def test_ascii_char():
    assert _read_key(_reader([b"a"])) == "a"


def test_utf8_multibyte_char():
    # "你" is U+4F60 -> 3 UTF-8 bytes E4 BD A0.
    chunks = [bytes([b]) for b in "你".encode("utf-8")]
    assert _read_key(_reader(chunks)) == "你"


# --- editing actions on the buffer -----------------------------------------

def _apply(keys):
    """Run a list of logical keys / chars through the dispatch table."""
    line = _Line()
    for key in keys:
        action = lineedit._ACTIONS.get(key)
        if action is not None:
            action(line)
        elif len(key) == 1 and key.isprintable():
            line.buf.insert(line.pos, key)
            line.pos += 1
    return "".join(line.buf), line.pos


def test_insert_and_left_then_insert_in_middle():
    text, pos = _apply(["a", "b", "c", "LEFT", "X"])
    assert text == "abXc"
    assert pos == 3


def test_backspace():
    text, pos = _apply(["a", "b", "c", "BACKSPACE"])
    assert text == "ab"
    assert pos == 2


def test_home_end_delete_actions():
    text, pos = _apply(["a", "b", "c", "HOME", "DELETE"])  # delete 'a' at start
    assert text == "bc"
    assert pos == 0
    text, pos = _apply(["a", "b", "HOME", "END", "X"])
    assert text == "abX"
    assert pos == 3


def test_kill_line():
    text, pos = _apply(["a", "b", "c", "KILL_LINE"])
    assert text == ""
    assert pos == 0


def test_kill_to_end():
    text, pos = _apply(["a", "b", "c", "LEFT", "KILL_TO_END"])
    assert text == "ab"
    assert pos == 2


def test_up_down_are_noops():
    text, pos = _apply(["a", "UP", "b", "DOWN"])
    assert text == "ab"
    assert pos == 2


def test_command_completion_prefers_prefix_matches():
    line = _Line()
    line.buf = list("/h")
    line.pos = len(line.buf)

    candidates = _completion_candidates(
        line,
        (("/history", "show history"), ("/help", "show help"), ("/new", "new context")),
    )

    assert candidates == [("/history", "show history"), ("/help", "show help")]


def test_command_completion_suggests_nearby_command_and_tab_replaces_input():
    line = _Line()
    line.buf = list("/histroy")
    line.pos = len(line.buf)

    candidates = _completion_candidates(line, (("/history", "show history"), ("/new", "new context")))

    assert candidates == [("/history", "show history")]
    _apply_completion(line, candidates[0][0])
    assert "".join(line.buf) == "/history"
    assert line.pos == len("/history")


def test_command_completion_stops_after_command_arguments_or_exact_command():
    line = _Line()
    line.buf = list("/fork turn-1")
    line.pos = len(line.buf)
    assert _completion_candidates(line, (("/fork", "fork"),)) == []

    line.buf = list("/fork")
    line.pos = len(line.buf)
    assert _completion_candidates(line, (("/fork", "fork"),)) == []


def test_command_completion_selection_is_bounded():
    assert _move_completion_selection(0, "UP", 3) == 0
    assert _move_completion_selection(0, "DOWN", 3) == 1
    assert _move_completion_selection(2, "DOWN", 3) == 2


@pytest.mark.skipif(not lineedit._HAS_TERMIOS, reason="raw terminal editor is Linux-only")
def test_raw_completion_uses_down_then_tab_before_submitting(monkeypatch):
    writes = []
    key_bytes = [b"/", b"\x1b", b"[", b"B", b"\t", b"\r"]

    monkeypatch.setattr(lineedit.sys, "stdin", SimpleNamespace(fileno=lambda: 0))
    monkeypatch.setattr(lineedit, "_make_reader", lambda _fd: _reader(key_bytes))
    monkeypatch.setattr(lineedit.tty, "setcbreak", lambda _fd: None)
    monkeypatch.setattr(lineedit.termios, "tcgetattr", lambda _fd: [])
    monkeypatch.setattr(lineedit.termios, "tcsetattr", lambda _fd, _when, _old: None)
    monkeypatch.setattr(lineedit, "_write", writes.append)

    result = lineedit._raw_read_line(
        "[ctx] > ",
        completions=(("/sessions", "list sessions"), ("/history", "show history")),
    )

    assert result == "/history"
    assert any("> /history" in output for output in writes)


# --- display width (CJK counts as 2 columns) -------------------------------

def test_display_width_cjk():
    assert _width("ab") == 2
    assert _width("你好") == 4
    assert _width("a你b") == 4

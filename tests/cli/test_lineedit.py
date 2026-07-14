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

from ar_cli import lineedit
from ar_cli.lineedit import _Line, _read_key, _width


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


def test_lone_esc_is_ignored():
    # ESC with no following bytes (timeout) -> not a recognised sequence.
    assert _read_key(_reader([b"\x1b"])) == "IGNORE"


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


# --- display width (CJK counts as 2 columns) -------------------------------

def test_display_width_cjk():
    assert _width("ab") == 2
    assert _width("你好") == 4
    assert _width("a你b") == 4

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

"""Minimal in-process line editor for the `adx exec` interactive prompt.

input() only supports cursor movement / history when the stdlib ``readline``
module is available. Some minimal Linux Python builds ship without it, so
arrow / Home / End keys would otherwise be echoed as raw escape sequences
(e.g. ``^[[D``). ``read_line()`` picks the best available reader:

  1. slash-command completions on a Linux TTY -> raw-mode editor below
  2. stdlib ``readline`` present -> input() (readline does the editing)
  3. stdin is a TTY + ``termios`` -> the small raw-mode editor below
  4. otherwise (pipe / no TTY)     -> plain input()

The raw-mode editor is dispatch-table driven (no long if/elif chains) and
covers the common editing keys plus UTF-8 (incl. CJK) text input. It is Unix
only; on other platforms / non-TTY we fall back to input().
"""

import os
import select
import sys
import unicodedata
from difflib import get_close_matches
from typing import Callable, List, Optional, Sequence, Tuple

try:
    import termios
    import tty

    _HAS_TERMIOS = True
except ImportError:  # non-Unix (e.g. Windows)
    _HAS_TERMIOS = False

# Short timeout (seconds) for reading the bytes that follow an ESC, so a lone
# ESC key does not block waiting for a sequence that never comes.
_ESC_TIMEOUT = 0.05

_readline_checked = False
_readline_available = False

_Completion = Tuple[str, str]


# --- key tables (data, not branches) ---------------------------------------

# Bytes following the leading ESC -> logical key name.
_ESC_KEYS = {
    "[D": "LEFT", "[C": "RIGHT",
    "[H": "HOME", "[1~": "HOME", "OH": "HOME",
    "[F": "END", "[4~": "END", "OF": "END",
    "[3~": "DELETE",
    "[A": "UP", "[B": "DOWN",
}

# Control byte -> logical key name.
_CTRL_KEYS = {
    "\r": "ENTER", "\n": "ENTER",
    "\t": "TAB",
    "\x7f": "BACKSPACE", "\x08": "BACKSPACE",
    "\x04": "EOF",
    "\x01": "HOME", "\x05": "END",
    "\x0b": "KILL_TO_END", "\x15": "KILL_LINE",
}


class _Line:
    """Editable buffer: a list of characters plus a cursor index."""

    __slots__ = ("buf", "pos")

    def __init__(self) -> None:
        self.buf: List[str] = []
        self.pos: int = 0


# Logical key -> action on the buffer (each is one or two lines).
def _act_left(s: _Line) -> None:
    s.pos = max(0, s.pos - 1)


def _act_right(s: _Line) -> None:
    s.pos = min(len(s.buf), s.pos + 1)


def _act_home(s: _Line) -> None:
    s.pos = 0


def _act_end(s: _Line) -> None:
    s.pos = len(s.buf)


def _act_backspace(s: _Line) -> None:
    if s.pos > 0:
        del s.buf[s.pos - 1]
        s.pos -= 1


def _act_delete(s: _Line) -> None:
    if s.pos < len(s.buf):
        del s.buf[s.pos]


def _act_kill_to_end(s: _Line) -> None:
    del s.buf[s.pos:]


def _act_kill_line(s: _Line) -> None:
    s.buf.clear()
    s.pos = 0


def _act_noop(s: _Line) -> None:
    pass  # UP/DOWN: history not implemented yet


_ACTIONS = {
    "LEFT": _act_left, "RIGHT": _act_right,
    "HOME": _act_home, "END": _act_end,
    "BACKSPACE": _act_backspace, "DELETE": _act_delete,
    "KILL_TO_END": _act_kill_to_end, "KILL_LINE": _act_kill_line,
    "UP": _act_noop, "DOWN": _act_noop,
}


# --- public entry point -----------------------------------------------------

def read_line(prompt: str, *, completions: Sequence[_Completion] = ()) -> Optional[str]:
    """Read one line of input. Returns ``None`` on EOF (closed stdin / Ctrl-D on
    an empty line).

    When ``completions`` are supplied on a Linux TTY, the raw-mode editor is
    used even if readline is installed so it can render candidates and process
    Up/Down/Tab consistently. Other calls use readline if available, else the
    raw-mode editor, else plain input().
    """
    if not sys.stdin.isatty():
        return _plain_input(prompt)
    if completions and _HAS_TERMIOS and sys.stdout.isatty():
        return _raw_read_line(prompt, completions=completions)
    if _ensure_readline():
        return _plain_input(prompt)  # readline-enabled input() does the editing
    if _HAS_TERMIOS:
        return _raw_read_line(prompt)
    return _plain_input(prompt)


def can_select_items() -> bool:
    """Whether the Linux raw-mode SessionCtx selector can be shown."""
    return _HAS_TERMIOS and sys.stdin.isatty() and sys.stdout.isatty()


def select_item(
    items: List[str],
    *,
    selected_index: int,
    heading: str,
    header: str,
) -> Optional[int]:
    """Select a rendered table row with Linux terminal navigation keys.

    Returns the selected row index, or ``None`` when the user cancels or when
    raw terminal selection is not available. The caller owns item formatting;
    this function only renders the selector marker and captures key events.
    """
    if not items or not can_select_items():
        return None

    index = min(max(selected_index, 0), len(items) - 1)
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    read_byte = _make_reader(fd)
    rendered_lines = 0

    def redraw() -> None:
        nonlocal rendered_lines
        if rendered_lines:
            _write(f"\x1b[{rendered_lines}A")
        lines = [heading, header]
        for row_index, row in enumerate(items):
            marker = ">" if row_index == index else " "
            lines.append(f"{marker} {row}")
        for line in lines:
            _write("\r\x1b[2K" + line + "\n")
        rendered_lines = len(lines)

    try:
        tty.setcbreak(fd)
        redraw()
        while True:
            key = _read_key(read_byte)
            if key in (None, "EOF", "ESC", "q", "Q"):
                return None
            if key == "ENTER":
                return index
            if key == "UP" and index > 0:
                index -= 1
                redraw()
            elif key == "DOWN" and index < len(items) - 1:
                index += 1
                redraw()
    except KeyboardInterrupt:
        return None
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        _write("\n")


def _ensure_readline() -> bool:
    global _readline_checked, _readline_available
    if not _readline_checked:
        _readline_checked = True
        try:
            import readline  # noqa: F401  (importing installs the input() editing hook)

            _readline_available = True
        except ImportError:
            _readline_available = False
    return _readline_available


def _plain_input(prompt: str) -> Optional[str]:
    try:
        return input(prompt)
    except EOFError:
        return None


# --- raw-mode editor --------------------------------------------------------

def _raw_read_line(prompt: str, *, completions: Sequence[_Completion] = ()) -> Optional[str]:
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    read_byte = _make_reader(fd)
    line = _Line()
    rendered_candidates = 0
    selected_candidate = 0
    try:
        tty.setcbreak(fd)  # no canonical mode / echo, but keep signals (Ctrl-C)
        candidates = _completion_candidates(line, completions)
        rendered_candidates = _redraw(prompt, line, candidates, selected_candidate, rendered_candidates)
        while True:
            key = _read_key(read_byte)
            if key is None:  # stdin closed
                _redraw(prompt, line, (), 0, rendered_candidates)
                _write("\r\n")
                return None
            if key == "EOF":  # Ctrl-D
                if not line.buf:
                    _redraw(prompt, line, (), 0, rendered_candidates)
                    _write("\r\n")
                    return None
                continue  # non-empty line: ignore
            candidates = _completion_candidates(line, completions)
            if key in ("ENTER", "TAB") and candidates:
                _apply_completion(line, candidates[selected_candidate][0])
                selected_candidate = 0
                candidates = ()
            elif key == "ENTER":
                _redraw(prompt, line, (), 0, rendered_candidates)
                _write("\r\n")
                return "".join(line.buf)
            elif key == "UP" and candidates:
                selected_candidate = _move_completion_selection(selected_candidate, key, len(candidates))
            elif key == "DOWN" and candidates:
                selected_candidate = _move_completion_selection(selected_candidate, key, len(candidates))
            else:
                action = _ACTIONS.get(key)
                if action is not None:
                    action(line)
                elif len(key) == 1 and key.isprintable():
                    line.buf.insert(line.pos, key)
                    line.pos += 1
            # else: unknown key (e.g. "ESC", "IGNORE") -> ignored
            new_candidates = _completion_candidates(line, completions)
            if new_candidates != candidates:
                selected_candidate = 0
            candidates = new_candidates
            rendered_candidates = _redraw(
                prompt,
                line,
                candidates,
                selected_candidate,
                rendered_candidates,
            )
    except KeyboardInterrupt:  # Ctrl-C cancels the current line
        _redraw(prompt, line, (), 0, rendered_candidates)
        _write("\r\n")
        return ""
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def _make_reader(fd: int) -> Callable[[Optional[float]], bytes]:
    def read_byte(timeout: Optional[float]) -> bytes:
        if timeout is not None:
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                return b""
        return os.read(fd, 1)

    return read_byte


def _read_key(read_byte: Callable[[Optional[float]], bytes]):
    """Read one logical key from ``read_byte``.

    Returns a key-name string (e.g. ``"LEFT"``, ``"ENTER"``, ``"EOF"``), a single
    printable character, ``"IGNORE"`` for keys we drop, or ``None`` on EOF.
    ``read_byte(timeout)`` returns one byte, or ``b""`` on timeout / EOF.
    """
    b = read_byte(None)  # blocking read of the first byte
    if not b:
        return None
    if b == b"\x1b":  # ESC -> escape sequence
        seq = ""
        for _ in range(5):
            nb = read_byte(_ESC_TIMEOUT)
            if not nb:
                break
            seq += nb.decode("latin-1")
            if seq in _ESC_KEYS:
                return _ESC_KEYS[seq]
            if seq[-1].isalpha() or seq[-1] == "~":  # CSI terminator
                break
        return _ESC_KEYS.get(seq, "ESC" if not seq else "IGNORE")
    ch = b.decode("latin-1")
    if ch in _CTRL_KEYS:
        return _CTRL_KEYS[ch]
    if b[0] < 0x20:  # other control bytes (tab, etc.): ignore
        return "IGNORE"
    # printable / UTF-8 multibyte: pull the continuation bytes and decode
    data = bytearray(b)
    for _ in range(_utf8_len(b[0]) - 1):
        nb = read_byte(None)
        if not nb:
            break
        data += nb
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return "IGNORE"


def _utf8_len(first_byte: int) -> int:
    if first_byte >= 0xF0:
        return 4
    if first_byte >= 0xE0:
        return 3
    if first_byte >= 0xC0:
        return 2
    return 1


# --- rendering --------------------------------------------------------------

def _char_width(ch: str) -> int:
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def _width(chars, end: Optional[int] = None) -> int:
    return sum(_char_width(c) for c in (chars if end is None else chars[:end]))


def _completion_candidates(line: _Line, completions: Sequence[_Completion]) -> List[_Completion]:
    """Return prefix matches, or nearby commands when the prefix has a typo."""
    text = "".join(line.buf)
    if not completions or not text.startswith("/") or any(char.isspace() for char in text):
        return []

    needle = text.casefold()
    if any(command.casefold() == needle for command, _ in completions):
        return []

    prefix_matches = [item for item in completions if item[0].casefold().startswith(needle)]
    if prefix_matches:
        return prefix_matches

    names = [command.casefold() for command, _ in completions]
    close_names = get_close_matches(needle, names, n=3, cutoff=0.55)
    by_name = {item[0].casefold(): item for item in completions}
    return [by_name[name] for name in close_names]


def _apply_completion(line: _Line, command: str) -> None:
    line.buf = list(command)
    line.pos = len(line.buf)


def _move_completion_selection(index: int, key: str, count: int) -> int:
    if key == "UP":
        return max(0, index - 1)
    if key == "DOWN":
        return min(count - 1, index + 1)
    return index


def _redraw(
    prompt: str,
    line: _Line,
    candidates: Sequence[_Completion] = (),
    selected_candidate: int = 0,
    previous_candidate_count: int = 0,
) -> int:
    text = "".join(line.buf)
    out = "\r" + prompt + text + "\x1b[K"
    candidate_lines = [
        f"{'>' if index == selected_candidate else ' '} {command:<10} {description}"
        for index, (command, description) in enumerate(candidates)
    ]
    rendered_count = max(previous_candidate_count, len(candidate_lines))
    for index in range(rendered_count):
        candidate_line = candidate_lines[index] if index < len(candidate_lines) else ""
        out += "\n\r\x1b[2K" + candidate_line
    if rendered_count:
        cursor_columns = _width(prompt) + _width(line.buf, line.pos)
        out += f"\x1b[{rendered_count}A\r"
        if cursor_columns:
            out += f"\x1b[{cursor_columns}C"
    else:
        back = _width(line.buf, len(line.buf)) - _width(line.buf, line.pos)
        if back > 0:
            out += f"\x1b[{back}D"
    _write(out)
    return len(candidate_lines)


def _write(s: str) -> None:
    sys.stdout.write(s)
    sys.stdout.flush()

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

"""Terminal-friendly rendering for SessionCtx and Turn API responses."""

import json
import unicodedata
from typing import Any, Dict, List, Sequence

from ar_cli.interactive.state import InteractiveCommandError


def objects(value: Any, label: str) -> List[Dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise InteractiveCommandError(f"invalid {label} response")
    return value


def session_row(item: Dict[str, Any]) -> List[str]:
    return [
        str(item.get("sessionContextId", "")),
        str(item.get("functionVersion", "")),
        format_timestamp(item.get("createdAt")),
    ]


def turn_row(item: Dict[str, Any]) -> List[str]:
    terminal_value = ""
    if "error" in item:
        terminal_value = message_preview(item["error"])
    elif "result" in item:
        terminal_value = message_preview(item["result"])
    return [
        str(item.get("turnId", "")),
        str(item.get("state", "")),
        message_preview(item.get("inputs", [])),
        message_preview(item.get("outputs", [])),
        terminal_value,
    ]


def format_timestamp(value: Any) -> str:
    text = str(value or "")
    return text.replace("T", " ").rstrip("Z")[:19]


def message_preview(value: Any) -> str:
    if isinstance(value, dict):
        parts = value.get("parts")
        if isinstance(parts, list):
            text_parts = [
                str(part["text"])
                for part in parts
                if isinstance(part, dict) and part.get("type") == "text" and "text" in part
            ]
            if text_parts:
                return " ".join(text_parts)
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(value)


def format_table(
    headers: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[int], selected_index: int = -1
) -> str:
    lines = [format_row(headers, widths)]
    for index, row in enumerate(rows):
        marker = ">" if index == selected_index else " "
        lines.append(f"{marker} {format_row(row, widths)}")
    return "\n".join(lines)


def format_row(values: Sequence[str], widths: Sequence[int]) -> str:
    return "  ".join(pad_display(str(value), width) for value, width in zip(values, widths))


def pad_display(value: str, width: int) -> str:
    text = truncate_display(value, width)
    return text + " " * max(0, width - display_width(text))


def truncate_display(value: str, width: int) -> str:
    if display_width(value) <= width:
        return value
    if width <= 3:
        return "." * width
    result: List[str] = []
    used = 0
    for char in value:
        char_width = char_width_of(char)
        if used + char_width > width - 3:
            break
        result.append(char)
        used += char_width
    return "".join(result) + "..."


def display_width(value: str) -> int:
    return sum(char_width_of(char) for char in value)


def char_width_of(char: str) -> int:
    if unicodedata.combining(char):
        return 0
    return 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1

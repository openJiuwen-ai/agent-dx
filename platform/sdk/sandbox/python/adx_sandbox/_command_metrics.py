"""Dependency-free process-local metrics for recoverable command operations."""

import threading
from collections import Counter
from typing import Dict


_LOCK = threading.Lock()
_COUNTERS: Counter[str] = Counter()
_WAIT_DURATION_SECONDS = 0.0


def increment(name: str) -> None:
    with _LOCK:
        _COUNTERS[name] += 1


def observe_wait(duration_seconds: float) -> None:
    global _WAIT_DURATION_SECONDS
    with _LOCK:
        _WAIT_DURATION_SECONDS += max(0.0, duration_seconds)


def snapshot() -> Dict[str, float]:
    """Return a stable snapshot for diagnostics or a hosting app exporter."""
    with _LOCK:
        return {
            "command_submit_total": float(_COUNTERS["command_submit_total"]),
            "command_get_total": float(_COUNTERS["command_get_total"]),
            "command_wait_total": float(_COUNTERS["command_wait_total"]),
            "command_wait_reconnect_total": float(
                _COUNTERS["command_wait_reconnect_total"]
            ),
            "command_wait_duration": _WAIT_DURATION_SECONDS,
        }

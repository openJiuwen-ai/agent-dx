"""Sequential Turn and EventLog persistence."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional
from uuid import uuid4

from .errors import DataSystemError, EventSerializationFailed
from .event import Event
from .keys import SessionKeys
from .serialization import from_json_bytes, to_json_bytes
from .storage import KVStore

_STATE_EVENTS = {
    "input.message": "WORKING",
    "turn.input_required": "INPUT_REQUIRED",
    "turn.completed": "COMPLETED",
    "turn.failed": "FAILED",
}


@dataclass(frozen=True)
class TurnRecord:
    session_context_id: str
    turn_index: int
    turn_id: str
    start_seq: int
    created_at: datetime
    schema_version: int = 1


class TurnWriter:
    def __init__(self, store: KVStore, keys: SessionKeys, session_context_id: str):
        self._store = store
        self._keys = keys
        self._session_context_id = session_context_id
        self._turns: List[TurnRecord] = []
        self._events: List[Event] = []
        self._next_turn_index = 1
        self._next_seq = 1
        self._append_lock = asyncio.Lock()

    async def recover(self) -> None:
        self._turns = []
        index = 1
        while True:
            raw = await self._store.get(self._keys.turn(index))
            if raw is None:
                break
            self._turns.append(_decode_turn(raw))
            index += 1
        self._next_turn_index = index

        self._events = []
        seq = 1
        while True:
            raw = await self._store.get(self._keys.event(seq))
            if raw is None:
                break
            self._events.append(_decode_event(raw))
            seq += 1
        self._next_seq = seq

    async def accept_input(self, message: Any) -> str:
        # Validate before allocating a durable Turn Record.
        to_json_bytes({"message": message})
        async with self._append_lock:
            turn = self._active_turn()
            if turn is None:
                turn = await self._create_turn_locked()
            await self._append_locked(
                turn.turn_id,
                "PLATFORM",
                "input.message",
                {"message": message},
            )
            return turn.turn_id

    async def append_platform(self, turn_id: str, event_type: str, data: Any) -> Event:
        return await self._append(turn_id, "PLATFORM", event_type, data)

    async def append_sdk(self, turn_id: str, event_type: str, data: Any) -> Event:
        return await self._append(turn_id, "SDK", event_type, data)

    async def get_events(
        self,
        *,
        after_seq: int = 0,
        limit: Optional[int] = None,
    ) -> List[Event]:
        if limit == 0:
            return []
        result: List[Event] = []
        seq = after_seq + 1
        while limit is None or len(result) < limit:
            raw = await self._store.get(self._keys.event(seq))
            if raw is None:
                break
            result.append(_decode_event(raw))
            seq += 1
        return result

    def state_for(self, turn_id: str) -> Optional[str]:
        for event in reversed(self._events):
            if event.turn_id == turn_id and event.type in _STATE_EVENTS:
                return _STATE_EVENTS[event.type]
        return None

    async def _create_turn(self) -> TurnRecord:
        async with self._append_lock:
            return await self._create_turn_locked()

    async def _create_turn_locked(self) -> TurnRecord:
        now = datetime.now(timezone.utc)
        index = self._next_turn_index
        turn = TurnRecord(
            session_context_id=self._session_context_id,
            turn_index=index,
            turn_id=f"turn-{index:06d}",
            start_seq=self._next_seq,
            created_at=now,
        )
        await self._store.set(self._keys.turn(index), _encode_turn(turn))
        self._turns.append(turn)
        self._next_turn_index += 1
        return turn

    def _active_turn(self) -> Optional[TurnRecord]:
        if not self._turns:
            return None
        last = self._turns[-1]
        state = self.state_for(last.turn_id)
        if state in ("COMPLETED", "FAILED"):
            return None
        return last

    async def _append(
        self,
        turn_id: str,
        source: str,
        event_type: str,
        data: Any,
    ) -> Event:
        async with self._append_lock:
            return await self._append_locked(turn_id, source, event_type, data)

    async def _append_locked(
        self,
        turn_id: str,
        source: str,
        event_type: str,
        data: Any,
    ) -> Event:
        now = datetime.now(timezone.utc)
        event = Event(
            session_context_id=self._session_context_id,
            turn_id=turn_id,
            seq=self._next_seq,
            event_id=str(uuid4()),
            source=source,  # type: ignore[arg-type]
            type=event_type,
            data=data,
            schema_version=1,
            created_at=now,
        )
        raw = _encode_event(event)
        await self._store.set(self._keys.event(event.seq), raw)
        self._events.append(event)
        self._next_seq += 1
        return event


def _encode_turn(turn: TurnRecord) -> bytes:
    return to_json_bytes(
        {
            "sessionContextId": turn.session_context_id,
            "turnIndex": turn.turn_index,
            "turnId": turn.turn_id,
            "startSeq": turn.start_seq,
            "createdAt": turn.created_at.isoformat(),
            "schemaVersion": turn.schema_version,
        }
    )


def _decode_turn(raw: bytes) -> TurnRecord:
    try:
        value = _stored_object(raw, "Turn")
        schema_version = _schema_version(value, "Turn")
        created_at = _timestamp(value, "createdAt", "Turn")
        return TurnRecord(
            session_context_id=_string(value, "sessionContextId", "Turn"),
            turn_index=_positive_int(value, "turnIndex", "Turn"),
            turn_id=_string(value, "turnId", "Turn"),
            start_seq=_positive_int(value, "startSeq", "Turn"),
            created_at=created_at,
            schema_version=schema_version,
        )
    except DataSystemError:
        raise
    except (EventSerializationFailed, KeyError, TypeError, ValueError) as exc:
        raise DataSystemError(f"stored Turn data is invalid: {exc}") from exc


def _encode_event(event: Event) -> bytes:
    return to_json_bytes(
        {
            "sessionContextId": event.session_context_id,
            "turnId": event.turn_id,
            "seq": event.seq,
            "eventId": event.event_id,
            "source": event.source,
            "type": event.type,
            "data": event.data,
            "schemaVersion": event.schema_version,
            "createdAt": event.created_at.isoformat(),
        }
    )


def _decode_event(raw: bytes) -> Event:
    try:
        value = _stored_object(raw, "Event")
        schema_version = _schema_version(value, "Event")
        source = _string(value, "source", "Event")
        if source not in ("PLATFORM", "SDK"):
            raise DataSystemError("stored Event field 'source' is invalid")
        if "data" not in value:
            raise DataSystemError("stored Event field 'data' is missing")
        return Event(
            session_context_id=_string(value, "sessionContextId", "Event"),
            turn_id=_string(value, "turnId", "Event"),
            seq=_positive_int(value, "seq", "Event"),
            event_id=_string(value, "eventId", "Event"),
            source=source,  # type: ignore[arg-type]
            type=_string(value, "type", "Event"),
            data=value["data"],
            schema_version=schema_version,
            created_at=_timestamp(value, "createdAt", "Event"),
        )
    except DataSystemError:
        raise
    except (EventSerializationFailed, KeyError, TypeError, ValueError) as exc:
        raise DataSystemError(f"stored Event data is invalid: {exc}") from exc


def _stored_object(raw: bytes, record_type: str) -> dict:
    value = from_json_bytes(raw)
    if not isinstance(value, dict):
        raise DataSystemError(f"stored {record_type} data must be a JSON object")
    return value


def _schema_version(value: dict, record_type: str) -> int:
    schema_version = value.get("schemaVersion")
    if type(schema_version) is not int or schema_version != 1:
        raise DataSystemError(
            f"stored {record_type} schemaVersion is missing or unsupported"
        )
    return schema_version


def _string(value: dict, field: str, record_type: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise DataSystemError(
            f"stored {record_type} field '{field}' must be a non-empty string"
        )
    return result


def _positive_int(value: dict, field: str, record_type: str) -> int:
    result = value.get(field)
    if type(result) is not int or result <= 0:
        raise DataSystemError(
            f"stored {record_type} field '{field}' must be a positive integer"
        )
    return result


def _timestamp(value: dict, field: str, record_type: str) -> datetime:
    text = _string(value, field, record_type)
    result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        raise DataSystemError(
            f"stored {record_type} field '{field}' must include a timezone"
        )
    return result

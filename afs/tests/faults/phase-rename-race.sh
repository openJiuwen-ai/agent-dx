#!/usr/bin/env bash
# R83: rename right after create/write/close; any failure (EBUSY seen once in S4a) fails the round.
set -u
python3 "$FAULTS/rename_race.py" "$ROOT/mnt" "${RENAME_ROUNDS:-100}"

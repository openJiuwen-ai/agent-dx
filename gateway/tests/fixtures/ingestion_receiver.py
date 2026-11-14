"""Loopback protocol substitute for Relay tests, not a product receiver.

Standard-library WSGI/SQLite only. Controlled test headers are not production
authentication. Actual DigitalMate receiver acceptance is deployment evidence.
"""

import hashlib
from http import HTTPStatus
import json
from pathlib import Path
import sqlite3
import sys
from wsgiref.simple_server import WSGIRequestHandler, make_server

PREFIX = "/api/digitalmate/ingestion/v1/batches"


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *_args):
        pass


def main():
    database, binding_path, port = sys.argv[1:]
    binding = json.loads(Path(binding_path).read_text(encoding="utf-8"))
    store = sqlite3.connect(database)
    store.execute("PRAGMA journal_mode=DELETE")
    store.execute("PRAGMA synchronous=FULL")
    store.executescript(
        "CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY,digest TEXT,wire_hash TEXT);"
        "CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,digest TEXT);"
    )

    def receiver(environment, start_response):
        def reply(status, response_value):
            raw = json.dumps(response_value, separators=(",", ":")).encode()
            start_response(f"{status} {HTTPStatus(status).phrase}", [
                ("Content-Type", "application/json"), ("Content-Length", str(len(raw))),
            ])
            return [raw]

        def ack(accepted_id, accepted_digest):
            return reply(200, {
                "version": 2, "batch_id": accepted_id, "digest": accepted_digest, "state": "accepted",
            })

        batch_id = environment.get("HTTP_X_TEST_BATCH_ID", "")
        digest = environment.get("HTTP_X_TEST_BATCH_DIGEST", "")
        if environment.get("HTTP_AUTHORIZATION") != "Bearer controlled-integration-purpose":
            return reply(403, {})
        if not batch_id or not digest:
            return reply(403, {})
        path, method = environment["PATH_INFO"], environment["REQUEST_METHOD"]
        if method == "GET" and path == PREFIX + "/" + batch_id:
            row = store.execute("SELECT digest FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                return reply(404, {})
            return ack(batch_id, digest) if row[0] == digest else reply(409, {})
        if method != "POST" or path != PREFIX:
            return reply(404, {})
        size = int(environment.get("CONTENT_LENGTH", "0"))
        if not 0 < size <= 2_097_152 or size != int(environment["HTTP_X_TEST_WIRE_BYTES"]):
            return reply(400, {})
        raw = environment["wsgi.input"].read(size)
        value = json.loads(raw)
        if value["version"] != 2 or value["binding"] != binding:
            return reply(403, {})
        if value["batch_id"] != batch_id or value["digest"] != digest:
            return reply(403, {})
        wire_hash = hashlib.sha256(raw).hexdigest()
        previous = store.execute("SELECT digest,wire_hash FROM batches WHERE id=?", (batch_id,)).fetchone()
        if previous is not None and previous != (digest, wire_hash):
            return reply(409, {})
        with store:
            store.execute("INSERT OR IGNORE INTO batches VALUES(?,?,?)", (batch_id, digest, wire_hash))
            for event in value["events"]:
                store.execute(
                    "INSERT OR IGNORE INTO events VALUES(?,?)", (event["source_event_id"], event["digest"]),
                )
        return ack(batch_id, digest)

    server = make_server("127.0.0.1", int(port), receiver, handler_class=QuietHandler)
    counts = []
    for table in ["batches", "events"]:
        counts.append(store.execute("SELECT count(*) FROM " + table).fetchone()[0])
    print(json.dumps({"port": server.server_port, "counts": counts}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()

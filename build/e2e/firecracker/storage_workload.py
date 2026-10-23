"""Matched storage probe with independently computed, full-memory checksums."""

import hashlib
from functools import lru_cache

MIB = 1024**2
PAYLOAD_BYTES = 512 * MIB
DIRTY_BYTES = 64 * MIB


def data_block(offset, dirty=False):
    seed = b"adx-storage-dirty-v1" if dirty else b"adx-storage-full-v1"
    return hashlib.shake_256(seed + offset.to_bytes(8, "little")).digest(MIB)


def expected_digest(dirty=False):
    return _expected_digest(PAYLOAD_BYTES, DIRTY_BYTES, MIB, dirty)


@lru_cache(maxsize=8)
def _expected_digest(payload_bytes, dirty_bytes, block_size, dirty):
    digest = hashlib.sha256()
    for offset in range(0, payload_bytes, block_size):
        seed = b"adx-storage-dirty-v1" if dirty and offset < dirty_bytes else b"adx-storage-full-v1"
        digest.update(hashlib.shake_256(seed + offset.to_bytes(8, "little")).digest(block_size))
    return digest.hexdigest()


def io_engine(environ):
    value = environ.get("ADX_STORAGE_IO_ENGINE", "Sync")
    if value not in ("Sync", "Async"):
        raise ValueError("ADX_STORAGE_IO_ENGINE must be Sync or Async")
    return value


def checkpoint_mode(environ):
    value = environ.get("ADX_STORAGE_CHECKPOINT_MODE", "full")
    if value not in ("full", "incremental"):
        raise ValueError("ADX_STORAGE_CHECKPOINT_MODE must be full or incremental")
    return value


def verify_checksum(answer, dirty=False):
    if (
        answer.get("checked_bytes") != PAYLOAD_BYTES
        or answer.get("sha256") != expected_digest(dirty)
        or answer.get("dirty_bytes") != (DIRTY_BYTES if dirty else 0)
    ):
        raise AssertionError("full guest memory checksum mismatch")


# Construct in-place; a temporary 512 MiB bytes copy would exceed the guest budget.
# Status/first HTTP never computes the checksum, so first-response and full-touch
# timings remain separate. A single lock makes dirtying and hashing consistent.
GUEST = """import hashlib, json, os, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
def block(offset, dirty=False):
 seed = b'adx-storage-dirty-v1' if dirty else b'adx-storage-full-v1'
 return hashlib.shake_256(seed + offset.to_bytes(8, 'little')).digest(1024*1024)
payload = bytearray(512*1024*1024)
for offset in range(0, len(payload), 1024*1024):
 payload[offset:offset+1024*1024] = block(offset)
seen = {}; lock = threading.Lock(); dirty_bytes = 0
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  global dirty_bytes
  q = parse_qs(urlsplit(self.path).query); nonce = q['nonce'][0]
  with lock:
   seen[nonce] = seen.get(nonce, 0) + 1; count = seen[nonce]
   if q.get('dirty'):
    for offset in range(0, 64*1024*1024, 1024*1024):
     payload[offset:offset+1024*1024] = block(offset, True)
    dirty_bytes = 64*1024*1024
   checksum = hashlib.sha256(payload).hexdigest() if q.get('touch') else None
   answer = {'pid': os.getpid(), 'nonce': nonce, 'count': count,
     'seen': dict(seen), 'dirty_bytes': dirty_bytes, 'sha256': checksum,
     'checked_bytes': len(payload) if checksum else 0,
     'file': open('/tmp/storage-file').read()}
  data = json.dumps(answer).encode()
  self.send_response(200); self.send_header('Content-Length', str(len(data)))
  self.end_headers(); self.wfile.write(data)
 def log_message(self, *args): pass
# Keep the twelve-request wake burst queued while accept is faulting pages in.
# The stdlib default of five introduces SYN retries unrelated to ADX routing.
class Server(ThreadingHTTPServer):
 request_queue_size = 128
server = Server(('0.0.0.0',8088), Handler)
open('/tmp/storage-ready', 'w').write(str(os.getpid()))
server.serve_forever()
"""


def identity(environ):
    return {
        "name": "deterministic-512m-v1",
        "memory_mib": 1024,
        "cpu_millis": 1000,
        "storage_mib": 32,
        "payload_bytes": PAYLOAD_BYTES,
        "dirty_bytes": DIRTY_BYTES,
        "source_sha256": hashlib.sha256(GUEST.encode()).hexdigest(),
        "expected_full_sha256": expected_digest(),
        "expected_dirty_sha256": expected_digest(True),
        "writable_io_engine": io_engine(environ),
        "writable_cache_type": "Writeback",
        "checkpoint_mode": checkpoint_mode(environ),
    }

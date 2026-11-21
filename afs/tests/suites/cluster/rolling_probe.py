"""Run I/O on an unchanged mount while a different AFS role is stopped."""

from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import time


def emit(directory, name, value):
    target = directory / (name + ".json")
    temporary = target.with_suffix(".pending")
    temporary.write_text(json.dumps(value))
    temporary.replace(target)


def await_signal(directory, name):
    deadline = time.monotonic() + 120
    while not (directory / (name + ".json")).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("missing rollout signal: " + name)
        time.sleep(0.05)


def read_sample(fd, size, expected_digest):
    body = bytearray()
    try:
        while len(body) <= size:
            chunk = os.pread(fd, min(65536, size + 1 - len(body)), len(body))
            if not chunk:
                break
            body.extend(chunk)
    except OSError as error:
        return {"errno": error.errno}
    digest = sha256(body).hexdigest()
    return {"bytes": len(body), "sha256": digest,
            "valid": len(body) == size and digest == expected_digest}


def write_sample(path, payload):
    try:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        return {"errno": error.errno, "acknowledged": False}
    return {"bytes": len(payload), "sha256": sha256(payload).hexdigest(), "acknowledged": True}


def main():
    control, seed, name, size, digest, payload = sys.argv[1:]
    control, seed = Path(control), Path(seed)
    size = int(size)
    with seed.open("rb", buffering=0) as stream:
        fd = stream.fileno()
        emit(control, "opened", {"pid": os.getpid(), "fd": fd, "sample": read_sample(fd, size, digest)})
        await_signal(control, "probe")
        emit(control, "issued", {"fd": fd})
        outcome = write_sample(seed.with_name(name), bytes.fromhex(payload))
        emit(control, "during", {"read": read_sample(fd, size, digest), "write": outcome})
        await_signal(control, "resume")
        deadline = time.monotonic() + 25
        transient = []
        while True:
            sample = read_sample(fd, size, digest)
            if "errno" not in sample or time.monotonic() >= deadline:
                break
            transient.append(sample["errno"])
            time.sleep(0.1)
        emit(control, "recovered", {"pid": os.getpid(), "fd": fd, "sample": sample,
                                    "transient_errno": transient})


if __name__ == "__main__":
    main()

"""Read and write administrator credentials without silent permission widening."""

import os
from pathlib import Path
import stat

from .errors import InvalidInput


def read_token(path: Path) -> str:
    metadata = path.stat()
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o077:
        raise InvalidInput(
            f"administrator Key file {path} must have mode 0600 or stricter"
        )
    token = path.read_text(encoding="utf-8").strip()
    _validate_secret(token, "administrator API Key")
    return token


def write_secret(path: Path, secret: str) -> None:
    _validate_secret(secret, "API Key")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(secret)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _validate_secret(secret: str, kind: str) -> None:
    if not 32 <= len(secret) <= 512 or any(
        ord(character) < 33 or ord(character) == 127 for character in secret
    ):
        raise InvalidInput(f"{kind} must contain 32..512 non-whitespace characters")

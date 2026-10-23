"""Resolve host executables before constructing subprocess argument lists."""

import os
import shutil


def executable(name):
    """Keep the executable name intact for symlink-based multicall programs."""
    path = shutil.which(name)
    if path is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(path)

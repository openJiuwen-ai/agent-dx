"""Locate the current container's limits in private or host cgroup v2 views."""
from pathlib import PurePosixPath


def v2_directory(root, membership):
    if (root / 'cpu.max').is_file() and (root / 'memory.max').is_file():
        return root
    for line in membership.splitlines():
        if line.startswith('0::/'):
            path = PurePosixPath(line[3:])
            if '..' in path.parts:
                raise ValueError('cgroup membership escapes the visible hierarchy')
            directory = root.joinpath(*path.parts[1:])
            if (directory / 'cpu.max').is_file() and (directory / 'memory.max').is_file():
                return directory
    raise ValueError('current container cgroup v2 limits are not visible')

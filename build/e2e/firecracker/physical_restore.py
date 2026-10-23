"""Read external sandboxd identities without assuming its public type was renamed."""

import re


def restores(text, environment_id):
    backends = set()
    identity = re.compile(r'(?<![\w-])' + re.escape(environment_id) + r'(?![\w-])')
    for line in text.splitlines():
        match = re.search(r'Type:create (?:EnvironmentID|SandboxID):(sbox-[\w-]+)', line)
        if match and identity.search(line):
            backends.add(match[1])
    return [{'backend': match[1], 'pid': int(match[2])}
            for match in re.finditer(r'firecracker: restored (?:environment|sandbox) (sbox-[\w-]+) pid=(\d+)', text)
            if match[1] in backends]


def serialized_restores(text, environment_id, traces):
    """Prove a fenced attempt exited before another VMM was executed."""
    rows = []
    for restored in restores(text, environment_id):
        trace = traces.get(restored['pid'], '')
        starts = re.findall(r'(?m)^(\d+\.\d+) execve\("[^"\n]*/firecracker".*= 0$', trace)
        exits = re.findall(r'(?m)^(\d+\.\d+) \+\+\+ (?:exited with|killed by).*\+\+\+$', trace)
        if len(starts) != 1:
            raise ValueError('missing unique Firecracker exec witness')
        rows.append({**restored, 'exec_at': float(starts[0]),
                     'exit_at': float(exits[0]) if exits else None})
    if not rows:
        raise ValueError('missing recovery restore')
    for old, new in zip(rows, rows[1:]):
        if old['exit_at'] is None or old['exit_at'] > new['exec_at']:
            raise ValueError('replacement restored before old VMM exited')
    return rows

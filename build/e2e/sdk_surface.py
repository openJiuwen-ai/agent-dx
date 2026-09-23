"""Machine-readable ownership of the public Sandbox SDK E2E surface."""

SURFACE = {
    'Sandbox': {
        'create': 'firecracker', 'get_snapshot': 'firecracker',
        'list_snapshots': 'firecracker', 'delete_snapshot': 'firecracker',
        'from_id': 'standalone', 'files': 'standalone', 'commands': 'standalone',
        'shells': 'standalone', 'pty': 'standalone', 'id': 'standalone',
        'sandbox_id': 'standalone', 'get_port_url': 'standalone',
        'get_port_auth_headers': 'standalone', 'get_tunnel_url': 'standalone',
        'wait_entrypoint': 'firecracker', 'entrypoint_exit_info': 'firecracker',
        'is_running': 'standalone', 'get_info': 'standalone',
        'create_snapshot': 'firecracker', 'pause': 'firecracker',
        'resume': 'firecracker', 'reload': 'firecracker',
        'update_network_policy': 'firecracker', 'close': 'standalone',
        'kill': 'standalone', 'delete': 'standalone',
    },
    'CommandHandle': {
        name: 'standalone' for name in (
            'id', 'sandbox_id', 'poll', 'wait', 'wait_async', 'kill',
            'send_stdin', 'close_stdin'
        )
    },
    'Commands': {
        name: 'standalone' for name in (
            'run', 'get', 'list', 'kill', 'send_stdin', 'close_stdin'
        )
    },
    'Filesystem': {
        name: 'standalone' for name in (
            'read', 'write', 'list', 'exists', 'remove', 'rename', 'make_dir',
            'get_info', 'copy_from_local', 'copy_to_local'
        )
    },
    'PtySession': {
        name: 'standalone' for name in (
            'session_id', 'exit_code', 'done', 'send_stdin', 'close_stdin',
            'resize', 'wait', 'close'
        )
    },
    'Pty': {'create': 'standalone'},
    'Shells': {'create': 'standalone', 'close': 'standalone'},
    'Shell': {
        name: 'standalone' for name in ('session_id', 'run', 'kill', 'close')
    },
    '<module>': {'resources': 'standalone'},
}

# Every supported public operation has at least one real end-to-end case owner.
# These IDs are emitted by the Standalone or Firecracker acceptance drivers and
# make the surface audit independently reviewable from the ten scenario groups.
OPERATION_CASES = {
    'Sandbox.create': ('snapshot clones inherit geometry and preserve process memory',),
    'Sandbox.get_snapshot': ('snapshot remains queryable after source deletion',),
    'Sandbox.list_snapshots': ('snapshot remains queryable after source deletion',),
    'Sandbox.delete_snapshot': ('snapshot deletion accepted for artifact collection',),
    'Sandbox.from_id': ('sandbox.create-query-reattach',),
    'Sandbox.files': ('filesystem.text-crud',),
    'Sandbox.commands': ('command.stdin-handle-recovery',),
    'Sandbox.shells': ('shell.state-env-cwd-and-exit',),
    'Sandbox.pty': ('pty.output-resize',),
    'Sandbox.id': ('sandbox.create-query-reattach',),
    'Sandbox.sandbox_id': ('sandbox.create-query-reattach',),
    'Sandbox.get_port_url': ('port-forward.tls-token-route', 'port-forward.per-instance-tls-route'),
    'Sandbox.get_port_auth_headers': ('port-forward.tls-token-route',),
    'Sandbox.get_tunnel_url': ('reverse-tunnel.sdk-upstream-roundtrip',),
    'Sandbox.wait_entrypoint': ('inherited image entrypoint reports structured exit status',),
    'Sandbox.entrypoint_exit_info': ('inherited image entrypoint reports structured exit status',),
    'Sandbox.is_running': ('lifecycle.context-manager-deletes',),
    'Sandbox.get_info': ('sandbox.create-query-reattach',),
    'Sandbox.create_snapshot': ('reusable snapshot preserves running source',),
    'Sandbox.pause': ('pause with persisted recovery point',),
    'Sandbox.resume': ('resume preserves process memory, PID and binary file',),
    'Sandbox.reload': ('reload restores the latest recovery point without a cold start',),
    'Sandbox.update_network_policy': ('runtime network policy replacement blocks egress and preserves EXECD control',),
    'Sandbox.close': ('lifecycle.attached-close-preserves-instance',),
    'Sandbox.kill': ('lifecycle.attached-close-preserves-instance',),
    'Sandbox.delete': ('lifecycle.detached-close-reattach-delete',),
    'CommandHandle.id': ('command.stdin-handle-recovery',),
    'CommandHandle.sandbox_id': ('command.stdin-handle-recovery',),
    'CommandHandle.poll': ('command.collection-stdin-async-wait',),
    'CommandHandle.wait': ('command.stdin-handle-recovery',),
    'CommandHandle.wait_async': ('command.collection-stdin-async-wait',),
    'CommandHandle.kill': ('command.not-found-and-wait-timeout',),
    'CommandHandle.send_stdin': ('command.stdin-handle-recovery',),
    'CommandHandle.close_stdin': ('command.stdin-handle-recovery',),
    'Commands.run': ('command.idempotent-replay-and-conflict',),
    'Commands.get': ('command.stdin-handle-recovery',),
    'Commands.list': ('command.stdin-handle-recovery',),
    'Commands.kill': ('command.collection-kill',),
    'Commands.send_stdin': ('command.collection-stdin-async-wait',),
    'Commands.close_stdin': ('command.collection-stdin-async-wait',),
    'Filesystem.read': ('filesystem.binary-read-write',),
    'Filesystem.write': ('filesystem.binary-read-write',),
    'Filesystem.list': ('filesystem.directory-copy-and-depth',),
    'Filesystem.exists': ('filesystem.text-crud',),
    'Filesystem.remove': ('filesystem.text-crud',),
    'Filesystem.rename': ('filesystem.text-crud',),
    'Filesystem.make_dir': ('filesystem.text-crud',),
    'Filesystem.get_info': ('filesystem.text-crud',),
    'Filesystem.copy_from_local': ('filesystem.directory-copy-and-depth',),
    'Filesystem.copy_to_local': ('filesystem.directory-copy-and-depth',),
    'PtySession.session_id': ('pty.stdin-eof-and-session-state',),
    'PtySession.exit_code': ('pty.output-resize',),
    'PtySession.done': ('pty.output-resize',),
    'PtySession.send_stdin': ('pty.stdin-eof-and-session-state',),
    'PtySession.close_stdin': ('pty.stdin-eof-and-session-state',),
    'PtySession.resize': ('pty.output-resize',),
    'PtySession.wait': ('pty.output-resize',),
    'PtySession.close': ('pty.output-resize',),
    'Pty.create': ('pty.output-resize',),
    'Shells.create': ('shell.state-env-cwd-and-exit',),
    'Shells.close': ('shell.terminal-exit-does-not-hang',),
    'Shell.session_id': ('shell.state-env-cwd-and-exit',),
    'Shell.run': ('shell.state-env-cwd-and-exit',),
    'Shell.kill': ('shell.state-env-cwd-and-exit',),
    'Shell.close': ('shell.terminal-exit-does-not-hang',),
    '<module>.resources': ('resources.current-capacity',),
}


def counts():
    values = [status for members in SURFACE.values() for status in members.values()]
    return {
        'public_operations': len(values),
        'standalone': values.count('standalone'),
        'firecracker': values.count('firecracker'),
        'unsupported': values.count('unsupported'),
    }


def supported_operations():
    return {
        f'{owner}.{name}'
        for owner, members in SURFACE.items()
        for name, status in members.items()
        if status != 'unsupported'
    }

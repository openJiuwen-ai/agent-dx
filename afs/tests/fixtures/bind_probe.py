"""Diagnose native bind syscalls inside an existing test-owned mount namespace."""

import ctypes
import errno
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import tempfile


class MountAttr(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in ('attr_set', 'attr_clr', 'propagation', 'userns_fd')]


class Native:
    def __init__(self):
        if platform.system() != 'Linux' or platform.machine() not in ('x86_64', 'aarch64'):
            raise ValueError('native probe requires a supported Linux architecture')
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.syscall.restype = ctypes.c_long

    def syscall(self, number, *args):
        value = self.libc.syscall(ctypes.c_long(number), *args)
        if value < 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
        return value

    def clone(self, source):
        # Match bind_mount.rs: OPEN_TREE_CLONE | O_CLOEXEC | AT_EMPTY_PATH.
        return self.syscall(428, ctypes.c_int(source), ctypes.c_char_p(b''),
                            ctypes.c_uint(1 | os.O_CLOEXEC | 0x1000))

    def policy(self, tree, private):
        # Match the product's NOSUID|NODEV and MS_PRIVATE policy exactly.
        attr = MountAttr(0x2 | 0x4, 0, (1 << 18) if private else 0, 0)
        self.syscall(442, ctypes.c_int(tree), ctypes.c_char_p(b''), ctypes.c_uint(0x1000),
                     ctypes.byref(attr), ctypes.c_size_t(ctypes.sizeof(attr)))

    def attach(self, tree, target):
        self.syscall(429, ctypes.c_int(tree), ctypes.c_char_p(b''), ctypes.c_int(target),
                     ctypes.c_char_p(b''), ctypes.c_uint(0x4 | 0x40))

    def close_tree(self, tree):
        os.close(tree)

    def detach(self, target):
        if self.libc.umount2(ctypes.c_char_p(os.fsencode(target)), ctypes.c_int(0)) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))

    def legacy_mount(self, source, target, flags):
        source_path = None if source is None else os.fsencode('/proc/thread-self/fd/' + str(source))
        target_path = os.fsencode(target)
        if self.libc.mount(ctypes.c_char_p(source_path), ctypes.c_char_p(target_path), None,
                           ctypes.c_ulong(flags), None) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))

    def legacy_bind(self, source, target):
        self.legacy_mount(source, target, 4096)

    def legacy_private(self, target):
        self.legacy_mount(None, target, 1 << 18)

    def legacy_policy(self, target, bind_remount=False):
        # The product currently requests MS_REMOUNT|NOSUID|NODEV, without MS_BIND.
        self.legacy_mount(None, target, 32 | 2 | 4 | (4096 if bind_remount else 0))


def failure(step, error):
    return {'step': step, 'status': 'ERROR', 'errno': error.errno, 'detail': str(error)}


def probe(root, token, operations=None):
    root = Path(root)
    marker = root / '.afs-test-owner'
    if root.is_symlink() or marker.is_symlink() or not marker.is_file() or marker.read_text() != token:
        raise ValueError('native bind probe requires the exact test-owned root/token')
    ops = operations or Native()
    directory = Path(tempfile.mkdtemp(prefix='.bind-probe-', dir=root))
    source, target = directory / 'source', directory / 'target'
    source.mkdir()
    target.mkdir()
    result = {
        'scope': 'native bind environment diagnosis, not product acceptance',
        'kernel': platform.release(), 'machine': platform.machine(), 'uid': os.geteuid(),
        'probe_directory': str(directory), 'steps': [], 'status': 'FAILED',
    }
    fds = []
    tree = None
    attached = False
    step = 'open_source_target'
    try:
        flags = getattr(os, 'O_PATH', os.O_RDONLY) | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
        for path in (source, target, directory):
            fds.append(os.open(path, flags))
        step = 'open_tree'
        tree = ops.clone(fds[0])
        result['steps'].append({'step': step, 'status': 'OK'})
        step = 'mount_setattr_private'
        ops.policy(tree, True)
        result['steps'].append({'step': step, 'status': 'OK'})
        step = 'move_mount'
        ops.attach(tree, fds[1])
        attached = True
        result['steps'].append({'step': step, 'status': 'OK'})
        result['status'] = 'CAPABLE'
    except OSError as error:
        result['first_error'] = failure(step, error)
        result['steps'].append(result['first_error'])
        if step == 'mount_setattr_private':
            # A control isolates the propagation flag; it cannot turn the failed product policy into PASS.
            try:
                ops.policy(tree, False)
                result['control_without_private'] = {'status': 'OK'}
            except OSError as control_error:
                result['control_without_private'] = failure('mount_setattr_no_private', control_error)
        if step == 'open_tree' and error.errno == errno.ENOSYS:
            # Match target_proc_path(): resolve the target beneath a held parent fd.
            legacy = {'status': 'FAILED', 'steps': []}
            result['legacy'] = legacy
            target_path = '/proc/thread-self/fd/' + str(fds[2]) + '/target'
            legacy_step = 'mount_bind'
            try:
                ops.legacy_bind(fds[0], target_path)
                attached = True
                legacy['steps'].append({'step': legacy_step, 'status': 'OK'})
                legacy_step = 'make_private'
                ops.legacy_private(target_path)
                legacy['steps'].append({'step': legacy_step, 'status': 'OK'})
                legacy_step = 'remount_without_bind'
                ops.legacy_policy(target_path)
                legacy['steps'].append({'step': legacy_step, 'status': 'OK'})
                legacy['status'] = 'CAPABLE'
            except OSError as legacy_error:
                legacy['first_error'] = failure(legacy_step, legacy_error)
                if attached and legacy_step == 'remount_without_bind':
                    # Control only: distinguish per-mount policy from filesystem remount.
                    try:
                        ops.legacy_policy(target_path, True)
                        legacy['control_bind_remount'] = {'status': 'OK'}
                    except OSError as control_error:
                        legacy['control_bind_remount'] = failure('remount_with_bind', control_error)
    finally:
        errors = []
        if tree is not None:
            try:
                ops.close_tree(tree)
            except OSError as error:
                errors.append(failure('close_tree', error))
        for fd in reversed(fds):
            try:
                os.close(fd)
            except OSError as error:
                errors.append(failure('close_path', error))
        if attached:
            try:
                ops.detach(target)
            except OSError as error:
                errors.append(failure('normal_unmount', error))
        if not errors:
            shutil.rmtree(directory)
        else:
            result['status'] = 'CLEANUP_FAILED'
        result['cleanup'] = {'status': 'FAIL' if errors else 'PASS', 'errors': errors}
    return result


if __name__ == '__main__':
    report = probe(sys.argv[1], sys.argv[2])
    print(json.dumps(report), flush=True)
    raise SystemExit(1 if report['cleanup']['status'] != 'PASS' else 0)

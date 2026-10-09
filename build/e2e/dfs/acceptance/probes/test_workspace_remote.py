import errno
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import workspace_remote as probe


class WorkspaceRemoteTests(unittest.TestCase):
    def test_both_sizes_fresh_open_and_growth(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'file'
            for size, byte in [(4096, 97), (65536, 98), (4096, 99)]:
                probe.operate('write', path, size, byte)
                self.assertTrue(probe.operate('read', path, size, byte)['eof'])

    def test_content_and_extra_bytes_are_failures(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'file'
            probe.operate('write', path)
            with self.assertRaises(ValueError):
                probe.operate('read', path, byte=98)
            with path.open('ab') as stream:
                stream.write(b'x')
            with self.assertRaises(ValueError):
                probe.operate('read', path)

    def test_errno_is_exact_and_success_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'file'
            self.assertEqual(probe.operate('open-error', path, expected_errno=errno.ENOENT)['errno'], 2)
            probe.operate('write', path)
            self.assertEqual(probe.operate('open-error', path, expected_errno=errno.EEXIST)['errno'], 17)
            with self.assertRaises(ValueError):
                probe.operate('open-error', path, expected_errno=errno.ENOENT)
            with patch.object(probe.os, 'open', side_effect=OSError(errno.EIO, 'I/O')):
                with self.assertRaises(OSError):
                    probe.operate('open-error', path, expected_errno=errno.EACCES)

    def test_short_write_is_fully_written(self):
        original = probe.os.write
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'file'
            with patch.object(probe.os, 'write', side_effect=lambda fd, data: original(fd, data[:127])):
                probe.operate('write', path)
            probe.operate('read', path)

    def test_write_refusal_uses_write_flags_and_preserves_unexpected_error(self):
        for code in (errno.EACCES, errno.EIO):
            with patch.object(probe.os, 'open', side_effect=OSError(code, 'refused')) as opened:
                if code == errno.EACCES:
                    self.assertEqual(probe.operate('write-open-error', '/file', expected_errno=code)['errno'], code)
                else:
                    with self.assertRaises(OSError):
                        probe.operate('write-open-error', '/file', expected_errno=errno.EACCES)
                self.assertEqual(opened.call_args.args[1], probe.os.O_WRONLY)

    def test_rename_delete_and_permission_transitions(self):
        with tempfile.TemporaryDirectory() as root:
            path, other = Path(root) / 'file', Path(root) / 'other'
            probe.operate('write', path)
            for mode in (0, 0o600):
                self.assertEqual(probe.operate('chmod', path, mode=mode)['mode'], mode)
            probe.operate('rename', path, destination=other)
            probe.operate('read', other)
            self.assertFalse(path.exists())
            probe.operate('unlink', other)
            self.assertFalse(other.exists())


if __name__ == '__main__':
    unittest.main()

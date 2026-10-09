import errno
import importlib.util
from pathlib import Path
import platform
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("workspace_host", Path(__file__).with_name("workspace_host.py"))
workspace_host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workspace_host)


def counters(**values):
    base = {name: 0 for name in ["read", "write", "create", "unlink", "mkdir", "rmdir"]}
    base.update(values)
    return base


class WorkspaceHostGuards(unittest.TestCase):
    def test_verify_window_accepts_native_bypass_and_fuse_positive_control(self):
        before = counters()
        native = workspace_host.verify_window(before, dict(before), native=True)
        self.assertEqual({name: native[name] for name in workspace_host.SELECTED_CALLBACKS}, before)
        after = counters(read=1, write=1, create=1, unlink=1, mkdir=1, rmdir=1, lookup=4, getattr=7)
        fuse = workspace_host.verify_window({**before, "lookup": 1, "getattr": 2}, after, native=False)
        self.assertEqual(fuse["lookup"], 3)
        self.assertEqual(fuse["getattr"], 5)

    def test_verify_window_rejects_missing_rollback_leak_and_incomplete_positive(self):
        before = counters()
        with self.assertRaisesRegex(ValueError, "not observed"):
            workspace_host.verify_window({"read": 0, "write": 0}, {"read": 0, "write": 0}, native=True)
        with self.assertRaisesRegex(ValueError, "invalid callback count"):
            workspace_host.verify_window(before, counters(read=-1), native=True)
        with self.assertRaisesRegex(ValueError, "entered FUSE"):
            workspace_host.verify_window(before, counters(read=1), native=True)
        with self.assertRaisesRegex(ValueError, "missing callbacks"):
            workspace_host.verify_window(before, counters(read=1, write=1, create=1, unlink=1, mkdir=1), native=False)

    def test_permission_denial_does_not_stat_an_inaccessible_parent(self):
        denied = PermissionError(errno.EACCES, "permission denied")
        with mock.patch.object(workspace_host.os, "open", side_effect=denied), mock.patch.object(
                Path, "exists", side_effect=denied) as exists:
            value = workspace_host.do_deny(Path("/private/denied-sentinel"))
        self.assertEqual(value["errno"], errno.EACCES)
        self.assertFalse(value["created"])
        exists.assert_not_called()

    def test_payload_contract_is_fixed(self):
        self.assertEqual(len(workspace_host.PAYLOAD), 65536)
        self.assertEqual(workspace_host.PAYLOAD[:258], bytes(range(256)) + b"\x00\x01")

    def test_cli_rejects_non_linux_before_payload_work(self):
        with mock.patch.object(platform, "system", return_value="Darwin"):
            self.assertEqual(workspace_host.main(["write", "/tmp/host-case", "501", "501"]), 1)


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest
from unittest.mock import MagicMock

import pytest

from adx_environment._transport import EnvironmentHTTPError
from adx_environment.filesystem import Filesystem


class _Client:
    def __init__(self):
        self.calls = 0
        self.direct_enabled = True

    def download_bytes_direct(self, *_args, **_kwargs):
        self.calls += 1
        return b"hello"


class _RpcClient:
    direct_enabled = False

    def __init__(self, data=b"hello"):
        self.data = bytearray(data)
        self.actions = []
        self.write_sizes = []

    def invoke(self, _environment_id, action, args):
        self.actions.append(action)
        if action == "file.read_chunk":
            offset = args["offset"]
            limit = args["limit"]
            end = offset + limit
            chunk = bytes(self.data[offset:end])
            return {
                "path": args["path"],
                "offset": offset,
                "data": chunk.hex(),
                "bytes_read": len(chunk),
                "eof": offset + len(chunk) >= len(self.data),
                "error": None,
            }
        if action == "file.write_chunk":
            offset = args["offset"]
            chunk = bytes.fromhex(args["data"])
            self.write_sizes.append(len(chunk))
            if offset == 0:
                self.data.clear()
            if len(self.data) < offset:
                self.data.extend(b"\0" * (offset - len(self.data)))
            end = offset + len(chunk)
            self.data[offset:end] = chunk
            return {"bytes_written": len(chunk), "error": None}
        if action == "file.stat":
            return {
                "name": os.path.basename(args["path"]),
                "path": args["path"],
                "type": "file",
                "size": len(self.data),
                "permissions": "600",
                "modified_time": 0.0,
                "error": None,
            }
        raise AssertionError(action)


class FilesystemTests(unittest.TestCase):
    def test_read_rejects_unknown_format_before_network(self):
        client = _Client()
        filesystem = Filesystem(client, "environment-1")
        with self.assertRaisesRegex(ValueError, "text.*bytes"):
            filesystem.read("/tmp/data", format="json")
        self.assertEqual(client.calls, 0)

    def test_read_preserves_text_and_bytes_boundaries(self):
        client = _Client()
        filesystem = Filesystem(client, "environment-1")
        self.assertEqual(filesystem.read("/tmp/data"), "hello")
        self.assertEqual(filesystem.read("/tmp/data", format="bytes"), b"hello")

    def test_read_and_write_use_runtime_rpc_when_direct_is_disabled(self):
        client = _RpcClient()
        filesystem = Filesystem(client, "environment-1")

        self.assertEqual(filesystem.read("/tmp/data", format="bytes"), b"hello")
        info = filesystem.write("/tmp/data", b"updated")

        self.assertEqual(bytes(client.data), b"updated")
        self.assertEqual(info.size, 7)
        self.assertIn("file.read_chunk", client.actions)
        self.assertIn("file.write_chunk", client.actions)

    def test_runtime_rpc_write_splits_large_payloads_into_bounded_chunks(self):
        client = _RpcClient()
        filesystem = Filesystem(client, "environment-1")
        chunk_size = 1024 * 1024
        payload = b"x" * (chunk_size * 2 + 1)

        info = filesystem.write("/tmp/data", payload)

        self.assertEqual(bytes(client.data), payload)
        self.assertEqual(info.size, len(payload))
        self.assertEqual(client.write_sizes, [chunk_size, chunk_size, 1])

    def test_file_copy_uses_runtime_rpc_when_direct_is_disabled(self):
        client = _RpcClient(b"")
        filesystem = Filesystem(client, "environment-1")
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "source.bin")
            target = os.path.join(directory, "target.bin")
            with open(source, "wb") as stream:
                stream.write(b"rpc-copy")

            filesystem.copy_from_local(source, "/tmp/remote.bin")
            self.assertEqual(bytes(client.data), b"rpc-copy")

            filesystem.copy_to_local("/tmp/remote.bin", target)
            with open(target, "rb") as stream:
                self.assertEqual(stream.read(), b"rpc-copy")


if __name__ == "__main__":
    unittest.main()


@pytest.mark.parametrize(
    "code,exception",
    [
        ("FILE_NOT_FOUND", FileNotFoundError),
        ("NOT_A_DIRECTORY", NotADirectoryError),
        ("PERMISSION_DENIED", PermissionError),
    ],
)
def test_list_preserves_typed_runtime_error(code, exception):
    client = MagicMock()
    error = EnvironmentHTTPError(400, {"error_code": code, "error": "detail"}, "detail")
    client.invoke.side_effect = error
    with pytest.raises(exception, match="/requested"):
        Filesystem(client, "env").list("/requested")


def test_list_does_not_reclassify_unknown_http_errors():
    client = MagicMock()
    error = EnvironmentHTTPError(400, {"error": "invalid request"}, "invalid request")
    client.invoke.side_effect = error
    with pytest.raises(EnvironmentHTTPError):
        Filesystem(client, "env").list("/requested")


@pytest.mark.parametrize("path", [None, "", 7, True, "/tmp/bad\0path"])
def test_mkdir_and_list_reject_invalid_paths_before_network(path):
    client = MagicMock()
    filesystem = Filesystem(client, "env")
    for operation in (filesystem.make_dir, filesystem.list):
        with pytest.raises(ValueError, match="path"):
            operation(path)
    client.invoke.assert_not_called()


@pytest.mark.parametrize("depth", [0, -1, 65, 100, "1", "abc", True, 1.0, None])
def test_list_rejects_invalid_depth_before_network(depth):
    client = MagicMock()
    with pytest.raises(ValueError, match="depth"):
        Filesystem(client, "env").list("/work", depth=depth)
    client.invoke.assert_not_called()


def test_mkdir_default_keeps_legacy_request_and_nonrecursive_is_forwarded():
    client = MagicMock()
    client.invoke.side_effect = [
        {"created": True, "error": None},
        {"created": False, "error": None},
    ]
    filesystem = Filesystem(client, "env")
    assert filesystem.make_dir("/work/a/b") is True
    assert filesystem.make_dir("/work/a/b", recursive=False) is False
    assert client.invoke.call_args_list[0].args == (
        "env",
        "file.make_dir",
        {"path": "/work/a/b"},
    )
    assert client.invoke.call_args_list[1].args == (
        "env",
        "file.make_dir",
        {"path": "/work/a/b", "recursive": False},
    )
    assert client.invoke.call_count == 2


@pytest.mark.parametrize("recursive", [None, "false", "aaa", 0])
def test_mkdir_rejects_invalid_recursive_before_network(recursive):
    client = MagicMock()
    with pytest.raises(ValueError, match="recursive"):
        Filesystem(client, "env").make_dir("/work/a/b", recursive=recursive)
    client.invoke.assert_not_called()


@pytest.mark.parametrize("status", [400, 403, 500])
def test_nonrecursive_mkdir_propagates_runtime_errors_without_retry(status):
    client = MagicMock()
    error = EnvironmentHTTPError(status, {"error": "runtime failure"}, "runtime failure")
    client.invoke.side_effect = error
    with pytest.raises(EnvironmentHTTPError):
        Filesystem(client, "env").make_dir("/work/a/b", recursive=False)
    client.invoke.assert_called_once_with(
        "env", "file.make_dir", {"path": "/work/a/b", "recursive": False}
    )


def test_native_search_uses_one_remote_search_and_maps_entries():
    client = MagicMock()
    client.invoke.return_value = {
        "entries": [
            {
                "name": "a.py",
                "path": "/tree/a.py",
                "type": "file",
                "size": 2,
                "permissions": "644",
                "modified_time": 0.0,
            }
        ],
        "error": None,
    }
    items = Filesystem(client, "env").search(
        "/tree", "*.py", exclude_patterns=["test_*"], depth=3
    )
    assert len(items) == 1 and items[0].path == "/tree/a.py"
    client.invoke.assert_called_once_with(
        "env",
        "file.search",
        {
            "path": "/tree",
            "pattern": "*.py",
            "exclude_patterns": ["test_*"],
            "depth": 3,
            "max_results": 1000,
            "max_entries": 100000,
            "timeout_ms": 10000,
        },
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"depth": 0},
        {"max_results": True},
        {"max_results": 10001},
        {"max_entries": 0},
        {"timeout_ms": 30001},
        {"exclude_patterns": "*.tmp"},
        {"exclude_patterns": [1]},
        {"pattern": None},
    ],
)
def test_native_search_rejects_invalid_parameters_before_network(kwargs):
    client = MagicMock()
    with pytest.raises((ValueError, TypeError)):
        Filesystem(client, "env").search("/tree", **kwargs)
    client.invoke.assert_not_called()


def test_native_search_does_not_hide_runtime_errors():
    client = MagicMock()
    client.invoke.side_effect = EnvironmentHTTPError(
        429, {"error_code": "RESOURCE_EXHAUSTED"}, "search max_results exceeded"
    )
    with pytest.raises(EnvironmentHTTPError, match="max_results"):
        Filesystem(client, "env").search("/tree")

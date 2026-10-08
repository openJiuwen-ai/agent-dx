import os
import tempfile
import unittest

from adx_sandbox.filesystem import Filesystem


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

    def invoke(self, _sandbox_id, action, args):
        self.actions.append(action)
        if action == "file.read_chunk":
            offset = args["offset"]
            limit = args["limit"]
            chunk = bytes(self.data[offset : offset + limit])
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
            self.data[offset : offset + len(chunk)] = chunk
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
        filesystem = Filesystem(client, "sandbox-1")
        with self.assertRaisesRegex(ValueError, "text.*bytes"):
            filesystem.read("/tmp/data", format="json")
        self.assertEqual(client.calls, 0)

    def test_read_preserves_text_and_bytes_boundaries(self):
        client = _Client()
        filesystem = Filesystem(client, "sandbox-1")
        self.assertEqual(filesystem.read("/tmp/data"), "hello")
        self.assertEqual(filesystem.read("/tmp/data", format="bytes"), b"hello")

    def test_read_and_write_use_runtime_rpc_when_direct_is_disabled(self):
        client = _RpcClient()
        filesystem = Filesystem(client, "sandbox-1")

        self.assertEqual(filesystem.read("/tmp/data", format="bytes"), b"hello")
        info = filesystem.write("/tmp/data", b"updated")

        self.assertEqual(bytes(client.data), b"updated")
        self.assertEqual(info.size, 7)
        self.assertIn("file.read_chunk", client.actions)
        self.assertIn("file.write_chunk", client.actions)

    def test_runtime_rpc_write_splits_large_payloads_into_bounded_chunks(self):
        client = _RpcClient()
        filesystem = Filesystem(client, "sandbox-1")
        chunk_size = 1024 * 1024
        payload = b"x" * (chunk_size * 2 + 1)

        info = filesystem.write("/tmp/data", payload)

        self.assertEqual(bytes(client.data), payload)
        self.assertEqual(info.size, len(payload))
        self.assertEqual(client.write_sizes, [chunk_size, chunk_size, 1])

    def test_file_copy_uses_runtime_rpc_when_direct_is_disabled(self):
        client = _RpcClient(b"")
        filesystem = Filesystem(client, "sandbox-1")
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

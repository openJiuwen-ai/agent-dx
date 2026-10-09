"""Linux-only socket protocol regressions for the administrator client."""
import importlib.util
import json
from pathlib import Path
import platform
import socket
import tempfile
import threading
import unittest

spec = importlib.util.spec_from_file_location('native_client', Path(__file__).with_name('native-workspace-control.py'))
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


@unittest.skipUnless(platform.system() == 'Linux', 'authoritative environment is Linux')
class ControlProtocolTests(unittest.TestCase):
    def test_fragmented_failure_response_preserves_error_and_request(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'control.sock'
            received = []
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(path))
            server.listen(1)
            def serve():
                with server, server.accept()[0] as connection:
                    with connection.makefile('rb') as stream:
                        received.append(json.loads(stream.readline()))
                    connection.sendall(b'{"status":"ERROR",')
                    connection.sendall(b'"error":"cleanup unresolved"}\n')
            thread = threading.Thread(target=serve)
            thread.start()
            request = {'operation': 'stop', 'id': 'recover-1'}
            result = client.exchange(path, request)
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(received, [request])
            self.assertEqual(result, {'status': 'ERROR', 'error': 'cleanup unresolved'})

    def test_incomplete_response_cannot_be_a_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'control.sock'
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(path))
            server.listen(1)
            def serve():
                with server, server.accept()[0] as connection:
                    connection.recv(8192)
                    connection.sendall(b'{"status":"Stopped"}')
            thread = threading.Thread(target=serve)
            thread.start()
            with self.assertRaisesRegex(RuntimeError, 'complete response'):
                client.exchange(path, {'operation': 'stop', 'id': 'recover-2'})
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())

    def test_oversized_request_rejected_before_connection(self):
        with self.assertRaisesRegex(ValueError, 'limit'):
            client.exchange('/nonexistent', {'operation': 'exec', 'argv': ['x' * 8192]})

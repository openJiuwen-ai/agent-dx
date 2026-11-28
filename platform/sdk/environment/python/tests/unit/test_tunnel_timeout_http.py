"""Exercise instance-owned timeouts with real HTTP and WebSocket sockets."""

import asyncio
import json
import os
import threading
import unittest
from unittest.mock import patch
from wsgiref.simple_server import WSGIRequestHandler, make_server

from adx_environment.tunnel_client import TunnelClient
from websockets.asyncio.server import serve


class QuietRequestHandler(WSGIRequestHandler):
    def log_message(self, *_args):
        pass


class TunnelTimeoutHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_instance_timeout_controls_delayed_upstream_response(self):
        for seconds, expected_type in ((0.5, "error"), (5.0, "http_resp")):
            with self.subTest(seconds=seconds):
                await self.check_delayed_response(seconds, expected_type)

    async def check_delayed_response(self, seconds, expected_type):
        started = threading.Event()
        release = threading.Event()

        def application(_environ, start_response):
            started.set()
            release.wait(timeout=10)
            start_response("200 OK", [("Content-Length", "2")])
            return [b"ok"]

        server = make_server("127.0.0.1", 0, application, handler_class=QuietRequestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        result = asyncio.get_running_loop().create_future()

        async def peer(websocket):
            await websocket.send(json.dumps({
                "type": "http_req", "id": "delayed-request", "method": "GET",
                "path": "/", "headers": {}, "body": "",
            }))
            async for message in websocket:
                frame = json.loads(message)
                if frame["type"] in ("http_resp", "error"):
                    result.set_result(frame)
                    break
            await websocket.wait_closed()

        with patch.dict(os.environ, {"ADX_TUNNEL_HTTP_TIMEOUT": "0.01"}):
            client = TunnelClient(f"127.0.0.1:{server.server_port}", http_timeout=seconds)
        # A later process-wide change must not alter an existing TunnelClient.
        with patch.dict(os.environ, {"ADX_TUNNEL_HTTP_TIMEOUT": "0.02"}):
            try:
                async with serve(peer, "127.0.0.1", 0) as websocket_server:
                    port = websocket_server.sockets[0].getsockname()[1]
                    connected = await asyncio.to_thread(client.start, f"ws://127.0.0.1:{port}", 2)
                    self.assertTrue(connected)
                    if expected_type == "http_resp":
                        self.assertTrue(await asyncio.to_thread(started.wait, 3))
                        await asyncio.sleep(0.15)
                        release.set()
                    frame = await asyncio.wait_for(result, timeout=5)
                    release.set()
                    self.assertEqual(frame["type"], expected_type)
                    self.assertEqual(frame["id"], "delayed-request")
                    if expected_type == "http_resp":
                        self.assertEqual(frame["status"], 200)
                    await asyncio.to_thread(client.stop)
            finally:
                release.set()
                await asyncio.to_thread(client.stop)
                await asyncio.to_thread(server.shutdown)
                server.server_close()
                thread.join(timeout=2)

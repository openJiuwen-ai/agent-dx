import asyncio
import base64
import json
import unittest

import websockets
from adx_sandbox.tunnel_client import TunnelClient

_STOP = object()


class _OuterTunnel:
    def __init__(self):
        self._incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self._incoming.get()
        if message is _STOP:
            raise StopAsyncIteration
        return json.dumps(message)

    async def send(self, message):
        await self.outgoing.put(json.loads(message))

    async def feed(self, frame):
        await self._incoming.put(frame)

    async def finish(self):
        await self._incoming.put(_STOP)


class TunnelClientWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_reverse_websocket_text_binary_and_close_round_trip(self):
        seen = {}

        async def echo(websocket):
            seen["path"] = websocket.request.path
            seen["header"] = websocket.request.headers["x-test"]
            seen["origin"] = websocket.request.headers["origin"]
            seen["subprotocol"] = websocket.subprotocol
            async for message in websocket:
                await websocket.send(message)

        server = await websockets.serve(
            echo,
            "127.0.0.1",
            0,
            subprotocols=["chat.v1"],
        )
        port = server.sockets[0].getsockname()[1]
        outer = _OuterTunnel()
        client = TunnelClient(f"http://127.0.0.1:{port}")
        proxy = asyncio.create_task(client._proxy_loop(outer))

        async def next_frame(frame_type):
            try:
                while True:
                    frame = await asyncio.wait_for(outer.outgoing.get(), timeout=1)
                    if frame.get("type") == frame_type:
                        return frame
            except asyncio.TimeoutError:
                self.fail(f"TunnelClient did not send {frame_type}")

        try:
            await outer.feed(
                {
                    "type": "ws_connect",
                    "id": "channel-1",
                    "path": "/chat?room=one",
                    "headers": {
                        "Origin": "https://sandbox.example",
                        "Sec-WebSocket-Protocol": "chat.v1",
                        "X-Test": "forwarded",
                    },
                }
            )
            connected = await next_frame("ws_connected")
            self.assertEqual(connected["id"], "channel-1")
            self.assertEqual(
                seen,
                {
                    "path": "/chat?room=one",
                    "header": "forwarded",
                    "origin": "https://sandbox.example",
                    "subprotocol": "chat.v1",
                },
            )

            await outer.feed(
                {
                    "type": "ws_message",
                    "id": "channel-1",
                    "data": "hello",
                    "binary": False,
                }
            )
            text = await next_frame("ws_message")
            self.assertEqual(
                text,
                {
                    "type": "ws_message",
                    "id": "channel-1",
                    "data": "hello",
                    "binary": False,
                },
            )

            binary_payload = b"\x00\x01\xff"
            await outer.feed(
                {
                    "type": "ws_message",
                    "id": "channel-1",
                    "data": base64.b64encode(binary_payload).decode("ascii"),
                    "binary": True,
                }
            )
            binary = await next_frame("ws_message")
            self.assertTrue(binary["binary"])
            self.assertEqual(base64.b64decode(binary["data"]), binary_payload)

            await outer.feed(
                {
                    "type": "ws_close",
                    "id": "channel-1",
                    "code": 1000,
                    "reason": "done",
                }
            )
            closed = await next_frame("ws_close")
            self.assertEqual(
                closed,
                {
                    "type": "ws_close",
                    "id": "channel-1",
                    "code": 1000,
                    "reason": "done",
                },
            )
        finally:
            await outer.finish()
            await asyncio.wait_for(proxy, timeout=2)
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()

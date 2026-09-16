#!/usr/bin/env python3
"""Real TLS WebSocket acceptance for the installed Sandbox SDK command watcher."""
import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import ssl

async def check(tls):
    from websockets.asyncio.server import serve
    from adx_sandbox._command_watch import _CommandWaitManager
    from adx_sandbox.types import ConnectionConfig
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(tls/'frontend.pem', tls/'frontend.key')
    done = asyncio.get_running_loop().create_future()
    desired = {('instance', 'command')}
    received = []
    async def server(ws):
        assert ws.request.path == '/api/sandbox/v1/commands/watch'
        assert ws.request.headers['Authorization'] == 'Bearer test-key'
        assert ws.request.headers['X-Auth'] == 'test-key'
        await ws.send(json.dumps({'op':'ready','protocolVersion':1,'capabilities':['multiplexed-command-watch']}))
        request = json.loads(await ws.recv())
        assert request == {'op':'subscribe','protocolVersion':1,'commands':[{'sandboxId':'instance','commandId':'command'}]}
        received.append(request)
        await ws.send(json.dumps({'sandboxId':'instance','commandId':'command','status':'SUCCEEDED'}))
        await ws.wait_closed()
    async with serve(server, '127.0.0.1', 0, ssl=server_tls) as listener:
        port = listener.sockets[0].getsockname()[1]
        manager = _CommandWaitManager(ConnectionConfig(server_address=f'localhost:{port}', token='test-key',use_tls=True,verify_tls=True))
        manager._desired = lambda: desired.copy()
        def notify(key, error=None):
            assert key == ('instance','command') and error is None, (key,error)
            desired.clear()
            if not done.done(): done.set_result(True)
        manager._notify = notify
        task = asyncio.create_task(manager._run())
        try:
            await asyncio.wait_for(done, 5)
            assert len(received) == 1
            print('PASS verified TLS command watch: authenticated subscription and terminal event, no polling fallback')
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await task

if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--tls',type=Path,required=True);a=p.parse_args()
    os.environ['SSL_CERT_FILE']=str((a.tls/'ca.pem').resolve())
    asyncio.run(check(a.tls))

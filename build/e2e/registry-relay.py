# Test registry transport only; no platform traffic passes through this relay.
import asyncio
async def handle(r,w):
 try:
  rr,ww=await asyncio.open_connection('coordinator',5000)
  async def copy(a,b):
   try:
    while data:=await a.read(65536):b.write(data);await b.drain()
   finally:b.close()
  await asyncio.gather(copy(r,ww),copy(rr,w))
 except Exception:w.close()
async def main():
 s=await asyncio.start_server(handle,'127.0.0.1',5000)
 async with s:await s.serve_forever()
asyncio.run(main())

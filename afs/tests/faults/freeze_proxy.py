#!/usr/bin/env python3
"""TCP proxy LISTEN -> TARGET that stops relaying while FREEZE exists (connections stay open, bytes stall).
Usage: freeze-proxy.py <listen-port> <target-port> <freeze-file>"""
import os, socket, sys, threading, time
lp, tp, freeze = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
def pump(a, b):
    try:
        while True:
            while os.path.exists(freeze):
                time.sleep(0.05)
            d = a.recv(65536)
            if not d:
                break
            while os.path.exists(freeze):
                time.sleep(0.05)
            b.sendall(d)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try: s.shutdown(socket.SHUT_RDWR)
            except OSError: pass
def serve(c):
    try:
        u = socket.create_connection(("127.0.0.1", tp))
    except OSError:
        c.close(); return
    threading.Thread(target=pump, args=(c, u), daemon=True).start()
    threading.Thread(target=pump, args=(u, c), daemon=True).start()
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", lp)); s.listen(256)
while True:
    c, _ = s.accept()
    threading.Thread(target=serve, args=(c,), daemon=True).start()

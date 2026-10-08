"""Port Forwarding Example

Create a sandbox with an HTTP server exposed via the sandbox router.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.
  - ADX_GATEWAY_ADDRESS should point at the sandbox router entrypoint
    (for example: <traefik-host>:28888). If omitted, the SDK falls back to
    ADX_SERVER_ADDRESS.

Usage:
  export ADX_SERVER_ADDRESS=<frontend-host>:8888
  export ADX_GATEWAY_ADDRESS=<router-host>:28888
  export ADX_TOKEN=<jwt-token>
  python port_forwarding.py
"""

import time
import urllib.error
import urllib.request

from adx_sandbox import Sandbox

PORT = 8080
EXPECTED_BODY = "ROUTER-PF-OK-808"

# EXECD/minimal images do not guarantee python3/nc/node inside the sandbox.
# Perl + Socket is available in the CI images and is enough for a tiny HTTP
# server that validates the fixed Traefik router -> frontend sandboxRouter path:
#   http://<gateway>/<safeID>/<port>
SERVER_CMD = rf'''perl -MSocket -e '$|=1; socket(S,PF_INET,SOCK_STREAM,getprotobyname("tcp")); setsockopt(S,SOL_SOCKET,SO_REUSEADDR,1); bind(S,sockaddr_in({PORT},INADDR_ANY)) or die $!; listen(S,10); while(accept(C,S)){{ print C "HTTP/1.1 200 OK\r\nContent-Length: {len(EXPECTED_BODY)}\r\nConnection: close\r\n\r\n{EXPECTED_BODY}"; close C; }}' '''


def fetch_text(
    url: str, timeout: int = 10, route_attempts: int = 30, retry_delay: float = 1
) -> str:
    for attempt in range(route_attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as exc:
            if exc.code != 404 or attempt + 1 == route_attempts:
                raise
            time.sleep(retry_delay)
    raise RuntimeError("unreachable")


def main():
    with Sandbox(
        cpu=1000,
        memory=2048,
        port_forwardings=[PORT],
    ) as sb:
        print(f"Sandbox created: {sb.id}")

        handle = sb.commands.run(SERVER_CMD, background=True)
        print(f"HTTP server started (pid={handle.pid})")
        time.sleep(2)

        url = sb.get_port_url(PORT)
        print(f"Forwarded-port URL: {url}")

        body = fetch_text(url)
        print(f"Router response: {body}")
        assert body == EXPECTED_BODY, f"unexpected response from router: {body!r}"

    print("Port forwarding through fixed router verified.")


if __name__ == "__main__":
    main()

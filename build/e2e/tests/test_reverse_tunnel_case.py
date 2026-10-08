import http.server
from pathlib import Path
import subprocess
import sys
import threading
import unittest


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(REPOSITORY / 'platform/sdk/sandbox/python'))

from functional_data_plane import (  # noqa: E402
    TUNNEL_BODY,
    _TunnelUpstream,
    _tunnel_fetch_command,
)


class ReverseTunnelCaseTests(unittest.TestCase):
    def test_sandbox_probe_flushes_request_and_reads_complete_response(self):
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), _TunnelUpstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        try:
            command = _tunnel_fetch_command(
                f'http://127.0.0.1:{upstream.server_address[1]}'
            )
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f'{TUNNEL_BODY}:/functional', result.stdout)
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()

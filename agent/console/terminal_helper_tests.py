"""Exercise the delivered helper against a real PTY, without an Execd mock."""
import base64
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import unittest
import pty


class TerminalHelperTests(unittest.TestCase):
    def test_outer_pty_handshake_raw_noecho_and_inner_resize(self):
        master, slave = pty.openpty()
        process = subprocess.Popen([sys.executable, str(Path(__file__).with_name("terminal_helper.py")), "--pty"],
                                   stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        pending = b""

        def receive():
            nonlocal pending
            while b"\n" not in pending:
                self.assertTrue(select.select([master], [], [], 3)[0], "PTY transport handshake/output timeout")
                pending += os.read(master, 32768)
            line, pending = pending.split(b"\n", 1)
            return json.loads(line)

        def send(frame):
            os.write(master, (json.dumps({"v": 1, **frame}) + "\n").encode())

        try:
            self.assertEqual(receive(), {"v": 1, "type": "transport_ready"})
            send({"type": "init", "uid": os.getuid(), "gid": os.getgid(), "groups": os.getgroups(),
                  "home": "/tmp", "cwd": "/tmp", "shell": "/bin/sh", "cols": 80, "rows": 24})
            self.assertEqual(receive()["type"], "ready")
            send({"type": "resize", "cols": 100, "rows": 40})
            send({"type": "input", "data": base64.b64encode(b"stty size; printf WS_PTY_OK; exit\n").decode()})
            output = b""
            while True:
                frame = receive()
                if frame["type"] == "output":
                    output += base64.b64decode(frame["data"])
                if frame["type"] == "exited":
                    break
            self.assertIn(b"40 100", output)
            self.assertIn(b"WS_PTY_OK", output)
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=3)
            os.close(master)

    def test_coalesced_init_and_input_frames_are_not_lost(self):
        helper = Path(__file__).with_name("terminal_helper.py")
        process = subprocess.Popen([sys.executable, str(helper)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            init = {"v": 1, "type": "init", "uid": os.getuid(), "gid": os.getgid(),
                    "groups": os.getgroups(), "home": "/tmp", "cwd": "/tmp", "shell": "/bin/sh",
                    "cols": 80, "rows": 24}
            command = {"v": 1, "type": "input", "data": base64.b64encode(b"printf COALESCED_OK; exit\n").decode()}
            process.stdin.write(json.dumps(init) + "\n" + json.dumps(command) + "\n")
            process.stdin.flush()
            output = b""
            while True:
                self.assertTrue(select.select([process.stdout], [], [], 2)[0], "coalesced input was lost")
                frame = json.loads(process.stdout.readline())
                if frame["type"] == "output":
                    output += base64.b64decode(frame["data"])
                if frame["type"] == "exited":
                    break
            self.assertIn(b"COALESCED_OK", output)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

    @unittest.skipUnless(os.geteuid() == 0, "requires root to verify dropping identity")
    def test_root_drops_to_numeric_business_identity(self):
        helper = Path(__file__).with_name("terminal_helper.py")
        frames = [
            {"v": 1, "type": "init", "uid": 65534, "gid": 65534, "groups": [],
             "home": "/tmp", "cwd": "/tmp", "shell": "/bin/sh", "cols": 80, "rows": 24},
            {"v": 1, "type": "input", "data": base64.b64encode(b"id -u; id -g; pwd; exit\n").decode()},
        ]
        process = subprocess.Popen([sys.executable, str(helper)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            process.stdin.write(json.dumps(frames[0]) + "\n")
            process.stdin.flush()
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            ready = json.loads(process.stdout.readline())
            self.assertEqual(ready["type"], "ready", ready)
            self.assertEqual((ready["uid"], ready["gid"], ready["groups"]), (65534, 65534, []))
            process.stdin.write(json.dumps(frames[1]) + "\n")
            process.stdin.flush()
            output = b""
            while True:
                self.assertTrue(select.select([process.stdout], [], [], 5)[0])
                frame = json.loads(process.stdout.readline())
                if frame["type"] == "output":
                    output += base64.b64decode(frame["data"])
                if frame["type"] == "exited":
                    break
            self.assertIn(b"65534", output)
            self.assertIn(b"/tmp", output)
            self.assertEqual(process.wait(timeout=5), 0)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

    def test_real_pty_identity_cwd_resize_and_exit(self):
        helper = Path(__file__).with_name("terminal_helper.py")
        process = subprocess.Popen([sys.executable, str(helper)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(cleanup)

        def send(frame):
            process.stdin.write(json.dumps({"v": 1, **frame}) + "\n")
            process.stdin.flush()

        def receive():
            self.assertTrue(select.select([process.stdout], [], [], 5)[0], "helper frame timeout")
            line = process.stdout.readline()
            self.assertTrue(line, "helper exited without protocol response")
            return json.loads(line)

        send({"type": "init", "uid": os.getuid(), "gid": os.getgid(),
              "groups": os.getgroups(), "home": "/tmp", "cwd": "/tmp", "shell": "/bin/sh",
              "cols": 80, "rows": 24})
        ready = receive()
        self.assertEqual(ready["type"], "ready")
        self.assertEqual(ready["uid"], os.getuid())
        self.assertEqual(ready["cwd"], "/tmp")
        send({"type": "resize", "cols": 100, "rows": 40})
        send({"type": "input", "data": base64.b64encode(b"stty size; pwd; printf 'PTY_OK\\n'; exit\n").decode()})
        output = b""
        for _ in range(100):
            frame = receive()
            if frame["type"] == "output":
                output += base64.b64decode(frame["data"])
            if frame["type"] == "exited":
                self.assertEqual(frame["exit_code"], 0)
                break
        else:
            self.fail("no exited frame")
        self.assertIn(b"40 100", output)
        self.assertIn(b"/tmp", output)
        self.assertIn(b"PTY_OK", output)
        self.assertEqual(process.wait(timeout=5), 0)

    def test_missing_cwd_never_reports_ready(self):
        helper = Path(__file__).with_name("terminal_helper.py")
        init = {"v": 1, "type": "init", "uid": os.getuid(), "gid": os.getgid(),
                "groups": os.getgroups(), "home": "/tmp", "cwd": "/nonexistent-console-cwd",
                "shell": "/bin/sh", "cols": 80, "rows": 24}
        result = subprocess.run([sys.executable, str(helper)], input=json.dumps(init) + "\n",
                                text=True, capture_output=True, timeout=5)
        frames = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertTrue(frames)
        self.assertEqual(frames[0]["type"], "error")
        self.assertEqual(frames[0]["code"], "terminal_cwd_unavailable")
        self.assertNotIn('"type": "ready"', result.stdout)


if __name__ == "__main__":
    unittest.main()

import importlib.util
import errno
import socket
import ssl
import struct
import threading
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("env_network", HERE / "env_network.py")
env_network = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(env_network)


class EnvNetworkUnitTest(unittest.TestCase):
    def test_bind_helpers_close_created_socket_on_setup_failure(self):
        for helper in (env_network._bind_tcp, env_network._bind_udp):
            with self.subTest(helper=helper.__name__):
                sock = mock.Mock()
                sock.bind.side_effect = OSError("address busy")
                with mock.patch.object(env_network.socket, "socket", return_value=sock):
                    with self.assertRaises(OSError):
                        helper("127.0.0.1", 19566)
                sock.close.assert_called_once()

    def test_tls13_post_handshake_rejection_retains_ssl_classification(self):
        raw, ctx, tls = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        ctx.wrap_socket.return_value.__enter__.return_value = tls
        tls.recv.side_effect = ssl.SSLError("TLSV13_ALERT_CERTIFICATE_REQUIRED")
        args = Namespace(source_ip="127.0.0.1", target_ip="127.0.0.1",
                         tls_port=19567, timeout=1)
        with mock.patch.object(env_network, "_connect_tcp", return_value=raw), \
                mock.patch.object(env_network, "_tls_context_client", return_value=ctx):
            result = env_network._tls_attempt(args, Path("ca.pem"), None, None, "server")
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("missing_client_cert", result["reason"])

    def test_ready_write_failure_closes_started_resources(self):
        tcp, udp, tls = mock.Mock(), mock.Mock(), mock.Mock()
        args = Namespace(bind_ip="127.0.0.1", port=19566, tls_port=19567,
                         ca=Path("ca.pem"), server_cert=Path("cert.pem"),
                         server_key=Path("key.pem"), peer_expected=None,
                         ready_json=Path("ready.json"))
        with mock.patch.object(env_network, "live_linux_arm64_guard"), \
                mock.patch.object(env_network.signal, "signal"), \
                mock.patch.object(env_network, "_bind_tcp", side_effect=[tcp, tls]), \
                mock.patch.object(env_network, "_bind_udp", return_value=udp), \
                mock.patch.object(env_network, "_tls_context_server"), \
                mock.patch.object(env_network.threading, "Thread") as factory, \
                mock.patch.object(env_network, "_write_ready", side_effect=OSError("readonly")):
            with self.assertRaises(OSError):
                env_network.server_main(args)
            self.assertEqual(3, factory.return_value.start.call_count)
            self.assertEqual(3, factory.return_value.join.call_count)
        for sock in (tcp, udp, tls):
            sock.close.assert_called_once()

    def test_partial_bind_failure_closes_prior_listener(self):
        tcp = mock.Mock()
        args = Namespace(bind_ip="127.0.0.1", port=19566, tls_port=19567)
        with mock.patch.object(env_network, "live_linux_arm64_guard"), \
                mock.patch.object(env_network.signal, "signal"), \
                mock.patch.object(env_network, "_bind_tcp", return_value=tcp), \
                mock.patch.object(env_network, "_bind_udp", side_effect=OSError("busy")):
            with self.assertRaises(OSError):
                env_network.server_main(args)
        tcp.close.assert_called_once()

    def test_server_rejects_other_architecture_before_binding(self):
        with mock.patch.object(env_network.platform, "system", return_value="Linux"), \
                mock.patch.object(env_network.platform, "machine", return_value="x86_64"), \
                mock.patch.object(env_network, "_bind_tcp") as tcp, \
                mock.patch.object(env_network, "_bind_udp") as udp:
            with self.assertRaises(env_network.ProbeError) as raised:
                env_network.server_main(Namespace())
            self.assertEqual(raised.exception.reason, "linux_arm64_required")
            tcp.assert_not_called()
            udp.assert_not_called()

    def _pair(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        return left, right

    def test_good_echo(self):
        left, right = self._pair()
        token = b"fresh-token"

        def echo():
            env_network.send_frame(right, env_network.recv_frame(right, 1.0))

        thread = threading.Thread(target=echo)
        thread.start()
        result = env_network.probe_stream(left, token, 1.0)
        thread.join(1.0)
        self.assertEqual("PASS", result["status"])
        self.assertEqual(len(token), result["bytes"])

    def test_mismatch_echo_fails(self):
        left, right = self._pair()

        def wrong():
            env_network.recv_frame(right, 1.0)
            env_network.send_frame(right, b"not-the-token")

        thread = threading.Thread(target=wrong)
        thread.start()
        result = env_network.probe_stream(left, b"expected-token", 1.0)
        thread.join(1.0)
        self.assertEqual({"status": "FAIL", "reason": "mismatch", "received_len": 13}, result)

    def test_timeout_fails(self):
        left, _ = self._pair()
        result = env_network.probe_stream(left, b"token", 0.05)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("timeout", result["reason"])

    def test_bounded_frame_rejects_oversize(self):
        left, right = self._pair()
        right.sendall(struct.pack("!I", env_network.MAX_FRAME + 1))
        with self.assertRaises(env_network.ProbeError) as raised:
            env_network.recv_frame(left, 1.0)
        self.assertEqual("frame_too_large", raised.exception.reason)

    def test_tls_negative_classification(self):
        cases = [
            (ssl.SSLCertVerificationError("certificate verify failed: unable to get local issuer certificate"), "untrusted_ca"),
            (ssl.CertificateError("hostname 'bad' doesn't match"), "wrong_hostname"),
            (ssl.SSLError("TLSV13_ALERT_CERTIFICATE_REQUIRED"), "missing_client_cert"),
            (ssl.SSLError("SSLV3_ALERT_BAD_CERTIFICATE"), "handshake_rejected"),
            (ConnectionResetError(errno.ECONNRESET, "reset"), "transport_reset_without_tls_alert"),
            (OSError("plain socket error"), "not_tls_handshake_rejection"),
        ]
        for error, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(expected, env_network.classify_tls_failure(error)["classification"])

    def test_check_selection_defaults_to_all_without_vacuous_empty(self):
        self.assertEqual({"tcp", "udp", "tls"}, env_network.selected_checks([]))
        self.assertEqual({"tcp", "udp", "tls"}, env_network.selected_checks(["all"]))
        self.assertEqual({"tcp"}, env_network.selected_checks(["tcp"]))

    def test_wrong_check_selection_is_rejected_by_parser(self):
        parser = env_network.build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args([
                "client",
                "--source-ip", "127.0.0.1",
                "--target-ip", "127.0.0.1",
                "--port", "19566",
                "--tls-port", "19567",
                "--ca", "ca.pem",
                "--client-cert", "client.pem",
                "--client-key", "client.key",
                "--server-hostname", "afs-env-a",
                "--check", "bogus",
            ])

    def test_connect_tcp_closes_socket_on_connect_failure(self):
        made = []

        class FakeSocket:
            def __init__(self, *args):
                self.closed = False
                made.append(self)

            def settimeout(self, timeout):
                self.timeout = timeout

            def bind(self, addr):
                self.bound = addr

            def connect(self, addr):
                raise OSError("connect failed")

            def close(self):
                self.closed = True

        original = env_network.socket.socket
        env_network.socket.socket = FakeSocket
        try:
            with self.assertRaises(OSError):
                env_network._connect_tcp("127.0.0.1", "127.0.0.1", 9, 0.1)
        finally:
            env_network.socket.socket = original
        self.assertEqual(1, len(made))
        self.assertTrue(made[0].closed)


if __name__ == "__main__":
    unittest.main()

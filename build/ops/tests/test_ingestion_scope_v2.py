"""Operator migration contracts; PostgreSQL cases require a fresh disposable DB."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import threading
import time
import unittest
from wsgiref.simple_server import WSGIRequestHandler, make_server


SPEC = importlib.util.spec_from_file_location(
    "ingestion_scope_v2", Path(__file__).parents[1] / "ingestion_scope_v2.py"
)
MIGRATION = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATION
SPEC.loader.exec_module(MIGRATION)

TARGET = {
    "tenant": "tenant", "user_id": "user", "workspace": "workspace",
    "binding_id": "binding", "binding_generation": "incarnation",
    "template": "mail", "template_ref": "1",
}
GRANT = "ab" * 32


def subscription():
    return {
        **TARGET, "connection_id": "connection", "subscription_id": "subscription",
        "subscription_generation": 5, "connection_generation": 2, "service_port": 8080,
        "trigger_id": "trigger", "trigger_slug": "GMAIL_NEW_GMAIL_MESSAGE",
        "account_id": "account", "auth_config_id": "config", "composio_user_id": "dm_user",
        "enabled": True,
    }


def identity():
    current = int(time.time())
    return {
        **TARGET, "purpose": "connector.tool", "grant_ref": GRANT,
        "expires_at": current + 300, "renewable": True,
        "recovery_expires_at": current + 30 * 86400, "bootstrap_until": None,
    }


class IdentityTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "POSIX openat/link semantics required")
    def test_any_ancestor_symlink_is_refused_without_reading_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            nested = private / "nested"
            nested.mkdir(mode=0o700)
            secret = nested / "normal-token"
            secret.write_text("example\n")
            secret.chmod(0o600)
            self.assertEqual(MIGRATION.private_text(secret), "example")
            ancestor = root / "linked-parent"
            ancestor.symlink_to(private, target_is_directory=True)
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.private_text(ancestor / "nested" / "normal-token")
            immediate = private / "linked-nested"
            immediate.symlink_to(nested, target_is_directory=True)
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.private_text(immediate / "normal-token")

    def test_private_regular_owner_file_or_explicit_platform_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "normal-token"
            secret.write_text("example\n")
            if os.name != "posix":
                with self.assertRaisesRegex(MIGRATION.MigrationError, "private_files_require_posix"):
                    MIGRATION.private_text(secret)
                return
            secret.chmod(0o600)
            self.assertEqual(MIGRATION.private_text(secret), "example")
            secret.chmod(0o644)
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.private_text(secret)
            secret.chmod(0o600)
            link = Path(directory) / "linked-token"
            link.symlink_to(secret)
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.private_text(link)

    def test_normal_owner_identity_only(self):
        self.assertEqual(MIGRATION.validate_identity(identity(), TARGET), GRANT)
        changes = {
            "purpose": "ingestion.delivery", "renewable": False, "bootstrap_until": 999,
            "grant_ref": "generation-hash", "binding_generation": "replacement",
            "user_id": "other", "workspace": "other", "template": "other",
            "expires_at": int(time.time()) - 1, "recovery_expires_at": 1,
        }
        for name, value in changes.items():
            with self.subTest(name=name):
                changed = {**identity(), name: value}
                with self.assertRaises(MIGRATION.MigrationError):
                    MIGRATION.validate_identity(changed, TARGET)

    def test_unknown_and_missing_proof_fields_refused(self):
        for value in [{**identity(), "environment_id": "env"}, {**identity(), "service_port": 80}]:
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.validate_identity(value, TARGET)
        value = identity()
        del value["grant_ref"]
        with self.assertRaises(MIGRATION.MigrationError):
            MIGRATION.validate_identity(value, TARGET)

    def test_future_expiry_boolean_epochs_and_far_recovery_refused(self):
        current = int(time.time())
        for field, value in {"expires_at": current + 361, "recovery_expires_at": current + 31 * 86400,
                             "renewable": 1, "grant_ref": None}.items():
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.validate_identity({**identity(), field: value}, TARGET)
        with self.assertRaises(MIGRATION.MigrationError):
            MIGRATION.validate_identity({**identity(), "expires_at": True}, TARGET)

    def test_target_is_exact_logical_seven(self):
        self.assertEqual(MIGRATION.validate_target(TARGET), TARGET)
        for value in [{**TARGET, "grant_ref": GRANT}, {**TARGET, "binding_generation": True}]:
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.validate_target(value)

    def test_json_duplicates_are_not_a_mapping_authority(self):
        with self.assertRaises(MIGRATION.MigrationError):
            MIGRATION.decode_json('{"tenant":"one","tenant":"two"}')

    def test_https_origin_is_fixed_and_no_bearer_in_url(self):
        self.assertEqual(MIGRATION.identity_url("https://gateway.example:443/"),
                         "https://gateway.example:443/v1/connector/identity")
        for origin in ["http://gateway", "https://user:secret@gateway", "https://gateway/other",
                       "https://gateway?token=secret", "https://gateway/#x"]:
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.identity_url(origin)


class ProjectionTests(unittest.TestCase):
    def test_only_existing_reference_replaces_two_execution_fields(self):
        old = subscription()
        projected = MIGRATION.project_subscription(old, TARGET, GRANT)
        expected = copy.deepcopy(old)
        del expected["binding_generation"]
        del expected["service_port"]
        expected["authority_ref"] = GRANT
        self.assertEqual(projected, expected)
        self.assertEqual(old, subscription())
        self.assertEqual(MIGRATION.project_subscription(projected, TARGET, GRANT), expected)

    def test_stale_and_unmapped_projection_is_never_rebound(self):
        for field in TARGET:
            with self.subTest(field=field):
                old = {**subscription(), field: "different"}
                with self.assertRaises(MIGRATION.MigrationError):
                    MIGRATION.project_subscription(old, TARGET, GRANT)
        updated = MIGRATION.project_subscription(subscription(), TARGET, GRANT)
        with self.assertRaises(MIGRATION.MigrationError):
            MIGRATION.project_subscription(updated, TARGET, "cd" * 32)

    def test_unknown_keys_and_mixed_formats_rejected(self):
        for value in [{**subscription(), "authority_ref": GRANT}, {**subscription(), "extra": 1},
                      {**subscription(), "service_port": True}]:
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.project_subscription(value, TARGET, GRANT)

    def test_control_characters_and_boolean_versions_are_not_legacy_input(self):
        for name, value in {"binding_generation": "bad\x7f", "workspace": "  ",
                            "subscription_generation": True}.items():
            with self.assertRaises(MIGRATION.MigrationError):
                MIGRATION.project_subscription({**subscription(), name: value}, TARGET, GRANT)


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format_string, *args):
        """The fixture never prints bearer-bearing request diagnostics."""


class HttpsTests(unittest.TestCase):
    """Actual TLS requests verify CA, hostname, exact endpoint and redirect refusal."""

    @classmethod
    def setUpClass(cls):
        from datetime import datetime, timedelta, timezone
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        current = datetime.now(timezone.utc)
        certificate = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                       .public_key(key.public_key()).serial_number(x509.random_serial_number())
                       .not_valid_before(current - timedelta(minutes=1))
                       .not_valid_after(current + timedelta(days=1))
                       .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
                       .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                       .sign(key, hashes.SHA256()))
        cls.ca_file = root / "test-ca.pem"
        key_file = root / "test-key.pem"
        cls.ca_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                              serialization.NoEncryption()))
        cls.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cls.context.load_cert_chain(cls.ca_file, key_file)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        self.requests = []
        self.redirect = False

        def application(environ, start_response):
            self.requests.append((environ["REQUEST_METHOD"], environ["PATH_INFO"],
                                  environ["wsgi.input"].read(int(environ["CONTENT_LENGTH"])),
                                  environ.get("HTTP_AUTHORIZATION")))
            if self.redirect:
                start_response("302 Found", [("Location", "https://localhost:1/steal")])
                return [b""]
            body = json.dumps(identity()).encode()
            start_response("200 OK", [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
            return [body]

        self.server = make_server("127.0.0.1", 0, application, handler_class=QuietHandler)
        self.server.socket = self.context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f"https://localhost:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_real_https_exact_normal_identity_only(self):
        reader = MIGRATION.identity_reader(self.origin, self.ca_file, "dm-connector-v1.test.signature")
        self.assertEqual(MIGRATION.validate_identity(reader(), TARGET), GRANT)
        self.assertEqual(self.requests, [
            ("POST", "/v1/connector/identity", b"{}", "Bearer dm-connector-v1.test.signature"),
        ])

    def test_redirect_is_not_followed(self):
        self.redirect = True
        reader = MIGRATION.identity_reader(self.origin, self.ca_file, "dm-connector-v1.test.signature")
        with self.assertRaisesRegex(MIGRATION.MigrationError, "identity_redirect_refused"):
            reader()
        self.assertEqual(len(self.requests), 1)

    def test_wrong_hostname_is_rejected_before_bearer_is_sent(self):
        reader = MIGRATION.identity_reader(self.origin.replace("localhost", "127.0.0.1"), self.ca_file,
                                           "dm-connector-v1.test.signature")
        with self.assertRaises(MIGRATION.MigrationError):
            reader()
        self.assertEqual(self.requests, [])


@unittest.skipUnless(os.environ.get("ADX_INGESTION_SCOPE_V2_TEST_DSN_FILE"), "fresh PostgreSQL DB not supplied")
class PostgresTests(unittest.TestCase):
    """Uses real transactions; refuses a DB with an existing ingestion schema."""

    @classmethod
    def setUpClass(cls):
        import psycopg  # pylint: disable=import-outside-toplevel
        cls.psycopg = psycopg
        cls.dsn = Path(os.environ["ADX_INGESTION_SCOPE_V2_TEST_DSN_FILE"]).read_text().strip()
        cls.connection = psycopg.connect(cls.dsn, autocommit=True)
        with cls.connection.cursor() as cursor:
            cursor.execute("SELECT to_regnamespace('digitalmate_ingestion')")
            if cursor.fetchone()[0] is not None:
                cls.connection.close()
                raise RuntimeError("test requires a fresh DB without ingestion schema")
            schema = Path(__file__).parents[3] / "gateway/src/ingress/digitalmategateway/ingestion/schema.sql"
            cursor.execute(schema.read_text())

    @classmethod
    def tearDownClass(cls):
        with cls.connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA digitalmate_ingestion CASCADE")
        cls.connection.close()

    def setUp(self):
        with self.connection.cursor() as cursor:
            cursor.execute("TRUNCATE digitalmate_ingestion.events, digitalmate_ingestion.batches, "
                           "digitalmate_ingestion.subscriptions, digitalmate_ingestion.workspaces CASCADE")
            cursor.execute("INSERT INTO digitalmate_ingestion.workspaces(workspace_key) VALUES('workspace')")
            cursor.execute("INSERT INTO digitalmate_ingestion.subscriptions "
                           "(subscription_id,workspace_key,generation,connection_generation,"
                           "trigger_id,binding_json,enabled) "
                           "VALUES('subscription','workspace',5,2,'trigger',%s,true)", (json.dumps(subscription()),))
            cursor.execute("INSERT INTO digitalmate_ingestion.batches "
                           "(batch_id,workspace_key,subscription_id,generation,digest,payload,state) "
                           "VALUES('historical','workspace','subscription',5,'digest',%s,'accepted')", (b"original",))
            cursor.execute("INSERT INTO digitalmate_ingestion.events "
                           "(subscription_id,generation,event_id,digest,raw,state,batch_id) "
                           "VALUES('subscription',5,'event','digest',%s,'delivered','historical')", (b"original",))

    def snapshot(self):
        with self.connection.cursor() as cursor:
            return {
                table: self.read_table(cursor, table)
                for table in ("subscriptions", "events", "batches", "workspaces", "capacity", "version")
            }

    @staticmethod
    def read_table(cursor, table):
        # Table names are fixed above, never supplied by operator input.
        cursor.execute("SELECT * FROM digitalmate_ingestion." + table + " ORDER BY 1")
        return cursor.fetchall()

    def migrate(self, apply=True, proof=None):
        with self.psycopg.connect(self.dsn) as connection:
            return MIGRATION.migrate(connection, TARGET, proof or identity, apply=apply)

    def test_real_commit_idempotency_and_original_terminal_bytes(self):
        before = self.snapshot()
        self.assertEqual(self.migrate()["updated"], 1)
        after = self.snapshot()
        for table in before:
            if table != "subscriptions":
                self.assertEqual(before[table], after[table])
        self.assertEqual(self.migrate()["updated"], 0)
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT binding_json FROM digitalmate_ingestion.subscriptions")
            self.assertEqual(json.loads(cursor.fetchone()[0]),
                             MIGRATION.project_subscription(subscription(), TARGET, GRANT))

    def test_dry_run_preserves_every_byte(self):
        before = self.snapshot()
        self.assertEqual(self.migrate(apply=False)["updated"], 1)
        self.assertEqual(before, self.snapshot())

    def test_unsettled_inputs_and_active_batch_abort_without_changes(self):
        changes = [
            "UPDATE digitalmate_ingestion.events SET state='pending'",
            "UPDATE digitalmate_ingestion.events SET state='batched'",
            "UPDATE digitalmate_ingestion.batches SET state='ready',last_error='delivery_unknown'",
            "UPDATE digitalmate_ingestion.workspaces SET active_batch='historical'",
            "UPDATE digitalmate_ingestion.workspaces SET lease_until=clock_timestamp()-interval '1 day'",
        ]
        for statement in changes:
            with self.subTest(statement=statement):
                self.setUp()
                with self.connection.cursor() as cursor:
                    cursor.execute(statement)
                before = self.snapshot()
                with self.assertRaises(MIGRATION.MigrationError):
                    self.migrate()
                self.assertEqual(before, self.snapshot())

    def test_second_identity_failure_rolls_back_all_updates(self):
        count = 0

        def proof():
            nonlocal count
            count += 1
            if count == 2:
                return {**identity(), "grant_ref": "cd" * 32}
            return identity()

        before = self.snapshot()
        with self.assertRaises(MIGRATION.MigrationError):
            self.migrate(proof=proof)
        self.assertEqual(before, self.snapshot())

    def test_unmapped_same_workspace_aborts_without_changes(self):
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE digitalmate_ingestion.subscriptions SET binding_json=%s",
                           (json.dumps({**subscription(), "binding_generation": "stale"}),))
        before = self.snapshot()
        with self.assertRaises(MIGRATION.MigrationError):
            self.migrate()
        self.assertEqual(before, self.snapshot())

    def test_other_enabled_legacy_and_wrong_existing_ref_refused(self):
        changes = [
            {**subscription(), "user_id": "another-owner"},
            {**MIGRATION.project_subscription(subscription(), TARGET, GRANT), "authority_ref": "cd" * 32},
            {**subscription(), "service_port": True},
        ]
        for binding in changes:
            with self.subTest(binding=binding):
                self.setUp()
                with self.connection.cursor() as cursor:
                    cursor.execute("UPDATE digitalmate_ingestion.subscriptions SET binding_json=%s",
                                   (json.dumps(binding),))
                before = self.snapshot()
                with self.assertRaises(MIGRATION.MigrationError):
                    self.migrate()
                self.assertEqual(before, self.snapshot())

    def test_all_writer_tables_are_locked_before_owner_lookup(self):
        statements = [
            "UPDATE digitalmate_ingestion.events SET digest='changed'",
            "UPDATE digitalmate_ingestion.batches SET digest='changed'",
            "UPDATE digitalmate_ingestion.workspaces SET last_attempt=clock_timestamp()",
            "UPDATE digitalmate_ingestion.capacity SET pending_events=1",
            "UPDATE digitalmate_ingestion.subscriptions SET enabled=false",
            "UPDATE digitalmate_ingestion.version SET version=1",
        ]

        def proof():
            for statement in statements:
                with self.psycopg.connect(self.dsn) as contender:
                    with contender.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout='50ms'")
                        with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                            cursor.execute(statement)
                    contender.rollback()
            return identity()

        before = self.snapshot()
        self.migrate(apply=False, proof=proof)
        self.assertEqual(before, self.snapshot())


if __name__ == "__main__":
    unittest.main()

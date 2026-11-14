#!/usr/bin/env python3
"""Explicit, offline operator cutover of settled Inbox subscription projections.

Requires psycopg 3 and POSIX private token/DSN/target files. Stop old writers and
validate receiver/downstream terminal state separately before use. This tool
never renews credentials, creates grants, reads account tables, upgrades pending
work, changes subscription generations, or rewrites terminal rows. The physical
PostgreSQL schema remains 1; only the current subscription projection becomes
the wire-v2 form. Non-target enabled legacy projections block apply, because a
v2-only runtime must not silently leave them unreadable.

Default is a locked dry run. --apply is required to commit. Errors contain only
stable reason codes: neither DSN, bearer, grant reference nor response is logged.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import ssl
import stat
import sys
import time
import unicodedata
from urllib import error, parse, request


LOGICAL_FIELDS = frozenset({
    "tenant", "user_id", "workspace", "binding_id", "binding_generation", "template", "template_ref",
})
SCOPE_FIELDS = LOGICAL_FIELDS - {"binding_generation"}
IDENTITY_FIELDS = LOGICAL_FIELDS | {
    "purpose", "grant_ref", "expires_at", "renewable", "recovery_expires_at", "bootstrap_until",
}
BUSINESS_FIELDS = frozenset({
    "tenant", "user_id", "workspace", "binding_id", "template", "template_ref", "connection_id",
    "subscription_id", "subscription_generation", "connection_generation", "trigger_id", "trigger_slug",
    "account_id", "auth_config_id", "composio_user_id", "enabled",
})
LEGACY_FIELDS = BUSINESS_FIELDS | {"binding_generation", "service_port"}
CURRENT_FIELDS = BUSINESS_FIELDS | {"authority_ref"}
GRANT_PATTERN = re.compile(r"[a-f0-9]{64}\Z")
MAX_FILE = 65536


class MigrationError(Exception):
    """A fail-closed, secret-free operator error code."""


def decode_json(raw):
    """Decode exact JSON; duplicate keys never supply authority."""

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise MigrationError("duplicate_json_field")
            value[key] = item
        return value

    try:
        return json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise MigrationError("invalid_json") from exc


def bounded(value):
    if not isinstance(value, str) or not 0 < len(value) <= 256 or not value.strip():
        return False
    return all(unicodedata.category(char) != "Cc" for char in value)


def integer(value, minimum, maximum):
    return isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum


def validate_target(target):
    """The operator states the exact old logical7; owner verifies its authority."""
    if not isinstance(target, dict) or set(target) != LOGICAL_FIELDS:
        raise MigrationError("invalid_target")
    if not all(bounded(value) for value in target.values()):
        raise MigrationError("invalid_target")
    return dict(target)


def validate_identity(proof, target):
    """Accept only a current, normal connector identity issued by the real owner."""
    validate_target(target)
    if not isinstance(proof, dict) or set(proof) != IDENTITY_FIELDS:
        raise MigrationError("invalid_identity_fields")
    if any(proof[field] != target[field] for field in LOGICAL_FIELDS):
        raise MigrationError("identity_target_mismatch")
    if proof["purpose"] != "connector.tool" or proof["renewable"] is not True:
        raise MigrationError("normal_connector_required")
    if proof["bootstrap_until"] is not None:
        raise MigrationError("normal_connector_required")
    current = int(time.time())
    if not integer(proof["expires_at"], current + 1, current + 360):
        raise MigrationError("identity_expired_or_invalid")
    if not integer(proof["recovery_expires_at"], proof["expires_at"], current + 30 * 86400 + 61):
        raise MigrationError("identity_recovery_invalid")
    reference = proof["grant_ref"]
    if not isinstance(reference, str) or GRANT_PATTERN.fullmatch(reference) is None:
        raise MigrationError("invalid_grant_reference")
    return reference


def private_text(path):
    """Read a bounded owner-only regular file without following any symlink.

    Live use is POSIX-only: accepting Windows mode bits would pretend to verify
    its ACL. Source/unit checks remain portable; run the operator in its trusted
    Linux deployment environment with private files, without pasting secrets.
    """
    if os.name != "posix":
        raise MigrationError("private_files_require_posix")
    components = Path(path).parts
    if ".." in components:
        raise MigrationError("private_file_parent_traversal_refused")
    components = Path(os.path.abspath(path)).parts
    directory = None
    descriptor = None
    try:
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        directory = os.open(components[0], directory_flags)
        # Walk using pinned directory descriptors. A path replacement cannot
        # redirect a later lookup, and no ancestor is resolved through a link.
        for component in components[1:-1]:
            next_directory = os.open(component, directory_flags, dir_fd=directory)
            os.close(directory)
            directory = next_directory
        descriptor = os.open(components[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise MigrationError("private_file_required")
        if stat.S_IMODE(metadata.st_mode) & 0o077 or metadata.st_size > MAX_FILE:
            raise MigrationError("private_file_required")
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            descriptor = None
            value = stream.read(MAX_FILE + 1)
        if len(value) > MAX_FILE:
            raise MigrationError("private_file_required")
        return value.strip()
    except (OSError, UnicodeError) as exc:
        raise MigrationError("private_file_unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


def identity_url(origin):
    """Fixed HTTPS origin; no token, path override, query or redirect target."""
    try:
        parsed = parse.urlsplit(origin)
        valid_origin = parsed.scheme == "https" and parsed.hostname is not None
        clean_origin = parsed.username is None and parsed.password is None
        clean_tail = parsed.path in ("", "/") and not parsed.query and not parsed.fragment
        if not valid_origin or not clean_origin or not clean_tail:
            raise MigrationError("trusted_https_origin_required")
        if parsed.port == 0:
            raise MigrationError("trusted_https_origin_required")
    except ValueError as exc:
        raise MigrationError("trusted_https_origin_required") from exc
    return parse.urlunsplit((parsed.scheme, parsed.netloc, "/v1/connector/identity", "", ""))


class RejectRedirect(request.HTTPRedirectHandler):
    """Never resend an operator bearer to a Location supplied by a server."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise MigrationError("identity_redirect_refused")


def identity_reader(origin, ca_file, token):
    """Build one purpose-limited reader with explicit CA and no environment proxy."""
    url = identity_url(origin)
    if not token.startswith("dm-connector-v1.") or len(token) > 2048:
        raise MigrationError("normal_connector_required")
    if any(char.isspace() for char in token):
        raise MigrationError("normal_connector_required")
    try:
        context = ssl.create_default_context(cafile=str(ca_file))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        opener = request.build_opener(
            request.ProxyHandler({}), request.HTTPSHandler(context=context), RejectRedirect(),
        )
    except (OSError, ssl.SSLError) as exc:
        raise MigrationError("trusted_ca_unavailable") from exc

    def read():
        packet = request.Request(url, data=b"{}", method="POST", headers={
            "Authorization": "Bearer " + token, "Content-Type": "application/json",
        })
        try:
            with opener.open(packet, timeout=5) as response:
                if response.status != 200 or response.geturl() != url:
                    raise MigrationError("identity_response_refused")
                raw = response.read(MAX_FILE + 1)
                if len(raw) > MAX_FILE:
                    raise MigrationError("identity_response_too_large")
                return decode_json(raw)
        except (error.URLError, OSError, TimeoutError) as exc:
            raise MigrationError("identity_unavailable") from exc

    return read


def validate_subscription(binding):
    """The legacy decoder exists only at this explicit operator boundary."""
    if not isinstance(binding, dict):
        raise MigrationError("invalid_subscription")
    if set(binding) not in (LEGACY_FIELDS, CURRENT_FIELDS):
        raise MigrationError("unsupported_subscription_format")
    for name, value in binding.items():
        if name in ("subscription_generation", "connection_generation"):
            valid = integer(value, 1, 2**63 - 1)
        elif name == "service_port":
            valid = integer(value, 1, 65535)
        elif name == "enabled":
            valid = isinstance(value, bool)
        else:
            valid = bounded(value)
        if not valid:
            raise MigrationError("invalid_subscription")
    return binding


def project_subscription(binding, target, reference):
    """Preserve business versions and IDs, replace only the old execution tuple."""
    validate_target(target)
    validate_subscription(binding)
    if not isinstance(reference, str) or GRANT_PATTERN.fullmatch(reference) is None:
        raise MigrationError("invalid_grant_reference")
    if any(binding[field] != target[field] for field in SCOPE_FIELDS):
        raise MigrationError("subscription_target_mismatch")
    if set(binding) == CURRENT_FIELDS:
        if binding["authority_ref"] != reference:
            raise MigrationError("subscription_authority_mismatch")
        return dict(binding)
    if binding["binding_generation"] != target["binding_generation"]:
        raise MigrationError("subscription_incarnation_mismatch")
    projected = {name: value for name, value in binding.items() if name not in {"binding_generation", "service_port"}}
    projected["authority_ref"] = reference
    return projected


def check_settled(cursor):
    """Conservatively require global zero pending/in-flight input and leases."""
    cursor.execute("SELECT version FROM digitalmate_ingestion.version")
    if cursor.fetchall() != [(1,)]:
        raise MigrationError("unsupported_physical_schema")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM digitalmate_ingestion.events "
                   "WHERE state NOT IN ('delivered','revoked'))")
    if cursor.fetchone()[0]:
        raise MigrationError("nonterminal_events")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM digitalmate_ingestion.batches "
                   "WHERE state NOT IN ('accepted','revoked'))")
    if cursor.fetchone()[0]:
        raise MigrationError("nonterminal_batches")
    cursor.execute("SELECT EXISTS(SELECT 1 FROM digitalmate_ingestion.workspaces "
                   "WHERE active_batch IS NOT NULL OR lease_until IS NOT NULL)")
    if cursor.fetchone()[0]:
        raise MigrationError("workspace_not_settled")
    cursor.execute("SELECT pending_events,pending_bytes FROM digitalmate_ingestion.capacity WHERE singleton=true")
    if cursor.fetchall() != [(0, 0)]:
        raise MigrationError("capacity_not_settled")


def plan_updates(cursor, target, reference):
    """Refuse unmapped records instead of silently disabling or advancing them."""
    cursor.execute("SELECT subscription_id,binding_json,enabled FROM digitalmate_ingestion.subscriptions "
                   "ORDER BY subscription_id")
    updates = []
    matched = 0
    for subscription_id, raw, enabled in cursor.fetchall():
        binding = validate_subscription(decode_json(raw))
        if binding["subscription_id"] != subscription_id or binding["enabled"] != enabled:
            raise MigrationError("subscription_row_mismatch")
        same_owner = all(binding[field] == target[field] for field in ("tenant", "user_id", "workspace"))
        if not same_owner:
            if enabled and set(binding) == LEGACY_FIELDS:
                raise MigrationError("other_enabled_legacy_subscription")
            continue
        projected = project_subscription(binding, target, reference)
        matched += 1
        if projected != binding:
            updates.append((json.dumps(projected, ensure_ascii=False, separators=(",", ":")), subscription_id, raw))
    if matched == 0:
        raise MigrationError("target_subscriptions_not_found")
    return updates, matched


def migrate(connection, target, read_identity, *, apply=False):
    """Atomic Inbox-only update; callers supply a real owner HTTPS reader.

    The caller must stop old writers and separately validate the receiver.
    Owner proof is re-read under all Inbox table locks and before commit. A
    changed/revoked authority rolls the whole transaction back. Dry run uses the
    same locks and checks without writing a byte.
    """
    validate_target(target)
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout='5s'")
            cursor.execute("SET LOCAL statement_timeout='30s'")
            cursor.execute("LOCK TABLE digitalmate_ingestion.version,digitalmate_ingestion.workspaces,"
                           "digitalmate_ingestion.subscriptions,digitalmate_ingestion.events,"
                           "digitalmate_ingestion.batches,digitalmate_ingestion.capacity IN EXCLUSIVE MODE")
            check_settled(cursor)
            reference = validate_identity(read_identity(), target)
            updates, matched = plan_updates(cursor, target, reference)
            if apply:
                for value, subscription_id, original in updates:
                    cursor.execute("UPDATE digitalmate_ingestion.subscriptions SET binding_json=%s "
                                   "WHERE subscription_id=%s AND binding_json=%s", (value, subscription_id, original))
                    if cursor.rowcount != 1:
                        raise MigrationError("subscription_changed")
            if validate_identity(read_identity(), target) != reference:
                raise MigrationError("authority_changed")
    return {"applied": apply, "matched": matched, "updated": len(updates), "physical_schema": 1, "wire_version": 2}


def main(argv=None):
    """Linux operator entrypoint; no credentials accepted as command arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--connector-token-file", type=Path, required=True)
    parser.add_argument("--target-file", type=Path, required=True)
    parser.add_argument("--gateway-origin", required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    settings = parser.parse_args(argv)
    try:
        import psycopg  # pylint: disable=import-outside-toplevel
        target = validate_target(decode_json(private_text(settings.target_file)))
        reader = identity_reader(settings.gateway_origin, settings.ca_file, private_text(settings.connector_token_file))
        with psycopg.connect(private_text(settings.dsn_file), connect_timeout=5) as connection:
            result = migrate(connection, target, reader, apply=settings.apply)
        print(json.dumps(result, separators=(",", ":")))
        return 0
    except MigrationError as exc:
        print(json.dumps({"error": str(exc)}, separators=(",", ":")), file=sys.stderr)
    except ImportError:
        print('{"error":"psycopg3_required"}', file=sys.stderr)
    except Exception:  # noqa: BLE001 -- never expose a DSN, bearer or driver diagnostic
        print('{"error":"migration_unavailable"}', file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())

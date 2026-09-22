"""Command-line entry point for remote ADX administration."""

import argparse
from importlib import metadata
import json
import os
from pathlib import Path
import sys
from typing import Mapping, Sequence, TextIO

from .client import AdminClient, ClientOptions
from .credentials import read_token, write_secret
from .errors import AdminError


def build_parser(environ: Mapping[str, str] | None = None) -> argparse.ArgumentParser:
    values = os.environ if environ is None else environ
    parser = argparse.ArgumentParser(
        prog="adxadmin", description="Administer a remote ADX cluster"
    )
    parser.add_argument(
        "--version", action="version", version=f"adxadmin {_package_version()}"
    )
    parser.add_argument(
        "--endpoint",
        default=values.get("ADX_ENDPOINT"),
        required="ADX_ENDPOINT" not in values,
        help="public ADX HTTPS origin (env: ADX_ENDPOINT)",
    )
    parser.add_argument(
        "--ca",
        dest="ca_file",
        default=values.get("ADX_CA_FILE"),
        help="additional PEM CA file (env: ADX_CA_FILE)",
    )
    parser.add_argument(
        "--token-file",
        type=Path,
        default=values.get("ADX_ADMIN_TOKEN_FILE"),
        required="ADX_ADMIN_TOKEN_FILE" not in values,
        help="administrator API Key file (env: ADX_ADMIN_TOKEN_FILE)",
    )
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default=values.get("ADX_ADMIN_OUTPUT", "table"),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(values.get("ADX_ADMIN_TIMEOUT_SECONDS", "30")),
    )
    parser.add_argument("--allow-loopback-http", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    key = commands.add_parser("key", help="manage tenant API Keys")
    key_commands = key.add_subparsers(dest="key_command", required=True)

    create = key_commands.add_parser("create", help="create a tenant API Key")
    create.add_argument("--tenant", required=True)
    expiry = create.add_mutually_exclusive_group()
    expiry.add_argument("--expires-at", type=int, default=0)
    expiry.add_argument("--no-expiry", action="store_true")
    create.add_argument("--output-file", type=Path)

    listing = key_commands.add_parser("list", help="list tenant Key metadata")
    listing.add_argument("--tenant")
    listing.add_argument("--page-size", type=int, default=100)
    listing.add_argument("--page-token")

    revoke = key_commands.add_parser("revoke", help="revoke a tenant Key")
    revoke.add_argument("key_id")
    return parser


def run(
    arguments: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO = sys.stdout,
) -> int:
    options = build_parser(environ).parse_args(arguments)
    token = read_token(Path(options.token_file))
    client_options = ClientOptions(
        endpoint=options.endpoint,
        token=token,
        timeout_seconds=options.timeout_seconds,
        ca_file=options.ca_file,
        allow_loopback_http=options.allow_loopback_http,
    )
    with AdminClient(client_options) as client:
        if options.key_command == "create":
            created = client.create_key(options.tenant, options.expires_at)
            if options.output_file:
                write_secret(options.output_file, created["apiKey"])
                created = {"key": created["key"], "outputFile": str(options.output_file)}
            _print_created(created, options.output, stdout)
        elif options.key_command == "list":
            page = client.list_keys(
                tenant=options.tenant,
                page_size=options.page_size,
                page_token=options.page_token,
            )
            _print_page(page, options.output, stdout)
        else:
            client.revoke_key(options.key_id)
            _print({"revoked": options.key_id}, options.output, stdout)
    return 0


def main() -> None:
    try:
        raise SystemExit(run())
    except AdminError as error:
        print(f"adxadmin: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    except OSError as error:
        print(f"adxadmin: {error}", file=sys.stderr)
        raise SystemExit(1) from error


def _print_created(value: dict, output: str, stream: TextIO) -> None:
    if output == "json":
        _print(value, output, stream)
        return
    key = value["key"]
    print("ID\tTENANT\tEXPIRES_AT", file=stream)
    print(
        f"{key['id']}\t{key['tenantId']}\t{_expiry(key['expiresAtUnixSeconds'])}",
        file=stream,
    )
    if "apiKey" in value:
        print(f"API_KEY\t{value['apiKey']}", file=stream)
    elif "outputFile" in value:
        print(f"OUTPUT_FILE\t{value['outputFile']}", file=stream)


def _print_page(value: dict, output: str, stream: TextIO) -> None:
    if output == "json":
        _print(value, output, stream)
        return
    print("ID\tTENANT\tEXPIRES_AT", file=stream)
    for key in value["items"]:
        print(
            f"{key['id']}\t{key['tenantId']}\t{_expiry(key['expiresAtUnixSeconds'])}",
            file=stream,
        )
    if value["nextPageToken"]:
        print(f"NEXT_PAGE_TOKEN\t{value['nextPageToken']}", file=stream)


def _print(value: dict, output: str, stream: TextIO) -> None:
    if output == "json":
        print(json.dumps(value, separators=(",", ":")), file=stream)
    else:
        for key, item in value.items():
            print(f"{key.upper()}\t{item}", file=stream)


def _expiry(value: int) -> str:
    return "never" if value == 0 else str(value)


def _package_version() -> str:
    try:
        return metadata.version("adxadmin")
    except metadata.PackageNotFoundError:
        return "source"


if __name__ == "__main__":
    main()

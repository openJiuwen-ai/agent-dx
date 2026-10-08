#!/usr/bin/env python3
"""Render the base build's OBS manifest as a Buildkite artifact index."""

import argparse
import html
import json
from pathlib import Path
import re
from urllib.parse import urlsplit


SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def checked_url(value, host):
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.netloc != host or not parsed.path.startswith("/adx/"):
        raise ValueError("invalid artifact URL")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("invalid artifact URL")
    return html.escape(value, quote=True)


def size_label(size):
    if not isinstance(size, int) or size < 0:
        raise ValueError("invalid artifact size")
    if size < 1024:
        return f"{size} B"
    for unit in ("KiB", "MiB", "GiB"):
        size /= 1024
        if size < 1024 or unit == "GiB":
            return f"{size:.1f} {unit}"


def render(manifest, *, commit, build_id, build_url):
    parsed_build = urlsplit(build_url)
    if parsed_build.scheme != "https" or parsed_build.netloc != "buildkite.com":
        raise ValueError("invalid Buildkite build URL")
    build_link = html.escape(build_url, quote=True)
    rows = []
    if manifest is None:
        contents = ("<p>OBS publication disabled. "
                    f'<a href="{build_link}#artifacts">View Buildkite artifacts</a>.</p>')
    else:
        if manifest.get("commit") != commit:
            raise ValueError("OBS manifest commit does not match this build")
        if manifest.get("build_id") != build_id:
            raise ValueError("OBS manifest build ID does not match this build")
        if manifest.get("schema_version") != 1 or not manifest.get("artifacts"):
            raise ValueError("invalid OBS manifest")
        host = f'{manifest["bucket"]}.{manifest["endpoint"]}'
        manifest_link = checked_url(manifest["manifest_url"], host)
        for artifact in manifest["artifacts"]:
            name = html.escape(artifact["name"])
            url = checked_url(artifact["url"], host)
            digest = artifact["sha256"]
            if not isinstance(digest, str) or not SHA256.fullmatch(digest):
                raise ValueError("invalid artifact SHA256")
            size = size_label(artifact["bytes"])
            rows.append(f'<tr><td><a href="{url}">{name}</a></td>'
                        f'<td>{size}</td><td><code>{digest}</code></td></tr>')
        contents = (f'<p>{len(rows)} verified OBS artifacts · '
                    f'<a href="{manifest_link}">manifest.json</a></p>'
                    '<table><thead><tr><th>Artifact</th><th>Size</th><th>SHA256</th>'
                    '</tr></thead><tbody>' + "".join(rows) + '</tbody></table>')
    return ("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>ADX Build Artifacts</title>'
            '<style>body{font:16px system-ui,sans-serif;max-width:1100px;margin:40px auto;'
            'padding:0 20px;color:#20242b}a{color:#0969da}table{width:100%;border-collapse:collapse}'
            'th,td{padding:10px;border-bottom:1px solid #ddd;text-align:left}'
            'code{font-size:12px;overflow-wrap:anywhere}</style></head><body>'
            '<h1>ADX Build Artifacts</h1>'
            f'<p>Build <a href="{build_link}">{html.escape(build_id)}</a> · '
            f'commit <code>{html.escape(commit)}</code></p>{contents}</body></html>\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--build-url", required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text()) if args.manifest else None
    page = render(manifest, commit=args.commit, build_id=args.build_id,
                  build_url=args.build_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(page, encoding="utf-8")


if __name__ == "__main__":
    main()

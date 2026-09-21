#!/usr/bin/env python3
"""Upload immutable ADX build artifacts to Huawei Cloud OBS."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import quote


DEFAULT_BUCKET = "openyuanrong"
DEFAULT_ENDPOINT = "obs.cn-southwest-2.myhuaweicloud.com"
SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
TIMESTAMP = re.compile(r"^[0-9]{14}$")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def component(value, label):
    if not value or not SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"invalid {label}")
    return value


def normalized_endpoint(endpoint):
    value = endpoint.strip()
    for scheme in ("https://", "http://"):
        if value.startswith(scheme):
            value = value[len(scheme):]
            break
    value = value.rstrip("/")
    if not value or "/" in value or any(ch.isspace() for ch in value):
        raise ValueError("invalid OBS endpoint")
    return value


def public_url(endpoint, bucket, object_path):
    return f"https://{bucket}.{normalized_endpoint(endpoint)}/{quote(object_path, safe='/')}"


def plan(files, channel, version, platform, arch, timestamp, commit, build_id, bucket, endpoint):
    if channel not in ("daily", "release"):
        raise ValueError("channel must be daily or release")
    component(platform, "platform")
    component(arch, "architecture")
    component(bucket, "bucket")
    endpoint = normalized_endpoint(endpoint)
    if not COMMIT.fullmatch(commit):
        raise ValueError("commit must be a lowercase 40-character SHA")
    if channel == "release":
        component(version, "release version")
        root = f"adx/release/{version}/{platform}/{arch}"
    else:
        if not TIMESTAMP.fullmatch(timestamp):
            raise ValueError("daily timestamp must use YYYYmmddHHMMSS")
        root = f"adx/daily/{timestamp}-{commit[:12]}/{platform}/{arch}"

    paths = [Path(path) for path in files]
    if not paths:
        raise ValueError("at least one artifact is required")
    names = set()
    artifacts = []
    for path in paths:
        if path.is_symlink():
            raise ValueError(f"artifact symlink rejected: {path}")
        if not path.is_file():
            raise ValueError(f"artifact is not a regular file: {path}")
        if path.name == "manifest.json":
            raise ValueError("manifest.json is reserved for the OBS upload manifest")
        component(path.name, "artifact filename")
        if path.name in names:
            raise ValueError(f"duplicate artifact filename: {path.name}")
        names.add(path.name)
        object_path = f"{root}/{path.name}"
        artifacts.append({
            "name": path.name,
            "bytes": path.stat().st_size,
            "sha256": digest(path),
            "object": object_path,
            "url": public_url(endpoint, bucket, object_path),
        })
    manifest_object = f"{root}/manifest.json"
    return {
        "schema_version": 1,
        "channel": channel,
        "version": version if channel == "release" else None,
        "platform": platform,
        "arch": arch,
        "commit": commit,
        "build_id": build_id,
        "bucket": bucket,
        "endpoint": endpoint,
        "manifest_object": manifest_object,
        "manifest_url": public_url(endpoint, bucket, manifest_object),
        "artifacts": artifacts,
    }


def checked_upload(client, bucket, object_path, path):
    response = client.putFile(bucket, object_path, str(path))
    if response.status >= 300:
        raise RuntimeError(
            f"OBS upload failed for {path.name}: status={response.status} "
            f"code={getattr(response, 'errorCode', '')} request={getattr(response, 'requestId', '')}"
        )
    metadata = client.getObjectMetadata(bucket, object_path)
    length = getattr(getattr(metadata, "body", None), "contentLength", None)
    if metadata.status >= 300 or length is None or int(length) != path.stat().st_size:
        raise RuntimeError(f"OBS readback verification failed for {path.name}")


def publish(*, client, files, output, bucket, endpoint, channel, version, platform, arch,
            timestamp, commit, build_id, dry_run):
    try:
        manifest = plan(
            files, channel, version, platform, arch, timestamp, commit, build_id, bucket, endpoint
        )
        output = Path(output)
        if not dry_run:
            for path, artifact in zip(map(Path, files), manifest["artifacts"]):
                checked_upload(client, bucket, artifact["object"], path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(manifest, indent=2) + "\n")
        if not dry_run:
            checked_upload(client, bucket, manifest["manifest_object"], output)
        return manifest
    finally:
        if client is not None:
            client.close()


def obs_client(access_key, secret_key, endpoint):
    try:
        from obs import ObsClient
    except ModuleNotFoundError as error:
        raise RuntimeError("OBS SDK unavailable; install esdk-obs-python in the CI image") from error
    return ObsClient(
        access_key_id=access_key,
        secret_access_key=secret_key,
        server=normalized_endpoint(endpoint),
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--channel", choices=("daily", "release"), default="daily")
    parser.add_argument("--version")
    parser.add_argument("--platform", default="linux")
    parser.add_argument("--arch", required=True)
    parser.add_argument("--timestamp", required=True)
    parser.add_argument("--commit", default=os.getenv("BUILDKITE_COMMIT"))
    parser.add_argument("--build-id", default=os.getenv("BUILDKITE_BUILD_ID", "local"))
    parser.add_argument("--bucket", default=os.getenv("ADX_OBS_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--endpoint", default=os.getenv("ADX_OBS_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("files", nargs="+")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    client = None
    if not args.dry_run:
        access_key = os.getenv("OBS_ACCESS_KEY_ID")
        secret_key = os.getenv("OBS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            raise ValueError("OBS_ACCESS_KEY_ID and OBS_SECRET_ACCESS_KEY are required")
        client = obs_client(access_key, secret_key, args.endpoint)
    manifest = publish(
        client=client,
        files=args.files,
        output=args.output,
        bucket=args.bucket,
        endpoint=args.endpoint,
        channel=args.channel,
        version=args.version,
        platform=args.platform,
        arch=args.arch,
        timestamp=args.timestamp,
        commit=args.commit,
        build_id=args.build_id,
        dry_run=args.dry_run,
    )
    for artifact in manifest["artifacts"]:
        print(f"url: {artifact['url']}")
    print(f"manifest: {manifest['manifest_url']}")


if __name__ == "__main__":
    main()

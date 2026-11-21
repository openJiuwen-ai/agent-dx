#!/usr/bin/env python3
"""Upload immutable ADX build artifacts to Huawei Cloud OBS."""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote

LOG = logging.getLogger(__name__)
DEFAULT_BUCKET = "agent-dx"
DEFAULT_ENDPOINT = "obs.cn-southwest-2.myhuaweicloud.com"
SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
TIMESTAMP = re.compile(r"^[0-9]{14}$")
ROOTFS_ARCHES = ("amd64", "arm64")
BOOTSTRAP_ROOTFS_AMD64 = (
    "adx/daily/20261117235108-4104c42e7783/linux/amd64/adx-execd.tar.gz",
    "bc073cceac7c09317ede57110ccbb93522177cba02f24a2ef79ba593c0920fa5",
)


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
            prefix_length = len(scheme)
            value = value[prefix_length:]
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
        artifacts.append(
            {
                "name": path.name,
                "bytes": path.stat().st_size,
                "sha256": digest(path),
                "object": object_path,
                "url": public_url(endpoint, bucket, object_path),
            }
        )
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


def rootfs_candidate_key(arch):
    return f"adx/rootfs-reuse/linux/{arch}/manifest.json"


def read_rootfs_candidate(client, bucket, endpoint, arch):
    key = rootfs_candidate_key(arch)
    with tempfile.TemporaryDirectory(prefix="adx-rootfs-candidate-") as tmp:
        path = Path(tmp) / "manifest.json"
        response = client.getObject(bucket, key, downloadPath=str(path))
        if response.status == 404:
            return None
        if response.status != 200:
            raise RuntimeError(f"OBS EROFS reuse manifest read failed: status={response.status}")
        if path.stat().st_size > 4096:
            raise ValueError("invalid EROFS reuse manifest size")
        candidate = json.loads(path.read_bytes())

    if not isinstance(candidate, dict) or candidate.get("schema_version") != 1 or candidate.get("arch") != arch:
        raise ValueError("invalid EROFS reuse manifest")
    number = candidate.get("build_number")
    if not isinstance(number, int) or isinstance(number, bool) or number < 1:
        raise ValueError("invalid EROFS reuse manifest build number")
    commit, checksum = candidate.get("commit"), candidate.get("sha256")
    if not isinstance(commit, str) or not COMMIT.fullmatch(commit):
        raise ValueError("invalid EROFS reuse manifest commit")
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError("invalid EROFS reuse manifest package checksum")
    prefix = re.escape(public_url(endpoint, bucket, "adx/daily/"))
    url = candidate.get("url")
    expected_url = rf"{prefix}[0-9]{{14}}-{commit[:12]}/linux/{arch}/adx-execd\.tar\.gz"
    if not isinstance(url, str) or not re.fullmatch(expected_url, url):
        raise ValueError("invalid EROFS reuse manifest package URL")
    return candidate


def advance_rootfs_candidate(client, manifest, build_number):
    bucket, endpoint, arch = (manifest[key] for key in ("bucket", "endpoint", "arch"))
    if manifest["channel"] != "daily" or manifest["platform"] != "linux" or arch not in ROOTFS_ARCHES:
        raise ValueError("an EROFS reuse candidate requires a daily Linux release")
    execd = next((item for item in manifest["artifacts"] if item["name"] == "adx-execd.tar.gz"), None)
    if execd is None:
        raise ValueError("an EROFS reuse candidate requires a daily Linux Execd archive")
    if build_number < 1:
        raise ValueError("an EROFS reuse candidate requires a positive Buildkite build number")
    # The Buildkite publish jobs serialize writers per architecture. Keep the
    # newer record if an older build or a retry reaches this step afterwards.
    current = read_rootfs_candidate(client, bucket, endpoint, arch)
    if current is not None and current["build_number"] >= build_number:
        LOG.info("Keeping EROFS reuse candidate for %s from build #%s", arch, current["build_number"])
        return
    candidate = {"schema_version": 1, "arch": arch, "build_number": build_number,
                 "commit": manifest["commit"], "url": execd["url"], "sha256": execd["sha256"]}
    with tempfile.TemporaryDirectory(prefix="adx-rootfs-candidate-") as tmp:
        path = Path(tmp) / "manifest.json"
        path.write_text(json.dumps(candidate, indent=2) + "\n")
        checked_upload(client, bucket, rootfs_candidate_key(arch), path)
    LOG.info("Updated EROFS reuse candidate for %s to build #%s", arch, build_number)


def publish(
    *, client, files, output, bucket, endpoint, channel, version, platform, arch, timestamp, commit, build_id, dry_run
):
    try:
        manifest = plan(files, channel, version, platform, arch, timestamp, commit, build_id, bucket, endpoint)
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


def configured_obs_client(endpoint):
    access_key = os.getenv("OBS_ACCESS_KEY_ID")
    secret_key = os.getenv("OBS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError("OBS_ACCESS_KEY_ID and OBS_SECRET_ACCESS_KEY are required")
    return obs_client(access_key, secret_key, endpoint)


def stdout_logger():
    logger = logging.Logger(f"{__name__}.stdout", level=logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger


def rootfs_candidate_main(argv):
    parser = argparse.ArgumentParser(description="Resolve the previous ADX Execd package from OBS.")
    parser.add_argument("--arch", choices=ROOTFS_ARCHES, required=True)
    arch = parser.parse_args(argv).arch
    bucket = os.getenv("ADX_OBS_BUCKET", DEFAULT_BUCKET)
    endpoint = os.getenv("ADX_OBS_ENDPOINT", DEFAULT_ENDPOINT)
    try:
        client = configured_obs_client(endpoint)
        try:
            candidate = read_rootfs_candidate(client, bucket, endpoint, arch)
        finally:
            client.close()
    except (OSError, ValueError, RuntimeError) as error:
        LOG.warning("EROFS reuse candidate unavailable: %s", error)
        return

    if candidate is not None:
        stdout_logger().info("%s\n%s", candidate["url"], candidate["sha256"])
    elif arch == "amd64" and bucket == DEFAULT_BUCKET and normalized_endpoint(endpoint) == DEFAULT_ENDPOINT:
        key, checksum = BOOTSTRAP_ROOTFS_AMD64
        stdout_logger().info("%s\n%s", public_url(endpoint, bucket, key), checksum)


def advance_rootfs_candidate_main(argv):
    parser = argparse.ArgumentParser(description="Update the EROFS reuse candidate after OBS publication.")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--build-number", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text())
        client = configured_obs_client(manifest["endpoint"])
        try:
            advance_rootfs_candidate(client, manifest, args.build_number)
        finally:
            client.close()
    except (OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"EROFS reuse candidate update failed: {error}\n")


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
    manifest = publish(
        client=None if args.dry_run else configured_obs_client(args.endpoint),
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
    output_logger = stdout_logger()
    for artifact in manifest["artifacts"]:
        output_logger.info("url: %s", artifact["url"])
    output_logger.info("manifest: %s", manifest["manifest_url"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if sys.argv[1:2] == ["rootfs-candidate"]:
        rootfs_candidate_main(sys.argv[2:])
    elif sys.argv[1:2] == ["advance-rootfs-candidate"]:
        advance_rootfs_candidate_main(sys.argv[2:])
    else:
        main()

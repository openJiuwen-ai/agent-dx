#!/usr/bin/env python3
"""Runs in mgmt. Publish a deterministic OCI fixture, convert and republish it."""

import gzip, hashlib, io, json, pathlib, subprocess, tarfile
import urllib.request as u

ROOT = pathlib.Path("/workspace/fixture")
ROOT.mkdir(exist_ok=True)
OUT = pathlib.Path("/output/fixture")
OUT.mkdir(parents=True, exist_ok=True)
BASE = "http://10.203.0.10:5000"


def req(path, payload=None, method="GET", headers=None):
    return u.urlopen(u.Request(BASE + path, data=payload, method=method, headers=headers or {}), timeout=120)


def digest(payload):
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def upload(payload):
    d = digest(payload)
    loc = req("/v2/lab/input/blobs/uploads/", b"", "POST").headers["Location"]
    url = loc + ("&" if "?" in loc else "?") + "digest=" + d
    u.urlopen(u.Request(url, data=payload, method="PUT", headers={"Content-Type": "application/octet-stream"})).close()
    return d


files = {
    "startup.txt": b"NYDUS-AFS-real-mount-v1\n",
    "hot.bin": hashlib.shake_256(b"afs-hot-v1").digest(192 * 1024),
    "cold.bin": hashlib.shake_256(b"afs-cold-v1").digest(8 * 1024 * 1024),
}
raw = io.BytesIO()
with tarfile.open(fileobj=raw, mode="w") as t:
    for name, data in files.items():
        entry = tarfile.TarInfo(name)
        entry.size = len(data)
        entry.mode = 0o644
        t.addfile(entry, io.BytesIO(data))
plain = raw.getvalue()
layer = gzip.compress(plain, mtime=0)
cfg = json.dumps(
    {"architecture": "arm64", "os": "linux", "config": {}, "rootfs": {"type": "layers", "diff_ids": [digest(plain)]}}
).encode()
manifest = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": upload(cfg), "size": len(cfg)},
    "layers": [
        {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": upload(layer), "size": len(layer)}
    ],
}
req("/v2/lab/input/manifests/v1", json.dumps(manifest).encode(), "PUT", {"Content-Type": manifest["mediaType"]}).close()
(OUT / "input-manifest.json").write_text(json.dumps(manifest, indent=2))
(OUT / "expected-files.json").write_text(
    json.dumps({n: {"sha256": hashlib.sha256(d).hexdigest(), "size": len(d)} for n, d in files.items()}, indent=2)
)
del files, raw, plain, layer
with (OUT / "convert.log").open("w") as log:
    subprocess.run(
        [
            "/artifacts/tools/nydusify",
            "convert",
            "--source",
            "10.203.0.10:5000/lab/input:v1",
            "--target",
            "10.203.0.10:5000/lab/nydus:v1",
            "--source-insecure",
            "--target-insecure",
            "--plain-http",
            "--nydus-image",
            "/artifacts/tools/nydus-image",
            "--work-dir",
            str(ROOT / "conversion"),
            "--output-json",
            str(OUT / "conversion.json"),
        ],
        check=True,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
response = req(
    "/v2/lab/nydus/manifests/v1",
    headers={
        "Accept": (
            "application/vnd.oci.image.manifest.v1+json, "
            "application/vnd.docker.distribution.manifest.v2+json, "
            "application/vnd.oci.image.index.v1+json"
        )
    },
)
target_bytes = response.read()
target = json.loads(target_bytes)
(OUT / "manifest-digest.txt").write_text(digest(target_bytes))
(OUT / "manifest.raw").write_bytes(target_bytes)
(OUT / "target-manifest.json").write_text(json.dumps(target, indent=2))
print(json.dumps(target, indent=2))

# Bootstrap is image metadata consumed by nydusd; no range checksum inventory.
for layer in target["layers"]:
    if layer.get("annotations", {}).get("containerd.io/snapshot/nydus-bootstrap") == "true":
        data = req("/v2/lab/nydus/blobs/" + layer["digest"]).read()
        assert digest(data) == layer["digest"]
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            member = next(m for m in archive.getmembers() if m.isfile() and m.name.endswith("/image.boot"))
            (OUT / "bootstrap").write_bytes(archive.extractfile(member).read())
    else:
        (OUT / "data-blob.json").write_text(json.dumps(layer))

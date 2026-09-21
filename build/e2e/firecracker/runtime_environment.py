"""Render the immutable ADX runtime environment used by Firecracker nodes."""

from pathlib import Path


def resolve(package: Path, kubernetes: bool, runtime_image_file: Path) -> dict:
    artifact = package / "runtime/adx-runtime-rootfs.img"
    if kubernetes:
        if not runtime_image_file.is_file():
            raise RuntimeError("Kubernetes OCI runtime image is missing")
        image = runtime_image_file.read_text().strip()
        if "@sha256:" not in image:
            raise RuntimeError("Kubernetes OCI runtime image must be digest pinned")
        return {
            "rootfs": {
                "runtime": "firecracker",
                "type": "image",
                "image": image,
                "readonly": False,
            },
            "bootstrap": {
                "type": "image",
                "image": image,
                "target": "/__adx",
                "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"],
            },
            "env": {
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            },
        }
    if not artifact.is_file():
        raise RuntimeError(f"local ADX runtime image is missing: {artifact}")
    value = str(artifact.resolve())
    return {
        "rootfs": {
            "runtime": "firecracker",
            "type": "local",
            "path": value,
            "readonly": False,
        },
        "bootstrap": {
            "type": "erofs",
            "root": value,
            "target": "/__adx",
            "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"],
        },
        "env": {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        },
    }

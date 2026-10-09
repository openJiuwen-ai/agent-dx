#!/usr/bin/env bash
# Architecture-independent verification and OBS publication on the K8s worker.
set -euo pipefail
export ADX_BUILD_ARCH=arm64 ADX_EXTERNAL_BACKEND=0 ADX_OBS_METADATA=0
export ADX_OBS_UPLOAD=${ADX_ARM_OBS_UPLOAD:-${ADX_OBS_UPLOAD:-1}}
case "$ADX_OBS_UPLOAD" in
  0) echo 'ARM OBS publication disabled'; exit 0 ;;
  1) ;;
  *) echo 'ADX_ARM_OBS_UPLOAD must be 0 or 1' >&2; exit 2 ;;
esac
root=out/buildkite
rm -rf "$root/arm64" "$root/sdk" "$root/admin" "$root/obs"
for pattern in '*.tar.gz' '*.sha256' '*manifest.json' 'sdk/*' 'admin/*'; do
  buildkite-agent artifact download "$root/arm64/$pattern" . --step platform-build-arm64
done
for item in "$root/arm64"/*; do mv "$item" "$root/"; done
bash .buildkite/upload-obs.sh
url=$(python3 -c 'import json; print(json.load(open("out/buildkite/obs/manifest.json"))["manifest_url"])')
buildkite-agent meta-data set obs-manifest-url-arm64 "$url"
mkdir -p "$root/arm64"
mv "$root/obs" "$root/arm64/"

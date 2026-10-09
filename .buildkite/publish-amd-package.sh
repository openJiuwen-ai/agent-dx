#!/usr/bin/env bash
# Publish this build's assembled x86 release without rebuilding.
set -euo pipefail
root=out/buildkite
rm -rf "$root/sdk" "$root/admin" "$root/obs"
for pattern in 'adx-release.tar.gz*' 'adx-execd.tar.gz*' 'backend.tar.gz' '*manifest.json' 'sdk/*' 'admin/*'; do
  buildkite-agent artifact download "$root/$pattern" . --step platform-build
done
bash .buildkite/upload-obs.sh

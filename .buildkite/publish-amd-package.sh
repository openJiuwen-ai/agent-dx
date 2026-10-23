#!/usr/bin/env bash
# Publish this build's assembled x86 release without rebuilding.
set -euo pipefail
root=out/buildkite
rm -rf "$root/sdk" "$root/admin" "$root/obs"
case "${ADX_ARTIFACT_TRANSPORT:-obs}" in
  obs)
    source .buildkite/obs-python.sh
    echo '--- :arrow_down: Download verified release and Execd via OBS staging'
    "$OBS_PYTHON" build/release/ci_transfer.py download release "$root"
    "$OBS_PYTHON" build/release/ci_transfer.py download execd "$root/components"
    cp "$root/components/execd.tar.gz" "$root/adx-execd.tar.gz"
    (cd "$root" && sha256sum adx-execd.tar.gz > adx-execd.tar.gz.sha256)
    tar -xOf "$root/adx-release.tar.gz" ./manifest.json > "$root/release-manifest.json"
    ;;
  buildkite)
    for pattern in 'adx-release.tar.gz*' 'adx-execd.tar.gz*' 'backend.tar.gz' '*manifest.json'; do
      buildkite-agent artifact download "$root/$pattern" . --step platform-build
    done
    ;;
  *) echo 'ADX_ARTIFACT_TRANSPORT must be obs or buildkite' >&2; exit 2 ;;
esac
for pattern in 'sdk/*' 'admin/*'; do
  buildkite-agent artifact download "$root/$pattern" . --step platform-build
done
bash .buildkite/upload-obs.sh

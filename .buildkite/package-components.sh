#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]

export ADX_RELEASE_TARGET=x86_64-unknown-linux-gnu
export ADX_RELEASE_OUTPUT="$PWD/out/buildkite/package"
source .buildkite/bootstrap-build.sh
rm -rf out/buildkite/components "$ADX_RELEASE_OUTPUT" \
  out/buildkite/sdk out/buildkite/backend
mkdir -p out/buildkite/logs out/buildkite/sdk out/buildkite/components

echo "--- :arrow_down: Download and verify component artifacts"
download_component() {
  local component=$1
  bash .buildkite/component-transfer.sh download "$component"
}
pids=()
for component in platform gateway execd; do
  download_component "$component" &
  pids+=("$!")
done
for pid in "${pids[@]}"; do
  wait "$pid"
done
for component in platform gateway execd; do
  mkdir "out/buildkite/components/$component"
  tar -xzf "out/buildkite/components/$component.tar.gz" \
    -C "out/buildkite/components/$component"
  python3 build/release/component.py verify \
    --component "$component" \
    --directory "out/buildkite/components/$component" \
    --commit "$BUILDKITE_COMMIT" \
    --target "$ADX_RELEASE_TARGET"
  find "out/buildkite/components/$component" -maxdepth 1 -type f \
    ! -name manifest.json -exec chmod 0755 {} \;
  if [[ $component == execd ]]; then
    cp "out/buildkite/components/$component.tar.gz" out/buildkite/adx-execd.tar.gz
    (cd out/buildkite && sha256sum adx-execd.tar.gz > adx-execd.tar.gz.sha256)
  fi
  rm "out/buildkite/components/$component.tar.gz"
done

stage=$(mktemp -d "${TMPDIR:-/tmp}/adx-components.XXXXXX")
trap 'rm -rf "$stage"' EXIT
for component in platform gateway execd; do
  find "out/buildkite/components/$component" -maxdepth 1 -type f \
    ! -name manifest.json -exec cp {} "$stage/" \;
done

echo "--- :python: Consume tested Sandbox SDK candidate"
buildkite-agent artifact download 'out/buildkite/sdk/*' . --step sdk-package
python3 build/sdk/candidate.py --verify --directory out/buildkite/sdk
python3 - <<'CHECK'
import json, os
from pathlib import Path
candidate = json.loads(Path('out/buildkite/sdk/sdk-candidate.json').read_text())
if candidate['commit'] != os.environ['BUILDKITE_COMMIT'] or candidate['build_id'] != os.environ['BUILDKITE_BUILD_ID']:
    raise SystemExit('SDK candidate belongs to another build')
CHECK
wheel=(out/buildkite/sdk/adx_sandbox-*.whl)
[[ ${#wheel[@]} == 1 && -f ${wheel[0]} ]]

echo "--- :package: Assemble unified ADX release"
python3 build/release/package.py assemble \
  --binary-dir "$stage" \
  --redis "$ADX_REDIS_SERVER" \
  --redis-cli "$ADX_REDIS_CLI" \
  --wheel "${wheel[0]}" \
  --target "$ADX_RELEASE_TARGET" \
  --profile release \
  --output "$ADX_RELEASE_OUTPUT"
echo "--- :test_tube: Install assembled release in an isolated prefix"
install_root=$(mktemp -d "$stage/install.XXXXXX")
bash "$ADX_RELEASE_OUTPUT/install.sh" --prefix "$install_root/adx" --bin-dir "$install_root/bin"
"$install_root/bin/adxctl" --help > out/buildkite/logs/install-smoke.log

tar -czf out/buildkite/adx-release.tar.gz -C "$ADX_RELEASE_OUTPUT" .
(cd out/buildkite && sha256sum adx-release.tar.gz > adx-release.tar.gz.sha256)
cp "$ADX_RELEASE_OUTPUT/manifest.json" out/buildkite/release-manifest.json


echo "--- :package: Verify pinned sandboxd backend artifacts"
if [[ -n ${ADX_BACKEND_ARTIFACT_BUILD:-} ]]; then
  buildkite-agent artifact download 'out/buildkite/backend/*' . \
    --step platform-build --build "$ADX_BACKEND_ARTIFACT_BUILD" \
    2>&1 | tee out/buildkite/logs/backend.log
  python3 build/e2e/verify_backend.py \
    --directory out/buildkite/backend \
    --target "$ADX_RELEASE_TARGET" \
    2>&1 | tee -a out/buildkite/logs/backend.log
else
  python3 build/e2e/build_backend.py \
    --output out/buildkite/backend \
    --redis-cli "$ADX_REDIS_CLI" \
    --jobs "${JOBS:-2}" \
    2>&1 | tee out/buildkite/logs/backend.log
fi
tar -czf out/buildkite/backend.tar.gz -C out/buildkite/backend .

wheel=(out/buildkite/sdk/adx_sandbox-*.whl)
[[ ${#wheel[@]} == 1 && -f ${wheel[0]} ]]
python3 build/release/component.py aggregate \
  --component-root out/buildkite/components \
  --commit "$BUILDKITE_COMMIT" \
  --target "$ADX_RELEASE_TARGET" \
  --package-manifest out/buildkite/release-manifest.json \
  --release-archive out/buildkite/adx-release.tar.gz \
  --wheel "${wheel[0]}" \
  --backend-manifest out/buildkite/backend/manifest.json \
  --backend-archive out/buildkite/backend.tar.gz \
  --output out/buildkite/build-manifest.json

# adxadmin is an independent Python artifact produced by this base build.
buildkite-agent artifact download 'out/buildkite/admin/*' . --step admin-package
python3 build/admin/candidate.py --verify --directory out/buildkite/admin
python3 - <<'CHECK'
import json, os
from pathlib import Path
candidate = json.loads(Path('out/buildkite/admin/admin-candidate.json').read_text())
assert candidate['commit'] == os.environ['BUILDKITE_COMMIT'], 'admin commit mismatch'
assert candidate['build_id'] == os.environ['BUILDKITE_BUILD_ID'], 'admin build mismatch'
CHECK

# Upload these local bytes; never re-download the assembled archives.
bash .buildkite/upload-obs.sh

if [[ ${ADX_ARTIFACT_TRANSPORT:-obs} == obs ]]; then
  source .buildkite/obs-python.sh
  "$OBS_PYTHON" build/release/ci_transfer.py upload release     out/buildkite/adx-release.tar.gz out/buildkite/adx-release.tar.gz.sha256     out/buildkite/build-manifest.json out/buildkite/backend.tar.gz
fi

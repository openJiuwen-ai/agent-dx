#!/usr/bin/env bash
# Runs inside the digest-pinned native Linux ARM container.
set -euo pipefail
source .buildkite/build-architecture.sh
[[ $ADX_BUILD_ARCH == arm64 ]]
[[ $(uname -m) == aarch64 ]]
[[ $(rustc -vV | sed -n 's/^host: //p') == "$ADX_RELEASE_TARGET" ]]
source .buildkite/bootstrap-build.sh
export ADX_COMPONENT_LOCAL=1
for component in platform gateway execd; do
  # build-component owns the component-tests.sh partition before compilation.
  bash .buildkite/build-component.sh "$component"
done
for package in sdk admin; do
  python3 "build/$package/candidate.py" --verify --directory "out/buildkite/$package"
done
python3 - <<'PY'
import json, os
from pathlib import Path
for package in ('sdk', 'admin'):
    candidate = json.loads(Path(f'out/buildkite/{package}/{package}-candidate.json').read_text())
    if candidate['commit'] != os.environ['BUILDKITE_COMMIT'] or candidate['build_id'] != os.environ['BUILDKITE_BUILD_ID']:
        raise SystemExit(f'{package} candidate belongs to another build')
PY
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
for component in platform gateway execd; do
  find "out/buildkite/components/$component" -maxdepth 1 -type f ! -name manifest.json -exec cp {} "$stage/" \;
done
wheel=(out/buildkite/sdk/adx_sandbox-*.whl)
[[ ${#wheel[@]} == 1 && -f ${wheel[0]} ]]
output=out/buildkite/package
rm -rf "$output"
python3 build/release/package.py assemble --binary-dir "$stage" \
  --redis "$ADX_REDIS_SERVER" --redis-cli "$ADX_REDIS_CLI" \
  --wheel "${wheel[0]}" --target "$ADX_RELEASE_TARGET" --profile release --output "$output"
python3 build/release/package.py verify "$output"
echo '--- :test_tube: Native ARM install, Redis and ELF smoke'
bash "$output/install.sh" --prefix "$stage/install/adx" --bin-dir "$stage/install/bin"
"$stage/install/bin/adxctl" --help
"$stage/install/adx/current/bin/adx-inspect" --help
for binary in "$output"/bin/* "$output"/runtime/adx-execd; do
  readelf -h "$binary" | grep -F 'AArch64'
done
! readelf -l "$output/runtime/adx-execd" | grep -F 'INTERP'
fsck.erofs "$output/runtime/adx-runtime-rootfs.img"
cleanup() {
  if [[ -f "$stage/redis.pid" ]]; then
    "$output/bin/redis-cli" -s "$stage/redis.sock" shutdown nosave >/dev/null 2>&1 || true
  fi
  rm -rf "$stage"
}
trap cleanup EXIT
"$output/bin/redis-server" --port 0 --unixsocket "$stage/redis.sock" --daemonize yes --pidfile "$stage/redis.pid" --dir "$stage"
"$output/bin/redis-cli" -s "$stage/redis.sock" ping | grep -Fx PONG
"$output/bin/redis-cli" -s "$stage/redis.sock" shutdown nosave
tar -czf out/buildkite/adx-release.tar.gz -C "$output" .
cp out/buildkite/components/execd.tar.gz out/buildkite/adx-execd.tar.gz
(cd out/buildkite && sha256sum adx-release.tar.gz > adx-release.tar.gz.sha256 && sha256sum adx-execd.tar.gz > adx-execd.tar.gz.sha256)
cp "$output/manifest.json" out/buildkite/release-manifest.json
python3 build/release/component.py aggregate --component-root out/buildkite/components \
  --commit "$BUILDKITE_COMMIT" --target "$ADX_RELEASE_TARGET" \
  --package-manifest out/buildkite/release-manifest.json --release-archive out/buildkite/adx-release.tar.gz \
  --wheel "${wheel[0]}" --output out/buildkite/build-manifest.json

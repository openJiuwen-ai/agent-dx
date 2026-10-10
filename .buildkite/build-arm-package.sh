#!/usr/bin/env bash
# macOS ARM worker hosts a native Linux ARM64 Docker builder.
set -euo pipefail
phase=${1:?build or package}
component=${2:-}
case "$phase:$component" in
  build:platform|build:gateway|build:execd|build:afs|package:) ;;
  *) echo 'expected build <platform|gateway|execd|afs> or package' >&2; exit 2 ;;
esac
ADX_WITH_AFS=${ADX_WITH_AFS:-0}
case "$ADX_WITH_AFS" in
  0|1) ;;
  *) echo 'ADX_WITH_AFS must be 0 or 1' >&2; exit 2 ;;
esac
if [[ $component == afs && $ADX_WITH_AFS != 1 ]]; then
  echo 'AFS component requires ADX_WITH_AFS=1' >&2
  exit 2
fi
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
[[ $(uname -m) == arm64 ]] || { echo 'native macOS ARM worker required' >&2; exit 1; }
[[ -z $(git status --porcelain) && $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
docker info >/dev/null
rm -rf out/buildkite/arm64 out/buildkite/sdk out/buildkite/admin \
  out/buildkite/logs out/buildkite/components out/buildkite/package out/buildkite/obs
rm -f out/buildkite/adx-release.tar.gz out/buildkite/adx-release.tar.gz.sha256 \
  out/buildkite/adx-execd.tar.gz out/buildkite/adx-execd.tar.gz.sha256 \
  out/buildkite/release-manifest.json out/buildkite/build-manifest.json
mkdir -p out/buildkite/arm64 out/buildkite/logs
# Retain compiler and artifact evidence, also on failure.
finish() {
  for item in logs components package sdk admin obs adx-release.tar.gz adx-release.tar.gz.sha256 adx-execd.tar.gz adx-execd.tar.gz.sha256 release-manifest.json build-manifest.json; do
    if [[ -e out/buildkite/$item ]]; then mv "out/buildkite/$item" out/buildkite/arm64/; fi
  done
}
trap finish EXIT
if [[ $phase == package ]]; then
  buildkite-agent artifact download 'out/buildkite/sdk/*' . --step sdk-package
  buildkite-agent artifact download 'out/buildkite/admin/*' . --step admin-package
  parts=(platform gateway execd)
  if [[ $ADX_WITH_AFS == 1 ]]; then parts+=(afs); fi
  for part in "${parts[@]}"; do
    buildkite-agent artifact download "out/buildkite/arm64/components/$part.tar.gz" . --step "build-$part-arm64"
  done
  mv out/buildkite/arm64/components out/buildkite/components
fi
# Docker Desktop maps the worker-owned output directory into the Linux builder.
# Only generated artifacts are writable; registry credentials live outside checkout.
chmod -R a+rwX out/buildkite
image=$(python3 -c 'import json; print(json.load(open("build/images/build-environment-arm64.json"))["ci_image"])')
[[ $image == *@sha256:* ]]
env_args=()
for key in BUILDKITE_COMMIT BUILDKITE_BUILD_ID JOBS ADX_ARM_TESTS ADX_WITH_AFS; do
  if [[ -n ${!key:-} ]]; then env_args+=(-e "$key"); fi
done
# Publication runs on the existing Kubernetes worker with its OBS Secret.
docker run --rm --platform linux/arm64 \
  -v "$PWD:/workspace" -w /workspace \
  -v adx-arm64-cargo-home:/mnt/paas/build-cache/adx/cargo-home \
  -v adx-arm64-cargo-target:/mnt/paas/build-cache/adx/cargo-target \
  -v adx-arm64-go-build:/mnt/paas/build-cache/adx/go-build \
  -v adx-arm64-go-mod:/mnt/paas/build-cache/adx/go-mod \
  -e ADX_BUILD_ARCH=arm64 -e RUSTUP_TOOLCHAIN=1.95.0 \
  -e "CARGO_TARGET_DIR=/mnt/paas/build-cache/adx/cargo-target/${component:-package}-arm64-rust1950" \
  -e ADX_CARGO_HOME=/mnt/paas/build-cache/adx/cargo-home \
  -e GOCACHE=/mnt/paas/build-cache/adx/go-build/arm64 \
  -e GOMODCACHE=/mnt/paas/build-cache/adx/go-mod \
  -e CC_aarch64_unknown_linux_musl=musl-gcc \
  -e CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
  "${env_args[@]}" "$image" bash .buildkite/package-arm-native.sh "$phase" "$component" \
  2>&1 | tee out/buildkite/logs/step-release-arm64.log

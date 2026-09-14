#!/usr/bin/env bash
# Build the adx-sandbox distribution (pure-Python wheel + sdist).
#
# This is the single release/packaging entrypoint, callable standalone or from the
# adx build pipeline (Makefile `agentruntime` target). adx-sandbox is a
# pure-Python package (py3-none-any) — no toolchain/compile needed.
#
# Usage:
#   bash build.sh [OUTDIR]   # default OUTDIR=dist
# Env:
#   PYTHON        python interpreter to use (default: python3)
#   BUILD_VERSION package version supplied by the parent adx build
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTDIR="${1:-${SCRIPT_DIR}/dist}"
PYTHON="${PYTHON:-python3}"

cd "${SCRIPT_DIR}"
rm -rf build ./*.egg-info
mkdir -p "${OUTDIR}"

# Release pipelines may explicitly override the component VERSION.
version="${ADX_RELEASE_TAG:-${BUILDKITE_TAG:-${BUILD_VERSION:-${SETUPTOOLS_SCM_PRETEND_VERSION:-}}}}"
version="${version#refs/tags/}"
version="${version#v}"
if [ -n "${version}" ]; then
	export SETUPTOOLS_SCM_PRETEND_VERSION="${version}"
	echo "[adx-sandbox] building version ${version} -> ${OUTDIR}"
else
	echo "[adx-sandbox] building (version from component VERSION) -> ${OUTDIR}"
fi

# Prefer the PEP 517 'build' frontend (wheel + sdist); fall back to pip wheel
# (wheel only) when 'build' is unavailable in the environment.
if ${PYTHON} -c "import build" >/dev/null 2>&1; then
	${PYTHON} -m build --wheel --sdist --outdir "${OUTDIR}"
else
	echo "[adx-sandbox] 'build' module absent; falling back to 'pip wheel' (wheel only)"
	${PYTHON} -m pip wheel . --no-deps -w "${OUTDIR}"
fi

echo "[adx-sandbox] artifacts:"
ls -1 "${OUTDIR}"/adx_sandbox-*

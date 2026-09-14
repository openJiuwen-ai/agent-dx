#!/usr/bin/env bash
# Compatibility entrypoint for the sandbox SDK workspace.
# Defaults to building the Python SDK because it is the only implemented
# language package today. Future language SDKs should add their own build
# entrypoints under go/, rust/, or java/ without changing this default.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTDIR="${1:-${SCRIPT_DIR}/dist}"

cd "${SCRIPT_DIR}/python"
exec bash build.sh "${OUTDIR}"

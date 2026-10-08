#!/usr/bin/env bash
set -euo pipefail
component=${1:?component}
case "$component" in
  platform)
    packages=(adx-deployment adx-coordinator adxlet adx-core adx-scheduling adx-protocol adx-discovery
              adx-error adx-observability adx-process adx-transport)
    ;;
  gateway)
    packages=(adx-apiserver data-plane-gateway adx-agent-core adx-agent-store adx-agent-api adx-activator adx-cli)
    ;;
  execd) packages=(adx-execd) ;;
  *) echo "unknown component: $component" >&2; exit 2 ;;
esac
args=()
for package in "${packages[@]}"; do args+=(-p "$package"); done
if [[ $component == gateway ]]; then args+=(--features adx-agent-store/test-memory); fi
echo "--- :test_tube: $component unit and component tests"
cargo test --locked -j "${JOBS:-4}" "${args[@]}"

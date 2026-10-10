#!/usr/bin/env bash
set -euo pipefail
component=${1:?component}
# Keep real subprocess, fsync and checkpoint tests from competing inside one
# test executable. Cargo build concurrency and the complete test set stay intact.
case "$component" in
  platform)
    packages=(adx-deployment adx-coordinator adxlet adx-core adx-scheduling adx-protocol adx-discovery
              adx-error adx-observability adx-process adx-transport)
    export RUST_TEST_THREADS=1
    ;;
  gateway)
    packages=(adx-apiserver data-plane-gateway adx-agent-core adx-agent-store adx-agent-api adx-activator adx-cli)
    ;;
  execd)
    packages=(adx-execd)
    export RUST_TEST_THREADS=1
    ;;
  afs)
    packages=(afs afs-client afs-error afs-logging afs-metrics afs-protocol afs-tracing afs-transport)
    ;;
  *) echo "unknown component: $component" >&2; exit 2 ;;
esac
args=()
for package in "${packages[@]}"; do args+=(-p "$package"); done
if [[ $component == gateway ]]; then args+=(--features adx-agent-store/test-memory); fi
echo "--- :test_tube: $component unit and component tests"
cargo test --locked -j "${JOBS:-4}" "${args[@]}"

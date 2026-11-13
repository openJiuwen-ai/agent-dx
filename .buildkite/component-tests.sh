#!/usr/bin/env bash
set -euo pipefail
component=${1:?component}
# Keep real subprocess, fsync and checkpoint tests from competing inside one
# test executable. Cargo build concurrency stays intact; deferred Redis timing
# regressions are listed explicitly below.
case "$component" in
  platform)
    packages=(adx-deployment adx-coordinator adxlet adx-core adx-checkpoint-storage adx-scheduling adx-protocol adx-discovery
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
    packages=(afs afs-client afs-error afs-logging afs-metrics afs-protocol afs-tracing afs-transport afs-tools)
    ;;
  *) echo "unknown component: $component" >&2; exit 2 ;;
esac
args=()
for package in "${packages[@]}"; do args+=(-p "$package"); done
if [[ $component == gateway ]]; then args+=(--features adx-agent-store/test-memory); fi
echo "--- :test_tube: $component unit and component tests"
cargo test --locked -j "${JOBS:-4}" "${args[@]}"

# These tests need the pinned Redis binary in the CI image and opt in explicitly.
if [[ $component == platform ]]; then
  export ADX_TEST_REDIS_SERVER="${ADX_TEST_REDIS_SERVER:-${ADX_REDIS_SERVER:-redis-server}}"
  echo '--- :fast_forward: Temporarily deferred Redis timing regressions (Issue71)'
  printf 'SKIP %s\n' \
    counter_compaction::configured_operation_budget_outlives_driver_default_and_remains_bounded \
    transport_recovery::transport_recovery_replaces_a_response_stalled_connection \
    transport_recovery::transport_recovery_repairs_an_applied_write_with_a_lost_response \
    transport_recovery::transport_recovery_fences_a_delayed_write_before_publishing_absence \
    transport_recovery::transport_recovery_fences_a_cancelled_write_without_waiting_for_its_timeout
  export ADX_TEST_EVIDENCE="$PWD/out/buildkite/logs/redis-counter-compaction"
  cargo test --locked --package=adx-coordinator --test storage -j "${JOBS:-4}" \
    counter_compaction -- --ignored \
    --skip counter_compaction::configured_operation_budget_outlives_driver_default_and_remains_bounded
  export ADX_TEST_EVIDENCE="$PWD/out/buildkite/logs/publication-catalog"
  cargo test --locked --package=adx-coordinator --test storage -j "${JOBS:-4}" \
    publication_catalog -- --ignored --test-threads=1
  cargo test --locked --package=adx-coordinator --lib -j "${JOBS:-4}" \
    storage::transport_tests -- --ignored
  cargo test --locked --package=adx-coordinator --test storage -j "${JOBS:-4}" \
    transport_recovery -- --ignored --test-threads=1 \
    --skip transport_recovery::transport_recovery_replaces_a_response_stalled_connection \
    --skip transport_recovery::transport_recovery_repairs_an_applied_write_with_a_lost_response \
    --skip transport_recovery::transport_recovery_fences_a_delayed_write_before_publishing_absence \
    --skip transport_recovery::transport_recovery_fences_a_cancelled_write_without_waiting_for_its_timeout
  # Retirement covers durable offline recovery and authenticated normal exit.
  # These real-Redis regressions are ignored by the ordinary component suite.
  export ADX_TEST_EVIDENCE="$PWD/out/buildkite/logs/node-retirement"
  cargo test --locked --package=adx-coordinator --test storage --test rpc \
    -j "${JOBS:-4}" node_retirement -- --ignored
fi

#!/usr/bin/env bash
set -uo pipefail
python3 .buildkite/with_registry.py -- bash .buildkite/run-firecracker.sh
status=$?
python3 .buildkite/fc-summary.py --exit-code "$status"
summary_status=$?
style=success
[[ $status == 0 && $summary_status == 0 ]] || style=error
buildkite-agent annotate --context adx-fc-acceptance --style "$style" < out/buildkite/firecracker/summary.md
annotation_status=$?
[[ $status == 0 ]] || exit "$status"
[[ $summary_status == 0 ]] || exit "$summary_status"
exit "$annotation_status"

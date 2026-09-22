#!/usr/bin/env bash
# Stream job output, retain it as an artifact, and publish a cumulative summary.
set -uo pipefail
stage=$1
shift
mkdir -p out/buildkite/logs out/buildkite/summaries
"$@" 2>&1 | tee "out/buildkite/logs/step-${stage}.log"
statuses=("${PIPESTATUS[@]}")
status=${statuses[0]}
if [[ $status == 0 && ${statuses[1]} != 0 ]]; then status=${statuses[1]}; fi
summary_status=0
case "$stage" in
  release|images|e2e)
    python3 .buildkite/summary.py --stage "$stage" --exit-code "$status"
    summary_status=$?
    if [[ $summary_status == 0 ]]; then
      style=success
      [[ $status == 0 ]] || style=error
      buildkite-agent artifact upload "out/buildkite/summaries/${stage}.*" || summary_status=$?
      buildkite-agent annotate --context adx-build-summary --style "$style" < "out/buildkite/summaries/${stage}.md" || summary_status=$?
    fi
    ;;
esac
if [[ $status != 0 ]]; then exit "$status"; fi
exit "$summary_status"

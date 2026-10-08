#!/usr/bin/env bash
# Explicit maintenance run: reuse CI registry auth, without compiling ADX.
set -euo pipefail
mkdir -p out/buildkite/collector-sync
exec > >(tee out/buildkite/collector-sync/sync.log) 2>&1
: "${ADX_E2E_IMAGE_REPOSITORY:?set the destination registry repository}"
daemon_pid=''
cleanup() {
  if [[ -n "$daemon_pid" ]]; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
if ! docker info >/dev/null 2>&1; then
  dockerd --host="${DOCKER_HOST:-unix:///var/run/docker.sock}" --storage-driver="${DOCKER_DRIVER:-overlay2}" > out/buildkite/collector-sync/dockerd.log 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then break; fi
    kill -0 "$daemon_pid" 2>/dev/null || { echo 'Docker daemon exited'; exit 1; }
    sleep 1
  done
  docker info >/dev/null
fi
digest=$(python3 -c 'import json; print(json.load(open("build/observability/source.json"))["platform_manifests"]["linux/amd64"])')
version=$(python3 -c 'import json; print(json.load(open("build/observability/source.json"))["version"])')
source_image="${ADX_COLLECTOR_SYNC_SOURCE:-ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib}@$digest"
target_image="${ADX_E2E_IMAGE_REPOSITORY}:collector-${version}-amd64-${digest:7:16}"
echo "--- Pull pinned linux/amd64 Collector: $source_image"
timeout 1200 docker pull --platform linux/amd64 "$source_image"
[[ $(docker image inspect "$source_image" --format '{{.Os}}/{{.Architecture}}') == linux/amd64 ]]
echo "--- Publish Collector: $target_image"
docker tag "$source_image" "$target_image"
timeout 600 docker push "$target_image"
echo "--- Verify remote digest: ${ADX_E2E_IMAGE_REPOSITORY}@$digest"
docker pull --platform linux/amd64 "${ADX_E2E_IMAGE_REPOSITORY}@$digest"
python3 - "$source_image" "$target_image" "${ADX_E2E_IMAGE_REPOSITORY}@$digest" <<'PY'
import json,sys
from pathlib import Path
source=json.loads(Path('build/observability/source.json').read_text())
result={'status':'passed','platform':'linux/amd64','upstream':source,'source':sys.argv[1],'tag':sys.argv[2],'image':sys.argv[3]}
Path('out/buildkite/collector-sync/result.json').write_text(json.dumps(result,indent=2)+'\n')
Path('out/buildkite/collector-sync/summary.md').write_text('### Collector SWR mirror\n\nVerified linux/amd64, upstream version '+source['version']+' and unchanged native manifest digest.\n\n`'+sys.argv[3]+'`\n')
print(json.dumps(result,indent=2))
PY
buildkite-agent annotate --context adx-collector-sync --style success < out/buildkite/collector-sync/summary.md

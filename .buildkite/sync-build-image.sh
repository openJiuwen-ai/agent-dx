#!/usr/bin/env bash
set -euo pipefail
: "${BUILDKITE_COMMIT:?Buildkite revision required}"

case "${1:-rust}" in
  rust)
    config=build/images/build-environment.json
    output=out/buildkite/build-image
    recipe_file=build/images/Dockerfile.ci
    verifier=/usr/local/bin/adx-verify-build-image
    ;;
  python)
    config=build/images/python-environment.json
    output=out/buildkite/python-build-image
    recipe_file=build/images/Dockerfile.python
    verifier=/usr/local/bin/adx-verify-python-image
    ;;
  *) echo 'unknown image kind' >&2; exit 2 ;;
esac
mkdir -p "$output"
daemon_pid=''
cleanup() {
  if [[ -n "$daemon_pid" ]]; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
if ! docker info >/dev/null 2>&1; then
  dockerd --host="${DOCKER_HOST:-unix:///var/run/docker.sock}" \
    --storage-driver="${DOCKER_DRIVER:-overlay2}" > "$output/dockerd.log" 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
      echo "Docker daemon exited; see $output/dockerd.log" >&2
      exit 1
    fi
    sleep 1
  done
  docker info >/dev/null
fi
readarray -t values < <(python3 - "$config" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
print(config['source_image'])
print(config['repository'])
print(config['platform'])
PY
)
source_image=${values[0]}
repository=${values[1]}
platform=${values[2]}
[[ $source_image == *@sha256:* ]]
[[ $repository != *:latest ]]

tag="$repository:${BUILDKITE_COMMIT:0:12}"
cache_tag="$repository:buildcache"
verify_image() {
  docker run --rm --platform "$platform" "$1" "$verifier"
}

docker pull "$cache_tag" >/dev/null 2>&1 || true
docker build --progress=plain --provenance=false --platform "$platform" \
  --build-arg BUILDKIT_INLINE_CACHE=1 --cache-from "$cache_tag" \
  --build-arg "BASE=$source_image" -f "$recipe_file" -t "$tag" .
verify_image "$tag"
docker push "$tag"
docker tag "$tag" "$cache_tag"
docker push "$cache_tag"
published=$(docker image inspect "$tag" --format '{{range .RepoDigests}}{{println .}}{{end}}' \
  | awk -v prefix="$repository@sha256:" 'index($0,prefix)==1 {print; exit}')
[[ $published == "$repository@sha256:"* ]]
docker image rm "$tag" >/dev/null
docker pull "$published"
verify_image "$published"

recipe_sha256=$(sha256sum "$recipe_file" | awk '{print $1}')
python3 - "$output/result.json" "$source_image" "$published" "$platform" "$recipe_sha256" <<'PY'
import json, os, sys
path, source, published, platform, recipe = sys.argv[1:]
result = {
    'schema_version': 1,
    'commit': os.environ['BUILDKITE_COMMIT'],
    'source_image': source,
    'reference': published,
    'platform': platform,
    'dockerfile_sha256': recipe,
}
open(path, 'w').write(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
PY

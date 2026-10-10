#!/usr/bin/env bash
set -euo pipefail
: "${BUILDKITE_COMMIT:?Buildkite revision required}"

case "${1:-rust}" in
  rust)
    config=${ADX_BUILD_IMAGE_CONFIG:-build/images/build-environment.json}
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
  if [[ $(uname -s) != Linux ]]; then
    echo "Docker is not running; non-Linux build image maintenance requires a native Docker service" >&2
    exit 1
  fi
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
config_assignments=$(python3 - "$config" <<'PY'
import json, sys
from shlex import quote
config = json.load(open(sys.argv[1]))
for key in ('source_image', 'repository', 'platform'):
    print(f"{key}={quote(str(config[key]))}")
PY
)
eval "$config_assignments"
[[ $source_image == *@sha256:* ]]
[[ $repository != *:latest ]]

arch=${platform#linux/}
suffix=""
[[ $arch == amd64 ]] || suffix="-$arch"
tag="$repository:${BUILDKITE_COMMIT:0:12}$suffix"
cache_tag="$repository:buildcache$suffix"
verify_image() {
  docker run --rm --platform "$platform" "$1" "$verifier"
}

normalize_platform() {
  case "$1" in
    linux/aarch64) echo linux/arm64 ;;
    linux/x86_64) echo linux/amd64 ;;
    *) echo "$1" ;;
  esac
}

capture_command() {
  local label=$1
  shift
  local output
  output=$("$@" 2>&1) || {
    local status=$?
    printf '%s failed:\n%s\n' "$label" "$output" >&2
    exit "$status"
  }
  printf '%s' "$output"
}

docker_version=$(capture_command 'docker version' docker version)
buildx_version=$(capture_command 'docker buildx version' docker buildx version)
buildx_inspect=$(capture_command 'docker buildx inspect --bootstrap' docker buildx inspect --bootstrap)
export ADX_DOCKER_VERSION="$docker_version"
export ADX_BUILDX_VERSION="$buildx_version"
export ADX_BUILDX_INSPECT="$buildx_inspect"
docker_platform=$(normalize_platform "$(docker info --format '{{.OSType}}/{{.Architecture}}')")
if [[ $docker_platform != "$platform" ]]; then
  echo "Docker daemon platform $docker_platform does not match requested $platform" >&2
  exit 1
fi
docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)
docker pull --platform "$platform" "$source_image"
source_probe=$(docker run --rm --platform "$platform" "$source_image" /bin/sh -c 'uname -m; df -Pk /')
export ADX_SOURCE_IMAGE_PROBE="$source_probe"
python3 - "$output/preflight.json" "$platform" "$docker_platform" "$docker_root" <<'PY'
import json, os, shutil, sys
path, requested, docker_platform, docker_root = sys.argv[1:]
def usage(path):
    try:
        total, used, free = shutil.disk_usage(path)
    except OSError:
        return None
    return {'path': path, 'total_bytes': total, 'free_bytes': free}
result = {
    'schema_version': 1,
    'commit': os.environ['BUILDKITE_COMMIT'],
    'requested_platform': requested,
    'docker_platform': docker_platform,
    'workspace_disk': usage(os.getcwd()),
    'docker_root_disk': usage(docker_root) if docker_root else None,
    'docker_version': os.environ.get('ADX_DOCKER_VERSION', ''),
    'buildx_version': os.environ.get('ADX_BUILDX_VERSION', ''),
    'buildx_inspect': os.environ.get('ADX_BUILDX_INSPECT', ''),
    'docker_probe': os.environ.get('ADX_SOURCE_IMAGE_PROBE', ''),
}
open(path, 'w').write(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
PY
docker pull "$cache_tag" >/dev/null 2>&1 || true
build_args=()
if [[ $arch == arm64 && ${1:-rust} == rust ]]; then
  build_args=(--build-arg GO_SHA256=b00b694903d126c588c378e72d3545549935d3982635ba3f7a964c9fa23fe3b9)
fi
if [[ ${1:-rust} == rust && -n ${ADX_EROFS_SOURCE_ARCHIVE:-} ]]; then
  [[ -f $ADX_EROFS_SOURCE_ARCHIVE ]]
  cp "$ADX_EROFS_SOURCE_ARCHIVE" build/images/source-cache/erofs-utils-1.8.10.tar.gz
fi
if [[ ${1:-rust} == rust && -n ${ADX_REDIS_SOURCE_ARCHIVE:-} ]]; then
  [[ -f $ADX_REDIS_SOURCE_ARCHIVE ]]
  cp "$ADX_REDIS_SOURCE_ARCHIVE" build/images/source-cache/redis-7.2.5.tar.gz
fi
docker build --progress=plain --provenance=false --platform "$platform" \
  --build-arg BUILDKIT_INLINE_CACHE=1 --cache-from "$cache_tag" \
  --build-arg "BASE=$source_image" --build-arg "TARGETARCH=$arch" "${build_args[@]}" -f "$recipe_file" -t "$tag" .
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

recipe_sha256=$(python3 - "$recipe_file" <<'PY'
import hashlib, sys
digest = hashlib.sha256()
with open(sys.argv[1], 'rb') as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b''):
        digest.update(chunk)
print(digest.hexdigest())
PY
)
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

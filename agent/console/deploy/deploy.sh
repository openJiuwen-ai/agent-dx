#!/usr/bin/env bash
# Repository-owned Console deployment. No external IaC checkout is required.
set -Eeuo pipefail
umask 077
REPO=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
WORK=${WORK:-$HOME/adx-console-cluster}
KUBECONFIG=${KUBECONFIG:-/home/wld/beijing/openjiuwen-adx-test-kubeconfig.yaml}
CONTEXT=${CONTEXT:-externalTLSVerify}
NAMESPACE=${NAMESPACE:-adx-wld}
RELEASE=${RELEASE:-adx-console}
IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-swr.cn-north-4.myhuaweicloud.com/openjiuwen-online-test/adx-agent-console}
PROXY_IMAGE=${PROXY_IMAGE:-nginx:stable-alpine}
CREDENTIALS_DIR=${CREDENTIALS_DIR:-$HOME/adx-wld}
PUBLIC_ORIGIN=${PUBLIC_ORIGIN:-https://localhost:28443}
MODE=deploy
BUILD=true
for arg in "$@"; do
  case "$arg" in
    --help|-h)
      printf '%s\n' 'Usage: bash deploy.sh [--prepare-only | --check-only] [--skip-build]' \
        'First deployment: set GATEWAY_URL, PLATFORM_URL (HTTPS, verified certificate SAN).' \
        'IMAGE_TAG: required with --skip-build; otherwise generated once per invocation.' \
        'WORK: private configuration directory; existing config and credentials are preserved.' \
        'Optional: MAPPINGS_FILE, TERMINAL_PROFILES_FILE, VALUES_FILE, IMAGE_PULL_SECRET.' \
        'Optional: TLS_CERT_FILE + TLS_KEY_FILE for browser HTTPS; otherwise localhost test certificate.' \
        '--prepare-only: create local configuration; no cluster changes/build/push.' \
        '--check-only: validate existing files and Helm render; no writes or cluster changes.'
      exit 0 ;;
    --prepare-only) MODE=prepare ;;
    --check-only) MODE=check ;;
    --skip-build) BUILD=false ;;
    *) printf 'Unknown argument: %s\n' "$arg" >&2; exit 2 ;;
  esac
done
export REPO WORK CREDENTIALS_DIR PUBLIC_ORIGIN
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
for tool in python3 helm openssl; do command -v "$tool" >/dev/null || die "Missing tool: $tool"; done
if [[ $MODE != check ]]; then
  mkdir -p "$WORK/config" "$WORK/credentials" "$WORK/tls"
  chmod 700 "$WORK" "$WORK/config" "$WORK/credentials" "$WORK/tls"
  python3 - <<'PY'
import getpass
import hashlib
import json
import os
import secrets
from pathlib import Path

root = Path(os.environ['WORK'])
path = root / 'config/config.json'
def save(path, data):
    if path.exists():
        path.chmod(0o600)
    with path.open('wb') as file:
        path.chmod(0o600)
        file.write(data)

if not path.exists():
    for key in ('GATEWAY_URL', 'PLATFORM_URL'):
        if not os.environ.get(key):
            raise SystemExit(f'First initialization requires {key}; verify upstream certificate SAN first')
    config = json.loads((Path(os.environ['REPO']) / 'agent/console/config.example.json').read_text())
    tokens = {}
    for identity in ('operator', 'wld'):
        token = getpass.getpass(f'Set {identity} Console login token: ')
        if not token or token != getpass.getpass('Confirm token: '):
            raise SystemExit('Tokens must be nonempty and match')
        tokens[identity] = hashlib.sha256(token.encode()).hexdigest()
    gateway = os.environ['GATEWAY_URL'].rstrip('/')
    config.update(listen='127.0.0.1:8088', public_origin=os.environ['PUBLIC_ORIGIN'],
                  upstream_ca_file='/run/secrets/ca.pem', shared_files=None,
                  terminal_profiles_file=None)
    config['agent_api']['base_url'] = gateway + '/api/agent/v2'
    config['apiserver']['base_url'] = os.environ['PLATFORM_URL'].rstrip('/')
    config['ingress']['base_url'] = gateway
    config['platform_admins'] = [{'admin_id': 'operator', 'display_name': 'Operator', 'token_sha256': tokens['operator']}]
    config['tenants'] = [{
        'tenant_id': 'wld', 'display_name': '北京 adx-wld', 'type': 'digitalmate',
        'admin_token_sha256': tokens['wld'], 'agent': {'enabled': True},
        'upstream_token_file': '/run/secrets/tenant-key',
        'business': {'accounts_api': {'base_url': gateway + '/api/accounts/v1/admin',
                                     'timeout_ms': 30000, 'token_file': '/run/secrets/tenant-key'},
                     'bindings': [{'kind': 'digitalmate', 'template': 'digitalmate',
                                   'ref': {'kind': 'alias', 'name': 'production'}},
                                  {'kind': 'orbit', 'template': 'orbit',
                                   'ref': {'kind': 'alias', 'name': 'stable'}}]}}]
    # Check all source credentials before writing configuration.
    source = Path(os.environ['CREDENTIALS_DIR'])
    credentials = {name: (source / name).read_bytes() for name in ('ca.pem', 'tenant-key')}
    if any(not data.strip() for data in credentials.values()):
        raise SystemExit('Empty upstream credential')
    for name, data in credentials.items():
        save(root / 'credentials' / name, data)
    session = root / 'credentials/console-session'
    if not session.exists():
        save(session, secrets.token_bytes(32))
    save(path, json.dumps(config, ensure_ascii=False, indent=2).encode())
else:
    config = json.loads(path.read_text())
    print('Preserving existing config, login hashes and credentials; edit WORK files to change them.')
for variable, filename, field in (
        ('MAPPINGS_FILE', 'storage-mappings.json', 'shared_files'),
        ('TERMINAL_PROFILES_FILE', 'terminal-profiles.json', 'terminal_profiles_file')):
    if os.environ.get(variable):
        data = Path(os.environ[variable]).read_bytes()
        json.loads(data)
        save(root / 'config' / filename, data)
        config[field] = ({'mappings_file': '/etc/adx-console/' + filename, 'directory_entries_limit': 1000}
                         if field == 'shared_files' else '/etc/adx-console/' + filename)
        save(path, json.dumps(config, ensure_ascii=False, indent=2).encode())
print('Local configuration prepared; login tokens are not printed.')
PY
  if [[ -n ${TLS_CERT_FILE:-} || -n ${TLS_KEY_FILE:-} ]]; then
    [[ -f ${TLS_CERT_FILE:-} && -f ${TLS_KEY_FILE:-} ]] || die 'Provide both TLS_CERT_FILE and TLS_KEY_FILE'
    install -m 600 "$TLS_CERT_FILE" "$WORK/tls/tls.crt"
    install -m 600 "$TLS_KEY_FILE" "$WORK/tls/tls.key"
  elif [[ ! -f $WORK/tls/tls.crt && ! -f $WORK/tls/tls.key ]]; then
    [[ $PUBLIC_ORIGIN == https://localhost:* ]] || die 'Non-localhost origin requires TLS_CERT_FILE and TLS_KEY_FILE'
    openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 7 \
      -keyout "$WORK/tls/tls.key" -out "$WORK/tls/tls.crt" \
      -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
  fi
fi
# Validate local input without printing credentials. This is not the Rust config validator.
export RELEASE IMAGE_REPOSITORY PROXY_IMAGE
python3 - <<'PY'
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
root = Path(os.environ['WORK'])
c = json.loads((root / 'config/config.json').read_text())
if c['listen'] != '127.0.0.1:8088':
    raise SystemExit('Chart requires listen=127.0.0.1:8088')
for url in (c['public_origin'], c['agent_api']['base_url'], c['apiserver']['base_url'], c['ingress']['base_url']):
    p = urlsplit(url)
    if p.scheme != 'https' or not p.hostname or p.username or p.password or p.query or p.fragment:
        raise SystemExit('Invalid HTTPS endpoint')
if len((root / 'credentials/console-session').read_bytes()) < 32:
    raise SystemExit('Session secret must contain at least 32 bytes')
for name in ('ca.pem', 'tenant-key'):
    if not (root / 'credentials' / name).read_bytes().strip():
        raise SystemExit('Empty credential: ' + name)
for field in ('shared_files', 'terminal_profiles_file'):
    if c.get(field):
        filename = c[field]['mappings_file'] if field == 'shared_files' else c[field]
        prefix = '/etc/adx-console/'
        if not filename.startswith(prefix) or '/' in filename[len(prefix):]:
            raise SystemExit('Optional configuration must be a direct file under /etc/adx-console')
        data = json.loads((root / 'config' / filename[len(prefix):]).read_text())
        if field == 'shared_files':
            if not data['revision'] or not data['domains']:
                raise SystemExit('Empty storage mapping')
            for domain in data['domains']:
                if domain['read_only_root'] != '/srv/adx-shared' or not domain.get('mount_source'):
                    raise SystemExit('Expected /srv/adx-shared and verified mount_source')
                if '<' in json.dumps(domain):
                    raise SystemExit('Replace storage mapping placeholders')
print('Local input checks passed; SAN/Volume/permissions require deployed verification.')
PY
openssl x509 -in "$WORK/credentials/ca.pem" -noout >/dev/null
openssl x509 -in "$WORK/tls/tls.crt" -checkend 0 -noout >/dev/null || die 'Browser certificate missing or expired'
cert_key=$(openssl x509 -in "$WORK/tls/tls.crt" -pubkey -noout)
private_key=$(openssl pkey -in "$WORK/tls/tls.key" -pubout)
[[ $cert_key == "$private_key" ]] || die 'Browser certificate/private key mismatch'
if [[ $MODE == prepare ]]; then
  printf 'Prepared: %s\nNext: edit config/optional profiles/mappings and run this script to deploy.\n' "$WORK"
  exit 0
fi
if [[ -z ${IMAGE_TAG:-} ]]; then
  [[ $BUILD == true && $MODE == deploy ]] || die 'Set IMAGE_TAG when using --skip-build or --check-only'
  IMAGE_TAG="console-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "$REPO" rev-parse --short HEAD)"
fi
[[ $IMAGE_TAG != *REPLACE* ]] || die 'Replace placeholder IMAGE_TAG'
args=(-f "$REPO/agent/console/deploy/values.yaml")
[[ -z ${VALUES_FILE:-} ]] || args+=(-f "$VALUES_FILE")
args+=(--set-string "image.repository=$IMAGE_REPOSITORY" --set-string "image.tag=$IMAGE_TAG" --set-string "proxyImage=$PROXY_IMAGE")
[[ -z ${IMAGE_PULL_SECRET:-} ]] || args+=(--set-string "imagePullSecrets[0].name=$IMAGE_PULL_SECRET")
shared=$(python3 -c 'import json,os;from pathlib import Path;print(str(bool(json.loads((Path(os.environ["WORK"])/"config/config.json").read_text()).get("shared_files"))).lower())')
args+=(--set "sharedStorage.enabled=$shared" --set-string sharedStorage.claimName=sfs-workspaces
  --set-string sharedStorage.mountPath=/srv/adx-shared
  --set-string configSecret=adx-console-config --set-string credentialsSecret=adx-console-credentials
  --set-string tlsSecret=adx-console-browser-tls)
helm lint "$REPO/agent/console/deploy" "${args[@]}"
if [[ $MODE == check ]]; then
  helm template "$RELEASE" "$REPO/agent/console/deploy" -n "$NAMESPACE" "${args[@]}" >/dev/null
  printf '%s\n' 'Check passed; no build, cluster writes or file changes performed.'
  exit 0
fi
for tool in kubectl curl; do command -v "$tool" >/dev/null || die "Missing tool: $tool"; done
k() { kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT" -n "$NAMESPACE" "$@"; }
k get namespace "$NAMESPACE" >/dev/null
k get service ingress apiserver >/dev/null
[[ $shared != true ]] || k get pvc sfs-workspaces >/dev/null
if [[ $BUILD == true ]]; then
  command -v docker >/dev/null || die 'Missing docker'
  docker build -f "$REPO/build/images/Dockerfile.console" -t "$IMAGE_REPOSITORY:$IMAGE_TAG" "$REPO"
  docker push "$IMAGE_REPOSITORY:$IMAGE_TAG"
  docker image inspect "$IMAGE_REPOSITORY:$IMAGE_TAG" --format '{{json .RepoDigests}}'
fi
# Explicit file allowlist prevents uploading unrelated backups from WORK.
config_args=(--from-file="config.json=$WORK/config/config.json")
for name in storage-mappings.json terminal-profiles.json; do
  [[ ! -f $WORK/config/$name ]] || config_args+=(--from-file="$name=$WORK/config/$name")
done
helm template "$RELEASE" "$REPO/agent/console/deploy" -n "$NAMESPACE" "${args[@]}" > "$WORK/rendered.yaml"
k apply --dry-run=server -f "$WORK/rendered.yaml" >/dev/null
k create secret generic adx-console-config "${config_args[@]}" --dry-run=client -o yaml | k apply -f -
k create secret generic adx-console-credentials \
  --from-file="ca.pem=$WORK/credentials/ca.pem" --from-file="tenant-key=$WORK/credentials/tenant-key" \
  --from-file="console-session=$WORK/credentials/console-session" --dry-run=client -o yaml | k apply -f -
k create secret tls adx-console-browser-tls --cert="$WORK/tls/tls.crt" --key="$WORK/tls/tls.key" --dry-run=client -o yaml | k apply -f -
helm upgrade --install "$RELEASE" "$REPO/agent/console/deploy" \
  --kubeconfig "$KUBECONFIG" --kube-context "$CONTEXT" -n "$NAMESPACE" \
  "${args[@]}" --wait --timeout 5m
# External Secret/subPath updates must reload even when the Pod spec did not change.
k rollout restart "deployment/$RELEASE"
k rollout status "deployment/$RELEASE" --timeout=180s
k get pods -l "app.kubernetes.io/name=$RELEASE" -o wide
printf '\nDeployed image: %s:%s\n' "$IMAGE_REPOSITORY" "$IMAGE_TAG"
printf 'Access (leave running):\nkubectl --kubeconfig %q --context %q -n %q port-forward service/%q 28443:8443 --address=127.0.0.1\n' "$KUBECONFIG" "$CONTEXT" "$NAMESPACE" "$RELEASE"
printf '%s\n' 'Open configured public_origin + /console/. Verify login, templates, files, PTY and Accounts separately.'

#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
gateway_dir="${repo_root}/gateway"
bin_dir=${1:-"${repo_root}/target/release"}
etcd_image=${ADX_GATEWAY_MOCK_ETCD_IMAGE:-quay.io/coreos/etcd:v3.5.24}
container_name="adx-data-plane-gateway-etcd-$$"
mock_dir=$(mktemp -d /tmp/adx-data-plane-gateway-mock.XXXXXX)
service_log_dir="${mock_dir}/service-logs"
mkdir -p "${service_log_dir}"
pids=()

cleanup() {
    for pid in "${pids[@]:-}"; do
        kill "${pid}" 2>/dev/null || true
        wait "${pid}" 2>/dev/null || true
    done
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
    rm -rf "${mock_dir}"
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_http_status() {
    local url=$1
    local expected=$2
    local attempt
    for attempt in $(seq 1 100); do
        if [[ $(curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "${url}" 2>/dev/null || true) == "${expected}" ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for ${url} status ${expected}" >&2
    return 1
}

host_ip=$(ipconfig getifaddr en0 || ipconfig getifaddr en1 || true)
if [[ -z "${host_ip}" ]]; then
    echo "cannot determine a non-loopback host IP for Node target admission" >&2
    exit 1
fi

etcd_port=$(free_port)
target_port=$(free_port)
node_port=$(free_port)
node_health_port=$(free_port)
ingress_tls_port=$(free_port)
ingress_plain_port=$(free_port)
ingress_health_port=$(free_port)
forward_port=$(free_port)
mock_token='e30.eyJzdWIiOiJtb2NrLXRlbmFudCIsImV4cCI6MH0.signature'

cat >"${mock_dir}/ingress-cert.conf" <<'EOF'
[req]
distinguished_name = subject
prompt = no
[subject]
CN = 127.0.0.1
EOF
cat >"${mock_dir}/ingress-cert.ext" <<'EOF'
[extensions]
subjectAltName = IP:127.0.0.1
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
EOF
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=adx-mock-ca \
    -keyout "${mock_dir}/ca.key" -out "${mock_dir}/ca.crt" >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes -config "${mock_dir}/ingress-cert.conf" \
    -keyout "${mock_dir}/ingress.key" -out "${mock_dir}/ingress.csr" >/dev/null 2>&1
openssl x509 -req -days 1 -in "${mock_dir}/ingress.csr" \
    -CA "${mock_dir}/ca.crt" -CAkey "${mock_dir}/ca.key" -CAcreateserial \
    -extfile "${mock_dir}/ingress-cert.ext" -extensions extensions \
    -out "${mock_dir}/ingress.crt" >/dev/null 2>&1

printf '%s\n' 'gateway-real-process-ok' > "${mock_dir}/index.html"
python3 -m http.server "${target_port}" --bind 0.0.0.0 --directory "${mock_dir}" \
    >"${mock_dir}/sandbox.log" 2>&1 &
pids+=("$!")

cat >"${mock_dir}/command_watch_mock.py" <<'PY'
import base64, hashlib, json, socket, struct, threading, time

def read_exact(conn, size):
    out = b""
    while len(out) < size:
        chunk = conn.recv(size - len(out))
        if not chunk: raise EOFError()
        out += chunk
    return out

def recv_frame(conn):
    first, second = read_exact(conn, 2)
    length = second & 0x7f
    if length == 126: length = struct.unpack("!H", read_exact(conn, 2))[0]
    elif length == 127: length = struct.unpack("!Q", read_exact(conn, 8))[0]
    mask = read_exact(conn, 4) if second & 0x80 else b""
    data = read_exact(conn, length)
    if mask: data = bytes(value ^ mask[index % 4] for index, value in enumerate(data))
    return first & 0x0f, data

def send_json(conn, value):
    data = json.dumps(value).encode()
    header = bytes((0x81, len(data))) if len(data) < 126 else bytes((0x81, 126)) + struct.pack("!H", len(data))
    conn.sendall(header + data)

def handle(conn):
    try:
        head = b""
        while b"\r\n\r\n" not in head: head += conn.recv(4096)
        headers = {}
        for line in head.decode().split("\r\n")[1:]:
            if ":" in line:
                key, value = line.split(":", 1); headers[key.lower()] = value.strip()
        accept = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n").encode())
        versions = {}
        while True:
            opcode, payload = recv_frame(conn)
            if opcode == 8: return
            if opcode != 1: continue
            request = json.loads(payload)
            for command_id in request.get("commandIds", []):
                if request.get("op") == "subscribe":
                    versions[command_id] = versions.get(command_id, 0) + 1
                    send_json(conn, {"op":"state","commandId":command_id,"status":"RUNNING","stateVersion":versions[command_id]})
                    time.sleep(0.05)
                    versions[command_id] += 1
                    send_json(conn, {"op":"state","commandId":command_id,"status":"SUCCEEDED","stateVersion":versions[command_id]})
    except (EOFError, OSError, KeyError, ValueError):
        pass
    finally:
        conn.close()

listener = socket.socket(); listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", 50090)); listener.listen()
while True:
    conn, _ = listener.accept(); threading.Thread(target=handle, args=(conn,), daemon=True).start()
PY
python3 "${mock_dir}/command_watch_mock.py" >"${mock_dir}/command-watch.log" 2>&1 &
pids+=("$!")

docker run -d --rm --name "${container_name}" \
    -p "127.0.0.1:${etcd_port}:2379" \
    "${etcd_image}" /usr/local/bin/etcd \
    --listen-client-urls=http://0.0.0.0:2379 \
    --advertise-client-urls=http://0.0.0.0:2379 \
    --listen-peer-urls=http://0.0.0.0:2380 \
    --initial-advertise-peer-urls=http://0.0.0.0:2380 \
    --initial-cluster=default=http://0.0.0.0:2380 >/dev/null

for attempt in $(seq 1 100); do
    if docker exec "${container_name}" /usr/local/bin/etcdctl \
        --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
        break
    fi
    if [[ ${attempt} == 100 ]]; then
        echo "etcd did not become healthy" >&2
        exit 1
    fi
    sleep 0.1
done

ADX_DATA_PLANE_RELAY_BIND="127.0.0.1:${node_port}" \
ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS="127.0.0.1:${node_port}" \
ADX_DATA_PLANE_RELAY_HEALTH_BIND="127.0.0.1:${node_health_port}" \
ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS="${host_ip}/32" \
ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS="127.0.0.1/32" \
ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \
ADX_DATA_PLANE_LOG_DIR="${service_log_dir}" \
ADX_DATA_PLANE_LOG_MAX_SIZE_MB=1 \
ADX_DATA_PLANE_LOG_MAX_FILES=2 \
ADX_DATA_PLANE_LOG_STDOUT=false \
"${bin_dir}/adx-relay" >"${mock_dir}/node.log" 2>&1 &
pids+=("$!")
wait_http_status "http://127.0.0.1:${node_health_port}/readyz" 200

route_key=/adx/route/business/adxk/mock-instance
running_route=$(printf '{"instanceID":"mock-instance","instanceStatus":{"code":3},"tenantID":"mock-tenant","sandboxID":"mock-sandbox","nodeProxyAddress":"127.0.0.1:%s","sandboxIP":"%s"}' "${node_port}" "${host_ip}")
docker exec "${container_name}" /usr/local/bin/etcdctl \
    --endpoints=http://127.0.0.1:2379 put "${route_key}" "${running_route}" >/dev/null

ADX_DATA_PLANE_INGRESS_ETCD_ENDPOINTS="http://127.0.0.1:${etcd_port}" \
ADX_DATA_PLANE_INGRESS_TLS_BIND="127.0.0.1:${ingress_tls_port}" \
ADX_DATA_PLANE_INGRESS_PLAIN_BIND="127.0.0.1:${ingress_plain_port}" \
ADX_DATA_PLANE_INGRESS_HEALTH_BIND="127.0.0.1:${ingress_health_port}" \
ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS="${host_ip}:${target_port}" \
ADX_DATA_PLANE_INGRESS_TLS_CERT="${mock_dir}/ingress.crt" \
ADX_DATA_PLANE_INGRESS_TLS_KEY="${mock_dir}/ingress.key" \
ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \
ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS="127.0.0.1/32" \
ADX_DATA_PLANE_INGRESS_VALIDATE_IAM=0 \
ADX_DATA_PLANE_INGRESS_DIRECT_PORT="${target_port}" \
ADX_DATA_PLANE_LOG_DIR="${service_log_dir}" \
ADX_DATA_PLANE_LOG_MAX_SIZE_MB=1 \
ADX_DATA_PLANE_LOG_MAX_FILES=2 \
ADX_DATA_PLANE_LOG_STDOUT=false \
ADX_DATA_PLANE_INGRESS_ACCESS_LOG_ENABLED=true \
"${bin_dir}/adx-ingress" >"${mock_dir}/ingress.log" 2>&1 &
pids+=("$!")
if ! wait_http_status "http://127.0.0.1:${ingress_health_port}/readyz" 200; then
    tail -n 120 "${mock_dir}/ingress.log" >&2 || true
    exit 1
fi

plain_direct_status=$(curl -sS -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:${ingress_plain_port}/direct/mock-instance/index.html")
[[ ${plain_direct_status} == 426 ]]

plain_control_status=$(curl -sS -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:${ingress_plain_port}/")
[[ ${plain_control_status} == 426 ]]

control_response=$(printf 'GET / HTTP/1.1\r\nHost: public.example.test\r\nConnection: close\r\n\r\n' | \
    openssl s_client -quiet -connect "127.0.0.1:${ingress_tls_port}" \
        -CAfile "${mock_dir}/ca.crt" -verify_return_error 2>/dev/null)
[[ ${control_response} == *"HTTP/1.1 200"* ]]
[[ ${control_response} == *gateway-real-process-ok* ]]

tls_direct_without_token=$(printf 'GET /direct/mock-instance/index.html HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' | \
    openssl s_client -quiet -connect "127.0.0.1:${ingress_tls_port}" \
        -CAfile "${mock_dir}/ca.crt" -verify_return_error 2>/dev/null || true)
[[ ${tls_direct_without_token} == *"HTTP/1.1 401"* ]]

direct_response=$(printf 'GET /direct/mock-instance/index.html HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n' \
    "${mock_token}" | openssl s_client -quiet \
    -connect "127.0.0.1:${ingress_tls_port}" -CAfile "${mock_dir}/ca.crt" \
    -verify_return_error 2>/dev/null)
[[ ${direct_response} == *"HTTP/1.1 200"* ]]
[[ ${direct_response} == *gateway-real-process-ok* ]]

cat >"${mock_dir}/command_watch_client.py" <<'PY'
import asyncio
import sys
import time

from adx_sandbox._command_watch import manager_for
from adx_sandbox.types import ConnectionConfig

host, port, token, output = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
connection = ConnectionConfig(
    server_address=f"{host}:{port}",
    token=token,
    use_tls=True,
    verify_tls=False,
)
manager = manager_for(connection)

async def wait_for_commands():
    await asyncio.gather(*(
        manager.wait_async("mock-instance", command_id, 5)
        for command_id in ("cmd-mock-1", "cmd-mock-2")
    ))

asyncio.run(wait_for_commands())

open(output, "w").write("sdk-multiplex-watch-ok\n")
# Keep the hidden SDK connection observable long enough for metrics assertions.
time.sleep(2)
PY
PYTHONPATH="${repo_root}/platform/sdk/sandbox/python" uv run --with httpx --with websockets \
    python "${mock_dir}/command_watch_client.py" 127.0.0.1 "${ingress_tls_port}" \
    "${mock_token}" "${mock_dir}/command-watch-client.ok" \
    >"${mock_dir}/command-watch-client.log" 2>&1 &
pids+=("$!")
for attempt in $(seq 1 100); do
    [[ -s "${mock_dir}/command-watch-client.ok" ]] && break
    if [[ ${attempt} == 100 ]]; then
        echo "multiplexed command watch did not converge" >&2
        cat "${mock_dir}/command-watch-client.log" >&2 || true
        exit 1
    fi
    sleep 0.1
done
watch_metrics=$(curl -sS "http://127.0.0.1:${ingress_health_port}/metrics")
[[ ${watch_metrics} == *"data_plane_ingress_active_sessions 1"* ]]
[[ ${watch_metrics} == *"command_watch_connections 1"* ]]
[[ ${watch_metrics} == *"command_watch_subscriptions 2"* ]]
[[ ${watch_metrics} == *"command_watch_sandboxes 1"* ]]
[[ ${watch_metrics} == *"command_watch_downstream_streams 1"* ]]

"${bin_dir}/adx-data-plane-forward" \
    port-forward "127.0.0.1:${ingress_plain_port}" mock-instance \
    "${target_port}" "127.0.0.1:${forward_port}" \
    >"${mock_dir}/forward.log" 2>&1 &
pids+=("$!")
if ! wait_http_status "http://127.0.0.1:${forward_port}/index.html" 200; then
    tail -n 120 "${mock_dir}/forward.log" >&2 || true
    tail -n 120 "${mock_dir}/ingress.log" >&2 || true
    exit 1
fi

token_route=$(printf '{"instanceID":"mock-instance","instanceStatus":{"code":3},"tenantID":"mock-tenant","sandboxID":"mock-sandbox","nodeProxyAddress":"127.0.0.1:%s","sandboxIP":"%s","portForwardRoutes":[{"targetPort":%s,"authMode":"TOKEN"}]}' "${node_port}" "${host_ip}" "${target_port}")
docker exec "${container_name}" /usr/local/bin/etcdctl \
    --endpoints=http://127.0.0.1:2379 put "${route_key}" "${token_route}" >/dev/null
for attempt in $(seq 1 100); do
    plain_status=$(curl -sS -o /dev/null -w '%{http_code}' \
        "http://127.0.0.1:${ingress_plain_port}/mock-instance/${target_port}/index.html" || true)
    if [[ ${plain_status} == 426 ]]; then
        break
    fi
    if [[ ${attempt} == 100 ]]; then
        echo "Ingress did not apply the target-port authentication policy" >&2
        exit 1
    fi
    sleep 0.1
done

tls_port_without_token=$(printf 'GET /mock-instance/%s/index.html HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' \
    "${target_port}" | openssl s_client -quiet \
    -connect "127.0.0.1:${ingress_tls_port}" -CAfile "${mock_dir}/ca.crt" \
    -verify_return_error 2>/dev/null || true)
[[ ${tls_port_without_token} == *"HTTP/1.1 401"* ]]

tls_port_with_token=$(printf 'GET /mock-instance/%s/index.html HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n' \
    "${target_port}" "${mock_token}" | openssl s_client -quiet \
    -connect "127.0.0.1:${ingress_tls_port}" -CAfile "${mock_dir}/ca.crt" \
    -verify_return_error 2>/dev/null)
[[ ${tls_port_with_token} == *"HTTP/1.1 200"* ]]

docker exec "${container_name}" /usr/local/bin/etcdctl \
    --endpoints=http://127.0.0.1:2379 put "${route_key}" "${running_route}" >/dev/null

failed_route=$(printf '{"instanceID":"mock-instance","instanceStatus":{"code":5,"msg":"mock sandbox stopped","errCode":42},"tenantID":"mock-tenant","sandboxID":"mock-sandbox","nodeProxyAddress":"127.0.0.1:%s","sandboxIP":"%s"}' "${node_port}" "${host_ip}")
docker exec "${container_name}" /usr/local/bin/etcdctl \
    --endpoints=http://127.0.0.1:2379 put "${route_key}" "${failed_route}" >/dev/null
for attempt in $(seq 1 100); do
    status_response=$(printf 'GET /direct/mock-instance/index.html HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n' \
        "${mock_token}" | openssl s_client -quiet \
        -connect "127.0.0.1:${ingress_tls_port}" -CAfile "${mock_dir}/ca.crt" \
        -verify_return_error 2>/dev/null || true)
    if [[ ${status_response} == *"HTTP/1.1 409"* ]] && [[ ${status_response} == *"mock sandbox stopped"* ]]; then
        break
    fi
    if [[ ${attempt} == 100 ]]; then
        echo "Ingress did not apply the watched instance status" >&2
        exit 1
    fi
    sleep 0.1
done

for attempt in $(seq 1 50); do
    if grep -q 'event="request"' "${service_log_dir}/ingress-frontend-access.log" 2>/dev/null &&
       grep -q 'event="stream_close"' "${service_log_dir}/ingress-frontend-access.log" 2>/dev/null; then
        break
    fi
    if [[ ${attempt} == 50 ]]; then
        echo "Ingress access log did not contain request and stream-close audit records" >&2
        tail -n 120 "${service_log_dir}/ingress-frontend-access.log" >&2 || true
        exit 1
    fi
    sleep 0.1
done
[[ -s "${service_log_dir}/ingress-frontend.log" ]]
[[ -s "${service_log_dir}/relay.log" ]]
if grep -Eq 'adx_(access|audit)' "${service_log_dir}/ingress-frontend.log"; then
    echo "Ingress access/audit record was duplicated into the service log" >&2
    exit 1
fi
if grep -Fq "${mock_token}" "${service_log_dir}/ingress-frontend-access.log"; then
    echo "Ingress access log leaked the bearer token" >&2
    exit 1
fi

echo "real process mock passed: etcd watch, multiplexed command WSS, passive Ingress-to-Node watch stream, TLS/plain policy, static control proxy, per-port auth, Relay CONNECT, status propagation, access/audit logging"

#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
lima_home=${LIMA_HOME:-${HOME}/.lima-adx-local-3vm}
limactl=${LIMACTL:-limactl}
bin_dir=${ADX_DATA_PLANE_3VM_BIN_DIR:-${repo_root}/build/output/data_plane/bin}
etcd_bin_dir=${ADX_DATA_PLANE_3VM_ETCD_BIN_DIR:-${repo_root}/.adx-cache/data-plane-gateway-3vm/tools}
perf_bin=${ADX_DATA_PLANE_3VM_PERF_BIN:-${repo_root}/.adx-cache/data-plane-gateway-perf/bin/relay_perf}
run_id=${ADX_DATA_PLANE_3VM_RUN_ID:-$(date +%Y%m%d-%H%M%S)}
evidence_dir=${ADX_DATA_PLANE_3VM_EVIDENCE_DIR:-${repo_root}/.adx-cache/data-plane-gateway-3vm/${run_id}}
idle_seconds=${ADX_DATA_PLANE_3VM_IDLE_SECONDS:-65}
keep_running=${ADX_DATA_PLANE_3VM_KEEP_RUNNING:-false}

coordinator=adx-coordinator
worker1=adx-worker-1
worker2=adx-worker-2
remote_root=/tmp/adx-data-plane-3vm
token='e30.eyJzdWIiOiJtb2NrLXRlbmFudCIsImV4cCI6MH0.signature'
test_ok=false

export LIMA_HOME="${lima_home}"
mkdir -p "${evidence_dir}"

remote() {
    local node=$1
    shift
    local bypass="127.0.0.1,localhost,${coordinator_ip:-},${worker1_ip:-},${worker2_ip:-}"
    "${limactl}" shell "${node}" bash -lc \
        "export NO_PROXY='${bypass}' no_proxy='${bypass}'; $*"
}

# Lima user-v2 addresses are intentionally treated as dynamic and must be
# rediscovered after every VM restart.
coordinator_ip=$(remote "${coordinator}" "hostname -I | awk '{print \$1}'")
worker1_ip=$(remote "${worker1}" "hostname -I | awk '{print \$1}'")
worker2_ip=$(remote "${worker2}" "hostname -I | awk '{print \$1}'")

copy_to() {
    local source=$1
    local node=$2
    local destination=$3
    "${limactl}" copy --backend=scp "${source}" "${node}:${destination}"
}

collect() {
    set +e
    for node in "${coordinator}" "${worker1}" "${worker2}"; do
        mkdir -p "${evidence_dir}/${node}"
        "${limactl}" copy --backend=scp --recursive \
            "${node}:${remote_root}/logs" "${evidence_dir}/${node}/" >/dev/null 2>&1
    done
    "${limactl}" copy --backend=scp --recursive \
        "${coordinator}:${remote_root}/results" "${evidence_dir}/" >/dev/null 2>&1
    set -e
}

cleanup() {
    collect
    if [ "${keep_running}" = true ] || [ "${keep_running}" = 1 ]; then
        printf 'KEPT_RUNNING\n' >"${evidence_dir}/runtime-state.txt"
        printf '%s\n' "${evidence_dir}" >"${repo_root}/.adx-cache/data-plane-gateway-3vm/latest"
        return
    fi
    for node in "${coordinator}" "${worker1}" "${worker2}"; do
        remote "${node}" \
            "for pid_file in ${remote_root}/*.pid; do test -f \"\${pid_file}\" && kill \"\$(cat \"\${pid_file}\")\" 2>/dev/null || true; done" \
            >/dev/null 2>&1 || true
    done
    for node in "${worker1}" "${worker2}"; do
        remote "${node}" \
            "sudo ip netns delete adx-sandbox 2>/dev/null || true; sudo ip link delete yrsb-host 2>/dev/null || true" \
            >/dev/null 2>&1 || true
    done
    if "${test_ok}"; then
        printf 'PASS\n' >"${evidence_dir}/verdict.txt"
    else
        printf 'FAIL\n' >"${evidence_dir}/verdict.txt"
    fi
    printf '%s\n' "${evidence_dir}" >"${repo_root}/.adx-cache/data-plane-gateway-3vm/latest"
}
trap cleanup EXIT

for binary in adx-relay adx-ingress adx-data-plane-forward; do
    test -x "${bin_dir}/${binary}"
done
test -x "${etcd_bin_dir}/etcd"
test -x "${etcd_bin_dir}/etcdctl"
test -x "${perf_bin}"

for node in "${coordinator}" "${worker1}" "${worker2}"; do
    # A retained previous run may still own the fixed test ports. Terminate
    # only processes whose executable or interpreter argument is rooted in
    # this harness directory before replacing its pid files and certificates.
    remote "${node}" "
        sudo pkill -f '^${remote_root}/' 2>/dev/null || true
        sudo pkill -f '^python3 ${remote_root}/' 2>/dev/null || true
        sudo ip netns delete adx-sandbox 2>/dev/null || true
        sudo ip link delete yrsb-host 2>/dev/null || true
        rm -rf ${remote_root}
        mkdir -p ${remote_root}/bin ${remote_root}/logs ${remote_root}/results
    "
done

copy_to "${bin_dir}/adx-ingress" "${coordinator}" "${remote_root}/bin/adx-ingress"
copy_to "${bin_dir}/adx-data-plane-forward" "${coordinator}" "${remote_root}/bin/adx-data-plane-forward"
copy_to "${perf_bin}" "${coordinator}" "${remote_root}/bin/relay_perf"
copy_to "${etcd_bin_dir}/etcd" "${coordinator}" "${remote_root}/bin/etcd"
copy_to "${etcd_bin_dir}/etcdctl" "${coordinator}" "${remote_root}/bin/etcdctl"
for node in "${worker1}" "${worker2}"; do
    copy_to "${bin_dir}/adx-relay" "${node}" "${remote_root}/bin/adx-relay"
    copy_to "${perf_bin}" "${node}" "${remote_root}/bin/relay_perf"
    copy_to "${repo_root}/gateway/tests/idle_echo_server.py" \
        "${node}" "${remote_root}/idle_echo_server.py"
    copy_to "${repo_root}/gateway/tests/keepalive_http_server.py" \
        "${node}" "${remote_root}/keepalive_http_server.py"
    copy_to "${repo_root}/gateway/tests/process_resource_sampler.py" \
        "${node}" "${remote_root}/process_resource_sampler.py"
done
copy_to "${repo_root}/gateway/tests/idle_connection_probe.py" \
    "${coordinator}" "${remote_root}/idle_connection_probe.py"
copy_to "${repo_root}/gateway/tests/http_keepalive_bench.py" \
    "${coordinator}" "${remote_root}/http_keepalive_bench.py"
copy_to "${repo_root}/gateway/tests/process_resource_sampler.py" \
    "${coordinator}" "${remote_root}/process_resource_sampler.py"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=adx-ingress.local \
    -addext "subjectAltName=DNS:adx-ingress.local,IP:127.0.0.1,IP:${coordinator_ip}" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth" \
    -keyout "${evidence_dir}/ingress.key" -out "${evidence_dir}/ingress.crt" >/dev/null 2>&1
cp "${evidence_dir}/ingress.crt" "${evidence_dir}/ca.crt"
copy_to "${evidence_dir}/ca.crt" "${coordinator}" "${remote_root}/ca.crt"
copy_to "${evidence_dir}/ingress.crt" "${coordinator}" "${remote_root}/ingress.crt"
copy_to "${evidence_dir}/ingress.key" "${coordinator}" "${remote_root}/ingress.key"

start_sandbox() {
    local node=$1
    local host_ip=$2
    local sandbox_ip=$3
    local node_ip=$4
    remote "${node}" "
        sudo ip netns delete adx-sandbox 2>/dev/null || true
        sudo ip link delete yrsb-host 2>/dev/null || true
        mkdir -p ${remote_root}/sandbox
        printf '%s\n' 'three-vm-${node}' >${remote_root}/sandbox/small.txt
        truncate -s 33554432 ${remote_root}/sandbox/blob.bin
        sudo ip netns add adx-sandbox
        sudo ip link add yrsb-host type veth peer name yrsb-net
        sudo ip link set yrsb-net netns adx-sandbox
        sudo ip address add ${host_ip}/24 dev yrsb-host
        sudo ip link set yrsb-host up
        sudo ip netns exec adx-sandbox ip link set lo up
        sudo ip netns exec adx-sandbox ip address add ${sandbox_ip}/24 dev yrsb-net
        sudo ip netns exec adx-sandbox ip link set yrsb-net up
        sudo ip netns exec adx-sandbox bash -lc 'nohup python3 ${remote_root}/keepalive_http_server.py ${sandbox_ip} 18080 ${remote_root}/sandbox >${remote_root}/logs/sandbox-http.log 2>&1 & echo \$! >${remote_root}/sandbox-http.pid'
        sudo ip netns exec adx-sandbox bash -lc 'nohup python3 ${remote_root}/idle_echo_server.py ${sandbox_ip} 18081 >${remote_root}/logs/sandbox-echo.log 2>&1 & echo \$! >${remote_root}/sandbox-echo.pid'
        sudo ip netns exec adx-sandbox bash -lc 'nohup ${remote_root}/bin/relay_perf server ${sandbox_ip}:19001 >${remote_root}/logs/sandbox-relay-perf.log 2>&1 & echo \$! >${remote_root}/sandbox-relay-perf.pid'
        nohup python3 ${remote_root}/keepalive_http_server.py ${node_ip} 18082 ${remote_root}/sandbox >${remote_root}/logs/direct-http.log 2>&1 &
        echo \$! >${remote_root}/direct-http.pid
        nohup ${remote_root}/bin/relay_perf server ${node_ip}:19000 >${remote_root}/logs/direct-relay-perf.log 2>&1 &
        echo \$! >${remote_root}/direct-relay-perf.pid
    "
}

start_sandbox "${worker1}" 10.88.1.1 10.88.1.2 "${worker1_ip}"
start_sandbox "${worker2}" 10.88.2.1 10.88.2.2 "${worker2_ip}"

start_node() {
    local node=$1
    remote "${node}" "
        chmod 0755 ${remote_root}/bin/adx-relay
        nohup env \
          ADX_DATA_PLANE_RELAY_BIND=0.0.0.0:8443 \
          ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS=\$(hostname -I | awk '{print \$1}'):8443 \
          ADX_DATA_PLANE_RELAY_HEALTH_BIND=0.0.0.0:18443 \
          ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS=10.88.0.0/16 \
          ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS=${coordinator_ip}/32,127.0.0.1/32 \
          ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \
          ADX_DATA_PLANE_LOG_DIR=${remote_root}/logs \
          ADX_DATA_PLANE_LOG_MAX_SIZE_MB=1 \
          ADX_DATA_PLANE_LOG_MAX_FILES=2 \
          ADX_DATA_PLANE_LOG_STDOUT=false \
          RUST_LOG=info \
          ${remote_root}/bin/adx-relay >${remote_root}/logs/node-launcher.log 2>&1 &
        echo \$! >${remote_root}/relay.pid
        for attempt in \$(seq 1 100); do
          curl -fsS http://127.0.0.1:18443/readyz >/dev/null && exit 0
          sleep 0.1
        done
        exit 1
    "
}

start_node "${worker1}"
start_node "${worker2}"

remote "${coordinator}" "
    chmod 0755 ${remote_root}/bin/*
    mkdir -p ${remote_root}/etcd-data ${remote_root}/frontend
    printf '%s\n' control-plane-forwarded >${remote_root}/frontend/control.txt
    nohup ${remote_root}/bin/etcd \
      --data-dir=${remote_root}/etcd-data \
      --listen-client-urls=http://${coordinator_ip}:2379,http://127.0.0.1:2379 \
      --advertise-client-urls=http://${coordinator_ip}:2379 \
      --listen-peer-urls=http://127.0.0.1:2380 \
      --initial-advertise-peer-urls=http://127.0.0.1:2380 \
      --initial-cluster=default=http://127.0.0.1:2380 \
      >${remote_root}/logs/etcd.log 2>&1 &
    echo \$! >${remote_root}/etcd.pid
    nohup python3 -m http.server 18888 --bind 127.0.0.1 --directory ${remote_root}/frontend \
      >${remote_root}/logs/frontend-mock.log 2>&1 &
    echo \$! >${remote_root}/frontend.pid
    for attempt in \$(seq 1 100); do
      ${remote_root}/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1 && exit 0
      sleep 0.1
    done
    exit 1
"

route1=$(printf '{"instanceID":"vm-sandbox-1","instanceStatus":{"code":3},"tenantID":"mock-tenant","sandboxID":"vm-sandbox-1","nodeProxyAddress":"%s:8443","sandboxIP":"10.88.1.2"}' "${worker1_ip}")
route2=$(printf '{"instanceID":"vm-sandbox-2","instanceStatus":{"code":3},"tenantID":"mock-tenant","sandboxID":"vm-sandbox-2","nodeProxyAddress":"%s:8443","sandboxIP":"10.88.2.2"}' "${worker2_ip}")
remote "${coordinator}" "${remote_root}/bin/etcdctl --endpoints=http://127.0.0.1:2379 put /adx/route/business/adxk/vm-sandbox-1 '${route1}' >/dev/null"
remote "${coordinator}" "${remote_root}/bin/etcdctl --endpoints=http://127.0.0.1:2379 put /adx/route/business/adxk/vm-sandbox-2 '${route2}' >/dev/null"

remote "${coordinator}" "
    nohup env \
      ADX_DATA_PLANE_INGRESS_ETCD_ENDPOINTS=http://${coordinator_ip}:2379 \
      ADX_DATA_PLANE_INGRESS_TLS_BIND=0.0.0.0:8443 \
      ADX_DATA_PLANE_INGRESS_PLAIN_BIND=0.0.0.0:8080 \
      ADX_DATA_PLANE_INGRESS_HEALTH_BIND=0.0.0.0:18080 \
      ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS=127.0.0.1:18888 \
      ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES=exact:/control.txt \
      ADX_DATA_PLANE_INGRESS_TLS_CERT=${remote_root}/ingress.crt \
      ADX_DATA_PLANE_INGRESS_TLS_KEY=${remote_root}/ingress.key \
      ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \
      ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS=127.0.0.0/8,192.168.104.0/24 \
      ADX_DATA_PLANE_INGRESS_VALIDATE_IAM=false \
      ADX_DATA_PLANE_INGRESS_DIRECT_PORT=18080 \
      ADX_DATA_PLANE_LOG_DIR=${remote_root}/logs \
      ADX_DATA_PLANE_LOG_MAX_SIZE_MB=1 \
      ADX_DATA_PLANE_LOG_MAX_FILES=2 \
      ADX_DATA_PLANE_LOG_STDOUT=false \
      ADX_DATA_PLANE_INGRESS_ACCESS_LOG_ENABLED=true \
      RUST_LOG=info \
      ${remote_root}/bin/adx-ingress >${remote_root}/logs/ingress-launcher.log 2>&1 &
    echo \$! >${remote_root}/ingress-frontend.pid
    for attempt in \$(seq 1 100); do
      curl -fsS http://127.0.0.1:18080/readyz >/dev/null && exit 0
      sleep 0.1
    done
    exit 1
"

remote "${coordinator}" "
    set -e
    status=\$(curl -sS -o /dev/null -w '%{http_code}' --cacert ${remote_root}/ca.crt \
      https://127.0.0.1:8443/direct/vm-sandbox-1/small.txt)
    test \"\${status}\" = 401
    curl -fsS --cacert ${remote_root}/ca.crt \
      -H 'Authorization: Bearer ${token}' \
      https://127.0.0.1:8443/direct/vm-sandbox-1/small.txt \
      | grep -q three-vm-adx-worker-1
    curl -fsS --cacert ${remote_root}/ca.crt \
      https://127.0.0.1:8443/control.txt | grep -q control-plane-forwarded
    curl -fsS http://${worker1_ip}:18443/metrics >${remote_root}/results/worker1-metrics-before.txt
    curl -fsS http://${worker2_ip}:18443/metrics >${remote_root}/results/worker2-metrics-before.txt
"

remote "${coordinator}" "
    nohup ${remote_root}/bin/adx-data-plane-forward port-forward \
      127.0.0.1:8080 vm-sandbox-2 18080 127.0.0.1:19080 \
      >${remote_root}/logs/forward-http.log 2>&1 &
    echo \$! >${remote_root}/forward-http.pid
    nohup ${remote_root}/bin/adx-data-plane-forward port-forward \
      127.0.0.1:8080 vm-sandbox-2 18081 127.0.0.1:19081 \
      >${remote_root}/logs/forward-idle.log 2>&1 &
    echo \$! >${remote_root}/forward-idle.pid
    for attempt in \$(seq 1 100); do
      curl -fsS http://127.0.0.1:19080/small.txt >/dev/null && exit 0
      sleep 0.1
    done
    exit 1
"

remote "${coordinator}" "python3 ${remote_root}/idle_connection_probe.py 127.0.0.1 19081 ${idle_seconds} | tee ${remote_root}/results/idle.txt"

remote "${coordinator}" "
    set -e
    python3 ${remote_root}/http_keepalive_bench.py \
      http://${worker1_ip}:18082/small.txt --requests 200 --warmup 10 \
      | tee ${remote_root}/results/direct-keepalive-c1.json
    python3 ${remote_root}/http_keepalive_bench.py \
      https://127.0.0.1:8443/direct/vm-sandbox-1/small.txt \
      --requests 200 --warmup 10 --ca ${remote_root}/ca.crt --token '${token}' \
      | tee ${remote_root}/results/ingress-tls-keepalive-c1.json
    python3 ${remote_root}/http_keepalive_bench.py \
      http://${worker1_ip}:18082/blob.bin --requests 4 --warmup 0 \
      | tee ${remote_root}/results/direct-throughput-c1.json
    python3 ${remote_root}/http_keepalive_bench.py \
      https://127.0.0.1:8443/direct/vm-sandbox-1/blob.bin \
      --requests 4 --warmup 0 --ca ${remote_root}/ca.crt --token '${token}' \
      | tee ${remote_root}/results/ingress-tls-throughput-c1.json
    python3 ${remote_root}/http_keepalive_bench.py \
      http://${worker1_ip}:18082/blob.bin --requests 16 --concurrency 8 --warmup 0 \
      | tee ${remote_root}/results/direct-throughput-c8.json
    python3 ${remote_root}/http_keepalive_bench.py \
      https://127.0.0.1:8443/direct/vm-sandbox-1/blob.bin \
      --requests 16 --concurrency 8 --warmup 0 --ca ${remote_root}/ca.crt --token '${token}' \
      | tee ${remote_root}/results/ingress-tls-throughput-c8.json
    ${remote_root}/bin/relay_perf bench-direct ${worker1_ip}:19000 download 33554432 4 1 \
      | tee ${remote_root}/results/raw-direct-c1.json
    ${remote_root}/bin/relay_perf bench-node ${worker1_ip}:8443 10.88.1.2 19001 download 33554432 4 1 \
      | tee ${remote_root}/results/raw-node-h2-c1.json
    ${remote_root}/bin/relay_perf bench-ingress 127.0.0.1:8080 vm-sandbox-1 19001 download 33554432 4 1 \
      | tee ${remote_root}/results/raw-ingress-node-c1.json
    ${remote_root}/bin/relay_perf bench-direct ${worker1_ip}:19000 download 33554432 16 8 \
      | tee ${remote_root}/results/raw-direct-c8.json
    ${remote_root}/bin/relay_perf bench-node ${worker1_ip}:8443 10.88.1.2 19001 download 33554432 16 8 \
      | tee ${remote_root}/results/raw-node-h2-c8.json
    ${remote_root}/bin/relay_perf bench-ingress 127.0.0.1:8080 vm-sandbox-1 19001 download 33554432 16 8 \
      | tee ${remote_root}/results/raw-ingress-node-c8.json
    curl -fsS http://${worker1_ip}:18443/metrics >${remote_root}/results/worker1-metrics-after.txt
    curl -fsS http://${worker2_ip}:18443/metrics >${remote_root}/results/worker2-metrics-after.txt
    awk '/data_plane_relay_bytes_down / {print \$2}' ${remote_root}/results/worker1-metrics-after.txt \
      | awk '{if (\$1 <= 0) exit 1}'
"

remote "${coordinator}" "
    set -e
    for batch in \$(seq 1 12); do
      seq 1 500 | xargs -P 24 -I@ \
        curl -sS -o /dev/null http://127.0.0.1:8080/direct/vm-sandbox-1/small.txt
      test -f ${remote_root}/logs/ingress-frontend-access.log.1.gz && break
    done
    test -s ${remote_root}/logs/ingress-frontend-access.log
    test -s ${remote_root}/logs/ingress-frontend-access.log.1.gz
    test ! -e ${remote_root}/logs/ingress-frontend-access.log.3.gz
    test -s ${remote_root}/logs/ingress-frontend.log
    { cat ${remote_root}/logs/ingress-frontend-access.log; gzip -cd ${remote_root}/logs/ingress-frontend-access.log.*.gz; } \
      >${remote_root}/results/ingress-access-combined.log
    ! grep -F '${token}' ${remote_root}/results/ingress-access-combined.log
    grep 'event=\"request\"' ${remote_root}/results/ingress-access-combined.log >/dev/null
    grep 'event=\"stream_close\"' ${remote_root}/results/ingress-access-combined.log >/dev/null
    curl -fsS http://127.0.0.1:18080/metrics >${remote_root}/results/ingress-metrics.txt
    ls -l ${remote_root}/logs >${remote_root}/results/log-files.txt
"

remote "${worker1}" "test -s ${remote_root}/logs/relay.log"
remote "${worker2}" "test -s ${remote_root}/logs/relay.log"

test_ok=true
echo "local 3VM data-plane E2E passed; evidence=${evidence_dir}"

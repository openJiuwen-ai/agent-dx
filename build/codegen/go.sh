#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
server="$root/platform/control-plane/sandbox-api"
proto="$root/platform/api/proto/legacy/frontend"
# protoc-gen-go v1.36.6 and protoc-gen-go-grpc v1.5.1 are the source pins.
# Compatible newer generator versions may be used explicitly by the caller.
mkdir -p "$server/internal/gen"
protoc -I "$proto" --go_out="$server" --go_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api \
  --go-grpc_out="$server" --go-grpc_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api "$proto"/*.proto

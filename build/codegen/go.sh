#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
server="$root/platform/control-plane/sandbox-api"
proto="$root/platform/api/proto/legacy/frontend"
# protoc-gen-go v1.36.6 and protoc-gen-go-grpc v1.5.1 are the source pins.
# Compatible newer generator versions may be used explicitly by the caller.
# internal/gen is ignored generated output. Remove stale service stubs so an old
# RuntimeRPC/CoreService client cannot survive a protocol-generation change.
rm -rf "$server/internal/gen"
mkdir -p "$server/internal/gen"
protoc -I "$proto" --go_out="$server" --go_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api \
  --go-grpc_out="$server" --go-grpc_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api "$proto"/*.proto
protoc -I "$root/platform/api/proto" --go_out="$server" --go_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api \
  --go-grpc_out="$server" --go-grpc_opt=module=gitcode.com/robbluo/agent-dx/platform/control-plane/sandbox-api "$root/platform/api/proto/control.proto" "$root/platform/api/proto/node.proto"

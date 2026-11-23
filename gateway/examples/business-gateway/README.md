# 从业务网关访问 Agent v2

本例把本地演示登录凭据映射到用户 alice，通过一个绑定转发 HTTP 与 WebSocket。沙箱检查收到的业务 Authorization，并检查平台头已经移除。示例仅监听开发机回环地址。

## 准备

需要 Go 1.22 或以上、Docker、adx CLI、websocat，以及支持 X-ADX-AUTH 的 AgentDX HTTPS 入口。准备平台签发的租户密钥。如使用私有 CA，请将 ADX_CA_CERT（adx CLI）和 SSL_CERT_FILE（Go 示例）均指向 CA 的 PEM 文件。以下命令均在本目录执行。ECHO_IMAGE 指向当前租户的节点可以拉取的仓库和新标签。

```sh
export ADX_SERVER_ADDRESS=https://gateway.example.com:8443
export ECHO_IMAGE=registry.example.com/team/gateway-echo:v1
read -rs -p 'AgentDX tenant key: ' ADX_TOKEN
export ADX_TOKEN
docker build -t "$ECHO_IMAGE" .
docker push "$ECHO_IMAGE"
python3 - <<'PY' > /tmp/gateway-echo-template.json
import json, os
print(json.dumps({
    "name": "gateway-echo", "image": os.environ["ECHO_IMAGE"],
    "isolation_runtime": "runsc", "resources": {"cpu_millis": 500, "memory_mib": 256},
    "service": [{"protocol": p, "port": 8080} for p in ("http", "ws")],
    "readiness_probe": {"tcp_socket": {"port": 8080}, "period_seconds": 2}
}))
PY
adx template publish -f /tmp/gateway-echo-template.json
```

## 运行

把发布响应中的数字版本填入 DEMO_TEMPLATE_VERSION。首次会话流量将触发建绑与启动。每次使用同一版本和同一演示用户都会发送稳定的 Binding ID。

```sh
export DEMO_TEMPLATE_VERSION=1
export DEMO_SESSION=local-demo-alice
go run .
```

另开终端发送请求。

```sh
curl -fsS -H 'Authorization: Bearer local-demo-alice' http://127.0.0.1:8181/echo
websocat -H='Authorization: Bearer local-demo-alice' ws://127.0.0.1:8181/ws
```

HTTP 输出 `hello from sandbox`。在 WebSocket 终端输入 `hello` 并回车，应收到 `hello`。两个请求分别走 /agent/http 与 /agent/ws。示例在业务网关验证演示登录凭据后，重新构造到 AgentDX 的请求，使用 X-ADX-AUTH 传租户凭据，使用 Authorization 传演示沙箱凭据。实际业务应以自己的用户会话和沙箱授权替换这两个演示值。

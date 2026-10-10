# adxadmin

`adxadmin` manages an ADX cluster through its public HTTPS administration API.

```sh
pipx install ./adxadmin-0.1.0-py3-none-any.whl
adxadmin key create --tenant team-a
adxadmin key list --tenant team-a
```

HTTPS certificate and hostname verification is skipped by default; HTTPS and
administrator API Key authentication are still required for remote endpoints.
Pass `--verify-tls` to trust system CAs, or `--ca` to enable verification with a
PEM CA file.

Table output aligns columns by display width. Key expiration is shown as a
local date and time with a UTC offset (or `never`). JSON output and
`--expires-at` retain Unix seconds for automation.

See the
[administration guide](https://gitcode.com/openJiuwen/agent-dx/blob/refactor/docs/deployment/adxadmin.md)
for connection, credential, output and retry semantics.

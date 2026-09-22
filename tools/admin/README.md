# adxadmin

`adxadmin` manages an ADX cluster through its public HTTPS administration API.

```sh
pipx install ./adxadmin-0.1.0-py3-none-any.whl
adxadmin key create --tenant team-a
adxadmin key list --tenant team-a
```

See the repository [administration guide](../../docs/deployment/adxadmin.md) for
connection, credential, output and retry semantics.

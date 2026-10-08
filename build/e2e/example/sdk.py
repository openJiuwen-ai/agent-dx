#!/usr/bin/env python3
"""Exercise the installed SDK, leaving one instance for documented CLI stop."""
import json,os,pathlib,sys
os.environ['SSL_CERT_FILE']='/opt/adx/config/tls/public-ca.pem'
from adx_sandbox import ConnectionConfig,Sandbox
root=pathlib.Path(sys.argv[1]);image=sys.argv[2]
connection=ConnectionConfig(server_address='localhost:8443',token=pathlib.Path('/opt/adx/config/secrets/tenant-key').read_text().strip(),use_tls=True,verify_tls=True)
options=dict(image=image,runtime='firecracker',cpu=1000,memory=512,idle_timeout=0,connection=connection,create_timeout=180)
one=Sandbox(**options)
try:
    value=one.commands.run('printf ready; printf diagnostic >&2; exit 7')
    assert (value.exit_code,value.stdout,value.stderr)==(7,'ready','diagnostic'),value
    payload=b'example-install\x00\xff'*4096
    one.files.write('/tmp/example.bin',payload)
    assert one.files.read('/tmp/example.bin',format='bytes')==payload
finally:
    try:one.kill()
    finally:one.close()
print('PASS SDK command/file and explicit deletion',flush=True)
two=Sandbox(**options)
try:
    value=two.commands.run('printf stop-cleanup')
    assert value.exit_code==0 and value.stdout=='stop-cleanup'
    (root/'evidence/sdk.json').write_text(json.dumps({'status':'passed','deleted':one.id,'left_for_stop':two.id}))
finally:two.close()
print('PASS live instance prepared for adxctl stop',flush=True)

#!/usr/bin/env python3
"""Generate isolated short-lived test certificates, never deployment credentials."""
import pathlib
import subprocess
import sys
out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=False)
def run(*args):
    subprocess.run(['openssl', *map(str,args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
run('req','-x509','-newkey','rsa:2048','-nodes','-keyout',out/'ca.key','-out',out/'ca.pem','-days','2','-subj','/CN=ADX RPC test CA')
(out/'extensions.cnf').write_text('basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n')
for name in ['master','node','api-server','edge','unknown']:
    run('req','-newkey','rsa:2048','-nodes','-keyout',out/f'{name}.key','-out',out/f'{name}.csr','-subj',f'/CN=ADX test {name}')
    run('x509','-req','-in',out/f'{name}.csr','-CA',out/'ca.pem','-CAkey',out/'ca.key','-CAcreateserial','-out',out/f'{name}.pem','-days','2','-extfile',out/'extensions.cnf')
    run('x509','-in',out/f'{name}.pem','-outform','DER','-out',out/f'{name}.der')
    (out/f'{name}.key').chmod(0o600)
(out/'ca.key').chmod(0o600)
print('Generated isolated RPC test certificates')

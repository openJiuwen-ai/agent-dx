#!/usr/bin/env python3
"""Generate isolated short-lived test certificates, never deployment credentials."""

import os
import pathlib
import shutil
import subprocess
import sys


def _executable(command_name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(command_name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {command_name}")
    return os.path.abspath(executable)


out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=False)


def run(*args):
    subprocess.run(
        [_executable('openssl'), *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


ca_config = out / 'ca.cnf'
ca_config.write_text(
    '[req]\ndistinguished_name=dn\n[dn]\n[ca]\n'
    'basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n'
    'subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n'
)
run(
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-keyout',
    out / 'ca.key',
    '-out',
    out / 'ca.pem',
    '-days',
    '2',
    '-subj',
    '/CN=ADX RPC test CA',
    '-config',
    ca_config,
    '-extensions',
    'ca',
)
(out / 'extensions.cnf').write_text(
    (
        'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncip'
        'herment\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n'
        'extendedKeyUsage=serverAuth,clientAuth\nsubjectAltNam'
        'e=DNS:localhost,DNS:coordinator,DNS:node2,IP:127.0.0.1\n'
    )
)
for name in ['coordinator', 'node', 'apiserver', 'ingress', 'unknown']:
    run(
        'req',
        '-newkey',
        'rsa:2048',
        '-nodes',
        '-keyout',
        out / f'{name}.key',
        '-out',
        out / f'{name}.csr',
        '-subj',
        f'/CN=ADX test {name}',
    )
    run(
        'x509',
        '-req',
        '-in',
        out / f'{name}.csr',
        '-CA',
        out / 'ca.pem',
        '-CAkey',
        out / 'ca.key',
        '-CAcreateserial',
        '-out',
        out / f'{name}.pem',
        '-days',
        '2',
        '-extfile',
        out / 'extensions.cnf',
    )
    run('x509', '-in', out / f'{name}.pem', '-outform', 'DER', '-out', out / f'{name}.der')
    (out / f'{name}.key').chmod(0o600)
(out / 'ca.key').chmod(0o600)
sys.stdout.write('Generated isolated RPC test certificates\n')

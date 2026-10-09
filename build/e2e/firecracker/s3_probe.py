#!/usr/bin/env python3
import json
import pathlib
import sys
import uuid
import xml.etree.ElementTree as ET

from s3_client import Client

run = pathlib.Path(sys.argv[1])
artifact = sys.argv[sys.argv.index('--artifact') + 1] if '--artifact' in sys.argv else None
if artifact:
    uuid.UUID(artifact)
creating = '--create' in sys.argv
path = '/checkpoints' + ('/adx/' + artifact + '/manifest.json' if artifact else '')
raw = Client(run).request('PUT' if creating else 'GET', path, query='' if creating or artifact else 'list-type=2')
if creating:
    print('S3 BUCKET CREATED', flush=True)
    sys.exit(0)
if artifact:
    manifest = json.loads(raw)
    if not (manifest['version'] == 1 and manifest['size'] > 0 and manifest['files']):
        raise AssertionError()
    (pathlib.Path(sys.argv[1]) / 'evidence/s3-paused-manifest.json').write_bytes(raw)
    print('S3 PAUSED MANIFEST VERIFIED', artifact, flush=True)
    sys.exit(0)
root = ET.fromstring(raw)
objects = root.findall('{*}Contents')
if not (not objects):
    raise AssertionError(raw)
(pathlib.Path(sys.argv[1]) / 'evidence/s3-final.xml').write_bytes(raw)
print('S3 OBJECT INVENTORY EMPTY', flush=True)

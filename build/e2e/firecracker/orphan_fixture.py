"""Inject an abandoned capture inventory entry; exercise Coordinator S3 reclamation."""

import json
import os
import subprocess
from pathlib import Path
import time
import urllib.error
import uuid

from s3_client import Client


class OrphanFixture:
    def __init__(self, run, artifact, session):
        self.run, self.client = run, Client(run)
        self.artifact = artifact
        owner = json.loads(self.client.request('GET', f'/checkpoints/adx/{artifact}/owner.json'))
        if not (owner == {'version': 1, 'node_id': 'node1', 'session_id': session}):
            raise AssertionError(owner)
        self.keys = []
        self.orphan = self.seed(owner)
        self.foreign = self.seed({**owner, 'node_id': 'foreign-fixture'})
        self.legacy = self.seed(None)
        for key in self.keys:
            self.client.request('HEAD', key)
        # Simulate a capture reservation whose writer has already stopped and
        # whose Environment no longer retains it. This only seeds metadata;
        # the production Coordinator performs the real deletion.
        location = self.orphan[0].split('/')[-2]
        inventory = {'storage': 'shared', 'location': location, 'size_bytes': 0}
        base = Path(os.environ.get('ADX_FC_BASE', '/opt/adx'))
        auth = {**os.environ, 'REDISCLI_AUTH': (run / 'secrets/redis-key').read_text().strip()}
        subprocess.run([str(base / 'tools/redis-cli'), 'HSET', 'adx:{acceptance}:control:v1:artifacts',
            'shared:' + location, json.dumps(inventory)], env=auth, check=True, capture_output=True, timeout=5)

    def seed(self, owner):
        prefix = f'/checkpoints/adx/{uuid.uuid4()}'
        keys = [prefix + '/partial.bin']
        if owner is not None:
            marker = prefix + '/owner.json'
            self.client.request('PUT', marker, json.dumps(owner).encode())
            keys.append(marker)
        self.client.request('PUT', keys[0], b'interrupted-upload-fixture')
        self.keys.extend(keys)
        return keys

    def verify(self):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            absent = 0
            for key in self.orphan:
                try:
                    self.client.request('HEAD', key)
                except urllib.error.HTTPError as error:
                    if error.code != 404:
                        raise
                    absent += 1
            if absent == len(self.orphan):
                break
            time.sleep(0.5)
        else:
            raise TimeoutError('Coordinator did not reclaim abandoned capture')
        for key in self.foreign + self.legacy:
            self.client.request('HEAD', key)
        self.client.request('GET', f'/checkpoints/adx/{self.artifact}/manifest.json')
        evidence = {
            'passed': True,
            'injection': 'partial objects plus durable abandoned capture inventory',
            'coordinator_collected': True,
            'foreign_preserved': True,
            'unmarked_preserved': True,
            'registered_checkpoint_preserved': self.artifact,
        }
        (self.run / 'evidence/orphan-gc.json').write_text(json.dumps(evidence, indent=2))
        # Remove only test-owned survivors, leaving the actual recovery point intact.
        for key in self.foreign + self.legacy:
            self.client.request('DELETE', key)
        print('PASS Coordinator reclaims abandoned capture and preserves registered checkpoint', flush=True)

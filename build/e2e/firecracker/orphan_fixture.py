"""Inject interrupted-publication objects; test real S3 collection on node restart."""
import json
import time
import uuid
import urllib.error
from s3_client import Client

class OrphanFixture:
    def __init__(self, run, artifact, session):
        self.run, self.client = run, Client(run)
        self.artifact = artifact
        owner = json.loads(self.client.request('GET', f'/checkpoints/adx/{artifact}/owner.json'))
        assert owner == {'version': 1, 'node_id': 'node1', 'session_id': session}, owner
        self.keys = []
        self.orphan = self.seed(owner)
        self.foreign = self.seed({**owner, 'node_id': 'foreign-fixture'})
        self.legacy = self.seed(None)
        # The real periodic collector must skip the current upload session.
        time.sleep(5)
        for key in self.keys:
            self.client.request('HEAD', key)

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
            time.sleep(.5)
        else:
            raise TimeoutError('retired-session orphan was not collected')
        for key in self.foreign + self.legacy:
            self.client.request('HEAD', key)
        self.client.request('GET', f'/checkpoints/adx/{self.artifact}/manifest.json')
        evidence = {'passed': True, 'injection': 'partial upload objects with real prior node session',
            'current_session_preserved': True, 'retired_session_removed': True,
            'foreign_preserved': True, 'unmarked_preserved': True,
            'registered_checkpoint_preserved': self.artifact}
        (self.run / 'evidence/orphan-gc.json').write_text(json.dumps(evidence, indent=2))
        # Remove only test-owned survivors, leaving the actual recovery point intact.
        for key in self.foreign + self.legacy:
            self.client.request('DELETE', key)
        print('PASS remote orphan GC removes retired uploads and preserves registered checkpoint', flush=True)

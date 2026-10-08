import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'build/release'))
import ci_transfer as transfer


class Store:
    def __init__(self):
        self.objects = {}
    def putFile(self, bucket, key, path):
        self.objects[key] = Path(path).read_bytes()
        return SimpleNamespace(status=200)
    def getObjectMetadata(self, bucket, key):
        return SimpleNamespace(status=200, body=SimpleNamespace(contentLength=len(self.objects[key])))
    def getObject(self, bucket, key, downloadPath):
        if key not in self.objects:
            return SimpleNamespace(status=404)
        Path(downloadPath).write_bytes(self.objects[key])
        return SimpleNamespace(status=200)


class TransferTests(unittest.TestCase):
    def test_roundtrip_is_bound_to_build_commit_and_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'platform.tar.gz'
            source.write_bytes(b'archive')
            store = Store()
            transfer.upload(store, 'bucket', 'build-1', 'a'*40, 'platform', [source])
            target = root / 'download'
            transfer.download(store, 'bucket', 'build-1', 'a'*40, 'platform', target)
            self.assertEqual((target / source.name).read_bytes(), b'archive')
            with self.assertRaises(RuntimeError):
                transfer.download(store, 'bucket', 'build-2', 'a'*40, 'platform', root / 'wrong')
            keys = list(store.objects)
            store.objects[keys[0]] = b'corrupt'
            with self.assertRaisesRegex(ValueError, 'digest'):
                transfer.download(store, 'bucket', 'build-1', 'a'*40, 'platform', root / 'bad')
            self.assertFalse((root / 'bad' / source.name).exists())

    def test_rejects_wrong_identity_and_unsafe_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'file.tar.gz'
            source.write_bytes(b'archive')
            store = Store()
            transfer.upload(store, 'bucket', 'b1', 'a'*40, 'platform', [source])
            manifest_key = next(k for k in store.objects if k.endswith('/manifest.json'))
            original = json.loads(store.objects[manifest_key])
            for update in ({'commit': 'b'*40}, {'build_id': 'other'}, {'group': 'gateway'},
                           {'files': {'../escape': {'bytes': 7, 'sha256': 'x'}}}):
                store.objects[manifest_key] = json.dumps(dict(original, **update)).encode()
                with self.assertRaises(ValueError):
                    transfer.download(store, 'bucket', 'b1', 'a'*40, 'platform', root / 'bad')

    def test_upload_failure_never_publishes_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'file.tar.gz'
            source.write_bytes(b'archive')
            store = Store()
            store.putFile = lambda *args: SimpleNamespace(status=500)
            with self.assertRaises(RuntimeError):
                transfer.upload(store, 'bucket', 'b1', 'a'*40, 'platform', [source])
            self.assertEqual(store.objects, {})

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
import verify_backend as cached
sys.path.pop(0)

class BackendCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        for name in cached.BACKEND_BINARIES:(self.root/name).write_bytes(name.encode())
        pinned=json.loads((cached.ROOT/'third_party/sandboxd/source.json').read_text())
        self.manifest={'sandboxd_revision':pinned['revision'],
                       'target':'x86_64-unknown-linux-gnu',
                       'files':{name:cached.sha(self.root/name) for name in cached.BACKEND_BINARIES}}
        self.save()
    def save(self):(self.root/'manifest.json').write_text(json.dumps(self.manifest))
    def test_verified_pinned_artifacts_are_accepted(self):
        with contextlib.redirect_stdout(io.StringIO()):cached.verify(self.root,self.manifest['target'])
    def test_changed_binary_is_rejected(self):
        (self.root/'runc').write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError,'integrity'):cached.verify(self.root,self.manifest['target'])
    def test_wrong_revision_and_architecture_are_rejected(self):
        with self.assertRaisesRegex(ValueError,'revision or architecture'):cached.verify(self.root,'aarch64-unknown-linux-gnu')
        self.manifest['sandboxd_revision']='0'*40;self.save()
        with self.assertRaisesRegex(ValueError,'revision or architecture'):cached.verify(self.root,self.manifest['target'])
    def test_incomplete_artifact_set_is_rejected(self):
        del self.manifest['files']['runc'];self.save()
        with self.assertRaisesRegex(ValueError,'file set'):cached.verify(self.root,self.manifest['target'])

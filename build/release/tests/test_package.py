import importlib.util
from pathlib import Path
import tempfile
import unittest
s = importlib.util.spec_from_file_location("package", Path(__file__).resolve().parents[1] / "package.py")
pkg = importlib.util.module_from_spec(s)
s.loader.exec_module(pkg)
class PackageTests(unittest.TestCase):
    def test_complete_package_verifies_and_tampering_fails(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t); binaries=root/"bin";binaries.mkdir()
            for name in pkg.BINARIES+("rrt-runtime",):
                (binaries/name).write_bytes(b"fixture")
            redis=root/"redis";redis.write_text("#!/bin/sh\necho 'Redis server v=7.2.5 sha=fixture'\n");redis.chmod(0o700)
            wheel=root/"adx_sandbox-1-py3-none-any.whl";wheel.write_bytes(b"fixture wheel")
            out=root/"package"
            m=pkg.assemble(binaries,redis,wheel,out,"a"*40,True,"test-fixture","debug")
            self.assertTrue(m["dirty"]);pkg.verify(out)
            (out/"bin/adxctl").write_bytes(b"changed")
            with self.assertRaises(ValueError):pkg.verify(out)
            with self.assertRaises(ValueError):pkg.assemble(binaries,redis,wheel,out,"a"*40,True,"test-fixture","debug")
    def test_missing_artifact_never_creates_package(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);out=root/"package"
            with self.assertRaises(ValueError):pkg.assemble(root,root/"redis",root/"adx_sandbox-1.whl",out,"a"*40,True,"fixture","debug")
            self.assertFalse(out.exists())
if __name__=="__main__":unittest.main()

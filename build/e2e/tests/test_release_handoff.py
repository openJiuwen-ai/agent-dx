import importlib.util
from pathlib import Path
import py_compile
import shutil
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('handoff_package',ROOT/'build/release/package.py')
package=importlib.util.module_from_spec(spec);spec.loader.exec_module(package)

class ReleaseArchiveTests(unittest.TestCase):
    def test_all_e2e_python_sources_compile(self):
        sources = sorted((ROOT / 'build/e2e').rglob('*.py'))
        self.assertTrue(sources)
        with tempfile.TemporaryDirectory() as temp:
            for index, source in enumerate(sources):
                py_compile.compile(
                    str(source),
                    cfile=str(Path(temp) / f'{index}.pyc'),
                    doraise=True,
                )

    def test_artifact_roundtrip_preserves_manifest_nested_files_and_modes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);bins=root/'bins';bins.mkdir()
            for name in (*package.BINARIES,'rrt-runtime','adx-runtime-rootfs.img'):
                path=bins/name;path.write_bytes(b'fixture binary');path.chmod(0o755)
            redis=root/'redis-server';redis.write_text('#!/bin/sh\necho "Redis server v=7.2.5 fixture"\n');redis.chmod(0o755)
            wheel=root/'adx_sandbox-0.10.0-py3-none-any.whl';wheel.write_bytes(b'fixture wheel')
            built=root/'built';package.assemble(bins,redis,wheel,built,'a'*40,False,'x86_64-unknown-linux-gnu','release')
            archive=root/'adx-release.tar.gz'
            subprocess.run(['tar','-czf',archive,'-C',built,'.'],check=True)
            # A fresh job receives only a normal non-executable archive file.
            downloaded=root/'downloaded.tar.gz';shutil.copyfile(archive,downloaded);downloaded.chmod(0o644)
            restored=root/'restored';restored.mkdir()
            subprocess.run(['tar','-xzf',downloaded,'-C',restored],check=True)
            self.assertEqual(package.verify(restored),package.verify(built))
            self.assertTrue((restored/'manifest.json').is_file())
            self.assertTrue((restored/'LICENSE').is_file())
            self.assertTrue((restored/'bin/adx-master').stat().st_mode & 0o111)
            self.assertTrue((restored/'etc/examples/deployment.yaml').is_file())
            self.assertTrue((restored/'third_party/sandboxd/source.json').is_file())
            self.assertFalse((restored/'third_party/sandboxd/patches').exists())

    def test_python_bytecode_does_not_dirty_build_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shutil.copyfile(ROOT / '.gitignore', root / '.gitignore')
            source = root / 'build/e2e/fixture.py'
            source.parent.mkdir(parents=True)
            source.write_text('value = 1\n')
            subprocess.run(['git', 'init', '-q', temp], check=True)
            subprocess.run(['git', '-C', temp, 'add', '.gitignore', 'build/e2e/fixture.py'], check=True)
            bytecode = Path(py_compile.compile(str(source), doraise=True))
            self.assertTrue(bytecode.is_file())
            untracked = subprocess.check_output(
                ['git', '-C', temp, 'ls-files', '--others', '--exclude-standard'], text=True)
            self.assertEqual(untracked, '')

"""Exercise the held FIFO and its cleanup against a real Linux reader."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'resource_block', Path(__file__).resolve().parents[1] / 'resource_block.py',
)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


@unittest.skipUnless(sys.platform == 'linux', 'requires Linux proc task wait channels')
class ResourceBlockTests(unittest.TestCase):
    def test_stalls_a_real_read_then_restores_capacity_and_releases_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'capacity.json').write_text('original-capacity')
            code = (
                'from pathlib import Path; import sys,time; p=Path(sys.argv[1]); '
                '\nwhile not (p/"resource-block.json").exists(): time.sleep(.01)'
                '\nwith (p/"capacity.json").open() as f: assert f.read() == ""'
            )
            reader = subprocess.Popen([sys.executable, '-c', code, directory])
            try:
                state = fixture.block(root, reader.pid)
                self.assertEqual(state['adxlet_pid'], reader.pid)
                self.assertIn('pipe_read', state['wait_channel'])
                self.assertIsNone(reader.poll())
                fixture.unblock(root)
                self.assertEqual(reader.wait(timeout=5), 0)
                self.assertEqual((root / 'capacity.json').read_text(), 'original-capacity')
                self.assertFalse((root / 'resource-block.json').exists())
            finally:
                fixture.unblock(root)
                if reader.poll() is None:
                    reader.terminate()
                    reader.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()

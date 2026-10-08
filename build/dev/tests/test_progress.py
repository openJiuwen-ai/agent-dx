import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


TOOL = Path(__file__).resolve().parents[1] / 'progress.py'


class ProgressTests(unittest.TestCase):
    def test_failed_command_preserves_output_and_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state.json'
            log = root / 'command.log'
            result = subprocess.run([
                sys.executable, str(TOOL), '--state', str(state), 'run',
                '--job', 'failure', '--stage', '1', '--label', 'Failure fixture',
                '--log', str(log), '--', sys.executable, '-c',
                'print("fixture failure", flush=True); raise SystemExit(7)',
            ], capture_output=True, text=True)
            self.assertEqual(result.returncode, 7, result.stderr)
            job = json.loads(state.read_text())['jobs']['failure']
            self.assertEqual(job['status'], 'failed')
            self.assertEqual(job['exit_code'], 7)
            self.assertIn('fixture failure', log.read_text())
            self.assertIn('fixture failure', result.stdout)

    def test_parallel_commands_do_not_lose_updates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state.json'
            processes = [subprocess.Popen([
                sys.executable, str(TOOL), '--state', str(state), 'run',
                '--job', str(i), '--stage', '1', '--label', 'Concurrent fixture',
                '--log', str(root / (str(i) + '.log')), '--',
                sys.executable, '-c', 'import time; time.sleep(0.1)',
            ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE) for i in range(4)]
            results = [(process, process.communicate(timeout=10)) for process in processes]
            for process, (_, error) in results:
                self.assertEqual(process.returncode, 0, error)
            jobs = json.loads(state.read_text())['jobs']
            self.assertEqual(len(jobs), 4)
            self.assertTrue(all(job['status'] == 'passed' for job in jobs.values()))


if __name__ == '__main__':
    unittest.main()

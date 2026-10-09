import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

TOOL = Path(__file__).resolve().parents[1] / 'progress.py'


class ProgressTests(unittest.TestCase):
    def test_invalid_requests_leave_state_unchanged_and_report_cli_error(self):
        cases = (
            ('existing', 'State already exists; use stage to update it.'),
            ('empty', 'No stages found in roadmap.'),
            ('stage', 'Unknown stage: 9'),
            ('run', 'Missing command after --'),
        )
        for action, message in cases:
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                state = root / 'state.json'
                roadmap = root / 'roadmap.md'
                roadmap.write_text('No stages')
                original = json.dumps({'revision': 1, 'stages': [], 'jobs': {}})
                if action != 'empty':
                    state.write_text(original)
                arguments = ['init', '--roadmap', str(roadmap)]
                if action == 'stage':
                    arguments = ['stage', '9', '--status', 'complete']
                elif action == 'run':
                    arguments = [
                        'run',
                        '--job',
                        'missing',
                        '--stage',
                        '1',
                        '--label',
                        'Missing fixture',
                        '--log',
                        str(root / 'command.log'),
                        '--',
                    ]
                result = subprocess.run(
                    [sys.executable, str(TOOL), '--state', str(state), *arguments],
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, '')
                self.assertEqual(result.stderr, message + '\n')
                if action == 'empty':
                    self.assertFalse(state.exists())
                else:
                    self.assertEqual(state.read_text(), original)
                self.assertFalse((root / 'command.log').exists())

    def test_failed_command_preserves_output_and_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state.json'
            log = root / 'command.log'
            result = subprocess.run(
                [
                    sys.executable,
                    str(TOOL),
                    '--state',
                    str(state),
                    'run',
                    '--job',
                    'failure',
                    '--stage',
                    '1',
                    '--label',
                    'Failure fixture',
                    '--log',
                    str(log),
                    '--',
                    sys.executable,
                    '-c',
                    'print("fixture failure", flush=True); raise SystemExit(7)',
                ],
                capture_output=True,
                text=True,
            )
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
            processes = [
                subprocess.Popen(
                    [
                        sys.executable,
                        str(TOOL),
                        '--state',
                        str(state),
                        'run',
                        '--job',
                        str(i),
                        '--stage',
                        '1',
                        '--label',
                        'Concurrent fixture',
                        '--log',
                        str(root / (str(i) + '.log')),
                        '--',
                        sys.executable,
                        '-c',
                        'import time; time.sleep(0.1)',
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                for i in range(4)
            ]
            results = [(process, process.communicate(timeout=10)) for process in processes]
            for process, (_, error) in results:
                self.assertEqual(process.returncode, 0, error)
            jobs = json.loads(state.read_text())['jobs']
            self.assertEqual(len(jobs), 4)
            self.assertTrue(all(job['status'] == 'passed' for job in jobs.values()))


if __name__ == '__main__':
    unittest.main()

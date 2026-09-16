#!/usr/bin/env python3
"""Local development progress: atomic state, command recording and read-only UI."""
import argparse
from datetime import datetime, timezone
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import uuid


def now():
    return datetime.now(timezone.utc).isoformat()


def read_state(path):
    if not path.exists():
        return {'title': 'ADX 开发进度', 'updated_at': None, 'revision': 0,
                'stages': [], 'jobs': {}}
    return json.loads(path.read_text())


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as output:
            json.dump(value, output, ensure_ascii=False, indent=2)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def update(path, mutation):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = read_state(path)
        mutation(state)
        state['revision'] += 1
        state['updated_at'] = now()
        atomic_json(path, state)


def initialize(args):
    if args.state.exists():
        raise SystemExit('State already exists; use stage to update it.')
    stages = []
    for line in args.roadmap.read_text().splitlines():
        columns = [column.strip() for column in line.split('|')]
        if len(columns) == 5 and columns[1][:1].isdigit():
            title = columns[1].split('. ', 1)[-1]
            stages.append({'id': str(len(stages) + 1), 'title': title,
                           'scope': columns[2], 'gate': columns[3],
                           'status': 'pending', 'note': ''})
    if not stages:
        raise SystemExit('No stages found in roadmap.')
    update(args.state, lambda state: state.update(stages=stages))


def change_stage(args):
    def mutate(state):
        stage = next((stage for stage in state['stages'] if stage['id'] == args.id), None)
        if stage is None:
            raise SystemExit('Unknown stage: ' + args.id)
        if args.status:
            stage['status'] = args.status
        if args.note is not None:
            stage['note'] = args.note
    update(args.state, mutate)


def run(args):
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        raise SystemExit('Missing command after --')
    args.log.parent.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    job = {'label': args.label, 'stage': args.stage, 'status': 'running',
           'started_at': now(), 'finished_at': None, 'exit_code': None,
           'log': str(args.log.resolve()), 'pid': os.getpid(), 'run_id': run_id}
    update(args.state, lambda state: state['jobs'].__setitem__(args.job, job))
    process = None
    received_signal = None
    original_handlers = {}

    def interrupt(signum, frame):
        nonlocal received_signal
        received_signal = signum
        if process and process.poll() is None:
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

    for signum in (signal.SIGTERM, signal.SIGINT):
        original_handlers[signum] = signal.signal(signum, interrupt)
    code = 1
    try:
        with args.log.open('ab', buffering=0) as log:
            log.write(('\n--- Started ' + job['started_at'] + ' ---\n').encode())
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            if received_signal:
                interrupt(received_signal, None)
            with process.stdout:
                while True:
                    chunk = process.stdout.read1(16384)
                    if not chunk:
                        break
                    log.write(chunk)
                    try:
                        sys.stdout.buffer.write(chunk)
                        sys.stdout.buffer.flush()
                    except BrokenPipeError:
                        pass
            code = process.wait()
    except OSError as error:
        print('Command could not run: ' + str(error), file=sys.stderr)
        code = 127
    finally:
        for signum, handler in original_handlers.items():
            signal.signal(signum, handler)
        job.update(status='interrupted' if received_signal or code < 0 else
                   ('passed' if code == 0 else 'failed'), finished_at=now(), exit_code=code)

        def finish(state):
            if state['jobs'].get(args.job, {}).get('run_id') == run_id:
                state['jobs'][args.job] = job
        update(args.state, finish)
    return 128 + abs(code) if code < 0 else code


def serve(args):
    page = Path(__file__).with_name('progress.html').read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = self.path.split('?', 1)[0]
            if route == '/':
                content, mime = page, 'text/html; charset=utf-8'
            elif route == '/api/state':
                try:
                    state = read_state(args.state)
                    for job in state['jobs'].values():
                        if job['status'] == 'running':
                            try:
                                os.kill(job['pid'], 0)
                            except ProcessLookupError:
                                job['status'] = 'unknown'
                    content = json.dumps(state, ensure_ascii=False).encode()
                    mime = 'application/json; charset=utf-8'
                except (OSError, ValueError):
                    self.send_error(503, 'Progress state unavailable')
                    return
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *unused):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    url = 'http://127.0.0.1:%s' % server.server_port
    atomic_json(args.state.parent / 'server.json', {'url': url, 'pid': os.getpid()})
    print(url, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, default=Path('out/dev/progress/state.json'))
    commands = parser.add_subparsers(dest='action', required=True)
    init = commands.add_parser('init')
    init.add_argument('--roadmap', type=Path, required=True)
    stage = commands.add_parser('stage')
    stage.add_argument('id')
    stage.add_argument('--status', choices=['pending', 'active', 'blocked', 'complete'])
    stage.add_argument('--note')
    command = commands.add_parser('run')
    command.add_argument('--job', required=True)
    command.add_argument('--stage', required=True)
    command.add_argument('--label', required=True)
    command.add_argument('--log', type=Path, required=True)
    command.add_argument('command', nargs=argparse.REMAINDER)
    server = commands.add_parser('serve')
    server.add_argument('--port', type=int, default=0)
    args = parser.parse_args()
    return {'init': initialize, 'stage': change_stage, 'run': run, 'serve': serve}[args.action](args)


if __name__ == '__main__':
    sys.exit(main())

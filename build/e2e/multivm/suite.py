#!/usr/bin/env python3
"""Run selected three-VM cases with one shared, resumable runtime budget."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Optional

if __package__:
    from .contract import inventory_digest, verify_inventory
else:
    from contract import inventory_digest, verify_inventory


@dataclass(frozen=True)
class Case:
    script: str
    report: str
    timeout: int


CASES = {
    'sdk': Case('sdk_accept.py', 'sdk-accept-result.json', 360),
    'capacity': Case('capacity_queue.py', 'capacity-queue-result.json', 540),
    'placement-pack': Case('placement_policy.py', 'placement-pack-result.json', 300),
    'placement-spread': Case('placement_policy.py', 'placement-spread-result.json', 300),
    'node-preferences': Case('node_preferences.py', 'node-preferences-result.json', 420),
    'runtime-affinity': Case('runtime_affinity.py', 'runtime-affinity-result.json', 300),
    'local-first': Case('local_first.py', 'local-first-result.json', 600),
    'worker-failure': Case('worker_failure.py', 'worker-failure-result.json', 480),
    'worker-restart': Case('worker_restart.py', 'worker-restart-result.json', 360),
    'session-fence': Case('worker_restart.py', 'worker-restart-result.json', 360),
    'control-restart': Case('control_restart.py', 'control-restart-result.json', 600),
    'ingress-restart': Case('control_restart.py', 'control-restart-result.json', 300),
    'stop': Case('stop.py', 'stop-result.json', 480),
}
MAX_BUDGET_SECONDS = 3 * 60 * 60


@dataclass
class RunConfig:
    inventory: dict
    inventory_path: Path
    output: Path
    endpoint: str
    token_file: Path
    admin_token_file: Path
    ca: Path
    image: str
    release: Path
    socket: str
    cases: tuple[str, ...]
    budget_seconds: int = MAX_BUDGET_SECONDS
    confirm_dedicated: bool = False
    session_probe: Optional[Path] = None


def write_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def case_command(config, case, destination):
    definition = CASES[case]
    command = [sys.executable, '-u', str(Path(__file__).parent / definition.script),
               '--inventory', str(config.inventory_path), '--endpoint', config.endpoint,
               '--token-file', str(config.token_file), '--ca', str(config.ca),
               '--image', config.image, '--socket', config.socket,
               '--output', str(destination)]
    if case == 'sdk':
        command += ['--release', str(config.release)]
    elif case == 'capacity':
        command += ['--admin-token-file', str(config.admin_token_file)]
    elif case.startswith('placement-'):
        command += ['--placement', case.removeprefix('placement-')]
    elif case == 'ingress-restart':
        command += ['--role', 'ingress']
    elif case == 'session-fence':
        command += ['--session-probe', str(config.session_probe)]
    elif case == 'stop':
        command.append('--confirm-dedicated')
    return command


def execute_subprocess(_case, command, log, timeout, _report_name):
    started = time.monotonic()
    pid_path = log.parent / 'case.pid'
    with log.open('wb') as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        pid_path.write_text(str(process.pid) + '\n')
        timed_out = False
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            try:
                code = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                code = process.wait()
        finally:
            pid_path.unlink(missing_ok=True)
    return code, time.monotonic() - started, timed_out


def active_process_group(log):
    pid_path = Path(log).parent / 'case.pid'
    try:
        pid = int(pid_path.read_text().strip())
        if pid < 1:
            return False
        os.killpg(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def result_from_state(state):
    records = state['cases']
    failed = [record['case'] for record in records if record['status'] != 'passed']
    return {'schema_version': 1, 'scope': 'selected-cases',
            'inventory_sha256': state['inventory_sha256'],
            'budget_seconds': state['budget_seconds'],
            'runtime_seconds': state['runtime_seconds'],
            'remaining_seconds': max(0, state['budget_seconds'] - state['runtime_seconds']),
            'status': 'failed' if failed else 'passed',
            'failed_cases': failed, 'cases': records}


def run_plan(config, execute=execute_subprocess, now=time.time):
    verify_inventory(config.inventory)
    if not 0 < config.budget_seconds <= MAX_BUDGET_SECONDS:
        raise ValueError('suite budget must be at most three hours')
    if not config.cases or len(set(config.cases)) != len(config.cases) \
            or any(case not in CASES for case in config.cases):
        raise ValueError('suite requires distinct known cases')
    if 'stop' in config.cases and config.cases[-1] != 'stop':
        raise ValueError('destructive stop case must be last')
    if 'stop' in config.cases and not config.confirm_dedicated:
        raise ValueError('stop requires explicit dedicated-VM confirmation')
    if 'session-fence' in config.cases \
            and (config.session_probe is None or not config.session_probe.is_file()
                 or not os.access(config.session_probe, os.X_OK)):
        raise ValueError('session-fence requires a built --session-probe executable')
    config.output.mkdir(parents=True, exist_ok=True)
    state_path = config.output / 'budget-state.json'
    digest = inventory_digest(config.inventory)
    if state_path.exists():
        state = json.loads(state_path.read_text())
        if state.get('schema_version') != 1:
            raise ValueError('unsupported suite budget state')
        if state.get('inventory_sha256') != digest:
            raise ValueError('suite inventory differs from the existing budget')
        if state.get('budget_seconds') != config.budget_seconds:
            raise ValueError('suite budget differs from the existing budget')
    else:
        state = {'schema_version': 1, 'inventory_sha256': digest,
                 'budget_seconds': config.budget_seconds,
                 'runtime_seconds': 0, 'cases': [], 'active': None}
    if state.get('active'):
        active = state['active']
        if active_process_group(active['log']):
            raise RuntimeError(f"previous case {active['case']} is still running; inspect {active['log']}")
        elapsed = min(max(now() - active['started_at'], 0),
                      max(0, config.budget_seconds - state['runtime_seconds']))
        state['runtime_seconds'] += elapsed
        state['cases'].append({'case': active['case'], 'status': 'interrupted',
                               'seconds': elapsed, 'log': active['log']})
        state['active'] = None
        write_json(state_path, state)
    seen = {record['case'] for record in state['cases']}
    if 'stop' in seen:
        raise ValueError('the destructive stop case already ended this suite')
    if seen.intersection(config.cases):
        raise ValueError('case already recorded in this budget; use a new suite for regression')
    for case in config.cases:
        remaining = max(0, config.budget_seconds - state['runtime_seconds'])
        if remaining <= 0:
            state['cases'].append({'case': case, 'status': 'not-run-budget-exhausted',
                                   'seconds': 0})
            write_json(state_path, state)
            continue
        definition = CASES[case]
        timeout = min(definition.timeout, remaining)
        destination = config.output / case
        destination.mkdir(parents=True, exist_ok=True)
        log = destination / 'case.log'
        command = case_command(config, case, destination)
        state['active'] = {'case': case, 'started_at': now(), 'log': str(log)}
        write_json(state_path, state)
        print(f'[RUN] {case} budget={timeout:.1f}s', flush=True)
        try:
            code, elapsed, timed_out = execute(case, command, log, timeout, definition.report)
        except Exception as error:
            code, elapsed, timed_out = 1, max(0, now() - state['active']['started_at']), False
            execution_error = f'{type(error).__name__}: {error}'
        else:
            execution_error = None
        state['runtime_seconds'] += min(max(elapsed, 0), remaining)
        report_path = destination / definition.report
        try:
            report = json.loads(report_path.read_text())
        except (OSError, ValueError):
            report = None
        passed = code == 0 and not timed_out and elapsed <= timeout \
            and isinstance(report, dict) \
            and report.get('status') == 'passed' and not report.get('cleanup_errors') \
            and not report.get('error')
        record = {'case': case, 'status': 'passed' if passed else 'failed',
                  'seconds': elapsed, 'exit_code': code, 'timed_out': timed_out,
                  'log': str(log), 'report': str(report_path) if report else None}
        if execution_error:
            record['error'] = execution_error
        state['cases'].append(record)
        state['active'] = None
        write_json(state_path, state)
        print(f"[{'PASS' if passed else 'FAIL'}] {case} {elapsed:.1f}s", flush=True)
    result = result_from_state(state)
    write_json(config.output / 'suite-result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--token-file', required=True, type=Path)
    parser.add_argument('--admin-token-file', type=Path)
    parser.add_argument('--ca', required=True, type=Path)
    parser.add_argument('--image', required=True)
    parser.add_argument('--release', type=Path)
    parser.add_argument('--socket', default='/run/sandboxd/sandboxd.sock')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--case', dest='cases', action='append', choices=tuple(CASES), required=True)
    parser.add_argument('--budget-seconds', type=int, default=MAX_BUDGET_SECONDS)
    parser.add_argument('--confirm-dedicated', action='store_true')
    parser.add_argument('--session-probe', type=Path)
    args = parser.parse_args()
    if 'sdk' in args.cases and not args.release:
        parser.error('--case sdk requires --release')
    if 'capacity' in args.cases and not args.admin_token_file:
        parser.error('--case capacity requires --admin-token-file')
    config = RunConfig(
        inventory=json.loads(args.inventory.read_text()), inventory_path=args.inventory,
        output=args.output, endpoint=args.endpoint, token_file=args.token_file,
        admin_token_file=args.admin_token_file or Path(''), ca=args.ca,
        image=args.image, release=args.release or Path(''), socket=args.socket,
        cases=tuple(args.cases), budget_seconds=args.budget_seconds,
        confirm_dedicated=args.confirm_dedicated,
        session_probe=args.session_probe,
    )
    config.output.mkdir(parents=True, exist_ok=True)
    with (config.output / 'suite.lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error('another suite runner is using this output directory')
        result = run_plan(config)
    print(json.dumps({'status': result['status'], 'failed_cases': result['failed_cases'],
                      'runtime_seconds': result['runtime_seconds']}), flush=True)
    if result['status'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()

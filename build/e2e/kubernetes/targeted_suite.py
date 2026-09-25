#!/usr/bin/env python3
"""Run isolated Full E2E cases against one immutable bundle within one deadline."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('e2e_common', HERE.parent / 'run.py')
common = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(common)

MAX_BUDGET_SECONDS = 3 * 60 * 60
CLEANUP_RESERVE_SECONDS = 10 * 60
DEFAULT_CASE_TIMEOUT_SECONDS = 30 * 60
MIN_CASE_SECONDS = 60


def execute_case(_case, command, log, timeout, deadline):
    """Stop the driver first so its namespace cleanup can run before the deadline."""
    timed_out = False
    with log.open('w') as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            return process.wait(timeout=timeout), timed_out
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
    return process.returncode, timed_out


def _read_json(path):
    if not path.is_file():
        return None
    try:
        report = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return report


def run_suite(cases, output, shared_args, budget_seconds, *, execute=execute_case,
              clock=time.monotonic, case_timeout_seconds=DEFAULT_CASE_TIMEOUT_SECONDS):
    """Continue after a case failure only when its Kubernetes cleanup is verified."""
    cases = tuple(cases)
    if not cases or len(set(cases)) != len(cases):
        raise ValueError('targeted cases must be nonempty and distinct')
    for case in cases:
        if common.selected_checks('full', case) != (case,):
            raise ValueError(f'{case} is not a Full E2E case')
        if case == 'redis-pod-restart':
            raise ValueError('redis-pod-restart needs a dedicated storage class run')
    if not 0 < budget_seconds <= MAX_BUDGET_SECONDS:
        raise ValueError('targeted suite budget must be within three hours')
    if case_timeout_seconds <= 0:
        raise ValueError('case timeout must be positive')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    start = clock()
    deadline = start + budget_seconds
    checks = []
    records = []
    cleanup_errors = []
    failed_cases = []
    placements = []
    errors = []
    harness = None
    stopped = False
    for case in cases:
        remaining = deadline - clock()
        if remaining < CLEANUP_RESERVE_SECONDS + MIN_CASE_SECONDS:
            errors.append('suite deadline: no time remains for another case and cleanup')
            stopped = True
            break
        destination = output / case
        destination.mkdir()
        log = destination / 'case.log'
        timeout = min(case_timeout_seconds, remaining - CLEANUP_RESERVE_SECONDS)
        command = [sys.executable, '-u', str(HERE / 'run.py'), *shared_args,
                   '--profile', 'full', '--case', case, '--output', str(destination)]
        print(f'[START] {case}: timeout={timeout:.1f}s, remaining={remaining:.1f}s', flush=True)
        try:
            exit_code, timed_out = execute(case, command, log, timeout, deadline)
        except Exception as error:
            exit_code, timed_out = 1, False
            errors.append(f'{case}: driver failed: {type(error).__name__}: {error}')
        child = _read_json(destination / 'result.json')
        if not isinstance(child, dict):
            child = None
        child_cleanup = child.get('cleanup_errors', []) if child else []
        if child_cleanup:
            cleanup_errors.extend(f'{case}: {error}' for error in child_cleanup)
        if child:
            child_harness = child.get('harness')
            if not child_harness:
                errors.append(f'{case}: missing harness identity')
                stopped = True
            elif harness is None:
                harness = child_harness
            elif child_harness != harness:
                errors.append(f'{case}: harness identity changed within suite')
                stopped = True
            placement = _read_json(destination / 'placement.json')
            if isinstance(placement, list):
                placements.extend(dict(item, case=case) for item in placement)
        else:
            errors.append(f'{case}: driver returned no valid result.json; cleanup is unverified')
            stopped = True
        if timed_out:
            errors.append(f'{case}: exceeded {timeout:.1f}s case timeout')
            stopped = True
        if child_cleanup:
            stopped = True
        passed = bool(child and exit_code == 0 and not timed_out
                      and child.get('status') == 'passed'
                      and child.get('checks') == [case]
                      and not child.get('missing_checks')
                      and not child_cleanup and not stopped)
        if passed:
            checks.append(case)
        else:
            failed_cases.append(case)
            if child and child.get('status') == 'passed' and not stopped:
                errors.append(f'{case}: driver result and process exit disagree')
                stopped = True
        child_case = child['cases'][0] if child and child.get('cases') else {}
        record = {'name': case, 'status': 'passed' if passed else 'failed',
                  'seconds': child_case.get('seconds', 0),
                  'error': None if passed else (child.get('error') if child else None)
                  or child_case.get('error') or f'driver exited {exit_code}',
                  'exit_code': exit_code, 'timed_out': timed_out,
                  'evidence': str(destination.relative_to(output))}
        if child_case:
            record['subcases'] = child_case.get('subcases', [])
        records.append(record)
        print(f"[{'PASS' if passed else 'FAIL'}] {case}: exit={exit_code}; "
              f'cleanup_errors={len(child_cleanup)}', flush=True)
        if stopped:
            break
    missing = [case for case in cases if case not in checks]
    report = {'status': 'passed' if not missing and not cleanup_errors and not errors else 'failed',
              'deployment': 'kubernetes', 'profile': 'targeted-suite',
              'source_profile': 'full', 'selected_cases': list(cases),
              'budget_seconds': budget_seconds, 'required_checks': list(cases),
              'checks': checks, 'missing_checks': missing,
              'failed_cases': failed_cases, 'cleanup_errors': cleanup_errors,
              'error': '; '.join(errors) if errors else None,
              'harness': harness, 'cases': records,
              'wall_elapsed_seconds': round(clock() - start, 3)}
    (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    (output / 'placement.json').write_text(json.dumps(placements, indent=2) + '\n')
    common.write_junit(output / 'junit.xml', report, 'platform-kubernetes-targeted-suite')
    print(f"[RESULT] {report['status'].upper()}: {len(checks)}/{len(cases)} cases passed; "
          f"elapsed={report['wall_elapsed_seconds']:.1f}/{budget_seconds}s", flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('bundle', 'registry-images', 'kubeconfig', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--case', action='append', required=True)
    parser.add_argument('--context')
    parser.add_argument('--node-name', action='append', default=[])
    parser.add_argument('--registry-auth', type=Path)
    parser.add_argument('--budget-seconds', type=int, default=MAX_BUDGET_SECONDS)
    parser.add_argument('--case-timeout-seconds', type=int,
                        default=DEFAULT_CASE_TIMEOUT_SECONDS)
    args = parser.parse_args()
    shared = []
    for name in ('bundle', 'registry_images', 'kubeconfig', 'context', 'registry_auth'):
        value = getattr(args, name)
        if value is not None:
            shared.extend(('--' + name.replace('_', '-'), str(value)))
    for name in args.node_name:
        shared.extend(('--node-name', name))
    report = run_suite(args.case, args.output, shared, args.budget_seconds,
                       case_timeout_seconds=args.case_timeout_seconds)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())

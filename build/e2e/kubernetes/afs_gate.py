"""Required public + AFS component + installed SDK acceptance for AFS ON builds."""

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent


def verify_part(directory, expected):
    report = json.loads((directory / 'result.json').read_text())
    records = report.get('cases', [])
    if (report.get('status') != 'passed' or report.get('cleanup_errors') or report.get('missing_checks')
            or len(report.get('checks', [])) != len(expected) or set(report.get('checks', [])) != set(expected)
            or set(report.get('required_checks', [])) != set(expected)
            or len(records) != len(expected) or {c.get('name') for c in records} != set(expected)
            or any(c.get('status') != 'passed' for c in records)):
        raise ValueError('required assertions were missing, failed or not cleaned: ' + str(directory))
    cleanup = json.loads((directory / 'kubernetes-cleanup.json').read_text())
    if cleanup.get('status') != 'PASS' or cleanup.get('errors'):
        raise ValueError('Kubernetes cleanup failed or is unconfirmed')
    if expected == ('afs-component',):
        from afs_component import validate_result
        validate_result(json.loads((directory / 'afs/result.json').read_text()),
                        json.loads((directory / 'afs/cleanup.json').read_text()))
    if 'afs-sdk-ownerfs' in expected:
        from afs_product import validate_sdk_result
        for mode in ('ownerfs', 'dfs'):
            path = directory / 'afs-sdk' / mode
            validate_sdk_result(json.loads((path / 'sdk-result.json').read_text()), mode)
            normal = json.loads((path / 'cleanup.json').read_text())
            if normal.get('status') != 'PASS' or normal.get('errors'):
                raise ValueError('AFS normal cleanup failed: ' + mode)
    return report


def main():
    from run import selected_checks, write_junit
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('bundle', 'registry-images', 'kubeconfig', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--profile', choices=('l0', 'k8s-basic', 'full'), required=True)
    parser.add_argument('--context')
    parser.add_argument('--registry-auth')
    parser.add_argument('--node-name', action='append', default=[])
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    plans = [('public', args.profile, None), ('afs-component', 'full', 'afs-component'),
             ('afs-sdk', 'full', 'afs-sdk')]
    report = {'status': 'failed', 'profile': 'afs-required-smoke', 'source_profile': args.profile,
              'required_checks': [p[0] for p in plans], 'checks': [], 'cleanup_errors': [], 'cases': []}

    def cancel(signum, _frame):
        raise InterruptedError('AFS gate canceled: ' + str(signum))

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, cancel)
    for name, profile, case in plans:
        started = time.monotonic()
        record = {'name': name, 'status': 'failed'}
        report['cases'].append(record)
        command = [sys.executable, '-u', str(HERE / 'run.py'), '--bundle', str(args.bundle.resolve()),
                   '--registry-images', str(args.registry_images.resolve()), '--kubeconfig',
                   str(args.kubeconfig.resolve()), '--output', str(output / name), '--profile', profile]
        if case:
            command += ['--case', case]
        for key in ('context', 'registry_auth'):
            value = getattr(args, key)
            if value:
                command += ['--' + key.replace('_', '-'), value]
        for node in args.node_name:
            command += ['--node-name', node]
        child = None
        try:
            child = subprocess.Popen(command)
            code = child.wait()
            if code:
                raise ValueError(name + ' failed, exit=' + str(code))
            verify_part(output / name, selected_checks(profile, case))
            report['checks'].append(name)
            record['status'] = 'passed'
        except Exception as exc:
            record['error'] = f'{type(exc).__name__}: {exc}'
            report['error'] = record['error']
            break
        finally:
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=240)
                except subprocess.TimeoutExpired:
                    report['cleanup_errors'].append(name + ': child cleanup timeout')
                    child.kill()
                    child.wait()
            record['seconds'] = time.monotonic() - started
    report['missing_checks'] = [name for name in report['required_checks'] if name not in report['checks']]
    if not report['missing_checks'] and not report['cleanup_errors']:
        report['status'] = 'passed'
    (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    write_junit(output / 'junit.xml', report)
    print(json.dumps(report), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())

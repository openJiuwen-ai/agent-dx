"""Required public + AFS component + installed SDK acceptance for AFS ON builds."""

import argparse
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent


def accept_harness(report, part, commit):
    harness = part.get('harness') or {}
    files = harness.get('files')
    if (harness.get('schema_version') != 1 or harness.get('commit') != commit
            or not re.fullmatch(r'[0-9a-f]{40}', harness.get('product_commit', ''))
            or not isinstance(files, dict) or not files
            or any(not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)
                   for digest in files.values())):
        raise ValueError('required child harness identity missing or invalid')
    if report.get('harness') and report['harness'] != harness:
        raise ValueError('required child harness/product identities differ')
    report['harness'] = harness


def verify_part(directory, expected):
    report = json.loads((directory / 'result.json').read_text())
    records = report.get('cases', [])
    if (report.get('status') != 'passed' or report.get('cleanup_errors') or report.get('missing_checks')
            or len(report.get('checks', [])) != len(expected) or set(report.get('checks', [])) != set(expected)
            or set(report.get('required_checks', [])) != set(expected)
            or len(records) != len(expected) or {c.get('name') for c in records} != set(expected)
            or any(c.get('status') != 'passed' for c in records)):
        raise ValueError('required assertions were missing, failed or not cleaned: ' + str(directory))
    native = report.get('deployment') == 'local-docker'
    cleanup = json.loads((directory / ('docker-cleanup.json' if native else 'kubernetes-cleanup.json')).read_text())
    if cleanup.get('status') != 'PASS' or cleanup.get('errors'):
        raise ValueError('deployment cleanup failed or is unconfirmed')
    # The public Full stop contract stays mandatory when nested under this gate.
    # CI summary reads top-level node paths, so validate these child receipts here.
    if 'stop' in expected and json.loads((directory / 'bundle.json').read_text()).get('collector'):
        for node in ('node1', 'node2'):
            for kind in ('collection', 'gateway-metrics', 'traces'):
                evidence = json.loads(((directory if native else directory / node) / f'{kind}-{node}.json').read_text())
                if evidence.get('status') != 'passed':
                    raise ValueError('public Full stop observability failed: ' + node + '/' + kind)
    if expected in (('afs-component',), ('afs-component-full',)):
        from afs_component import validate_result
        validate_result(json.loads((directory / 'afs/result.json').read_text()),
                        json.loads((directory / 'afs/cleanup.json').read_text()),
                        full=expected == ('afs-component-full',))
    if any(name.startswith('afs-sdk-ownerfs') for name in expected):
        from afs_product import validate_sdk_result
        for mode in ('ownerfs', 'dfs'):
            path = directory / 'afs-sdk' / mode
            validate_sdk_result(json.loads((path / 'sdk-result.json').read_text()), mode,
                                full=expected[0].endswith('-full'))
            normal = json.loads((path / 'cleanup.json').read_text())
            if normal.get('status') != 'PASS' or normal.get('errors'):
                raise ValueError('AFS normal cleanup failed: ' + mode)
    return report


def plans(profile, deployment='kubernetes'):
    suffix = '-full' if profile in ('full', 'standalone') else ''
    if deployment == 'local-docker':
        return [('public', 'standalone' if suffix else 'l0', None),
                ('afs-component' + suffix, 'standalone', 'afs-component' + suffix),
                ('afs-sdk' + suffix, 'standalone', 'afs-sdk' + suffix)]
    return [('public', profile, None), ('afs-component' + suffix, 'full', 'afs-component' + suffix),
            ('afs-sdk' + suffix, 'full', 'afs-sdk' + suffix)]


def main(default_deployment='kubernetes'):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('bundle', 'registry-images', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--kubeconfig', type=Path)
    parser.add_argument('--deployment', choices=('kubernetes', 'local-docker'), default=default_deployment)
    parser.add_argument('--image-export', type=Path)
    parser.add_argument('--cgroupns', choices=('private', 'host'), default='host')
    parser.add_argument('--profile', choices=('l0', 'k8s-basic', 'full'), required=True)
    parser.add_argument('--artifact-commit', help='require the exact clean product commit for every child')
    parser.add_argument('--context')
    parser.add_argument('--registry-auth')
    parser.add_argument('--node-name', action='append', default=[])
    args = parser.parse_args()
    native = args.deployment == 'local-docker'
    if native:
        if args.profile == 'k8s-basic':
            parser.error('local-docker supports l0 or full; k8s-basic is a different scope')
        if not args.image_export or not args.artifact_commit:
            parser.error('local-docker requires --image-export and --artifact-commit')
        import importlib.util
        module_spec = importlib.util.spec_from_file_location('local_docker_driver', HERE.parent / 'docker_run.py')
        driver = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(driver)
        common = driver.common
        fixed_candidate = driver.artifacts.fixed_candidate
        selected_checks = driver.selected_checks
        source_commit = driver.harness.source_commit
        write_junit = common.write_junit
    else:
        if args.kubeconfig is None:
            parser.error('--kubeconfig is required for Kubernetes')
        from run import common, fixed_candidate, selected_checks, source_commit, write_junit
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    plan = plans(args.profile, args.deployment)
    report = {'status': 'failed', 'profile': 'afs-required-full' if args.profile == 'full' else 'afs-required-smoke',
              'source_profile': args.profile,
              'required_checks': [p[0] for p in plan], 'checks': [], 'cleanup_errors': [], 'cases': [], 'error': None}
    if args.artifact_commit:
        try:
            fixed_candidate(args.bundle, args.registry_images, args.artifact_commit,
                            afs_required=True, contracts_required=args.profile == 'full')
        except (OSError, ValueError, KeyError, TypeError) as exc:
            report['error'] = f'{type(exc).__name__}: {exc}'
            report['missing_checks'] = list(report['required_checks'])
            (output / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
            write_junit(output / 'junit.xml', report)
            print(json.dumps(report), flush=True)
            return 1
    commit = source_commit()

    def cancel(signum, _frame):
        raise InterruptedError('AFS gate canceled: ' + str(signum))

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, cancel)
    for name, profile, case in plan:
        started = time.monotonic()
        record = {'name': name, 'status': 'failed'}
        report['cases'].append(record)
        command = [sys.executable, '-u', str((HERE.parent if native else HERE) / 'run.py'),
                   '--bundle', str(args.bundle.resolve()), '--registry-images', str(args.registry_images.resolve()),
                   '--output', str(output / name), '--profile', profile]
        if native:
            command += ['--image-export', str(args.image_export.resolve()), '--cgroupns', args.cgroupns]
        else:
            command += ['--kubeconfig', str(args.kubeconfig.resolve())]
        if case:
            command += ['--case', case]
        if args.artifact_commit:
            command += ['--artifact-commit', args.artifact_commit]
        for key in ('context', 'registry_auth'):
            value = getattr(args, key)
            if value:
                command += ['--' + key.replace('_', '-'), value]
        for node in args.node_name:
            command += ['--node-name', node]
        child = None
        try:
            if name == 'public' and args.profile == 'full':
                common.afs_contracts.verify_bundle(
                    args.bundle.parent, json.loads(args.bundle.read_text()), required=True)
            child = subprocess.Popen(command)
            code = child.wait()
            if code:
                raise ValueError(name + ' failed, exit=' + str(code))
            part = verify_part(output / name, selected_checks(profile, case))
            accept_harness(report, part, commit)
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

#!/usr/bin/env python3
"""Generation-aware DFS R3 write/read micro-observation driver.

This is intentionally separate from dfs_r3_small.py. It records one generated
64MiB counter file per invocation so the parent orchestrator can interleave each
write round with independent R3 physical-copy proof before moving to the next
round. It does not qualify 3FS parity, cache residency, or read/write locality.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

import dfs_manyread_small as sync
import dfs_r3_small as base

DATASET = 'counter-generation-1m-v1'
RELATIVE = base.RELATIVE
ROUND_COUNT = 6
WARMUP_ROUNDS = 1
MEASUREMENT_ROUNDS = 5
MIN_GENERATION = 1
MAX_GENERATION = 6


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def validate_round_index(value):
    require(type(value) is int and 0 <= value < ROUND_COUNT, 'round must be an integer 0..5')
    return value


def generation_for_round(round_index):
    return validate_round_index(round_index) + 1


def validate_generation(value):
    require(type(value) is int and MIN_GENERATION <= value <= MAX_GENERATION,
            'generation must be an integer 1..6')
    return value


def round_name(round_index):
    return f'round-{validate_round_index(round_index):02d}.bin'


def relative_path(round_index):
    return f'{RELATIVE}/{round_name(round_index)}'


def expected_content(generation):
    generation = validate_generation(generation)
    whole, chunks = hashlib.sha256(), []
    for chunk_index in range(16):
        chunk = hashlib.sha256()
        for index in range(chunk_index * 4, chunk_index * 4 + 4):
            prefix = ((generation << 32) | index).to_bytes(8, 'little')
            block = prefix + b'a' * (sync.BLOCK_BYTES - 8)
            whole.update(block)
            chunk.update(block)
        chunks.append(chunk.hexdigest())
    return {'sha256': whole.hexdigest(), 'chunk_sha256': chunks, 'bytes': sync.DATA_BYTES,
            'dataset': DATASET, 'generation': generation}


def validate_sample(value, operation, generation):
    generation = validate_generation(generation)
    sync.validate_io_result(value, 'seq-' + operation, 'fdatasync' if operation == 'write' else 'close')
    require(value.get('dataset') == DATASET, 'wrong generation dataset')
    require(validate_generation(value.get('generation')) == generation, 'wrong generation in C result')
    require(type(value.get('wall_ns')) is int and value['wall_ns'] > 0, 'invalid C wall timer')
    require(type(value.get('client_cpu_ns')) is int and value['client_cpu_ns'] >= 0, 'invalid C CPU timer')
    require(type(value.get('barrier_ns')) is int and value['barrier_ns'] >= 0, 'invalid C barrier timer')


def verify_content(payload, expected):
    require(payload.is_file() and not payload.is_symlink(), 'payload not an ordinary file')
    size = payload.stat().st_size
    digest = sync.sha256_file(payload)
    eof = sync.verify_eof(payload)
    require(size == sync.DATA_BYTES and digest == expected['sha256'] and eof['status'] == 'PASS',
            'full SHA/size/EOF mismatch')
    return {'bytes': size, 'sha256': digest, 'eof': eof, 'status': 'PASS'}


def run_sample(tool, payload, operation, generation, timeout):
    generation = validate_generation(generation)
    argv = [str(tool), operation, str(payload), str(generation)]
    record = {'rawargv': argv, 'rc': None, 'stdout': '', 'stderr': '', 'result': None,
              'status': 'FAIL'}
    try:
        done = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        record.update(rc=done.returncode, stdout=done.stdout, stderr=done.stderr)
        require(done.returncode == 0, f'probe exit {done.returncode}')
        parsed = json.loads(done.stdout)
        record['result'] = parsed
        validate_sample(parsed, operation, generation)
        record['status'] = 'PASS'
    except Exception as error:
        record['error'] = repr(error)
    return record


def prepare_round(args):
    output, root, _unused_payload, tool, identity = base.prepare(args)
    round_index = validate_round_index(args.round)
    generation = generation_for_round(round_index)
    require(not (root / RELATIVE).is_symlink(), 'payload directory is a symlink')
    (root / RELATIVE).mkdir(mode=0o755, exist_ok=True)
    payload = root / relative_path(round_index)
    require(not payload.is_symlink(), 'payload is a symlink')
    return output, root, payload, tool, identity, round_index, generation


def write_round(args):
    result, output = {'role': 'write-round', 'status': 'BLOCKED'}, None
    try:
        output, root, payload, tool, identity, round_index, generation = prepare_round(args)
        content = expected_content(generation)
        sample = run_sample(tool, payload, 'write', generation, args.round_timeout)
        result.update(identity=identity, relative_path=relative_path(round_index), generation=generation,
                      round=round_index, measured=round_index >= WARMUP_ROUNDS, content=content, **sample)
        require(sample['status'] == 'PASS', 'writer probe failed')
        sync.fsync_directory(payload.parent)
        sync.fsync_directory(root)
        result.update(parent_dir_fsync=True, root_dir_fsync=True,
                      content_verify=verify_content(payload, content), status='DATA_RECORDED')
        sync.write_json(output / 'write-round.json', result)
    except Exception as error:
        result.update(status='BLOCKED', error=repr(error))
    finally:
        if output is not None:
            sync.write_json(output / 'summary.json', result)
    return result


def validate_write_manifest(value, identity=None, round_index=None):
    require(isinstance(value, dict), 'write manifest must be an object')
    require(value.get('status') == 'DATA_RECORDED', 'write round was not confirmed')
    base.validate_identity(value.get('identity'), value.get('identity'))
    if identity is not None:
        require(value.get('identity') == identity, 'write identity mismatch')
    actual_round = validate_round_index(value.get('round'))
    if round_index is not None:
        require(actual_round == validate_round_index(round_index), 'round mismatch')
    generation = generation_for_round(actual_round)
    require(validate_generation(value.get('generation')) == generation, 'manifest generation does not match round')
    require(value.get('relative_path') == relative_path(actual_round), 'manifest relative path mismatch')
    require(value.get('measured') is (actual_round >= WARMUP_ROUNDS), 'manifest warmup/measured mismatch')
    require(value.get('content') == expected_content(generation), 'manifest content oracle mismatch')
    validate_sample(value.get('result', {}), 'write', generation)
    argv = value.get('rawargv')
    require(value.get('rc') == 0 and isinstance(argv, list) and len(argv) == 4
            and argv[1] == 'write' and argv[3] == str(generation), 'manifest raw write failed')
    require(json.loads(value.get('stdout', '{}')) == value.get('result'), 'manifest stdout/result mismatch')
    require(value.get('parent_dir_fsync') is True and value.get('root_dir_fsync') is True,
            'manifest missing directory fsync proof')
    validate_content_verify(value.get('content_verify', {}), value['content'])
    return value


def validate_content_verify(value, expected):
    require(value.get('status') == 'PASS', 'content verification did not pass')
    require(value.get('bytes') == sync.DATA_BYTES and value.get('sha256') == expected['sha256'],
            'content verification size/SHA mismatch')
    eof = value.get('eof')
    require(isinstance(eof, dict) and eof.get('status') == 'PASS' and eof.get('offset') == sync.DATA_BYTES
            and eof.get('extra_bytes') == 0, 'content verification EOF mismatch')


def read_check(args):
    result, output = {'role': 'read-check', 'status': 'BLOCKED', 'performance_claim': False}, None
    try:
        output, root, _unused_payload, tool, identity = base.prepare(args)
        manifest = validate_write_manifest(sync.read_json(Path(args.manifest)), identity)
        payload = root / manifest['relative_path']
        expected = manifest['content']
        pre = verify_content(payload, expected)
        sample = run_sample(tool, payload, 'read', manifest['generation'], args.round_timeout)
        result.update(identity=identity, manifest={'path': str(Path(args.manifest)),
                      'relative_path': manifest['relative_path'], 'round': manifest['round'],
                      'generation': manifest['generation']}, pre_content=pre, **sample)
        require(sample['status'] == 'PASS', 'read-check probe failed')
        result.update(final_content=verify_content(payload, expected), status='DATA_RECORDED')
        sync.write_json(output / 'read-check.json', result)
    except Exception as error:
        result.update(status='BLOCKED', error=repr(error))
    finally:
        if output is not None:
            sync.write_json(output / 'summary.json', result)
    return result


def write_timings(manifests):
    require(isinstance(manifests, list) and len(manifests) == ROUND_COUNT,
            'exact six write manifests required')
    seen_paths, seen_chunks, values = set(), set(), []
    checked = []
    identity = None
    for index, manifest in enumerate(manifests):
        if identity is None:
            identity = manifest.get('identity')
        validate_write_manifest(manifest, identity, index)
        require(manifest['relative_path'] not in seen_paths, 'duplicate write relative path')
        seen_paths.add(manifest['relative_path'])
        for digest in manifest['content']['chunk_sha256']:
            require(digest not in seen_chunks, 'duplicate generation chunk digest')
            seen_chunks.add(digest)
        if index >= WARMUP_ROUNDS:
            values.append(sync.DATA_BYTES / 2**20 * 1e9 / manifest['result']['wall_ns'])
        checked.append({'round': index, 'generation': manifest['generation'], 'measured': manifest['measured'],
                        'relative_path': manifest['relative_path'], 'wall_ns': manifest['result']['wall_ns']})
    require(len(values) == MEASUREMENT_ROUNDS and len(seen_chunks) == ROUND_COUNT * 16,
            'incomplete measured writes or duplicate chunks')
    return {'warmup_rounds': WARMUP_ROUNDS, 'measured_rounds': MEASUREMENT_ROUNDS,
            'writes_checked': ROUND_COUNT, 'no_duplicate_generation_chunks': True,
            'mib_per_second': values, 'median_mib_per_second': statistics.median(values),
            'rounds': checked, 'timer_scope': 'C open/write/fdatasync/close; excludes Python driver SHA/EOF checks and directory fsync',
            'client_concurrency': 1, 'cache_residency': 'unobserved', 'rpc_write_location': 'unobserved',
            'qualified_threefs_parity': False, 'g2_23_write_performance_observation': True}


def summarize(args):
    output = sync.require_absolute_path(args.output, 'output')
    sync.validate_output_path(output)
    output.mkdir(mode=0o700)
    result = {'role': 'summary', 'status': 'BLOCKED'}
    try:
        manifests = [sync.read_json(Path(path)) for path in args.manifest]
        result.update(timings=write_timings(manifests), status='DATA_RECORDED')
    except Exception as error:
        result.update(status='BLOCKED', error=repr(error))
    finally:
        sync.write_json(output / 'summary.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='role', required=True)
    for role in ('write-round', 'read-check'):
        item = sub.add_parser(role)
        for name in ('dfs-root', 'io-tool', 'identity', 'output'):
            item.add_argument('--' + name, required=True)
        item.add_argument('--candidate', help='separate expected source/map/ELF/probe identity manifest')
        item.add_argument('--round-timeout', type=float, default=60)
        if role == 'write-round':
            item.add_argument('--round', type=int, required=True)
        else:
            item.add_argument('--manifest', required=True)
    item = sub.add_parser('summary')
    item.add_argument('--manifest', action='append', required=True,
                      help='write-round manifest path; provide exactly six in round order')
    item.add_argument('--output', required=True)
    args = parser.parse_args()
    result = (write_round(args) if args.role == 'write-round' else read_check(args)
              if args.role == 'read-check' else summarize(args))
    print(json.dumps(result, indent=2))
    return 0 if result.get('status') == 'DATA_RECORDED' else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Bounded public-SDK mixed load for the two-node acceptance deployment."""

from concurrent.futures import ThreadPoolExecutor
import json
import math
import re
import threading
import time


MINIMUM_OPERATIONS = {'command': 40, 'file': 40, 'create': 5, 'delete': 5}


def exercise(sandbox, marker):
    """Prove a command and a binary file round trip on one live sandbox."""
    if not re.fullmatch(r'[a-z0-9-]+', marker):
        raise ValueError('invalid mixed-load marker')
    started = time.monotonic()
    command = sandbox.commands.run("printf '%s' '" + marker + "'")
    command_ms = (time.monotonic() - started) * 1000
    if command.exit_code != 0 or command.stdout != marker:
        raise AssertionError('mixed-load command result differs from request')
    path = '/tmp/adx-soak-' + marker + '.bin'
    payload = marker.encode() * 16 + b'\x00\xff'
    started = time.monotonic()
    sandbox.files.write(path, payload)
    if sandbox.files.read(path, format='bytes') != payload:
        raise AssertionError('mixed-load binary file differs from request')
    return {'command': command_ms, 'file': (time.monotonic() - started) * 1000}


def evaluate(samples, errors, elapsed, minimum_seconds):
    """Summarize observed SDK work and require a sustained, error-free mix."""
    operations = {}
    for name, minimum in MINIMUM_OPERATIONS.items():
        values = sorted(samples.get(name, []))
        count = len(values)
        operations[name] = {'count': count, 'minimum': minimum}
        if values:
            operations[name].update({
                'p50_ms': round(values[math.ceil(count * 0.50) - 1], 3),
                'p95_ms': round(values[math.ceil(count * 0.95) - 1], 3),
                'p99_ms': round(values[math.ceil(count * 0.99) - 1], 3),
                'max_ms': round(values[-1], 3),
            })
    passed = (elapsed >= minimum_seconds and not errors
              and all(operations[name]['count'] >= minimum
                      for name, minimum in MINIMUM_OPERATIONS.items()))
    cases = [{'id': 'mixed-soak.' + name,
              'status': 'passed' if operations[name]['count'] >= minimum else 'failed',
              'seconds': 0} for name, minimum in MINIMUM_OPERATIONS.items()]
    cases.extend((
        {'id': 'mixed-soak.duration',
         'status': 'passed' if elapsed >= minimum_seconds else 'failed', 'seconds': 0},
        {'id': 'mixed-soak.zero-errors',
         'status': 'passed' if not errors else 'failed', 'seconds': 0},
    ))
    return {
        'status': 'passed' if passed else 'failed',
        'elapsed_seconds': round(elapsed, 3),
        'minimum_seconds': minimum_seconds,
        'operations': operations,
        'errors': errors,
        'cases': cases,
    }


def run(connection, image, output, seconds=300):
    """Keep two pinned sandboxes active while a third worker creates and deletes."""
    from adx_sandbox import Sandbox

    if not 1 <= seconds <= 900:
        raise ValueError('mixed-load duration must be between 1 and 900 seconds')
    samples = {name: [] for name in MINIMUM_OPERATIONS}
    errors = []
    lock = threading.Lock()
    stop = threading.Event()
    anchors = []
    started = None

    def record(name, milliseconds):
        with lock:
            samples[name].append(milliseconds)

    def fail(error):
        with lock:
            errors.append(f'{type(error).__name__}: {error}')
        stop.set()

    def create(node):
        begin = time.monotonic()
        sandbox = Sandbox(image=image, runtime='runc', node_id=node,
                          cpu=250, memory=256, idle_timeout=0,
                          connection=connection, create_timeout=150)
        record('create', (time.monotonic() - begin) * 1000)
        return sandbox

    def delete(sandbox):
        begin = time.monotonic()
        try:
            sandbox.kill()
            record('delete', (time.monotonic() - begin) * 1000)
        finally:
            sandbox.close()

    def anchor_work(index, sandbox, deadline):
        cycle = 0
        try:
            while not stop.is_set() and time.monotonic() < deadline:
                for name, latency in exercise(sandbox, f'anchor-{index}-{cycle}').items():
                    record(name, latency)
                cycle += 1
                stop.wait(0.25)
        except Exception as error:
            fail(error)

    def churn_work(deadline):
        cycle = 0
        try:
            while not stop.is_set() and time.monotonic() < deadline:
                sandbox = None
                try:
                    sandbox = create('node1' if cycle % 2 == 0 else 'node2')
                    for name, latency in exercise(sandbox, f'churn-{cycle}').items():
                        record(name, latency)
                finally:
                    if sandbox is not None:
                        delete(sandbox)
                cycle += 1
                stop.wait(0.5)
        except Exception as error:
            fail(error)

    try:
        anchors.append(create('node1'))
        anchors.append(create('node2'))
        started = time.monotonic()
        deadline = started + seconds
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = [pool.submit(anchor_work, index, anchor, deadline)
                    for index, anchor in enumerate(anchors)]
            jobs.append(pool.submit(churn_work, deadline))
            while not stop.is_set() and any(not job.done() for job in jobs):
                stop.wait(30)
                with lock:
                    counts = {key: len(value) for key, value in samples.items()}
                print('[SOAK] elapsed=' + str(round(time.monotonic() - started, 1)) +
                      's operations=' + str(counts),
                      flush=True)
            for job in jobs:
                job.result()
    except Exception as error:
        fail(error)
    finally:
        stop.set()
        for anchor in anchors:
            try:
                delete(anchor)
            except Exception as error:
                fail(error)
        elapsed = time.monotonic() - started if started is not None else 0
        report = evaluate(samples, errors, elapsed, seconds)
        output.write_text(json.dumps(report, indent=2) + '\n')
    if report['status'] != 'passed':
        raise AssertionError('mixed-load acceptance failed: ' + str(report['errors']))
    return report

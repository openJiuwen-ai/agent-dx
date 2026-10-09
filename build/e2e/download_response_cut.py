"""Recover a real SDK download after its HTTP body is truncated."""

import hashlib
import json
import tempfile
import time
import uuid
from pathlib import Path

from route_ready import wait_for_route
from upload_response_cut import CHUNK_SIZE, UploadResponseCutProxy


def run(connection, image, output, secrets):
    from adx_sandbox import ConnectionConfig, Sandbox
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'download-cut-' + uuid.uuid4().hex[:12]
    remote_path = '/tmp/' + name + '.bin'
    sandbox = None
    attached = None
    deleted = False
    proxy = UploadResponseCutProxy(
        'https://127.0.0.1:8443',
        certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key',
        ca=secrets / 'tls/ca.pem',
        cut_upload=False,
        download_cut_bytes=CHUNK_SIZE,
    )
    try:
        sandbox = Sandbox(
            name=name,
            image=image,
            runtime='runc',
            node_id='node1',
            cpu=500,
            memory=512,
            idle_timeout=0,
            detached=True,
            connection=connection,
            create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + sandbox.id])
        backend = labeled_backend(sandbox.id)
        if not (len(backend) == 1):
            raise AssertionError(backend)
        wait_for_route(sandbox)

        with tempfile.TemporaryDirectory(prefix='adx-download-cut-') as directory:
            source = Path(directory) / 'source.bin'
            target = Path(directory) / 'target.bin'
            payload = bytes(range(256)) * (CHUNK_SIZE * 3 // 256) + b'final-chunk'
            source.write_bytes(payload)
            expected_digest = hashlib.sha256(payload).hexdigest()
            sandbox.files.copy_from_local(str(source), remote_path)

            with proxy:
                proxied = ConnectionConfig(
                    server_address=f'127.0.0.1:{proxy.port}',
                    token=connection.token,
                    use_tls=True,
                    verify_tls=True,
                )
                attached = Sandbox.from_id(sandbox.id, connection=proxied)
                attached.files.copy_to_local(remote_path, str(target))
                attached.close()
                attached = None

            cut = proxy.cut_download
            if not (cut is not None and cut['bytes_sent'] == CHUNK_SIZE):
                raise AssertionError(cut)
            if not (len(proxy.download_attempts) == 2):
                raise AssertionError(proxy.download_attempts)
            first, second = proxy.download_attempts
            if not (first['range'] is None and first['status'] == 200):
                raise AssertionError(first)
            if not (second['status'] == 206 and second['range']):
                raise AssertionError(second)
            if not (second['range'].startswith('bytes=') and second['range'].endswith('-')):
                raise AssertionError(second)
            resumed_at = int(second['range'][6:-1])
            if not (0 < resumed_at <= CHUNK_SIZE):
                raise AssertionError(resumed_at)
            if not (not Path(str(target) + '.part').exists()):
                raise AssertionError()
            actual_digest = hashlib.sha256(target.read_bytes()).hexdigest()
            if not (actual_digest == expected_digest):
                raise AssertionError((actual_digest, expected_digest))

        final = json.loads(catalog()['environment:' + sandbox.id])
        if not (final['assignment']['generation'] == initial['assignment']['generation']):
            raise AssertionError()
        if not (labeled_backend(sandbox.id) == backend):
            raise AssertionError()
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        if not ('environment:' + sandbox.id not in catalog()):
            raise AssertionError()
        if not (not labeled_backend(sandbox.id)):
            raise AssertionError()

        report['status'] = 'passed'
        report['cases'].append(
            {
                'id': 'file.resumable-download-response-cut',
                'status': 'passed',
                'seconds': round(time.monotonic() - started, 3),
                'instance_id': sandbox.id,
                'resume_offset': resumed_at,
                'sha256': expected_digest,
                'backend': backend[0],
            }
        )
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if attached is not None:
            attached.close()
        if sandbox is not None:
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            sandbox.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['download_attempts'] = proxy.download_attempts
        report['cut_download'] = proxy.cut_download
        output.write_text(json.dumps(report, indent=2) + '\n')

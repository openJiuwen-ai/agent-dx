"""Bounded, read-only network probes for the configured public envd source pin."""

import json
import hashlib
import logging
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'out/buildkite/logs/envd-fetch'
LOGGER = logging.getLogger(__name__)


def tool(name):
    value = shutil.which(name)
    if value is None:
        raise FileNotFoundError(name)
    return str(Path(value).absolute())


def probe(name, command, timeout=25, extra=None):
    LOGGER.info('START %s', name)
    started = time.monotonic()
    environment = dict(os.environ, GIT_TERMINAL_PROMPT='0')
    environment.update(extra or {})
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               env=environment, start_new_session=True, text=True)
    expired = False
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        expired = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate()
    output = re.sub(r'(?im)((?:proxy-)?authorization|(?:set-)?cookie):[^\r\n]*', r'\1: [redacted]', output)
    (OUTPUT / (name + '.log')).write_text(output)
    result = {'probe': name, 'exit': process.returncode, 'timeout': expired,
              'seconds': round(time.monotonic() - started, 3)}
    LOGGER.info('%s', json.dumps(result))
    return result


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    OUTPUT.mkdir(parents=True, exist_ok=False)
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    repository = urlsplit(pin['repository'])
    if repository.scheme != 'https' or repository.hostname != 'github.com':
        raise ValueError('diagnostic is scoped to the configured public GitHub source')
    project = repository.path[:-4] if repository.path.endswith('.git') else repository.path
    project = project.strip('/')
    results = [probe('git-version', [tool('git'), '--version']),
               probe('curl-version', [tool('curl'), '--version']),
               probe('dns', [tool('getent'), 'ahostsv4', repository.hostname], timeout=10)]
    urls = {
        'github': pin['repository'] + '/info/refs?service=git-upload-pack',
        'codeload': 'https://codeload.github.com/' + project + '/tar.gz/' + pin['revision'],
        'api': 'https://api.github.com/repos/' + project + '/git/commits/' + pin['revision'],
    }
    for name, version in (('refs-v0-get', None), ('refs-v2-get', 'version=2')):
        with tempfile.TemporaryDirectory(prefix='adx-envd-refs-') as directory:
            args = [tool('curl'), '--fail', '--silent', '--show-error', '--verbose', '--connect-timeout', '8',
                    '--max-time', '18', '--max-filesize', '8388608', '--output', str(Path(directory) / 'refs')]
            if version:
                args += ['--header', 'Git-Protocol: ' + version]
            result = probe(name, args + [urls['github']])
            path = Path(directory) / 'refs'
            result['bytes'] = path.stat().st_size if path.exists() else 0
            results.append(result)
    trace = {'GIT_TRACE': '1', 'GIT_TRACE_CURL': '1', 'GIT_TRACE_CURL_NO_DATA': '1', 'GIT_TRACE_REDACT': '1'}
    for name, version, filtered in (('protocol2-filtered', None, True),
                                    ('protocol2-http1-filtered', 'HTTP/1.1', True)):
        with tempfile.TemporaryDirectory(prefix='adx-envd-diagnostic-') as directory:
            if shutil.disk_usage(directory).free < 2 * 1024**3:
                raise ValueError('at least 2GiB temporary space required')
            git = [tool('git'), '-c', 'credential.helper=', '-c', 'http.extraHeader=', '-C', directory]
            probe(name + '-init', git + ['init'])
            probe(name + '-remote', git + ['remote', 'add', 'origin', pin['repository']])
            if filtered:
                for key, value in (('core.repositoryformatversion', '1'), ('extensions.partialClone', 'origin'),
                                   ('core.partialCloneFilter', 'blob:none'), ('remote.origin.promisor', 'true'),
                                   ('remote.origin.partialCloneFilter', 'blob:none')):
                    probe(name + '-' + key, git + ['config', key, value])
            arguments = git + ['-c', 'protocol.version=2']
            arguments += (['-c', 'http.version=' + version] if version else []) + ['fetch', '--depth=1']
            if filtered:
                arguments.append('--filter=blob:none')
            result = probe(name, arguments + ['origin', pin['revision']], timeout=40, extra=trace)
            if result['exit'] == 0:
                result['head'] = subprocess.check_output(git + ['rev-parse', 'FETCH_HEAD'], text=True).strip()
            results.append(result)
    results.append(probe('pinned-archive-get', [sys.executable, __file__, '--archive'], timeout=40))
    (OUTPUT / 'result.json').write_text(json.dumps({'pin': pin['revision'], 'results': results}, indent=2) + '\n')


def archive():
    pin = json.loads((ROOT / 'third_party/e2b/source.json').read_text())
    project = urlsplit(pin['repository']).path[:-4].strip('/')
    url = 'https://codeload.github.com/' + project + '/tar.gz/' + pin['revision']
    path = OUTPUT / 'envd-source.tar.gz'
    total = 0
    try:
        with urlopen(url, timeout=18) as response, path.open('wb') as output:
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                total += len(block)
                if total > 64 * 1024**2:
                    raise ValueError('source archive exceeds 64MiB limit')
                output.write(block)
        checks = []
        with tarfile.open(path, 'r:gz') as stream:
            entries = stream.getmembers()
            prefix = entries[0].name.split('/')[0] + '/'
            for expected in pin['files']:
                member = stream.getmember(prefix + expected['source'])
                if not member.isfile() or member.size > 2 * 1024**2:
                    raise ValueError('unexpected pinned member')
                digest = hashlib.sha256(stream.extractfile(member).read()).hexdigest()
                if digest != expected['sha256']:
                    raise ValueError('pinned member differs: ' + expected['source'])
                checks.append(expected['source'])
        receipt = {'url': url, 'revision': pin['revision'], 'bytes': total,
                   'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'pinned_file_checks': checks,
                   'members': len(entries), 'prefix': prefix}
        (OUTPUT / 'archive-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    except Exception:
        path.unlink(missing_ok=True)
        raise


if __name__ == '__main__':
    if sys.argv[1:] == ['--archive']:
        archive()
    else:
        main()

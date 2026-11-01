"""Bounded, read-only network probes for the configured public envd source pin."""

import json
import logging
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

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
    for name, url in urls.items():
        results.append(probe(name, [tool('curl'), '--head', '--silent', '--show-error', '--verbose',
                                    '--connect-timeout', '8', '--max-time', '18', url]))
    trace = {'GIT_TRACE': '1', 'GIT_TRACE_CURL': '1', 'GIT_TRACE_CURL_NO_DATA': '1', 'GIT_TRACE_REDACT': '1'}
    for name, version, filtered in (('default-filtered', None, True),
                                    ('http1-filtered', 'HTTP/1.1', True),
                                    ('http1-unfiltered', 'HTTP/1.1', False)):
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
            arguments = git + (['-c', 'http.version=' + version] if version else []) + ['fetch', '--depth=1']
            if filtered:
                arguments.append('--filter=blob:none')
            result = probe(name, arguments + ['origin', pin['revision']], timeout=40, extra=trace)
            if result['exit'] == 0:
                result['head'] = subprocess.check_output(git + ['rev-parse', 'FETCH_HEAD'], text=True).strip()
            results.append(result)
    (OUTPUT / 'result.json').write_text(json.dumps({'pin': pin['revision'], 'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()

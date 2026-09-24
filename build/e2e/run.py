#!/usr/bin/env python3
"""Deploy an immutable bundle into two isolated nodes; fail on any residual resource."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import signal
import shlex
import sys
import threading
import subprocess
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET

BASIC = ('sdk','auth','capacity','placement','local-first')
STANDARD = ('sdk','data-plane','lifecycle','auth','capacity','placement','local-first','node-failure','sandboxd-restart','restart','stop')
OPTIONAL_FAULTS = ('redis-restart',)
PROFILES = {
    'l0': ('l0','auth'),
    'standalone': STANDARD,
    'k8s-basic': BASIC,
    'full': STANDARD,
}
def required_for_profile(profile):
    try:
        return PROFILES[profile]
    except KeyError as error:
        raise ValueError('unknown E2E profile: '+profile) from error

def selected_checks(profile, case=None):
    required=required_for_profile(profile)
    if case is None:
        return required
    if profile in ('standalone','full') and case in OPTIONAL_FAULTS:
        return (case,)
    if case not in required:
        raise ValueError(f'{case} is not in E2E profile {profile}')
    return (case,)

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1024*1024),b''):h.update(data)
    return h.hexdigest()

def validate_identity(package, architecture, commit, ci):
    expected={'x86_64-unknown-linux-gnu':'amd64','aarch64-unknown-linux-gnu':'arm64'}.get(package['target'])
    if not expected or architecture != expected:raise ValueError('runtime architecture differs from package')
    if ci and (package['dirty'] or package['commit'] != commit):raise ValueError('CI package is dirty or from another commit')

def verify_bundle(directory):
    m=json.loads((directory/'bundle.json').read_text())
    if m.get('schema_version') != 1 or sha(directory/'images.tar') != m.get('archive_sha256'):
        raise ValueError('bundle integrity check failed')
    if sha(directory/'execd.tar') != m.get('execd_archive_sha256'):raise ValueError('EXECD archive integrity check failed')
    if sha(directory/'entrypoint.tar') != m.get('entrypoint_archive_sha256'):
        raise ValueError('entrypoint image archive integrity check failed')
    return m

def finish_report(error, cleanup_errors, checks, required=None):
    required=tuple(required or STANDARD)
    missing=[name for name in required if name not in checks]
    return {'status':'passed' if not error and not cleanup_errors and not missing else 'failed','error':error,'cleanup_errors':cleanup_errors,'checks':checks,'required_checks':list(required),'missing_checks':missing}

def sdk_subcases_from_output(output):
    """Return the last structured SDK case list printed by a scenario."""
    if not isinstance(output,str):
        return []
    for line in reversed(output.splitlines()):
        try:
            payload=json.loads(line)
        except (TypeError,json.JSONDecodeError):
            continue
        cases=payload.get('cases') if isinstance(payload,dict) else None
        if not isinstance(cases,list):
            continue
        normalized=[]
        for case in cases:
            if not isinstance(case,dict):
                raise ValueError('invalid SDK subcase result')
            case_id=case.get('id') or case.get('name')
            if not isinstance(case_id,str):
                raise ValueError('invalid SDK subcase result')
            status=case.get('status')
            if status is None and isinstance(case.get('passed'),bool):
                status='passed' if case['passed'] else 'failed'
            if status not in ('passed','failed'):
                raise ValueError('invalid SDK subcase status')
            normalized.append({
                'id':case_id,
                'status':status,
                'seconds':float(case.get('seconds',0)),
                **({'error':str(case['error'])} if 'error' in case else {}),
            })
        return normalized
    return []

def cleanup_cgroup(root):
    """Remove only empty cgroups after all test-owned container processes exit."""
    if not root.exists():
        return
    for directory in sorted((p for p in root.rglob('*') if p.is_dir()),
                            key=lambda p: len(p.parts), reverse=True):
        directory.rmdir()
    root.rmdir()


class Run:
    def __init__(self,output,cgroupns='private'):
        if cgroupns not in ('private', 'host'):
            raise ValueError('cgroupns must be private or host')
        self.cgroupns=cgroupns
        self.output=output;self.id='adx-e2e-'+uuid.uuid4().hex[:12]
        self.nodes=[];self.network=False;self.commands=0
        self.redactions=set();self.case_results=[]
    def event(self, message):
        print(time.strftime('%H:%M:%S', time.gmtime()) + ' ' + self.redact(message), flush=True)

    def redact(self, text):
        for secret in sorted(self.redactions, key=len, reverse=True):
            text = text.replace(secret, '[REDACTED]')
        return text

    def command(self, args, timeout=180, *, input_data=None, stream=True, label=None):
        self.commands += 1
        log = self.output / f'{self.commands:03d}.log'
        args = list(map(str, args))
        name = label or shlex.join(args)
        self.event(f'[EXEC {log.name}] {name}')
        start = time.monotonic()
        read_errors = []
        with log.open('w') as output:
            process = subprocess.Popen(args, stdin=subprocess.PIPE if input_data is not None else None,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, errors='replace', start_new_session=True)
            def forward():
                try:
                    for line in process.stdout:
                        line = self.redact(line)
                        output.write(line)
                        output.flush()
                        if stream:
                            print(line, end='', flush=True)
                except Exception as error:
                    read_errors.append(error)
            reader = threading.Thread(target=forward, daemon=True)
            reader.start()
            try:
                if input_data is not None:
                    process.stdin.write(input_data)
                    process.stdin.close()
                while True:
                    remaining = timeout - (time.monotonic() - start)
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(args, timeout)
                    try:
                        process.wait(timeout=min(10, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        if time.monotonic() - start >= timeout:
                            raise
                        self.event(f'[WAIT {log.name}] {time.monotonic() - start:.0f}s elapsed')
                reader.join(timeout=5)
                if reader.is_alive():
                    raise RuntimeError('command output did not close')
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                reader.join(timeout=5)
                self.event(f'[FAIL {log.name}] interrupted or timed out after {time.monotonic() - start:.1f}s')
                raise
            finally:
                process.stdout.close()
                if process.stdin and not process.stdin.closed:
                    process.stdin.close()
        if read_errors:
            raise read_errors[0]
        self.event(f'[EXIT {log.name}] code={process.returncode}, elapsed={time.monotonic() - start:.1f}s')
        if process.returncode:
            if not stream:
                print(''.join(log.read_text().splitlines(keepends=True)[-20:]), end='', flush=True)
            raise RuntimeError(f'command failed ({process.returncode}); see {log.name}')
        return log.read_text()

    @contextmanager
    def case(self, name, checks):
        print('--- Case: ' + name, flush=True)
        start = time.monotonic()
        self.event('[RUN] ' + name)
        record = {'name': name, 'status': 'failed'}
        try:
            yield record
        except Exception as error:
            record['error'] = self.redact(str(error))
            self.event(f'[FAIL] {name}: {error}')
            raise
        else:
            checks.append(name)
            record['status'] = 'passed'
        finally:
            record['seconds'] = round(time.monotonic() - start, 3)
            self.case_results.append(record)
            (self.output / 'case-results.json').write_text(json.dumps(self.case_results, indent=2) + '\n')
            self.event(f"[{'PASS' if record['status'] == 'passed' else 'FAIL'}] {name} ({record['seconds']:.3f}s)")

    def docker(self,*args,timeout=180):return self.command(['docker',*args],timeout)
    def execute(self,node,*args,timeout=180):return self.docker('exec',self.id+'-'+node,*args,timeout=timeout)
    def helper(self,node,*args,timeout=180):return self.execute(node,'python3','-u','/opt/adx/e2e/node.py',*args,timeout=timeout)
    def sdk_instances(self):
        return json.loads((self.output/'sdk/sdk-result.json').read_text())['instances']
    def cleanup(self):
        errors=[]
        for node in reversed(self.nodes):
            # Diagnostics are best effort; stop/remove failures are acceptance failures.
            try:self.helper(node,'collect',node,timeout=20)
            except Exception:pass
            try:
                self.docker('rm','-f',self.id+'-'+node,timeout=45)
                remaining=self.docker('ps','-a','--filter','name=^/'+self.id+'-'+node+'$','--format','{{.ID}}')
                if remaining.strip():raise RuntimeError('container remains')
                if self.cgroupns == 'host':
                    cleanup_cgroup(Path('/sys/fs/cgroup') / (self.id+'-'+node))
            except Exception as e:errors.append(f'{node}: {e}')
        if self.network:
            try:
                self.docker('network','rm',self.id,timeout=30)
                if self.docker('network','ls','--filter','name=^'+self.id+'$','--format','{{.ID}}').strip():raise RuntimeError('network remains')
            except Exception as e:errors.append(str(e))
        return errors
    def deploy(self,m,bundle,secrets):
        self.docker('load','-i',bundle/'images.tar',timeout=600)
        for identity in m['image_ids'].values():
            info=json.loads(self.docker('image','inspect',identity))[0]
            if info['Id'] != identity or info['Architecture'] != m['architecture']:raise ValueError('loaded image identity mismatch')
        info=json.loads(self.docker('info','--format','{{json .}}'))
        arch={'aarch64':'arm64','x86_64':'amd64'}.get(info['Architecture'],info['Architecture'])
        validate_identity(m['package'],arch,os.getenv('BUILDKITE_COMMIT'),bool(os.getenv('BUILDKITE')))
        self.docker('network','create','--label','adx.e2e.run='+self.id,self.id);self.network=True
        for node in ('node1','node2'):
            name=self.id+'-'+node;self.nodes.append(node)
            args=['run','-d','--name',name,'--label','adx.e2e.run='+self.id,'--network',self.id,'--network-alias','coordinator' if node=='node1' else 'node2','--privileged','--cgroupns='+self.cgroupns,'-e','ADX_E2E_CGROUP_ROOT='+name,'--cpus=3','--memory=4g','--tmpfs','/tmp/adx-e2e/sandboxd/image_manager:size=1g','-v',f'{secrets}:/secrets','-v',f'{self.output}:/evidence']
            args+=['-v',f'{bundle / "execd.tar"}:/execd.tar:ro']
            self.docker(*args,m['image_ids']['node'])
            self.helper(node,'setup',node)
            self.execute(node,'sh','-c','python3 /opt/adx/e2e/node.py services '+node+' > /evidence/services-'+node+'.log 2>&1 &')
        self.execute('node1','python3','/opt/adx/e2e/publish.py',timeout=300)
        for node in self.nodes:
            self.execute(node,'sh','-c','/opt/adx/package/bin/adxctl run --config /tmp/adx-e2e/deployment.yaml > /evidence/supervisor-'+node+'.log 2>&1 &')
        self.helper('node1','ready',timeout=120)
    def scenarios(self,checks,required=None):
        required=tuple(required or STANDARD);selected=set(required)
        if 'l0' in selected:
            with self.case('l0', checks):
                self.event('Run the minimum public SDK create/query/command/file/delete flow')
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','l0',timeout=600)
                self.helper('node1','postcheck','l0')
                for node in self.nodes:self.helper(node,'empty',node)
        if 'sdk' in selected:
            with self.case('sdk', checks) as record:
                self.event('Create/query instances; verify command stdout/stderr/exit code, binary files and deletion')
                for node in self.nodes:self.execute(node,'python3','/opt/adx/e2e/telemetry.py','outage-start',node)
                output=self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','sdk',timeout=600)
                record['subcases']=sdk_subcases_from_output(output)
                instances=self.sdk_instances()
                runtime_ids=set()
                for node in self.nodes:
                    observed=json.loads(self.execute(node,'python3','/opt/adx/e2e/runtime_logs.py',*instances))
                    runtime_ids.update(observed['runtime_ids'])
                if not all(any(runtime_id.startswith(instance_id+'-') for runtime_id in runtime_ids)
                           for instance_id in instances):
                    raise AssertionError('sandboxd runtime stdout/stderr files missing')
                record['subcases'].append({'id':'runtime.host-stdout-stderr','status':'passed','seconds':0})
                for node in self.nodes:self.execute(node,'python3','/opt/adx/e2e/telemetry.py','outage-end',node)
                self.helper('node1','postcheck')
                for node in self.nodes:self.helper(node,'empty',node)
                record['subcases'].append({
                    'id':'sandbox.delete-and-resource-cleanup',
                    'status':'passed',
                    'seconds':0,
                })
        for scenario, description in (
            ('data-plane', 'Exercise resource discovery, reattach, commands, files, PTY, forwarded ports and reverse tunnel'),
            ('lifecycle', 'Exercise detached reattach/delete and idle-timeout reclamation'),
        ):
            if scenario in selected:
                with self.case(scenario, checks) as record:
                    self.event(description)
                    output=self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py',scenario,timeout=600)
                    record['subcases']=sdk_subcases_from_output(output)
                    for node in self.nodes:self.helper(node,'empty',node)
        for scenario in ('auth','capacity','placement'):
            if scenario in selected:
                with self.case(scenario, checks) as record:
                    output=self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py',scenario,timeout=400)
                    record['subcases']=sdk_subcases_from_output(output)
                    for node in self.nodes:self.helper(node,'empty',node)
        if 'local-first' in selected:
            with self.case('local-first', checks):
                self.helper('node1','create-mode','local_first')
                try:
                    self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','local-first',timeout=600)
                    for node in self.nodes:self.helper(node,'empty',node)
                finally:
                    self.helper('node1','create-mode','central')
        if 'node-failure' in selected:
            with self.case('node-failure', checks):
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','create',timeout=300)
                try:
                    self.helper('node2','freeze','node2')
                    self.helper('node1','failure-observed',timeout=75)
                finally:
                    self.helper('node2','thaw','node2')
                self.helper('node1','ready',timeout=150)
                self.helper('node2','empty','node2')
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','failure-cleanup',timeout=90)
                for node in self.nodes:self.helper(node,'empty',node)
        if 'sandboxd-restart' in selected:
            with self.case('sandboxd-restart', checks):
                self.event('Create live instances, restart sandboxd, and verify identities and file contents')
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','create-marker',timeout=300)
                for node in self.nodes:self.helper(node,'restart-sandboxd',node,timeout=90)
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','recovered-marker',timeout=90)
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','cleanup-live',timeout=90)
                for node in self.nodes:self.helper(node,'empty',node)
        if 'restart' in selected:
            with self.case('restart', checks):
                self.event('Create live instances and record backend IDs before restarting Adxlets')
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','create',timeout=300)
                self.helper('node1','sessions')
                for node in self.nodes:self.helper(node,'restart',node)
                self.helper('node1','ready','restart',timeout=150)
                for node in self.nodes:self.helper(node,'unchanged',node)
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','recovered',timeout=90)
        if 'redis-restart' in selected:
            with self.case('redis-restart', checks):
                self.event('Create pinned instances, crash AOF Redis and verify ownership and backends')
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','create-marker',timeout=300)
                for node in self.nodes:self.helper(node,'capture-backend',node)
                self.helper('node1','redis-restart','node1',timeout=90)
                self.helper('node1','ready',timeout=150)
                for node in self.nodes:self.helper(node,'unchanged',node)
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','recovered-marker',timeout=90)
                self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','cleanup-live-redis',timeout=90)
                for node in self.nodes:self.helper(node,'empty',node)
        if 'stop' in selected:
            with self.case('stop', checks):
                self.event('Create live backends on both nodes, stop services, and verify physical cleanup')
                for node in self.nodes:
                    self.execute(node,'python3','/opt/adx/e2e/telemetry.py','outage-start',node)
                try:
                    self.execute('node1','/opt/adx/client/bin/python','-u','/opt/adx/e2e/scenarios.py','create-stop',timeout=300)
                finally:
                    for node in self.nodes:
                        self.execute(node,'python3','/opt/adx/e2e/telemetry.py','outage-end',node)
                for node in self.nodes:self.helper(node,'occupied',node)
                for node in reversed(self.nodes):
                    self.helper(node,'stop',node,timeout=180)
                    self.helper(node,'empty',node)

def write_junit(path, report, suite_name='platform-e2e'):
    suite=ET.Element('testsuite',name=suite_name)
    records={case['name']:case for case in report['cases']}
    for name in report.get('required_checks',STANDARD):
        record=records.get(name)
        subcases=record.get('subcases',[]) if record else []
        if record and record['status']=='passed' and subcases:
            for subcase in subcases:
                case=ET.SubElement(
                    suite,'testcase',name=f"{name}/{subcase['id']}",
                    time=str(subcase.get('seconds',0)),
                )
                if subcase['status']!='passed':
                    ET.SubElement(case,'failure').text=subcase.get('error','case failed')
            continue
        case=ET.SubElement(suite,'testcase',name=name,time=str(record['seconds'] if record else 0))
        if not record:
            ET.SubElement(case,'skipped').text='Not reached; see acceptance error'
        elif record['status']!='passed':
            ET.SubElement(case,'failure').text=record.get('error','case failed')
    cleanup=ET.SubElement(suite,'testcase',name='cleanup')
    if report['cleanup_errors']:
        ET.SubElement(cleanup,'failure').text='\n'.join(report['cleanup_errors'])
    if report['error'] and not any(case['status']=='failed' for case in report['cases']):
        ET.SubElement(ET.SubElement(suite,'testcase',name='deployment'),'failure').text=report['error']
    suite.set('tests',str(len(suite)))
    suite.set('failures',str(len(suite.findall('testcase/failure'))))
    suite.set('skipped',str(len(suite.findall('testcase/skipped'))))
    ET.ElementTree(suite).write(path,encoding='utf-8',xml_declaration=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('--bundle',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--profile',choices=('l0','standalone'),default='standalone');p.add_argument('--case',help='run one case as a targeted diagnostic, not a profile gate');p.add_argument('--cgroupns',choices=('private','host'),default='private');a=p.parse_args()
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    run=Run(out,cgroupns=a.cgroupns);error=None;checks=[];m=None;required=selected_checks(a.profile,a.case)
    def cancel(signum,frame):raise InterruptedError(f'canceled by signal {signum}')
    for s in (signal.SIGTERM,signal.SIGINT):signal.signal(s,cancel)
    with tempfile.TemporaryDirectory(prefix='adx-e2e-secrets-') as private:
        try:
            m=verify_bundle(a.bundle.resolve())
            validate_identity(m['package'],m['architecture'],os.getenv('BUILDKITE_COMMIT'),bool(os.getenv('BUILDKITE')))
            (out/'bundle.json').write_text(json.dumps(m,indent=2))
            run.deploy(m,a.bundle.resolve(),Path(private))
            run.scenarios(checks,required)
        except Exception as e:error=f'{type(e).__name__}: {e}'
        finally:
            # A second TERM must not interrupt cleanup of resources already owned.
            signal.signal(signal.SIGTERM,signal.SIG_IGN)
            signal.signal(signal.SIGINT,signal.SIG_IGN)
            errors=run.cleanup()
    report=finish_report(error,errors,checks,required);report.update(run_id=run.id,deployment='local-docker',profile='targeted' if a.case else a.profile,source_profile=a.profile,selected_case=a.case,cgroupns=a.cgroupns,cases=run.case_results)
    (out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    write_junit(out/'junit.xml',report)
    print(json.dumps(report));return 0 if report['status']=='passed' else 1
if __name__=='__main__':raise SystemExit(main())

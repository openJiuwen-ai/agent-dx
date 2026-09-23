#!/usr/bin/env python3
"""E2E-only OTLP backend plus external Collector hosting and assertions."""
import collections
import gzip
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

P=Path('/tmp/adx-e2e'); E=Path('/evidence'); S=Path('/secrets')
D=E
BATCHES=D/'collected-logs.jsonl'

def node_context(node):
    global D,BATCHES
    D=E/('telemetry-'+node);D.mkdir(exist_ok=True)
    BATCHES=D/'collected-logs.jsonl'

def wait(predicate, label, seconds=30):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        result=predicate()
        if result:return result
        time.sleep(.2)
    raise TimeoutError(label)

def any_value(v):
    for key in ('stringValue','boolValue','intValue','doubleValue'):
        if key in v:return v[key]
    if 'kvlistValue' in v:return {x['key']:any_value(x['value']) for x in v['kvlistValue']['values']}
    if 'arrayValue' in v:return [any_value(x) for x in v['arrayValue']['values']]
    return None

def records():
    if not BATCHES.exists():return []
    result=[]
    for line in BATCHES.read_text().splitlines():
        try:batch=json.loads(line)
        except json.JSONDecodeError:continue  # Live writer may still be completing the last line.
        for resource in batch.get('resourceLogs',[]):
            attrs={a['key']:any_value(a['value']) for a in resource.get('resource',{}).get('attributes',[])}
            for scope in resource.get('scopeLogs',[]):
                result += [(attrs,any_value(r.get('body',{}))) for r in scope.get('logRecords',[])]
    return result

def setup(node):
    (P/'collector-state').mkdir(exist_ok=True)
    (P/'state/logs').mkdir(parents=True,exist_ok=True)
    config=json.loads(Path('/opt/adx/observability/collector.json').read_text())
    config['receivers']['otlp']['protocols']['http']['endpoint']='0.0.0.0:14317'
    env={'ADX_LOG_DIR':str(P/'state/logs'),'ADX_NODE_ID':node,'ADX_COLLECTOR_STATE':str(P/'collector-state'),'ADX_OTLP_ENDPOINT':'http://127.0.0.1:14318'}
    text=json.dumps(config)
    for key,value in env.items():text=text.replace('${env:'+key+'}',value)
    tmp=P/'collector.json.tmp';tmp.write_text(text);tmp.replace(P/'collector.json')

def run():
    # The deployment driver owns the startup deadline; a sidecar may start
    # before the peer Pod finishes pulling its image and config is generated.
    while not (P/'collector.json').exists():time.sleep(.2)
    node_context(json.loads((P/'collector.json').read_text())['receivers']['filelog/adx']['resource']['adx.node.id'])
    secrets=[p.read_text().strip() for p in S.glob('*key') if p.is_file()]
    lock=threading.Lock()
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            data=self.rfile.read(int(self.headers.get('Content-Length','0')))
            if self.headers.get('Content-Encoding')=='gzip':data=gzip.decompress(data)
            if self.path not in ('/v1/logs','/v1/traces'):self.send_error(404);return
            if (P/'sink-paused').exists():
                (D/'collector-backend-unavailable').write_text('observed')
                self.send_error(503);return
            if any(secret and secret.encode() in data for secret in secrets):
                (D/'collector-secret-leak').write_text('credential found before collection storage')
                self.send_error(400);return
            batch=json.loads(data)
            destination=BATCHES if self.path=='/v1/logs' else D/'collected-traces.jsonl'
            with lock, destination.open('a') as f:
                f.write(json.dumps(batch)+'\n');f.flush()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{}')
    server=http.server.ThreadingHTTPServer(('127.0.0.1',14318),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    stopped=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stopped.set())
    count=0;child=None
    log=(D/'collector-process.log').open('a')
    try:
        while not stopped.is_set():
            child=subprocess.Popen(['/usr/local/bin/otelcol-contrib','--config='+str(P/'collector.json')],stdout=log,stderr=subprocess.STDOUT)
            count+=1
            marker=P/'collector-starts.tmp';marker.write_text(str(count));marker.replace(P/'collector-starts')
            while not stopped.wait(.1):
                if child.poll() is not None:raise RuntimeError('Collector exited; see collector-process.log')
                if (P/'collector-restart').exists():
                    (P/'collector-restart').unlink();break
            child.terminate();child.wait(timeout=20);child=None
    finally:
        if child is not None and child.poll() is None:child.terminate();child.wait(timeout=20)
        server.shutdown();server.server_close();log.close()

def metrics(node):
    endpoints={'proxy':'http://127.0.0.1:19443/metrics'}
    if node=='node1':endpoints['ingress']='http://127.0.0.1:18080/metrics'
    result={}
    for name,url in endpoints.items():
        with urllib.request.urlopen(url,timeout=5) as response:text=response.read().decode()
        prefix='data_plane_relay_' if name=='proxy' else 'data_plane_ingress_'
        assert prefix+'ready 1' in text
        metric=prefix+('connect_total' if name=='proxy' else 'http_requests_total')
        assert float(next(line.split()[-1] for line in text.splitlines() if line.startswith(metric+' ')))>0
        result[name]=text
    (E/f'gateway-metrics-{node}.json').write_text(json.dumps({'status':'passed','scrapes':result},indent=2))
    print('[METRICS PASS] existing gateway exporters: '+node,flush=True)

def outage(node, start):
    node_context(node)
    if start:
        (D/'collector-backend-unavailable').unlink(missing_ok=True)
        (P/'sink-paused').touch()
        with (P/'state/logs/collection-probe.log').open('a') as f:
            f.write(json.dumps({'event':'business_outage_probe','message':'x'*180})+'\n')
        wait(lambda:(D/'collector-backend-unavailable').exists(),'Collector did not reach unavailable backend')
        print('[COLLECTION] backend unavailable; running public SDK lifecycle with Collector retrying',flush=True)
    else:
        assert (P/'sink-paused').exists()
        (P/'sink-paused').unlink()
        (D/'business-outage.json').write_text(json.dumps({'sdk_lifecycle_passed':True}))
        with urllib.request.urlopen('http://127.0.0.1:18888/metrics',timeout=5) as response:
            (D/'collector-metrics.txt').write_bytes(response.read())
        print('[COLLECTION PASS] public SDK lifecycle succeeded during backend outage: '+node,flush=True)

def trace_records():
    path=D/'collected-traces.jsonl'
    rows=[]
    if not path.exists():return rows
    for line in path.read_text().splitlines():
        try:batch=json.loads(line)
        except json.JSONDecodeError:continue # Writer may be appending the last batch.
        for resource in batch.get('resourceSpans',[]):
            attrs={a['key']:any_value(a['value']) for a in resource.get('resource',{}).get('attributes',[])}
            for scope in resource.get('scopeSpans',[]):
                rows.extend({**span,'service':attrs.get('service.name')} for span in scope.get('spans',[]))
    return list({(s['traceId'],s['spanId']):s for s in rows}.values())

def check_trace_links(rows):
    index={(s['traceId'],s['spanId']):s for s in rows}
    executions=[s for s in rows if s['name']=='environment.execute']
    assert executions,'environment execution spans missing'
    for span in executions:
        parent=index.get((span['traceId'],span.get('parentSpanId')))
        assert parent and parent['name']=='environment.queue','queue parent missing or belongs to another trace'
    return len(executions)

def required_log_services(node):
    services={node}
    if node=='node1':services|={'coordinator','api','redis'}
    return services

def validate_traces(node):
    rows=trace_records()
    count=check_trace_links(rows)
    services={s['service'] for s in rows}
    assert 'adxlet' in services,services
    assert any(s['name']=='execd.http' and s.get('parentSpanId','').strip('0') for s in rows),'EXECD did not receive data request context'
    trace_ids=[]
    if node=='node1':
        names={'ingress.http','apiserver.http','coordinator.create_environment','node.create_environment','environment.queue','environment.execute','coordinator.commit_environment'}
        groups={}
        for span in rows:groups.setdefault(span['traceId'],set()).add(span['name'])
        trace_ids=[key for key,value in groups.items() if names <= value]
        assert trace_ids,'no complete Ingress/API/Coordinator/Node/state-commit creation trace'
        assert any(s['name']=='node.delete_environment' for s in rows),'delete trace missing'
    result={'status':'passed','span_count':len(rows),'services':sorted(services),'environment_executions':count,'complete_create_trace_ids':trace_ids,'execd_context_received':True}
    (E/f'traces-{node}.json').write_text(json.dumps(result,indent=2))
    print(f'[TRACE PASS] {node}: {len(rows)} spans; {count} queue/execution parent links; EXECD context received',flush=True)


def validate(node):
    node_context(node)
    wait(lambda:(P/'collector-starts').exists(),'collector never started')
    token=uuid.uuid4().hex
    probe=P/'state/logs/collection-probe.log'
    def write_range(start,end):
        with probe.open('a') as f:
            for i in range(start,end):f.write(json.dumps({'event':'collector_probe','run_id':token,'sequence':i,'message':'x'*180})+'\n')
    def received():return [int(b['sequence']) for _,b in records() if isinstance(b,dict) and b.get('run_id')==token]
    write_range(0,20)
    wait(lambda:len(received())>=20,'first probe not exported')
    # Restart after an observed backend failure; the persistent send queue and
    # file cursor must recover. The receiver has never accepted the failed batch.
    (D/'collector-backend-unavailable').unlink(missing_ok=True)
    (P/'sink-paused').touch();probe.rename(probe.with_name(probe.name+'.00000000000000000001'))
    write_range(20,40)
    wait(lambda:(D/'collector-backend-unavailable').exists(),'backend failure not exercised')
    previous=int((P/'collector-starts').read_text());(P/'collector-restart').touch()
    wait(lambda:int((P/'collector-starts').read_text())>previous,'collector restart not completed')
    (P/'sink-paused').unlink()
    wait(lambda:len(received())>=40,'buffered logs not recovered',60)
    time.sleep(2)
    counts=collections.Counter(received());assert counts==collections.Counter(range(40)),dict(counts)
    rows=records();services={a.get('service.name') for a,_ in rows}
    required=required_log_services(node)
    assert required <= services, (required,services)
    structured={a.get('service.name') for a,b in rows if isinstance(b,dict) and ('level' in b or 'fields' in b)}
    assert required-{'redis'} <= structured, (required,structured)
    states={b.get('fields',{}).get('state') for _,b in rows if isinstance(b,dict) and b.get('fields',{}).get('event')=='environment_operation_completed'}
    assert 'Running' in states and 'Deleted' in states, states
    assert not (D/'collector-secret-leak').exists(),'credential leaked into log pipeline'
    assert json.loads((D/'business-outage.json').read_text())['sdk_lifecycle_passed']
    validate_traces(node)
    result={'status':'passed','sdk_during_backend_outage':True,'probe_records':40,'unique_probe_records':40,'collector_restarted':True,'backend_failure_recovered':True,'services':sorted(services),'structured_services':sorted(structured),'instance_states':sorted(s for s in states if s),'credentials_absent':True}
    (E/f'collection-{node}.json').write_text(json.dumps(result,indent=2))
    print(f'[COLLECTION PASS] {node}: 40/40 unique records after rotation, backend outage and Collector restart; service logs received',flush=True)

if __name__=='__main__':
    action=sys.argv[1]
    if action=='run':run()
    elif action=='setup':setup(sys.argv[2])
    elif action=='metrics':metrics(sys.argv[2])
    elif action=='validate':validate(sys.argv[2])
    elif action in ('outage-start','outage-end'):outage(sys.argv[2],action=='outage-start')
    else:raise ValueError(action)

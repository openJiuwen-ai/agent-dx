"""Scrape running services and compare actual allocations across both nodes."""
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request


def values(text, metric, **wanted):
    found=[]
    for line in text.splitlines():
        match=re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)',line)
        if not match or match[1]!=metric:continue
        labels={k:json.loads('"'+v+'"') for k,v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"',match[2] or '')}
        if all(labels.get(k)==v for k,v in wanted.items()):found.append(float(match[3]))
    assert found, (metric,wanted,'missing samples')
    return found


def one(text,metric,**labels):
    result=values(text,metric,**labels)
    assert len(result)==1,(metric,labels,result)
    return result[0]


def check(label, running, reserved, pending):
    from node import nodes
    end=time.monotonic()+10
    while True:
        try:
            with urllib.request.urlopen('http://127.0.0.1:17090/metrics',timeout=2) as r:coordinator=r.read().decode()
            assert sum(values(coordinator,'adx_coordinator_environments',state='Running'))==running
            assert sum(values(coordinator,'adx_coordinator_node_reserved_cpu_millis'))==reserved
            assert sum(values(coordinator,'adx_coordinator_queued_requests'))==pending
            snapshots={'coordinator':coordinator}
            for node in nodes():
                nid=node['node']['id'];host=node['address'].rsplit(':',1)[0]
                with urllib.request.urlopen(f'http://{host}:17091/metrics',timeout=2) as r:local=r.read().decode()
                for resource in ('cpu_millis','memory_bytes','disk_bytes'):
                    for measure in ('capacity','reserved','available','overcommitted'):
                        assert one(coordinator,f'adx_coordinator_node_{measure}_{resource}',node_id=nid)==one(local,f'adx_node_{measure}_{resource}'),(nid,measure,resource)
                snapshots[nid]=local
            Path('/evidence/metrics-'+label+'.json').write_text(json.dumps({'status':'passed','running':running,'reserved_cpu_millis':reserved,'queued':pending,'scrapes':snapshots},indent=2))
            print(f'[METRICS PASS] {label}: running={running}, reserved_cpu_millis={reserved}, queued={pending}; Coordinator/Node ledgers agree',flush=True)
            return
        except (AssertionError,OSError,urllib.error.URLError):
            if time.monotonic()>=end:raise
            time.sleep(.1)

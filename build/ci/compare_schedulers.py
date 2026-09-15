#!/usr/bin/env python3
"""Sequential same-container benchmark replay; preserve raw evidence and all rows."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import time

SCENARIOS = {
    'open': 'DISABLED_ReportAggregationAndMixedAB',
    'closed': 'DISABLED_ReportResourceUpdateTailAB',
    'closed500': 'DISABLED_ReportResourceUpdateTailAB',
    'sustained': 'DISABLED_ReportSustainedScheduleUpdateDeleteAB',
    'retry': 'DISABLED_ReportConflictRetryStormAB',
    'update': 'DISABLED_ReportResourceUpdateApplyCostAB',
}

def execute(command, log, timeout=300):
    started=time.monotonic()
    with log.open('w') as f:
        result=subprocess.run(command,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'exit {result.returncode}: {log}')
    return time.monotonic()-started

def parse(log, engine, case, count):
    rows=[]
    for line in log.read_text(errors='replace').splitlines():
        match=re.search(r'(?:ADX_COMPARE|DOMAIN_SCHEDULER(?:_[A-Z]+)*_BENCH) (\{.*\})',line)
        if not match: continue
        row=json.loads(match.group(1))
        if engine=='old' and row.get('engine')!='unit_snapshot': continue
        if row.get('invalid_placement',0)!=0 or row.get('report_failures',0)!=0:
            raise RuntimeError(f'incorrect placement/report: {row}')
        if case=='retry' and engine=='old':
            assert row['changed_unit']==count and row['pending_reservations']==count,row
        else:
            assert row.get('success')==count,row
        if case=='sustained':
            assert row['final_instances']==0,row
            if engine=='old':
                assert row['add_reports']==count and row['delete_reports']==count,row
                assert row['journal_overflows']==0,row
        rows.append(row)
    assert rows, f'no benchmark results: {log}'
    if engine=='old': assert '[  PASSED  ]' in log.read_text(errors='replace'),log
    return rows

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--container',required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rounds',type=int,default=7)
    p.add_argument('--warmup',type=int,default=1)
    p.add_argument('--cases',default=','.join(SCENARIOS))
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    commands=[];rows=[]
    for iteration in range(-a.warmup,a.rounds):
        for case in a.cases.split(','):
            count=5000 if case=='sustained' else 1000
            for engine in (['old','adx'] if iteration%2==0 else ['adx','old']):
                command=['docker','exec','-e',f'DOMAIN_BENCH_REQUEST_COUNT={count}','-e','DOMAIN_BENCH_INFLIGHT=5000','-e',f'DOMAIN_BENCH_UPDATE_RATE={500 if case=="closed500" else 0}',a.container]
                if engine=='old':
                    variants=[command+['bash','/evidence/old.sh','DomainSchedulerCurrentPathBenchmark.'+SCENARIOS[case]]]
                else:
                    caches=[0,32] if case in ('open','sustained','closed','closed500') else [32]
                    variants=[command+['/evidence/adx-compare',case,str(cache)] for cache in caches]
                for variant,cmd in enumerate(variants):
                    log=a.output/f'{iteration:02}-{case}-{engine}-{variant}.log'
                    elapsed=execute(cmd,log)
                    parsed=parse(log,engine,case,count)
                    commands.append(dict(command=cmd,log=log.name,elapsed=elapsed,sha256=hashlib.sha256(log.read_bytes()).hexdigest()))
                    rows.extend(dict(r, iteration=iteration, source=engine, case=case) for r in parsed)
                    (a.output/'raw.json').write_text(json.dumps(rows,indent=2)+'\n')
                    (a.output/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
                    print(f'round={iteration} case={case} engine={engine} passed ({elapsed:.1f}s)',flush=True)
    groups={}
    for row in rows:
        if row['iteration']<0:continue
        # The payload's engine label is unit_snapshot; retain this identity.
        key=(row.get('engine',row['source']),row['case'],str(row.get('cache',row.get('aggregation','default'))))
        groups.setdefault(key,[]).append(row)
    summary=[]
    for (engine,case,mode),group in groups.items():
        metrics={}
        for field in ('qps','p50_us','p99_us','lifecycle_qps','lifecycle_p50_us','lifecycle_p99_us','retry_p50_us','retry_p99_us','report_p50_us','report_p99_us','mean_us','updates','snapshot_build_total_us','reconcile_total_us'):
            values=[r[field] for r in group if field in r]
            if values:metrics[field]=statistics.median(values)
        summary.append(dict(engine=engine,case=case,mode=mode,rounds=len(group),metrics=metrics))
    (a.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))
if __name__=='__main__':main()

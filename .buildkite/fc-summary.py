#!/usr/bin/env python3
"""Render the independent checkpoint job's concrete result and missing-case gate."""
import argparse,json,html,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'build/e2e/firecracker'))
from acceptance import CASES
required=set().union(*CASES.values())
p=argparse.ArgumentParser();p.add_argument('--exit-code',type=int,required=True);a=p.parse_args()
root=Path('out/buildkite/firecracker');root.mkdir(parents=True,exist_ok=True)
result_path=root/'result.json'
if result_path.exists():result=json.loads(result_path.read_text())
else:
 result={'status':'failed','error':'No acceptance result; inspect deployment or prerequisites','cases':[]}
 result_path.write_text(json.dumps(result,indent=2))
passed=a.exit_code==0 and result.get('status')=='passed' and len(result.get('cases',[]))==len(required) and {c.get('name') for c in result.get('cases',[])}==required and all(c.get('passed') is True for c in result.get('cases',[])) and not result.get('cleanup_errors')
lines=['### Firecracker Kubernetes acceptance: '+('PASS' if passed else 'FAIL'),'','Passed cases: '+str(len(result.get('cases',[])))+'/'+str(len(required)),'']
for c in result.get('cases',[]):lines.append('- '+html.escape(c['name']))
if result.get('error'):lines+=['','Error: '+html.escape(result['error'])]
if (root/'bundle.json').exists():
 bundle=json.loads((root/'bundle.json').read_text());lines+=['','Commit: `'+bundle['package']['commit']+'`','Runtime kit: `'+bundle['firecracker_kit']['sandboxd_revision']+'`']
lines+=['','Artifacts: result.json, junit.xml, placement.json, bundle.json, registry-images.json and node evidence under out/buildkite/firecracker/.']
(root/'summary.md').write_text('\n'.join(lines)+'\n')
raise SystemExit(0 if passed else 1)

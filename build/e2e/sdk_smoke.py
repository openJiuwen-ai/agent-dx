#!/usr/bin/env python3
"""Public, installed SDK acceptance against a real provisioned ADX platform."""
import argparse,json,pathlib,time,traceback,importlib.metadata,os
parser=argparse.ArgumentParser()
parser.add_argument('--endpoint',required=True)
parser.add_argument('--token-file',type=pathlib.Path,required=True)
parser.add_argument('--image',required=True)
parser.add_argument('--ca',type=pathlib.Path,required=True)
parser.add_argument('--output',type=pathlib.Path,required=True)
parser.add_argument('--runtime',default='runc')
args=parser.parse_args()
os.environ['SSL_CERT_FILE']=str(args.ca.resolve())
from adx_sandbox import Sandbox,ConnectionConfig
out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
result={'status':'failed','sdk_version':importlib.metadata.version('adx-sandbox'),'instances':[],'checks':[]}
connection=ConnectionConfig(server_address=args.endpoint,token=args.token_file.read_text().strip(),use_tls=True,verify_tls=True)
instances=[]
try:
 for i in range(2):
  start=time.monotonic()
  s=Sandbox(image=args.image,runtime=args.runtime,cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
  instances.append(s);result['instances'].append(s.id);assert s.is_running();print('CREATED',s.id,'seconds',round(time.monotonic()-start,3),flush=True)
 for s in instances:
  r=s.commands.run("printf 'adx-e2e'; printf 'stderr-e2e' >&2; exit 7")
  assert r.stdout=='adx-e2e' and r.stderr=='stderr-e2e' and r.exit_code==7, repr(r)
  payload=b'adx-binary\x00\xff\n'*10000
  s.files.write('/tmp/adx-e2e.bin',payload)
  assert s.files.read('/tmp/adx-e2e.bin',format='bytes')==payload
  result['checks'].append({'id':s.id,'command':True,'binary_file':True})
  print('COMMAND_AND_FILE_OK',s.id,flush=True)
 for s in instances:
  s.kill();print('DELETED',s.id,flush=True)
 result['status']='passed'
except Exception as e:
 result['error']=f'{type(e).__name__}: {e}';traceback.print_exc()
finally:
 for s in instances:
  try:s.kill()
  except Exception:pass
  s.close()
 (out/'sdk-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result),flush=True)
raise SystemExit(0 if result['status']=='passed' else 1)

import pathlib,subprocess,json
run=pathlib.Path('/var/lib/afs-acceptance/network-v67-r2/logs')
before=(run/'iptables-before.txt').read_text();assert '*filter\n' not in before
p=subprocess.run(['nft','--json','list','table','ip','filter'],capture_output=True,text=True,check=True)
(run/'filter-created.json').write_text(p.stdout)
data=json.loads(p.stdout)['nftables'];chains=[]
for row in data:
 assert len(row)==1
 kind=next(iter(row))
 assert kind in ('metainfo','table','chain'),kind
 if kind=='table':assert row[kind]['family']=='ip' and row[kind]['name']=='filter'
 if kind=='chain':
  c=row[kind];assert c['family']=='ip' and c['table']=='filter' and c['policy']=='accept' and c['type']=='filter' and c['prio']==0
  assert c['hook']==c['name'].lower();chains.append(c['name'])
assert 'INPUT' in chains and set(chains)<= {'FORWARD','INPUT','OUTPUT'}
subprocess.run(['nft','delete','table','ip','filter'],check=True)
p=subprocess.run(['iptables-save'],capture_output=True,text=True,check=True);(run/'iptables-final-restored.txt').write_text(p.stdout)
assert '*filter\n' not in p.stdout
print(json.dumps({'status':'PASS','scope':'delete only verified newly-created empty default filter table; original nat/config untouched'}))

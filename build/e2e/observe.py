import time,json,pathlib
p=pathlib.Path('/tmp/adx-e2e')
while True:
 c=json.loads((p/'capacity-base.json').read_text())
 (p/'capacity.tmp').write_text(json.dumps({'capacity':c,'devices':[],'valid_until_unix_seconds':int(time.time())+10}))
 (p/'capacity.tmp').replace(p/'capacity.json')
 time.sleep(2)

import pathlib,json,sys,os,signal,time
r=pathlib.Path('/var/lib/afs-acceptance/network-v67-r2');ready=json.loads((r/'ready.json').read_text());p=pathlib.Path('/proc',str(ready['pid']))
assert p.is_dir();assert int((p/'stat').read_text().rsplit(') ',1)[1].split()[19])==ready['start_ticks'];assert str(r/'env_network.py').encode() in (p/'cmdline').read_bytes().split(b'\0')
(r/'logs/server-live-after.json').write_text(json.dumps({'pid':ready['pid'],'start_ticks':ready['start_ticks'],'cmdline':(p/'cmdline').read_text().replace('\0',' '),'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'script_sha256':ready['script_sha256'],'status':'LIVE'},indent=2)+'\n')
os.kill(ready['pid'],signal.SIGTERM)
for _ in range(30):
 if not p.is_dir() or (p/'stat').read_text().rsplit(') ',1)[1].split()[0]=='Z':break
 time.sleep(.2)
else:raise RuntimeError('probe server did not stop')
(r/'logs/server-stopped.json').write_text(json.dumps({'pid':ready['pid'],'start_ticks':ready['start_ticks'],'status':'STOPPED'})+'\n')

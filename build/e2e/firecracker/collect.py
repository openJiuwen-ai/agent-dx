#!/usr/bin/env python3
"""Export fixture evidence and component logs, excluding credential files."""
import pathlib,sys
root=pathlib.Path(sys.argv[1]);target=root/'export';target.mkdir(exist_ok=True)
secrets=[]
for name in ('api-key','other-key','redis-key','s3-user','s3-key'):
 path=root/'secrets'/name
 if path.exists(): secrets.append(path.read_bytes().strip())
for directory in ('evidence','state/logs'):
 source=root/directory
 if not source.exists(): continue
 for path in source.rglob('*'):
  if path.is_symlink(): raise ValueError('symlink in evidence')
  if not path.is_file(): continue
  destination=target/directory/path.relative_to(source);destination.parent.mkdir(parents=True,exist_ok=True)
  data=path.read_bytes()
  for value in secrets:
   if value: data=data.replace(value,b'[REDACTED]')
  destination.write_bytes(data)
print('Fixture evidence exported without credential files',flush=True)

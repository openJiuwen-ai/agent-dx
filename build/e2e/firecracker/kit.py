"""Verify an externally built, immutable Firecracker runtime kit before packaging."""
import hashlib,json
from pathlib import Path
REQUIRED={'bin/sandboxd','bin/sbox','bin/checkpoint-restore','bin/firecracker','artifacts/Image','artifacts/initrd.img','tools/virtiofsd','tools/distill_fs','tools/minio','tools/docker-registry','tools/redis-cli'}

def verify(directory,backend):
 root=Path(directory)
 if root.is_symlink() or (root/'manifest.json').is_symlink():raise ValueError('runtime kit symlink rejected')
 manifest=json.loads((root/'manifest.json').read_text())
 if manifest.get('schema_version')!=1 or manifest.get('target')!=backend['target'] or manifest.get('sandboxd_revision')!=backend['sandboxd_revision'] or manifest.get('sandboxd_patches')!=backend.get('sandboxd_patches'):raise ValueError('runtime kit architecture or sandboxd source identity mismatch')
 if set(manifest.get('files',{}))!=REQUIRED:raise ValueError('runtime kit file inventory mismatch')
 for name,expected in manifest['files'].items():
  path=root/name
  if any(p.is_symlink() for p in (path,path.parent)) or not path.is_file():raise ValueError('runtime kit file missing or symlinked: '+name)
  h=hashlib.sha256()
  with path.open('rb') as f:
   for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
  if h.hexdigest()!=expected:raise ValueError('runtime kit checksum mismatch: '+name)
 for name in ('sandboxd','sbox','redis-cli'):
  prefix='tools/' if name=='redis-cli' else 'bin/'
  if manifest['files'][prefix+name]!=backend['files'][name]:raise ValueError('runtime kit and backend artifact differ: '+name)
 if manifest['files']['artifacts/initrd.img']!=backend['files']['firecracker-initrd.img']:
  raise ValueError('runtime kit and backend guest agent differ: initrd.img')
 return manifest

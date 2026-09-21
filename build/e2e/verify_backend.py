#!/usr/bin/env python3
"""Verify cached external runtime artifacts before using them in acceptance."""
import argparse
import json
from pathlib import Path
from prepare import BACKEND_FILES, ROOT, sha

def expected_patches(pinned):
    patches={entry['path']:entry['sha256'] for entry in pinned.get('patches',[])}
    for path,digest in patches.items():
        if sha(ROOT/path)!=digest:
            raise ValueError('sandboxd patch integrity mismatch: '+path)
    return patches

def verify(directory, target):
    pinned=json.loads((ROOT/'third_party/sandboxd/source.json').read_text())
    manifest=json.loads((directory/'manifest.json').read_text())
    if manifest['sandboxd_revision']!=pinned['revision'] or manifest['target']!=target:
        raise ValueError('cached backend revision or architecture mismatch')
    if manifest.get('sandboxd_patches')!=expected_patches(pinned):
        raise ValueError('cached backend patch set mismatch')
    if set(manifest['files'])!=set(BACKEND_FILES):
        raise ValueError('cached backend file set mismatch')
    for name in BACKEND_FILES:
        path=directory/name
        if path.is_symlink() or not path.is_file() or sha(path)!=manifest['files'][name]:
            raise ValueError('cached backend integrity mismatch: '+name)
    print('backend verified:',manifest['sandboxd_revision'],manifest['target'])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,required=True);p.add_argument('--target',required=True)
    args=p.parse_args();verify(args.directory,args.target)

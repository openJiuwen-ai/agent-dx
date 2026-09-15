#!/usr/bin/env python3
"""Verify cached external runtime artifacts before using them in acceptance."""
import argparse
import json
from pathlib import Path
from prepare import BACKEND_BINARIES, ROOT, sha

def verify(directory, target):
    pinned=json.loads((ROOT/'third_party/sandboxd/source.json').read_text())
    manifest=json.loads((directory/'manifest.json').read_text())
    if manifest['sandboxd_revision']!=pinned['revision'] or manifest['target']!=target:
        raise ValueError('cached backend revision or architecture mismatch')
    if set(manifest['files'])!=set(BACKEND_BINARIES):
        raise ValueError('cached backend file set mismatch')
    for name in BACKEND_BINARIES:
        path=directory/name
        if path.is_symlink() or not path.is_file() or sha(path)!=manifest['files'][name]:
            raise ValueError('cached backend integrity mismatch: '+name)
    print('backend verified:',manifest['sandboxd_revision'],manifest['target'])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,required=True);p.add_argument('--target',required=True)
    args=p.parse_args();verify(args.directory,args.target)

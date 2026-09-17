#!/usr/bin/env python3
"""Build a local EROFS runtime payload from verified native static executables."""
import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile


def static_executable(path):
    result = subprocess.run(['readelf', '-l', str(path)], text=True, capture_output=True, check=True)
    if 'INTERP' in result.stdout:
        raise ValueError(f'{path.name} requires a guest dynamic linker; a static executable is required')


def build(binary, output, busybox=Path('/usr/bin/busybox')):
    if output.exists():
        raise ValueError('output already exists')
    for path in (binary, busybox):
        static_executable(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='adx-runtime-') as temporary:
        root = Path(temporary)
        for name in ('usr/bin', 'usr/local/bin', 'bin', 'sbin', 'etc', 'tmp', 'var/tmp', 'root', 'home', 'proc', 'sys', 'dev', '__adx'):
            (root/name).mkdir(parents=True, exist_ok=True)
        for source, target in ((binary,'usr/local/bin/rrt-runtime'), (busybox,'bin/busybox')):
            shutil.copyfile(source,root/target); (root/target).chmod(0o755)
        for app in subprocess.check_output([str(busybox), '--list'],text=True).splitlines():
            if app != 'busybox': (root/'bin'/app).symlink_to('busybox')
        for name in ('usr', 'bin', 'sbin', 'etc', 'home', 'root'):
            (root/'__adx'/name).symlink_to('/'+name)
        (root/'tmp').chmod(0o1777); (root/'var/tmp').chmod(0o1777)
        (root/'etc/passwd').write_text('root:x:0:0:root:/root:/bin/sh\n')
        (root/'etc/group').write_text('root:x:0:\n')
        certs=Path('/etc/ssl/certs/ca-certificates.crt')
        if certs.is_file():
            (root/'etc/ssl/certs').mkdir(parents=True)
            shutil.copyfile(certs,root/'etc/ssl/certs/ca-certificates.crt')
        subprocess.run(['mkfs.erofs','-E','noinline_data',str(output),str(root)],check=True)
        subprocess.run(['fsck.erofs',str(output)],check=True)


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args(); build(a.binary,a.output)

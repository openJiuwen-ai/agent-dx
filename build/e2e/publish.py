#!/usr/bin/env python3
"""Import a built Docker archive into the isolated node's HTTP registry."""
import gzip
import hashlib
import json
from pathlib import Path
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
BASE='http://127.0.0.1:5000'
def request(path,method='GET',body=None,headers=None):
    return urllib.request.urlopen(urllib.request.Request(urllib.parse.urljoin(BASE,path),data=body,headers=headers or {},method=method),timeout=90)
def blob(repo, data):
    digest='sha256:'+hashlib.sha256(data).hexdigest()
    with request(repo+'/blobs/uploads/','POST',b'') as response:location=response.headers['Location']
    location+=('&' if '?' in location else '?')+'digest='+digest
    with request(location,'PUT',data,{'Content-Type':'application/octet-stream'}):pass
    return digest

def publish(archive, repository='adx-execd', tag='acceptance'):
    if not repository or '/' in repository or not tag:
        raise ValueError('simple repository and tag are required')
    repo='/v2/'+repository
    end=time.monotonic()+30
    while True:
        try:
            with request('/v2/'):break
        except OSError:
            if time.monotonic()>end:raise TimeoutError('registry not ready')
            time.sleep(.2)
    with tarfile.open(archive) as tar:
        manifest=json.load(tar.extractfile('manifest.json'))
        if len(manifest)!=1:raise ValueError('image archive must contain exactly one image')
        image=manifest[0]
        config=tar.extractfile(image['Config']).read()
        layers=[]
        for path in image['Layers']:
            data=tar.extractfile(path).read()
            if not data.startswith(b'\x1f\x8b'):data=gzip.compress(data,mtime=0)
            layers.append({'mediaType':'application/vnd.docker.image.rootfs.diff.tar.gzip','size':len(data),'digest':blob(repo,data)})
        body=json.dumps({'schemaVersion':2,'mediaType':'application/vnd.docker.distribution.manifest.v2+json','config':{'mediaType':'application/vnd.docker.container.image.v1+json','size':len(config),'digest':blob(repo,config)},'layers':layers},separators=(',',':')).encode()
        with request(repo+'/manifests/'+tag,'PUT',body,{'Content-Type':'application/vnd.docker.distribution.manifest.v2+json'}) as response:digest=response.headers['Docker-Content-Digest']
        if digest!='sha256:'+hashlib.sha256(body).hexdigest():raise ValueError('registry manifest digest mismatch')
        return digest
if __name__=='__main__':
    digest=publish('/execd.tar')
    Path('/secrets/image').write_text('127.0.0.1:5000/adx-execd@'+digest)
    Path('/evidence/execd-image.json').write_text(json.dumps({'registry_digest':digest}))
    print('EXECD manifest uploaded and verified',digest)

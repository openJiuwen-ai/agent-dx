"""Public HTTPS key-management acceptance through Edge; never log key material."""
import ssl
import time
import httpx


def check_management(secrets, event):
    headers={'Authorization':'Bearer '+(secrets/'admin-key').read_text().strip()}
    tenant={'Authorization':'Bearer '+(secrets/'api-key').read_text().strip()}
    path='/api/admin/v1/keys'
    with httpx.Client(base_url='https://127.0.0.1:8443',
                      verify=ssl.create_default_context(cafile=str(secrets/'tls/ca.pem')),
                      timeout=20) as client:
        assert client.get(path,headers=tenant).status_code==403, 'tenant could access key management'
        event('Checking administrator create/list/revoke through HTTPS Edge')
        response=client.post(path,headers=headers,json={'tenantId':'e2e-managed'})
        assert response.status_code==201, ('key create HTTP status',response.status_code)
        created=response.json();key_id=created['key']['id']
        credential={'Authorization':'Bearer '+created['apiKey']}
        revoked=False
        try:
            assert response.headers.get('cache-control')=='no-store'
            listed=client.get(path,headers=headers,params={'tenantId':'e2e-managed'})
            assert listed.status_code==200
            assert any(k['id']==key_id for k in listed.json()['items'])
            assert created['apiKey'] not in listed.text, 'list exposed key material'
            endpoint='/api/sandbox/v1/snapshots'
            assert client.get(endpoint,headers=credential).status_code==200, 'new key rejected'
            assert client.delete(path+'/'+key_id,headers=tenant).status_code==403, 'tenant revoked key'
            assert client.delete(path+'/'+key_id,headers=headers).status_code==204, 'admin revoke failed'
            revoked=True
            end=time.monotonic()+20
            while True:
                status=client.get(endpoint,headers=credential).status_code
                if status==401:break
                assert status==200, ('unexpected post-revoke HTTP status',status)
                if time.monotonic()>end:raise AssertionError('revoked key remained accepted beyond cache budget')
                time.sleep(.25)
            assert client.delete(path+'/'+key_id,headers=headers).status_code==204, 'repeat revoke failed'
        finally:
            if not revoked:
                assert client.delete(path+'/'+key_id,headers=headers).status_code==204, 'key cleanup failed'
    event('PASS: key management via Edge, tenant denial and cache-bounded revocation')
    return {'status':'passed','created_listed_revoked':True,'tenant_denied':True,'revocation_observed':True}

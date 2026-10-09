"""Public HTTPS key-management acceptance through Ingress; never log key material."""

import ssl
import time

import httpx


def check_management(secrets, event):
    headers = {'Authorization': 'Bearer ' + (secrets / 'admin-key').read_text().strip()}
    tenant = {'Authorization': 'Bearer ' + (secrets / 'api-key').read_text().strip()}
    path = '/api/admin/v1/keys'
    with httpx.Client(
        base_url='https://127.0.0.1:8443',
        verify=ssl.create_default_context(cafile=str(secrets / 'tls/ca.pem')),
        timeout=20,
    ) as client:
        if not (client.get(path, headers=tenant).status_code == 403):
            raise AssertionError('tenant could access key management')
        event('Checking administrator create/list/revoke through HTTPS Ingress')
        response = client.post(path, headers=headers, json={'tenantId': 'e2e-managed'})
        if not (response.status_code == 201):
            raise AssertionError(('key create HTTP status', response.status_code))
        created = response.json()
        key_id = created['key']['id']
        credential = {'Authorization': 'Bearer ' + created['apiKey']}
        revoked = False
        try:
            if not (response.headers.get('cache-control') == 'no-store'):
                raise AssertionError()
            listed = client.get(path, headers=headers, params={'tenantId': 'e2e-managed'})
            if not (listed.status_code == 200):
                raise AssertionError()
            if not (any(k['id'] == key_id for k in listed.json()['items'])):
                raise AssertionError()
            if not (created['apiKey'] not in listed.text):
                raise AssertionError('list exposed key material')
            endpoint = '/api/sandbox/v1/snapshots'
            if not (client.get(endpoint, headers=credential).status_code == 200):
                raise AssertionError('new key rejected')
            if not (client.delete(path + '/' + key_id, headers=tenant).status_code == 403):
                raise AssertionError('tenant revoked key')
            if not (client.delete(path + '/' + key_id, headers=headers).status_code == 204):
                raise AssertionError('admin revoke failed')
            revoked = True
            end = time.monotonic() + 20
            while True:
                status = client.get(endpoint, headers=credential).status_code
                if status == 401:
                    break
                if not (status == 200):
                    raise AssertionError(('unexpected post-revoke HTTP status', status))
                if time.monotonic() > end:
                    raise AssertionError('revoked key remained accepted beyond cache budget')
                time.sleep(0.25)
            if not (client.delete(path + '/' + key_id, headers=headers).status_code == 204):
                raise AssertionError('repeat revoke failed')
        finally:
            if not revoked:
                if not (client.delete(path + '/' + key_id, headers=headers).status_code == 204):
                    raise AssertionError('key cleanup failed')
    event('PASS: key management via Ingress, tenant denial and cache-bounded revocation')
    return {'status': 'passed', 'created_listed_revoked': True, 'tenant_denied': True, 'revocation_observed': True}

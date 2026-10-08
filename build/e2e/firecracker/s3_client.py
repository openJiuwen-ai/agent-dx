"""Small SigV4 client for the isolated MinIO acceptance fixture."""
import datetime
import hashlib
import hmac
import urllib.request

class Client:
    def __init__(self, run):
        self.access = (run / 'secrets/s3-user').read_text().strip()
        self.secret = (run / 'secrets/s3-key').read_text().strip()

    def request(self, method, path, data=b'', query=''):
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp, date = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        host = '127.0.0.1:19090'
        digest = hashlib.sha256(data).hexdigest()
        headers = f'host:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{stamp}\n'
        signed = 'host;x-amz-content-sha256;x-amz-date'
        canonical = '\n'.join([method, path, query, headers, signed, digest])
        scope = f'{date}/us-east-1/s3/aws4_request'
        message = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
        def sign(key, value):
            return hmac.new(key, value.encode(), hashlib.sha256).digest()
        key = sign(sign(sign(sign(('AWS4' + self.secret).encode(), date), 'us-east-1'), 's3'), 'aws4_request')
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request(f'http://{host}{path}' + ('?' + query if query else ''),
            method=method, data=data if method == 'PUT' else None, headers={
                'Host': host, 'x-amz-date': stamp, 'x-amz-content-sha256': digest,
                'Authorization': f'AWS4-HMAC-SHA256 Credential={self.access}/{scope}, SignedHeaders={signed}, Signature={signature}'})
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()

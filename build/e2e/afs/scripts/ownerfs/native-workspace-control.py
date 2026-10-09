#!/usr/bin/env python3
"""Administrator client for the experimental Node-owned workspace controller."""
import argparse
import json
import platform
import socket
import sys
import uuid
from pathlib import Path


def exchange(socket_path, request):
    payload = json.dumps(request, separators=(',', ':')).encode() + b'\n'
    if len(payload) > 8192:
        raise ValueError('request exceeds controller limit')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(135)
        client.connect(str(socket_path))
        client.sendall(payload)
        response = bytearray()
        while not response.endswith(b'\n'):
            chunk = client.recv(4096)
            if not chunk:
                raise RuntimeError('controller closed without a complete response')
            response.extend(chunk)
            if len(response) > 16384:
                raise RuntimeError('controller response exceeds limit')
    value = json.loads(response)
    if not isinstance(value, dict):
        raise ValueError('controller response must be an object')
    return value


def main():
    if platform.system() != 'Linux':
        raise RuntimeError('run this client on the admitted Linux Node')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', type=Path, required=True)
    parser.add_argument('--id', default=None, help='reuse only to replay identical input')
    actions = parser.add_subparsers(dest='operation', required=True)
    actions.add_parser('start').add_argument('workspace')
    actions.add_parser('status')
    actions.add_parser('stop')
    actions.add_parser('exec').add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.socket.is_absolute():
        parser.error('--socket must be absolute')
    request = {'operation': args.operation, 'id': args.id or uuid.uuid4().hex}
    if args.operation == 'start':
        request['workspace'] = args.workspace
    if args.operation == 'exec':
        request['argv'] = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
        if not request['argv']:
            parser.error('exec requires an absolute command')
    result = exchange(args.socket, request)
    print(json.dumps({'request_id': request['id'], 'response': result}, indent=2))
    return 1 if result.get('status') == 'ERROR' else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(json.dumps({'status': 'ERROR', 'error': str(error)}), file=sys.stderr)
        sys.exit(1)

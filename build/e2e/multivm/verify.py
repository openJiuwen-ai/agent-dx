#!/usr/bin/env python3
"""Verify a three-VM inventory and completed acceptance result."""
import argparse
import json
from pathlib import Path

from contract import verify_result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--result', required=True, type=Path)
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    result = json.loads(args.result.read_text())
    verify_result(result, inventory)
    print(json.dumps({'status': 'passed', 'checks': result['checks']}))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Check public Environment routes, runtime actions, references and payload examples."""

import argparse
import json
import re
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / 'platform/api/openapi'
METHODS = {'get', 'post', 'put', 'patch', 'delete', 'head', 'options', 'trace'}


class UniqueLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        keys = [self.construct_object(key, deep=deep) for key, _ in node.value]
        if len(keys) != len(set(keys)):
            raise ValueError(f'duplicate YAML mapping key at line {node.start_mark.line + 1}')
        return super().construct_mapping(node, deep=deep)


def load(name):
    return yaml.load((CONTRACTS / name).read_text(), Loader=UniqueLoader)


def resolve(doc, reference):
    if not reference.startswith('#/'):
        raise ValueError(f'external reference is not supported by this gate: {reference}')
    node = doc
    for key in reference[2:].split('/'):
        node = node[key.replace('~1', '/').replace('~0', '~')]
    return node


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def payload_errors(doc, schema_name, payload):
    schema = {'$ref': f'#/components/schemas/{schema_name}', 'components': doc['components']}
    return [error.message for error in Draft202012Validator(schema).iter_errors(payload)]


def example_errors(doc):
    errors = []
    for name, schema in doc['components']['schemas'].items():
        for example in schema.get('examples', []):
            errors.extend(f'{name}: {message}' for message in payload_errors(doc, name, example))
    return errors


def handler(source, name):
    match = re.search(rf'\basync fn {name}\(', source)
    if not match:
        raise ValueError(f'cannot find API handler {name}')
    rest = source[match.end():]
    return re.split(r'\n    (?:async )?fn ', rest, maxsplit=1)[0]


def control_routes():
    source = (ROOT / 'gateway/apiserver/src/http.rs').read_text()
    routes = set()
    for method, path in re.findall(r'if method == "([A-Z]+)"\s*&&\s*path == "([^"]+)"', source):
        routes.add((method, path))
    # The supported compatibility create alias deliberately has its own envelope.
    routes.add(('POST', '/api/environment/v1/environments'))
    for method in re.findall(r'\("([A-Z]+)", None\)', handler(source, 'keys')):
        routes.add((method, '/api/admin/v1/keys'))
    for method in re.findall(r'\("([A-Z]+)", Some\(id\)\)', handler(source, 'keys')):
        routes.add((method, '/api/admin/v1/keys/{id}'))
    for method in re.findall(r'\("([A-Z]+)", None\)', handler(source, 'snapshots')):
        routes.add((method, '/api/environment/v1/snapshots'))
    for method in re.findall(r'\("([A-Z]+)", Some\(id\)\)', handler(source, 'snapshots')):
        routes.add((method, '/api/environment/v1/snapshots/{snapshotId}'))
    for method, action in re.findall(r'\("([A-Z]+)", "([^"]*)"\) => Some\(Kind::', source):
        suffix = f'/{action}' if action else ''
        routes.add((method, f'/api/environment/v1/environments/{{environmentId}}{suffix}'))
    match = re.search(r'if matches!\(method.as_str\(\), ([^\n]+)\)\s*&&\s*path == "([^"]+)"', source)
    if not match:
        raise ValueError('cannot recognize node scheduling route')
    for method in re.findall(r'"([A-Z]+)"', match[1]):
        routes.add((method, match[2]))
    return routes


def runtime_actions():
    source = (ROOT / 'platform/runtime/execd/src/runtime/dispatch.rs').read_text()
    table = source.split('fn normalize_environment_action(', 1)[1]
    table = table.split('pub(crate) fn dispatch_runtime_action_with_trace', 1)[0]
    return set(re.findall(r'"((?:process|file|shell|entrypoint)\.[^"]+)"', table))


def create_fields():
    source = (ROOT / 'gateway/apiserver/src/contract.rs').read_text()
    fields = source.split('struct Create {', 1)[1].split('\n}', 1)[0]
    result = set()
    renamed = None
    for line in fields.splitlines():
        rename = re.search(r'#\[serde\(rename = "([^"]+)"\)\]', line)
        if rename:
            renamed = rename[1]
        field = re.match(r'    ([a-z_]+): ', line)
        if field:
            result.add(renamed or field[1])
            renamed = None
    return result


def audit(control, data, *, routes=None, actions=None):
    errors = []
    for label, doc in [('environment', control), ('data-plane', data)]:
        ids = []
        for path, entry in doc['paths'].items():
            for method, operation in entry.items():
                if method in METHODS:
                    ids.append(operation['operationId'])
                    for parameter in operation.get('parameters', []):
                        parameter = resolve(doc, parameter['$ref']) if '$ref' in parameter else parameter
                        if parameter['in'] == 'path' and f"{{{parameter['name']}}}" not in path:
                            errors.append(f'{label}: unmatched path parameter {parameter["name"]}: {path}')
        if len(ids) != len(set(ids)):
            errors.append(f'{label}: duplicate operationId')
        valid_refs = True
        for node in walk(doc):
            for key in ('$ref', 'x-adx-result-schema'):
                if key in node:
                    try:
                        resolve(doc, node[key])
                    except (KeyError, TypeError, ValueError):
                        valid_refs = False
                        errors.append(f'{label}: unresolved reference {node[key]}')
        for name, schema in doc['components']['schemas'].items():
            try:
                Draft202012Validator.check_schema(schema)
            except SchemaError as error:
                errors.append(f'{label}: invalid schema {name}: {error.message}')
        if valid_refs:
            errors.extend(f'{label}: {error}' for error in example_errors(doc))
    documented = {
        (method.upper(), path) for path, entry in control['paths'].items() for method in entry if method in METHODS
    }
    for method, path in sorted((control_routes() if routes is None else routes) - documented):
        errors.append(f'missing control operation: {method} {path}')
    control_schemas = control['components']['schemas']
    ordinary_create_fields = control_schemas['CreateEnvironmentRequest']['properties']
    implemented_create_fields = create_fields()
    # Both routes decode the same Rust struct, but only constrained create accepts this field.
    capability_field = 'requiredNodeCapabilities'
    for name in sorted(implemented_create_fields - ordinary_create_fields.keys() - {capability_field}):
        errors.append(f'missing create field: {name}')
    if capability_field in implemented_create_fields:
        branches = control_schemas['ConstrainedEnvironmentRequest']['allOf']
        if not any(capability_field in branch.get('properties', {}) for branch in branches):
            errors.append(f'missing constrained create field: {capability_field}')
    schemas = data['components']['schemas']
    covered = set()
    for branch in schemas['InvokeRequest'].get('oneOf', []):
        branch = resolve(data, branch['$ref'])
        names = branch['properties']['action']['enum']
        covered.update(names)
        args = branch['properties'].get('args', {})
        try:
            typed = '$ref' in args and 'properties' in resolve(data, args['$ref'])
        except (KeyError, TypeError, ValueError):
            typed = False
        if not typed:
            errors.append(f'missing typed args: {names}')
        if 'x-adx-result-schema' not in branch:
            errors.append(f'missing typed result: {names}')
    implemented = runtime_actions() if actions is None else actions
    for name in sorted(implemented - covered):
        errors.append(f'missing runtime action: {name}')
    for name in sorted(covered - implemented):
        errors.append(f'unsupported documented runtime action: {name}')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    control, data = load('environment.yaml'), load('data-plane.yaml')
    result = {
        'control_operations': sum(method in METHODS for entry in control['paths'].values() for method in entry),
        'runtime_actions': len(runtime_actions()),
        'errors': audit(control, data),
    }
    output = json.dumps(result, indent=2) + '\n'
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    sys.stdout.write(output)
    return bool(result['errors'])


if __name__ == '__main__':
    raise SystemExit(main())

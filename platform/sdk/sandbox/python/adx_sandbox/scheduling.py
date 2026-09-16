"""Encode public placement options without weakening explicit node selection."""
from collections.abc import Mapping
from copy import deepcopy


def instance_labels(labels):
    if labels is None:
        return {}
    if not isinstance(labels, Mapping) or len(labels) > 256:
        raise ValueError('labels must be a mapping with at most 256 entries')
    for key, value in labels.items():
        if not isinstance(key, str) or not key.strip() or ':' in key or '=' in key or not isinstance(value, str):
            raise ValueError('labels require nonempty string keys without : or = and string values')
    return dict(labels)


def encode_affinities(affinities, node_id):
    if affinities is None:
        affinities = []
    if not isinstance(affinities, (list, tuple)) or len(affinities) > 256:
        raise ValueError('schedule_affinities must contain at most 256 conditions')
    result = deepcopy(list(affinities))
    for item in result:
        if not isinstance(item, dict):
            raise ValueError('each scheduling condition must be a mapping')
        for key, maximum in (('kind', 1), ('affinity', 3)):
            value = item.get(key)
            if type(value) is not int or not 0 <= value <= maximum:
                raise ValueError('invalid affinity ' + key)
        weight = item.get('weight', 0)
        if type(weight) is not int or not 0 <= weight <= 1000:
            raise ValueError('affinity weight must be between 0 and 1000')
        for key in ('preferredPriority', 'preferredAntiOtherLabels'):
            if key in item and type(item[key]) is not bool:
                raise ValueError(key + ' must be boolean')
        ops = item.get('labelOps')
        if not isinstance(ops, list) or not 1 <= len(ops) <= 64:
            raise ValueError('labelOps requires 1..64 expressions')
        for op in ops:
            if not isinstance(op, dict) or type(op.get('type')) is not int or op['type'] not in range(4):
                raise ValueError('invalid label operator')
            if not isinstance(op.get('labelKey'), str) or not op['labelKey'].strip():
                raise ValueError('labelKey must be a nonempty string')
            values = op.get('labelValues', [])
            if not isinstance(values, list) or any(not isinstance(v, str) for v in values) or (op['type'] < 2 and not values):
                raise ValueError('invalid labelValues')
    if node_id:
        constraint = {'type': 0, 'labelKey': 'NODE_ID', 'labelValues': [node_id]}
        required = [item for item in result if item['kind'] == 0 and (
            item['affinity'] == 2 or (item['affinity'] == 0 and item.get('preferredPriority') and item.get('preferredAntiOtherLabels')))]
        if required:
            for item in required:
                if len(item['labelOps']) == 64:
                    raise ValueError('node_id requires room in each hard node condition')
                item['labelOps'].append(deepcopy(constraint))
        else:
            result.append({'kind': 0, 'affinity': 2, 'labelOps': [constraint]})
    return result

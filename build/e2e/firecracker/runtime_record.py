"""Read the persisted EnvironmentRecord runtime shape in FC fault oracles."""


def runtime_id(record):
    return record['runtime']['id']


def runtime_ip(record):
    return (record.get('runtime') or {}).get('ip')

"""Record a bounded public-SDK throughput and latency baseline."""

import json

if __package__ == 'e2e.load':
    from ..mixed_soak import run as run_mixed
else:
    from mixed_soak import run as run_mixed


def summarize(report):
    """Add comparable rates without treating a cluster-dependent number as an SLA."""
    elapsed = report['elapsed_seconds']
    if elapsed <= 0:
        raise ValueError('performance sample duration must be positive')
    for operation in report['operations'].values():
        operation['rate_per_second'] = round(operation['count'] / elapsed, 3)
    report['profile'] = 'load-performance'
    report['cases'] = [
        dict(case, id=case['id'].replace('mixed-soak.', 'load-performance.', 1))
        for case in report['cases']
    ]
    return report


def run(connection, image, output):
    """Use the same two-node mix as soak, shortened for a baseline sample."""
    report = summarize(run_mixed(connection, image, output, seconds=90))
    output.write_text(json.dumps(report, indent=2) + '\n')
    return report

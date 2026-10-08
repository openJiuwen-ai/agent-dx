"""Public SDK assertions surrounding a Coordinator outage and SQLite replay."""

import json
from pathlib import Path

from adx_sandbox import Sandbox
from functional_lifecycle import _wait_deleted


JOURNAL = Path('/tmp/adx-e2e/degraded/results.sqlite')


def create(connection, image, output, journal_path=JOURNAL):
    created = []
    try:
        keep = Sandbox(
            image=image, runtime='runc', cpu=500, memory=512,
            idle_timeout=0, detached=True, node_id='node1',
            connection=connection, create_timeout=150,
        )
        created.append(keep)
        idle = Sandbox(
            image=image, runtime='runc', cpu=500, memory=512,
            idle_timeout=6, detached=True, node_id='node1',
            connection=connection, create_timeout=150,
        )
        created.append(idle)
        assert keep.commands.run('printf retained-before-outage').stdout == 'retained-before-outage'
        assert idle.commands.run('printf idle-before-outage').stdout == 'idle-before-outage'
        assert not journal_path.exists(), 'healthy node opened its degradation journal'
        output.write_text(json.dumps({'keep_id': keep.id, 'idle_id': idle.id}) + '\n')
    except Exception:
        for instance in created:
            try:
                Sandbox.delete(instance.id, connection=connection)
            except Exception:
                pass
        raise
    finally:
        for instance in created:
            instance.close()


def verify(connection, evidence, output):
    live = json.loads((evidence / 'sqlite-live.json').read_text())
    journaled = json.loads((evidence / 'sqlite-journaled.json').read_text())
    reconciled = json.loads((evidence / 'sqlite-reconciled.json').read_text())
    kept = None
    deleted = False
    try:
        kept = Sandbox.from_id(live['keep_id'], connection=connection)
        command = kept.commands.run('printf retained-after-replay')
        assert command.exit_code == 0 and command.stdout == 'retained-after-replay'
        kept.close()
        Sandbox.delete(live['keep_id'], connection=connection)
        deleted = True
        _wait_deleted(live['keep_id'], connection, timeout=60)
        report = {'status': 'passed', 'cases': [
            {'id': 'reliability.sqlite-journaled-idle-delete', 'status': 'passed',
             'seconds': journaled['seconds']},
            {'id': 'reliability.sqlite-replay-retains-live-backend', 'status': 'passed',
             'seconds': reconciled['seconds']},
        ]}
        output.write_text(json.dumps(report, indent=2) + '\n')
        return report
    finally:
        if kept is not None:
            kept.close()
        if not deleted:
            Sandbox.delete(live['keep_id'], connection=connection)

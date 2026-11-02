"""Data payloads must not become retained command strings during a fixed-set soak."""

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


def module():
    here = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('soak_staged_payload', here / 'soak_sdk.py')
    value = importlib.util.module_from_spec(spec)
    with patch.object(sys, 'path', [str(here), *sys.path]):
        spec.loader.exec_module(value)
    return value


class BoundedPayloadControl(unittest.TestCase):
    def test_raw_bytes_use_file_api_and_sync_command_remains_small(self):
        value = module()
        handle = Mock()
        handle.commands.run.return_value = Mock(exit_code=0, stdout='')
        payload = bytes(range(256)) * 64
        value.publish_payload(handle, payload)
        handle.files.write.assert_called_once_with(value.PAYLOAD_STAGE, payload)
        command = handle.commands.run.call_args.args[0]
        self.assertLess(len(command), 2048)
        self.assertNotIn(payload.hex(), command)
        self.assertIn('setuid(10000)', command)
        self.assertIn('sync()', command)

    def test_staging_permission_is_prepared_before_the_workload(self):
        value = module()
        handle = Mock()
        handle.commands.run.return_value = Mock(exit_code=0, stdout='')
        value.prepare_payload(handle)
        handle.files.write.assert_called_once_with(value.PAYLOAD_STAGE, b'')
        self.assertIn('0644', handle.commands.run.call_args.args[0])


if __name__ == '__main__':
    unittest.main()

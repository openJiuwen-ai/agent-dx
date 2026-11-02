"""Frozen Docker roles must be checked before starting an AFS worker."""

import importlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))


class DockerTransportTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('fixtures.docker')
        self.config = {'transport': 'docker', 'base': '/a', 'docker': ['/fixture/docker'],
                       'run_id': 'adx-e2e-fixture', 'container': 'adx-e2e-fixture-afs-a',
                       'container_id': 'a' * 64, 'network_id': 'b' * 64, 'address': '172.20.0.2'}
        self.container = {'Id': 'a' * 64, 'Name': '/adx-e2e-fixture-afs-a', 'State': {'Running': True},
                          'Config': {'Labels': {'adx.e2e.run': 'adx-e2e-fixture'}},
                          'NetworkSettings': {'Networks': {'adx-e2e-fixture': {
                              'NetworkID': 'b' * 64, 'IPAddress': '172.20.0.2'}}}}

    def connect(self):
        with patch.object(self.module.subprocess, 'check_output', return_value=json.dumps([self.container])), \
                patch.object(self.module, 'Channel', return_value=Mock()) as factory:
            channel = self.module.connect(self.config)
        return channel, factory.call_args.args[0]

    def test_frozen_role_and_namespace_are_verified_without_kubectl(self):
        channel, command = self.connect()
        self.assertEqual(channel.docker_identity['container_id'], 'a' * 64)
        self.assertIn('unshare', command)
        self.assertIn('-i', command)
        self.assertNotIn('kubectl', command)

    def test_replacement_wrong_network_and_unowned_container_are_rejected(self):
        from fixtures.common import Blocked
        for key in ('container_id', 'network_id', 'address'):
            with self.subTest(key=key):
                original = self.config[key]
                self.config[key] = 'replacement'
                with self.assertRaises(Blocked):
                    self.connect()
                self.config[key] = original
        self.container['Config']['Labels'] = {}
        with self.assertRaises(Blocked):
            self.connect()

    def test_only_explicit_sdk_data_roles_share_container_mounts(self):
        from fixtures.common import Blocked
        self.config.update(container='adx-e2e-fixture-node1', mount_namespace='container')
        self.container['Name'] = '/adx-e2e-fixture-node1'
        with self.assertRaises(Blocked):
            self.connect()
        self.container['Config']['Labels']['afs.e2e.data'] = 'true'
        _, command = self.connect()
        self.assertNotIn('unshare', command)


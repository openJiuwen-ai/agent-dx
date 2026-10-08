import ast
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT.parents[1] / 'platform/sdk/sandbox/python/adx_sandbox'
spec = importlib.util.spec_from_file_location('sdk_surface', ROOT / 'sdk_surface.py')
surface = importlib.util.module_from_spec(spec)
spec.loader.exec_module(surface)


class PublicSdkSurfaceTests(unittest.TestCase):
    SOURCES = {
        'Sandbox': 'sandbox_api.py',
        'CommandHandle': 'commands.py',
        'Commands': 'commands.py',
        'Filesystem': 'filesystem.py',
        'PtySession': 'pty.py',
        'Pty': 'pty.py',
        'Shells': 'shell/shells.py',
        'Shell': 'shell/shell.py',
    }

    @staticmethod
    def public_members(path, class_name):
        tree = ast.parse(path.read_text())
        node = next(
            item for item in tree.body
            if isinstance(item, ast.ClassDef) and item.name == class_name
        )
        return {
            item.name for item in node.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not item.name.startswith('_')
        }

    def test_every_public_sdk_operation_has_an_e2e_disposition(self):
        for class_name, relative_path in self.SOURCES.items():
            self.assertEqual(
                self.public_members(SDK / relative_path, class_name),
                set(surface.SURFACE[class_name]),
                class_name,
            )
        init_tree = ast.parse((SDK / '__init__.py').read_text())
        exports = next(
            ast.literal_eval(node.value) for node in init_tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == '__all__' for target in node.targets)
        )
        self.assertEqual(len(exports), 41)
        self.assertTrue({'Sandbox', 'CommandHandle', 'PtySession', 'resources'}.issubset(exports))
        self.assertIn('resources', exports)
        self.assertEqual(surface.SURFACE['<module>'], {'resources': 'standalone'})

    def test_documented_operation_counts_do_not_drift(self):
        self.assertEqual(surface.counts(), {
            'public_operations': 66,
            'standalone': 55,
            'firecracker': 11,
            'unsupported': 0,
        })

    def test_reverse_tunnel_has_a_real_standalone_case_owner(self):
        self.assertEqual(
            surface.OPERATION_CASES['Sandbox.get_tunnel_url'],
            ('reverse-tunnel.sdk-upstream-roundtrip',),
        )

    def test_every_supported_operation_has_a_stable_e2e_case_owner(self):
        self.assertEqual(set(surface.OPERATION_CASES), surface.supported_operations())
        for operation, case_ids in surface.OPERATION_CASES.items():
            self.assertTrue(case_ids, operation)
            self.assertTrue(all(case_id.strip() for case_id in case_ids), operation)


if __name__ == '__main__':
    unittest.main()

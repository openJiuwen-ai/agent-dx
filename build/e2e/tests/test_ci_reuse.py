import base64
import importlib.util
from pathlib import Path
import unittest
ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('registry_env',ROOT/'.buildkite/with_registry.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class ExistingCiCredentialsTests(unittest.TestCase):
    def test_existing_pull_secret_takes_precedence(self):
        result=module.registry_config({'SWR_DOCKER_CONFIG_JSON':'{"auths":{"swr.example":{"auth":"encoded"}}}','SWR_USERNAME':'ignored','SWR_PASSWORD':'ignored'})
        self.assertEqual(result,{'auths':{'swr.example':{'auth':'encoded'}}})
    def test_existing_username_password_secret_is_adapted(self):
        result=module.registry_config({'SWR_USERNAME':'user','SWR_PASSWORD':'secret','ADX_E2E_IMAGE_REPOSITORY':'swr.example/team/adx'})
        self.assertEqual(base64.b64decode(result['auths']['swr.example']['auth']),b'user:secret')
    def test_invalid_secret_error_does_not_echo_its_value(self):
        with self.assertRaisesRegex(ValueError,'^invalid CI registry configuration$'):
            module.registry_config({'SWR_DOCKER_CONFIG_JSON':'sensitive-contents'})
    def test_absent_secret_preserves_external_credential_setup(self):
        self.assertIsNone(module.registry_config({}))
    def test_existing_target_path_is_the_default(self):
        script=(ROOT/'.buildkite/run-e2e.sh').read_text()
        self.assertIn('/var/run/yr-k8s/target/kubeconfig',script)
        self.assertIn('--step platform-images',script)
    def test_node_preparation_is_explicit_scoped_and_persistent(self):
        script=(ROOT/'.buildkite/prepare-k8s-node.sh').read_text()
        pipeline=(ROOT/'.buildkite/pipeline.yml').read_text()
        self.assertIn('ADX_K8S_PREPARE_NODE_NAMES',script)
        self.assertIn('nodeName',script)
        self.assertIn('hostNetwork',script)
        self.assertIn('modules-load.d/adx-br-netfilter.conf',script)
        self.assertIn('sysctl.d/99-adx-bridge-netfilter.conf',script)
        self.assertIn('build.env("ADX_K8S_NODE_PREPARE_ONLY") == "1"',pipeline)
    def test_secret_file_is_private_removed_and_child_failure_is_preserved(self):
        import json,os,subprocess,sys
        env={**os.environ,'SWR_DOCKER_CONFIG_JSON':'{"auths":{"swr.example":{"auth":"fixture"}}}'}
        child="import json,os,pathlib,stat; p=pathlib.Path(os.environ['ADX_E2E_REGISTRY_AUTH_FILE']); print(json.dumps({'path':str(p),'mode':stat.S_IMODE(p.stat().st_mode),'docker':os.environ['DOCKER_CONFIG']==str(p.parent)})); raise SystemExit(7)"
        result=subprocess.run([sys.executable,str(ROOT/'.buildkite/with_registry.py'),'--docker','--',sys.executable,'-c',child],env=env,text=True,capture_output=True)
        self.assertEqual(result.returncode,7)
        report=json.loads(result.stdout)
        self.assertEqual(report['mode'],0o600)
        self.assertTrue(report['docker'])
        self.assertFalse(Path(report['path']).exists())
        self.assertNotIn('fixture',result.stdout+result.stderr)

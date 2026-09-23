import ast
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "e2e_execd_direct.py"


class EXECDDirectE2EContractTests(unittest.TestCase):
    def test_sandbox_uses_deployment_default_image(self):
        source = SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(source)
        sandbox_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Sandbox"
        ]

        self.assertEqual(len(sandbox_calls), 1)
        keyword_names = {keyword.arg for keyword in sandbox_calls[0].keywords}
        self.assertNotIn("image", keyword_names)
        self.assertNotIn("ADX_SANDBOX_IMAGE", source)


if __name__ == "__main__":
    unittest.main()

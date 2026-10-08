import unittest
from pathlib import Path


class ExamplesContractTests(unittest.TestCase):
    def test_akernel_contract_e2e_is_grouped_under_e2e(self):
        tests_dir = Path(__file__).resolve().parents[1]
        self.assertTrue((tests_dir / "e2e" / "e2e_akernel_contract.py").is_file())
        self.assertFalse((tests_dir / "e2e_akernel_contract.py").exists())


if __name__ == "__main__":
    unittest.main()

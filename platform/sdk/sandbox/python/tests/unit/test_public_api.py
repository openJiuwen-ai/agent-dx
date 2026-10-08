import unittest

import adx_sandbox


class PublicApiTests(unittest.TestCase):
    def test_backend_owned_reverse_tunnel_type_is_not_public(self):
        self.assertNotIn("HttpReverseTunnel", adx_sandbox.__all__)
        self.assertFalse(hasattr(adx_sandbox, "HttpReverseTunnel"))


if __name__ == "__main__":
    unittest.main()

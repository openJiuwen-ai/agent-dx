import unittest
from adx_sandbox import RestartPolicy


class RestartPolicyTests(unittest.TestCase):
    def test_bounded_policy_is_serialized_for_the_public_api(self):
        self.assertEqual(RestartPolicy(max_attempts=3).to_dict(), {
            "maxAttempts": 3, "initialBackoffSeconds": 1, "maxBackoffSeconds": 30,
        })

    def test_invalid_retry_bounds_are_rejected(self):
        for options in ({"max_attempts": 0}, {"max_attempts": True},
                        {"initial_backoff_seconds": 10, "max_backoff_seconds": 2}):
            with self.assertRaises(ValueError):
                RestartPolicy(**options)

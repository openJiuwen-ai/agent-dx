"""Cache qualification must preserve the zero-FUSE-read assertion."""

import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

TESTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTS))
from suites.dfs import real_cache


def opened(cached=True, keep=True):
    return {"msg": "dfs.fuse_cache_open", "ino": 7, "fh": 9,
            "versioned_read": True, "cached": cached, "keep_cache": keep}


def resident():
    return {"pages": 4, "resident": 4, "vector_hex": "01010101",
            "page_bytes": 4096, "size_bytes": 16384}


def reusable():
    return {"warm_open": opened(keep=False), "reopen": opened(),
            "before_open": resident(), "after_open": resident(), "after_read": resident(),
            "read_callbacks_before": 22, "read_callbacks_after": 22}


class CacheEvidenceTests(unittest.TestCase):
    def api(self, name):
        function = getattr(real_cache, name, None)
        self.assertTrue(callable(function), "cache contract needs explicit evidence validator: " + name)
        return function

    def test_zero_new_fuse_reads_remains_strict(self):
        validate = self.api("assert_cached_reuse")
        validate(reusable())
        wrong = reusable()
        wrong["read_callbacks_after"] += 1
        with self.assertRaises(AssertionError):
            validate(wrong)

    def test_direct_or_missing_keep_cache_cannot_pass_as_cache_reuse(self):
        validate = self.api("assert_cached_reuse")
        for field in ("cached", "keep_cache", "versioned_read"):
            wrong = reusable()
            wrong["reopen"][field] = False
            with self.subTest(field=field), self.assertRaises(AssertionError):
                validate(wrong)

    def test_page_loss_on_open_is_failure_even_before_body_read(self):
        validate = self.api("assert_cached_reuse")
        for stage in ("before_open", "after_open", "after_read"):
            wrong = reusable()
            wrong[stage].update(resident=3, vector_hex="01010100")
            with self.subTest(stage=stage), self.assertRaises(AssertionError):
                validate(wrong)

    def test_residency_counts_must_agree_with_actual_vector(self):
        validate = self.api("assert_cached_reuse")
        wrong = reusable()
        wrong["before_open"]["vector_hex"] = "00000000"
        with self.assertRaises(AssertionError):
            validate(wrong)

    def test_conflict_requires_observed_direct_fallback_and_actual_reads(self):
        validate = self.api("assert_conflict_direct")
        value = {"open": opened(cached=False, keep=False),
                 "read_callbacks_before": 22, "read_callbacks_after": 41}
        validate(value)
        for key, replacement in [("open", opened()), ("read_callbacks_after", 22)]:
            wrong = copy.deepcopy(value)
            wrong[key] = replacement
            with self.subTest(key=key), self.assertRaises(AssertionError):
                validate(wrong)

    def test_open_parser_does_not_invent_missing_or_string_flags(self):
        parse = self.api("parse_cache_open")
        self.assertEqual(parse(json.dumps(opened())), opened())
        self.assertIsNone(parse('{"msg":"unrelated"}'))
        for field in ("cached", "keep_cache", "versioned_read"):
            wrong = opened()
            del wrong[field]
            with self.subTest(field=field), self.assertRaises(AssertionError):
                parse(json.dumps(wrong))
        wrong = opened()
        wrong["cached"] = "false"
        with self.assertRaises(AssertionError):
            parse(json.dumps(wrong))

    def test_residency_probe_observes_native_owned_file_without_touching_mapping(self):
        from suites.dfs.cache_probe import assert_resident, residency

        with tempfile.TemporaryFile() as stream:
            stream.write(b"p" * 16384)
            stream.flush()
            os.fsync(stream.fileno())
            value = residency(stream.fileno(), 16384)
            assert_resident(value)
            self.assertEqual(value["size_bytes"], 16384)
            self.assertEqual(value["method"], "mincore on owned MAP_PRIVATE; mapping not touched")

    def test_cache_cases_have_independent_results_and_explicit_observation(self):
        from fixtures.common import discover, select

        cases = discover()
        observed = {c.id for c in cases if c.observe_cache}
        self.assertEqual(observed, {
            "dfs.visibility.real_cross_handle_cache", "dfs.cache.same_version_reopen",
            "dfs.cache.warm_permission_change",
        })
        self.assertEqual(len(select(cases, suite="all-basic")), 22)
        self.assertEqual(len(select(cases, suite="all-special")), 20)

    def test_observation_is_boolean_and_dfs_only(self):
        from fixtures.common import Case
        from fixtures.multivm import validate

        config = json.loads((TESTS / "fixtures/singlevm.example.json").read_text())
        for mode, flag in [("dfs", "info"), ("ownerfs", True)]:
            case = Case("invalid", ("local-basic",), mode, (), None, observe_cache=flag)
            with self.subTest(mode=mode, flag=flag), self.assertRaises(ValueError):
                validate(config, [case])


if __name__ == "__main__":
    unittest.main()

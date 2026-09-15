"""Failure-contract tests for the core fuzz campaign wrapper."""

import importlib.machinery
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
loader = importlib.machinery.SourceFileLoader("core_fuzz", str(ROOT / "scripts/check-core-fuzz"))
spec = importlib.util.spec_from_loader(loader.name, loader)
runner = importlib.util.module_from_spec(spec)
loader.exec_module(runner)


class ParserTests(unittest.TestCase):
    def test_requires_profile_and_target(self):
        with self.assertRaises(SystemExit):
            runner.parse_args([])

    def test_execution_marker_is_parsed(self):
        self.assertEqual(runner._executions("#1 INITED\nDone 42 runs in 30 second(s)\n"), 42)

    def test_missing_marker_is_inconclusive(self):
        self.assertEqual(runner._executions("#1 INITED\n"), 0)

    def test_unknown_target_rejected(self):
        with self.assertRaises(SystemExit):
            runner.parse_args(["--profile", "pr", "--target", "other"])

    def test_unknown_profile_rejected(self):
        with self.assertRaises(SystemExit):
            runner.parse_args(["--profile", "weekly", "--target", "base32_codec"])


class EvidenceTests(unittest.TestCase):
    def test_initial_status_is_failed(self):
        evidence = runner.new_evidence("pr", "base32_codec", ROOT / "target" / "core-fuzz" / "x", "address")
        self.assertEqual(evidence["status"], "failed")

    def test_profiles_have_explicit_budgets(self):
        self.assertEqual(runner.PROFILES, {"pr": 30, "nightly": 1800})

    def test_targets_are_explicit(self):
        self.assertEqual(runner.TARGETS, {"base32_codec", "key_strings"})


if __name__ == "__main__":
    unittest.main()

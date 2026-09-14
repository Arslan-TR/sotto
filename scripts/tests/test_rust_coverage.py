"""Exercise the coverage entry point without compiling the workspace."""

import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "check-rust-coverage"


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.script = Path(self.directory.name) / "scripts" / SCRIPT.name
        self.script.parent.mkdir()
        self.script.write_text(SCRIPT.read_text())

    def run_report_fixture(self, failure=None):
        main = runpy.run_path(str(self.script))["main"]
        output = Path(self.directory.name).resolve() / "target" / "coverage"
        calls = []
        # Same top-level shape as the retained LLVM JSON report; the runner checks
        # availability, not individual line counts or a coverage percentage.
        summary = {"type": "llvm.coverage.json.export", "version": "2.0.1",
                   "data": [{"files": [], "totals": {"lines": {
                       "count": 10, "covered": 8, "percent": 80.0,
                   }}}]}

        def command(args, **kwargs):
            calls.append(args)
            if args == ["cargo", "llvm-cov", "--version"]:
                return "cargo-llvm-cov 0.9.1"
            if args == ["git", "rev-parse", "HEAD"]:
                return "fixture-commit"
            if args == ["git", "status", "--porcelain"]:
                return ""
            if args == ["rustc", "-vV"]:
                return "fixture-compiler"
            if args[:3] == ["cargo", "llvm-cov", "--workspace"]:
                self.assertIn("--json", args)
                self.assertIn("--summary-only", args)
                destination = Path(args[args.index("--output-path") + 1])
                if failure != "missing summary":
                    content = json.dumps(summary)
                    if failure == "malformed JSON":
                        content = "{"
                    elif failure == "empty data":
                        content = '{"data": []}'
                    destination.write_text(content)
                return None
            self.assertEqual(args, [
                "cargo", "llvm-cov", "report", "--html", "--ignore-filename-regex",
                r"(^|/)(tests|examples)/", "--output-dir", str(output),
            ])
            if failure == "report command":
                raise subprocess.CalledProcessError(9, args)
            if failure != "missing HTML":
                html = Path(args[args.index("--output-dir") + 1]) / "html"
                html.mkdir()
                (html / "index.html").write_text("<!doctype html><title>Coverage</title>")
            return None

        with patch.dict(main.__globals__, command=command), patch.dict(
            os.environ, SOTTO_RUN_DB_TESTS="1", DATABASE_URL="postgres://localhost/disposable"
        ), patch("sys.stdout", new_callable=io.StringIO), patch(
            "sys.stderr", new_callable=io.StringIO
        ):
            result = main()
        self.assertEqual(sum(args[:3] == ["cargo", "llvm-cov", "report"] for args in calls), 1)
        return result, json.loads((output / "run.json").read_text())

    def test_successful_reports_record_passing_evidence(self):
        result, evidence = self.run_report_fixture()
        self.assertEqual(result, 0)
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["commit"], "fixture-commit")
        self.assertEqual(evidence["tool"], "cargo-llvm-cov 0.9.1")
        self.assertFalse(evidence["working_tree_dirty"])
        self.assertGreaterEqual(evidence["elapsed_seconds"], 0)

    def test_failed_or_incomplete_reports_cannot_pass(self):
        for failure in (
            "report command", "missing summary", "malformed JSON", "empty data", "missing HTML",
        ):
            with self.subTest(failure=failure):
                result, evidence = self.run_report_fixture(failure)
                self.assertNotEqual(result, 0)
                self.assertEqual(evidence["status"], "failed")

    def test_failed_test_run_cannot_leave_successful_evidence(self):
        module = runpy.run_path(str(SCRIPT))
        main = module["main"]
        calls = []

        def command(args, **kwargs):
            calls.append(args)
            if args[:3] == ["cargo", "llvm-cov", "--workspace"]:
                raise subprocess.CalledProcessError(7, args)
            if args == ["cargo", "llvm-cov", "--version"]:
                return "cargo-llvm-cov 0.9.1"
            return "fixture"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "target" / "coverage"
            report.mkdir(parents=True)
            (report / "summary.json").write_text('{"stale": true}')
            with patch.dict(main.__globals__, ROOT=root, command=command), patch.dict(
                os.environ, SOTTO_RUN_DB_TESTS="1", DATABASE_URL="postgres://localhost/disposable"
            ):
                self.assertNotEqual(main(), 0)
            self.assertFalse((report / "summary.json").exists())
            self.assertEqual(json.loads((report / "run.json").read_text())["status"], "failed")
            self.assertFalse(any(args[:3] == ["cargo", "llvm-cov", "report"] for args in calls))

    def test_database_must_be_explicit_and_local(self):
        for url in (
            "", "postgres://remote.example/sotto",
            "postgres://localhost/sotto?host=remote.example",
        ):
            with self.subTest(url=url):
                env = os.environ.copy()
                env.update(SOTTO_RUN_DB_TESTS="1", DATABASE_URL=url)
                result = subprocess.run(
                    [sys.executable, str(self.script)], env=env, capture_output=True, text=True
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("local disposable PostgreSQL", result.stderr)

    def test_missing_database_opt_in_fails_before_running_cargo(self):
        env = os.environ.copy()
        env.pop("SOTTO_RUN_DB_TESTS", None)
        env.pop("DATABASE_URL", None)
        result = subprocess.run(
            [sys.executable, str(self.script)], env=env, capture_output=True, text=True
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SOTTO_RUN_DB_TESTS=1", result.stderr)


if __name__ == "__main__":
    unittest.main()

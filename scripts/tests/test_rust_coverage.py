"""Exercise the coverage entry point without compiling the workspace."""

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

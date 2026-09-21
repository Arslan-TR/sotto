from importlib.machinery import SourceFileLoader
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "check-cloud-coverage-races"
MODULE = SourceFileLoader("cloud_coverage_races", str(SCRIPT)).load_module()


class CloudCoverageRaceRunnerTests(unittest.TestCase):
    def test_requires_explicit_database_opt_in(self):
        with self.assertRaisesRegex(ValueError, "SOTTO_RUN_DB_TESTS"):
            MODULE.validate_database({"DATABASE_URL": "postgres://localhost/sotto"})

    def test_rejects_remote_database(self):
        with self.assertRaisesRegex(ValueError, "local disposable"):
            MODULE.validate_database(
                {"SOTTO_RUN_DB_TESTS": "1", "DATABASE_URL": "postgres://db.example/sotto"}
            )

    def test_rejects_query_parameters(self):
        with self.assertRaisesRegex(ValueError, "local disposable"):
            MODULE.validate_database(
                {
                    "SOTTO_RUN_DB_TESTS": "1",
                    "DATABASE_URL": "postgres://localhost/sotto?sslmode=disable",
                }
            )

    def test_accepts_local_disposable_database(self):
        MODULE.validate_database(
            {"SOTTO_RUN_DB_TESTS": "1", "DATABASE_URL": "postgres://localhost/sotto"}
        )


if __name__ == "__main__":
    unittest.main()

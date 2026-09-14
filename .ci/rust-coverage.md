# Native Rust coverage baseline

The `Rust coverage` workflow measures execution of the native workspace tests with a
disposable Postgres 16 database. It runs on PRs, main, and manual dispatch. Failed tests
or report generation fail the job. There is no minimum percentage and this report is
not a formal verification result or a release gate.

## Reproduce

Install Python 3.11 or newer and the pinned coverage tool:

```sh
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --version 0.9.1 --locked
```

Start a local Postgres server and install its `createdb`/`dropdb` client commands.
Create a fresh database for every run, including reruns. Adjust the example connection
details to match your local server:

```sh
sotto_coverage_db="sotto_coverage_$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
createdb --maintenance-db=postgres://sotto:sotto@localhost:5432/postgres \
  --template=template0 "$sotto_coverage_db" && \
SOTTO_RUN_DB_TESTS=1 \
DATABASE_URL="postgres://sotto:sotto@localhost:5432/$sotto_coverage_db" \
SOTTO_TELEMETRY=off \
scripts/check-rust-coverage
```

After inspecting the result, remove only the database created above:

```sh
dropdb --maintenance-db=postgres://sotto:sotto@localhost:5432/postgres "$sotto_coverage_db"
```

These tests modify database contents. Reusing a database can leave fixtures that affect
later runs; a dedicated but reused database is not a fresh baseline. Creation must succeed
before running coverage. CI already starts a fresh Postgres service for each job. The runner requires
explicit opt-in and a localhost/loopback PostgreSQL URL without query parameters. It
never reads `.env`. Existing DB test harnesses connect and migrate the supplied database;
connection/migration errors fail the test run instead of generating a DB-free baseline.

The runner owns and replaces `target/coverage/`. Run one instance at a time and do not
run other instrumented tests against its coverage build directory concurrently. Normal
`cargo llvm-cov` execution clears old profiles before testing. Tests run with one thread
per test binary to avoid shared database fixtures racing within a binary.

Reports:

- `target/coverage/html/index.html`: source-level HTML report.
- `target/coverage/summary.json`: LLVM per-file/totals JSON summary.
- `target/coverage/run.json`: completion status, source commit, working-tree dirty flag,
  compiler/host, coverage-tool version, test command, exclusions and elapsed time.

CI retains the directory for 14 days, including failed-run status when available. A
failed or interrupted run is not a passing baseline even if it produced partial reports.
Tool installation and checkout time are excluded from the runner's elapsed measurement;
use workflow/job timing to assess total PR delay. A local run with uncommitted or untracked
changes is not evidence for the clean commit alone. Ignored files are not identified by
the dirty flag; use a clean checkout for reproducible evidence.

## Measurement boundary

The report uses workspace default features and the executing native host. CI currently
measures Linux x86-64. Integration tests execute, but their own source and examples are
excluded from the coverage denominator. Inline unit-test source can still contribute to
file totals; use the annotated production functions when investigating gaps. Tool-default
filters exclude dependencies and generated/build infrastructure. Doctest coverage is not
enabled. Do not interpret the aggregate percentage as pure production-code coverage.

Not measured: WASM execution, browser/TypeScript, other operating systems or architectures,
non-default feature builds, shell/Python/deployment behaviour, real OS keychain integration
where tests use the in-memory substitute, or live external providers. Native compilation
of the WASM crate does not establish browser compatibility.

Keep existing CI checks. This initial measurement does not replace them or automatically
change required-check repository settings. Warm-cache total required PR runtime targets
15 minutes; the new job has a 30-minute operational timeout while its cold/warm baseline
is established. Timeout means failed collection, not permission to call a partial run
complete.

## Existing behaviour checks to inspect alongside coverage

File presence is an inventory entry, not a claim that every relevant case is covered.

| Behaviour | Existing evidence source | Execution boundary |
| --- | --- | --- |
| Encoding and crypto composition | Core `format.rs`, `aead.rs`, `tests/properties.rs`, vectors | Native tests; generated cases are not exhaustive proofs |
| Roles, ownership, grants and machine scopes | Server `tests/org_access.rs`, `grants.rs`, `machine.rs` | Real local Postgres; inspect allowed and denied operations |
| Rotation, stale writes and recovery | Server `tests/rotate.rs`, `sync.rs`, `recovery.rs`; CLI remote tests | Native client/server and database behaviour |
| Login codes, sessions and one-time shares | Server `tests/auth.rs`, `share.rs` | Inspect expiry, replay and competing-request cases |
| Deletion transitions and retries | Server deletion tests | Database and provider-substitute behaviour |
| CLI persistence, imports and subprocesses | CLI module tests and `tests/e2e.rs` | Current native host; keychain substitutes have limits |
| Native/WASM agreement | `crates/wasm/tests/cross_impl.rs`, web smoke | Separate existing WASM CI job, outside this report |
| Browser state and user flows | Web Vitest tests and `e2e/funnel.spec.ts` | Separate existing web/Chromium jobs |
| Published installation compatibility | Existing action-consumer matrix | Published CLI release, not current-source platform coverage |
| Operational and dependency policies | `scripts/tests`, supply-chain and operational workflows | Separate checks, outside Rust coverage |

Follow-up work should map uncovered production paths to missing behaviours, add meaningful
regressions and selected Kani proofs, and measure platform/browser gaps. No coverage
threshold should substitute for that assessment.

Tool behaviour and report formats: [cargo-llvm-cov documentation](https://github.com/taiki-e/cargo-llvm-cov/tree/v0.9.1).

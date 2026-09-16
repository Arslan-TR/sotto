# Server authorisation assurance

The server assurance binary runs production Axum handlers against a migrated PostgreSQL database:

```sh
DATABASE_URL='postgres://sotto:sotto@localhost:5432/sotto' \
SOTTO_RUN_DB_TESTS=1 \
cargo test -p sotto-server --test authorisation_transactions -- \
  --test-threads=1 --nocapture
```

`SOTTO_RUN_DB_TESTS=1` is required for acceptance. In that mode `DATABASE_URL` must be present,
reachable, and point at a loopback database. Migrations are applied before the test starts. Without
the opt-in, a local run prints an explicit skip so an ordinary workspace test cannot write to a
database accidentally. Use a fresh disposable database for each run.

The binary currently records four completed scenarios before printing
`SERVER_ASSURANCE_DONE N`: the access matrix, sequential authorisation rechecks after membership
changes, lifecycle and revision conflicts, and atomic removal or malformed batch behaviour. Assertions use the real router and also
inspect persisted membership, grant, token, revision, and secret state. A missing completion line,
zero scenarios, a failed assertion, or a database error fails the job.

The required CI and coverage jobs invoke this test target explicitly. This suite does not claim
cryptographic correctness, client or WebAssembly behaviour, hostile-workflow isolation, release
gating, or that sequential requests prove a race schedule. Forced lock schedules and competing
revision cases remain the next implementation slice described in `docs/kani/PR-05-IMPLEMENTATION.md`.

# Assurance checks

The repository's assurance checks run from four workflows: the normal CI matrix, Kani,
native coverage and codec fuzzing. Each workflow ends with a completion job that reads the
jobs from its own GitHub Actions run and checks the versioned list in
`.ci/assurance-checks.json`. A completion job is successful only when every expected leaf
job for that event and run attempt concluded `success`.

The completion job does not search for an older check with the same name. It binds the
verdict to the current run ID, attempt and workflow path, and compares the API's PR head SHA
with the event's expected source SHA while recording the checked-out SHA separately. A pull
request can legitimately have different values because Actions tests its synthetic merge ref.
Missing, skipped,
neutral, cancelled, timed-out, duplicated or unexpected jobs fail the completion check.
The codec workflow selects the PR or nightly group from the event; the skipped profile is
not part of that invocation's expected set.

The `sotto protect` ruleset requires the four completion contexts in addition to its existing
funnel and supply-chain requirements. The live ruleset is the authority for enforcement; this
document describes the contract those contexts implement.

These completion jobs run in the ordinary pull request workflows and use the reviewed workflow
revision. They enforce accidental omissions, skips and incomplete runs. They do not provide
hostile-contributor isolation from a change that rewrites the workflow, manifest or validator;
a future trusted default-branch workflow would be required for that threat model.

## Local validator fixtures

The validator can inspect a captured run without making a GitHub API request:

```sh
scripts/check-assurance --group kani --jobs-file /path/to/run.json \
  --expected-sha "$GIT_COMMIT"
```

The fixture contains `run` with `id`, `status`, `run_attempt`, `head_sha` and
`workflow_path`, plus a `jobs` array containing each job's `name` and `conclusion`.
For a live completion job, the workflow supplies `GITHUB_TOKEN`, `GITHUB_REPOSITORY`,
`GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT` and `GITHUB_SHA` automatically.

## Interpreting results

The completion check validates execution identity and conclusions. It does not replace the
leaf job's own evidence. Kani remains bounded formal verification; coverage identifies
tested paths without a threshold; fuzzing records its seed, corpus and campaign evidence.
Nightly campaigns are longer and are not PR requirements. A campaign finding or incomplete
nightly result is triaged separately and does not automatically clear a release hold.

Run the validator tests with:

```sh
python3 -B -m unittest scripts/tests/test_assurance.py -v
```

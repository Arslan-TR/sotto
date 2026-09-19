-- Durable, provider-neutral Cloud coverage snapshots.
--
-- These tables are intentionally additive and inactive until a reviewed projection adapter uses
-- them. A complete empty snapshot is different from a missing or unavailable projection.

CREATE TABLE cloud_coverage_revisions (
    beneficiary_id       TEXT NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    revision             BIGINT NOT NULL CHECK (revision > 0),
    operation_id         TEXT NOT NULL CHECK (btrim(operation_id) <> ''),
    expected_revision    BIGINT CHECK (expected_revision IS NULL OR expected_revision > 0),
    evidence_reference   TEXT NOT NULL CHECK (btrim(evidence_reference) <> ''),
    status               TEXT NOT NULL CHECK (status IN ('complete', 'unavailable')),
    unavailable_reason   TEXT,
    fact_count           BIGINT NOT NULL CHECK (fact_count >= 0),
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (beneficiary_id, revision),
    UNIQUE (beneficiary_id, operation_id),
    CHECK (
        (status = 'complete' AND unavailable_reason IS NULL)
        OR (status = 'unavailable' AND unavailable_reason IS NOT NULL
            AND unavailable_reason IN ('needs_reconciliation', 'conflicting_evidence'))
    ),
    CHECK (status = 'complete' OR fact_count = 0)
);

CREATE TABLE cloud_coverage_heads (
    beneficiary_id TEXT PRIMARY KEY REFERENCES users (id) ON DELETE RESTRICT,
    current_revision BIGINT,
    FOREIGN KEY (beneficiary_id, current_revision)
        REFERENCES cloud_coverage_revisions (beneficiary_id, revision)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (current_revision IS NULL OR current_revision > 0)
);

CREATE TABLE cloud_coverage_revision_facts (
    beneficiary_id     TEXT NOT NULL,
    revision           BIGINT NOT NULL,
    coverage_id        TEXT NOT NULL CHECK (btrim(coverage_id) <> ''),
    source_id          TEXT NOT NULL CHECK (btrim(source_id) <> ''),
    starts_at          BIGINT NOT NULL,
    paid_until         BIGINT NOT NULL,
    failed_renewal_id  TEXT CHECK (failed_renewal_id IS NULL OR btrim(failed_renewal_id) <> ''),
    PRIMARY KEY (beneficiary_id, revision, coverage_id),
    FOREIGN KEY (beneficiary_id, revision)
        REFERENCES cloud_coverage_revisions (beneficiary_id, revision)
        ON DELETE RESTRICT,
    CHECK (starts_at < paid_until)
);

CREATE UNIQUE INDEX cloud_coverage_revision_renewal_idx
    ON cloud_coverage_revision_facts (beneficiary_id, revision, failed_renewal_id)
    WHERE failed_renewal_id IS NOT NULL;

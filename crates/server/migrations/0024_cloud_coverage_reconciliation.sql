-- Durable source ownership and collection coordination for person-level Cloud coverage.
-- These records are inactive until a reviewed provider adapter supplies trusted evidence.

CREATE TABLE cloud_coverage_coordinators (
    beneficiary_id          TEXT PRIMARY KEY REFERENCES users (id) ON DELETE RESTRICT,
    source_set_generation   BIGINT NOT NULL DEFAULT 0 CHECK (source_set_generation >= 0),
    collection_epoch        BIGINT NOT NULL DEFAULT 0 CHECK (collection_epoch >= 0),
    current_attempt_id      TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cloud_coverage_sources (
    source_id                       TEXT PRIMARY KEY CHECK (btrim(source_id) <> ''),
    beneficiary_id                  TEXT NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    provider_namespace              TEXT NOT NULL CHECK (btrim(provider_namespace) <> ''),
    external_allocation_reference   TEXT NOT NULL
        CHECK (btrim(external_allocation_reference) <> ''),
    ownership_evidence_reference    TEXT NOT NULL
        CHECK (btrim(ownership_evidence_reference) <> ''),
    registration_operation_id       TEXT NOT NULL CHECK (btrim(registration_operation_id) <> ''),
    registration_source_set_generation BIGINT NOT NULL CHECK (registration_source_set_generation > 0),
    registration_projection_revision  BIGINT CHECK (
        registration_projection_revision IS NULL OR registration_projection_revision > 0
    ),
    registered_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cloud_coverage_sources_provider_allocation_key
        UNIQUE (provider_namespace, external_allocation_reference),
    CONSTRAINT cloud_coverage_sources_registration_operation_key
        UNIQUE (beneficiary_id, registration_operation_id)
);

ALTER TABLE cloud_coverage_sources
    ADD CONSTRAINT cloud_coverage_sources_registration_revision_fk
    FOREIGN KEY (beneficiary_id, registration_projection_revision)
    REFERENCES cloud_coverage_revisions (beneficiary_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE cloud_coverage_collection_attempts (
    attempt_id                    TEXT PRIMARY KEY CHECK (btrim(attempt_id) <> ''),
    beneficiary_id                TEXT NOT NULL
        REFERENCES cloud_coverage_coordinators (beneficiary_id) ON DELETE RESTRICT,
    collection_epoch              BIGINT NOT NULL CHECK (collection_epoch > 0),
    source_set_generation          BIGINT NOT NULL CHECK (source_set_generation > 0),
    expected_projection_revision  BIGINT CHECK (
        expected_projection_revision IS NULL OR expected_projection_revision > 0
    ),
    source_bindings                JSONB NOT NULL CHECK (jsonb_typeof(source_bindings) = 'array'),
    status                         TEXT NOT NULL CHECK (status IN ('pending', 'superseded', 'completed')),
    aggregate_evidence_reference  TEXT CHECK (
        aggregate_evidence_reference IS NULL OR btrim(aggregate_evidence_reference) <> ''
    ),
    canonical_result               JSONB CHECK (
        canonical_result IS NULL OR jsonb_typeof(canonical_result) = 'object'
    ),
    projection_revision            BIGINT CHECK (projection_revision IS NULL OR projection_revision > 0),
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                  TIMESTAMPTZ,
    CHECK (
        (status = 'completed'
            AND aggregate_evidence_reference IS NOT NULL
            AND canonical_result IS NOT NULL
            AND projection_revision IS NOT NULL
            AND completed_at IS NOT NULL)
        OR (status IN ('pending', 'superseded')
            AND aggregate_evidence_reference IS NULL
            AND canonical_result IS NULL
            AND projection_revision IS NULL
            AND completed_at IS NULL)
    ),
    UNIQUE (beneficiary_id, collection_epoch)
);

ALTER TABLE cloud_coverage_collection_attempts
    ADD CONSTRAINT cloud_coverage_attempts_beneficiary_key
    UNIQUE (beneficiary_id, attempt_id);

ALTER TABLE cloud_coverage_collection_attempts
    ADD CONSTRAINT cloud_coverage_attempts_expected_revision_fk
    FOREIGN KEY (beneficiary_id, expected_projection_revision)
    REFERENCES cloud_coverage_revisions (beneficiary_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE cloud_coverage_collection_attempts
    ADD CONSTRAINT cloud_coverage_attempts_completed_revision_fk
    FOREIGN KEY (beneficiary_id, projection_revision)
    REFERENCES cloud_coverage_revisions (beneficiary_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE cloud_coverage_coordinators
    ADD CONSTRAINT cloud_coverage_coordinators_attempt_fk
    FOREIGN KEY (beneficiary_id, current_attempt_id)
    REFERENCES cloud_coverage_collection_attempts (beneficiary_id, attempt_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX cloud_coverage_sources_beneficiary_idx
    ON cloud_coverage_sources (beneficiary_id, source_id);

CREATE INDEX cloud_coverage_attempts_beneficiary_status_idx
    ON cloud_coverage_collection_attempts (beneficiary_id, status);

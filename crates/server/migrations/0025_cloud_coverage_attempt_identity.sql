-- Scope collection attempt identities to their beneficiary.
--
-- Provider adapters may reuse an operation identifier for different people. The coordinator
-- already records beneficiary ownership, so the durable attempt key must do the same.

ALTER TABLE cloud_coverage_coordinators
    DROP CONSTRAINT cloud_coverage_coordinators_attempt_fk;

ALTER TABLE cloud_coverage_collection_attempts
    DROP CONSTRAINT cloud_coverage_attempts_beneficiary_key;

ALTER TABLE cloud_coverage_collection_attempts
    DROP CONSTRAINT cloud_coverage_collection_attempts_pkey;

ALTER TABLE cloud_coverage_collection_attempts
    ADD PRIMARY KEY (beneficiary_id, attempt_id);

ALTER TABLE cloud_coverage_coordinators
    ADD CONSTRAINT cloud_coverage_coordinators_attempt_fk
    FOREIGN KEY (beneficiary_id, current_attempt_id)
    REFERENCES cloud_coverage_collection_attempts (beneficiary_id, attempt_id)
    DEFERRABLE INITIALLY DEFERRED;

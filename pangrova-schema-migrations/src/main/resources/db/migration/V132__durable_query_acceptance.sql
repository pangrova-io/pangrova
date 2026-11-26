ALTER TABLE query_state_journal
    ADD COLUMN principal VARCHAR(256),
    ADD COLUMN idempotency_key_digest VARCHAR(64),
    ADD COLUMN request_fingerprint VARCHAR(64),
    ADD COLUMN acceptance_receipt_json TEXT,
    ADD COLUMN retention_ms BIGINT;

ALTER TABLE query_state_journal ADD CONSTRAINT query_state_journal_acceptance_shape CHECK (
    (principal IS NULL AND idempotency_key_digest IS NULL AND request_fingerprint IS NULL
        AND acceptance_receipt_json IS NULL AND retention_ms IS NULL)
    OR
    (state = 'QUEUED' AND principal IS NOT NULL AND principal <> ''
        AND idempotency_key_digest IS NOT NULL AND idempotency_key_digest ~ '^[0-9a-f]{64}$'
        AND request_fingerprint IS NOT NULL AND request_fingerprint ~ '^[0-9a-f]{64}$'
        AND acceptance_receipt_json IS NOT NULL
        AND retention_ms IS NOT NULL AND retention_ms > 0)
);

CREATE UNIQUE INDEX query_state_journal_single_queued
    ON query_state_journal (query_id) WHERE state = 'QUEUED';
CREATE UNIQUE INDEX query_state_journal_idempotency_acceptance
    ON query_state_journal (principal, idempotency_key_digest)
    WHERE state = 'QUEUED' AND idempotency_key_digest IS NOT NULL;
CREATE INDEX query_state_journal_acceptance_retention
    ON query_state_journal (retention_ms)
    WHERE idempotency_key_digest IS NOT NULL;

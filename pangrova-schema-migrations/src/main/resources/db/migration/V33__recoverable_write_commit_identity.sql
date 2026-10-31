-- A recoverable catalog commit must retain the original security and snapshot context.
-- COMMITTED rows are terminal records left by a crash after markCommitted and before the
-- normal purge. They cannot be replayed and are safe to purge during this upgrade.
-- Any unresolved legacy intent lacks the context required for safe replay: fail closed.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM write_commit_journal WHERE state <> 'COMMITTED') THEN
        RAISE EXCEPTION 'Unresolved legacy write commit intents require reconciliation before V33';
    END IF;
END $$;

DELETE FROM write_commit_journal WHERE state = 'COMMITTED';

-- No defaults: every new intent must supply its own identity and snapshot context.
ALTER TABLE write_commit_journal
    ADD COLUMN tenant_id VARCHAR(255) NOT NULL,
    ADD COLUMN base_snapshot_id BIGINT NOT NULL,
    ADD COLUMN write_output_binding TEXT NOT NULL,
    ADD COLUMN commit_fingerprint VARCHAR(64) NOT NULL;

ALTER TABLE write_commit_journal
    ADD CONSTRAINT chk_write_commit_base_snapshot
        CHECK (base_snapshot_id >= -1),
    ADD CONSTRAINT chk_write_commit_fingerprint
        CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT uq_write_commit_fingerprint
        UNIQUE (commit_fingerprint);

-- Durable connector commit events must preserve tenant identity through
-- maintenance-leader replay; table names are not globally unique.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM index_commit_log) THEN
        RAISE EXCEPTION
            'index_commit_log must be drained before enabling tenant-scoped commit replay';
    END IF;
END $$;

ALTER TABLE index_commit_log
    ADD COLUMN tenant_id VARCHAR(255) NOT NULL;

DROP INDEX IF EXISTS idx_icl_unprocessed;
CREATE INDEX idx_icl_unprocessed
    ON index_commit_log (processed, created_at, tenant_id, event_id);

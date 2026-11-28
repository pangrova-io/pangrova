ALTER TABLE write_commit_journal
    DROP CONSTRAINT write_staging_observed_cleanup_requires_drain,
    DROP COLUMN staging_observed_claim_token,
    ADD COLUMN staging_observed_due_at TIMESTAMPTZ,
    ADD COLUMN staging_observed_claim_id UUID,
    ADD COLUMN staging_observed_declined_by JSONB,
    ADD CONSTRAINT write_staging_cleanup_requires_proven_exit CHECK (
        (staging_observed_due_at IS NULL AND staging_observed_claim_id IS NULL
            AND staging_observed_reclaimed_at IS NULL)
        OR (staging_drain_token IS NOT NULL AND staging_drain_token > 0
            AND (staging_observed_due_at IS NOT NULL AND staging_observed_reclaimed_at IS NULL
                OR staging_observed_due_at IS NULL AND staging_observed_claim_id IS NULL
                    AND staging_observed_reclaimed_at IS NOT NULL))
    );

UPDATE write_commit_journal SET staging_observed_due_at = clock_timestamp()
WHERE staging_drain_token > 0 AND staging_observed_reclaimed_at IS NULL;

CREATE INDEX write_staging_cleanup_due_idx
    ON write_commit_journal (staging_observed_due_at, query_id)
    WHERE staging_observed_due_at IS NOT NULL;

CREATE FUNCTION notify_observed_write_staging() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.staging_observed_due_at IS NOT NULL AND (OLD.staging_observed_due_at IS NULL
        OR NEW.staging_observed_due_at < OLD.staging_observed_due_at) THEN
        PERFORM pg_notify('pangrova_write_staging_' || TG_RELID, '');
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER notify_observed_write_staging
AFTER UPDATE OF staging_observed_due_at ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION notify_observed_write_staging();

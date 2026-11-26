-- Publication retirement is a durable obligation independent of descriptor edits.
ALTER TABLE index_generation_manifests
    ADD COLUMN retired_at TIMESTAMPTZ(6),
    ADD COLUMN retirement_reason VARCHAR(32);

-- We cannot reconstruct an exact old retirement instant. Start its grace period
-- conservatively at migration time, not the older mutable descriptor timestamp.
UPDATE index_generation_manifests
SET retired_at=transaction_timestamp(), retirement_reason='legacy_retired'
WHERE state='RETIRED';

CREATE FUNCTION pangrova_stamp_index_retirement()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF OLD.retired_at IS NOT NULL AND (
            NEW.state IS DISTINCT FROM OLD.state
            OR NEW.retired_at IS DISTINCT FROM OLD.retired_at
            OR NEW.retirement_reason IS DISTINCT FROM OLD.retirement_reason
        ) THEN
            RAISE EXCEPTION 'Index generation retirement evidence is immutable';
        END IF;
    END IF;
    -- An older writer can retire without stamping. Its exact previous reason
    -- is unknown, so record the observation conservatively.
    IF NEW.state='RETIRED' AND NEW.retired_at IS NULL AND NEW.retirement_reason IS NULL THEN
        NEW.retired_at := transaction_timestamp();
        NEW.retirement_reason := 'legacy_retired';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER stamp_index_retirement
BEFORE INSERT OR UPDATE ON index_generation_manifests
FOR EACH ROW EXECUTE FUNCTION pangrova_stamp_index_retirement();

ALTER TABLE index_generation_manifests ADD CONSTRAINT chk_index_retirement_evidence CHECK (
    (state='ACTIVE' AND retired_at IS NULL AND retirement_reason IS NULL)
    OR (state='RETIRED' AND retired_at IS NOT NULL
        AND retirement_reason IS NOT NULL
        AND retirement_reason IN ('source_snapshot_drift','superseded','legacy_retired'))
);

-- An exact maintenance claim survives a failed/uncertain connector response.
-- The due timestamp rotates attempted work behind other eligible rows.
ALTER TABLE artifact_snapshot_pins ADD COLUMN cleanup_retry_at TIMESTAMPTZ(6);

CREATE INDEX idx_snapshot_pin_cleanup_due
    ON artifact_snapshot_pins ((COALESCE(cleanup_retry_at,created_at)),created_at,tenant_id,pin_owner)
    WHERE state IN ('INTENDED','PINNED','RELEASING');

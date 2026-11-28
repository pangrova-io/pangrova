-- Physical producer ordinals stay contiguous, while the original partition
-- coordinates and submitted page identities remain stable.
ALTER TABLE artifact_stage_exchanges
    ADD COLUMN physical_producer_count INTEGER CHECK (physical_producer_count > 0);
ALTER TABLE artifact_build_pages
    ADD COLUMN exchange_partition_start INTEGER,
    ADD COLUMN exchange_partition_end INTEGER,
    ADD CONSTRAINT artifact_page_input_range CHECK (
        (exchange_partition_start IS NULL AND exchange_partition_end IS NULL)
        OR (page_kind = 'RELATIONAL_COMPUTE' AND exchange_partition_start IS NOT NULL
            AND exchange_partition_end IS NOT NULL AND exchange_partition_start >= 0
            AND exchange_partition_start < exchange_partition_end AND state <> 'COALESCED')
    ),
    DROP CONSTRAINT chk_artifact_build_page_state,
    ADD CONSTRAINT chk_artifact_build_page_state
        CHECK (state IN ('PENDING', 'LEASED', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'COALESCED')),
    ADD CONSTRAINT artifact_retired_partition_unissued CHECK (
        state <> 'COALESCED' OR (page_kind = 'RELATIONAL_COMPUTE' AND attempt_id = -1)
    );

CREATE FUNCTION preserve_artifact_stage_ranges()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'Frozen artifact input ranges are immutable';
END;
$$;

CREATE TRIGGER artifact_exchange_producer_count_immutable
BEFORE UPDATE OF physical_producer_count ON artifact_stage_exchanges
FOR EACH ROW WHEN (OLD.physical_producer_count IS NOT NULL
    AND OLD.physical_producer_count IS DISTINCT FROM NEW.physical_producer_count)
EXECUTE FUNCTION preserve_artifact_stage_ranges();

CREATE TRIGGER artifact_page_input_range_immutable
BEFORE UPDATE OF exchange_partition_start, exchange_partition_end ON artifact_build_pages
FOR EACH ROW WHEN (OLD.exchange_partition_start IS NOT NULL AND
    (OLD.exchange_partition_start IS DISTINCT FROM NEW.exchange_partition_start
        OR OLD.exchange_partition_end IS DISTINCT FROM NEW.exchange_partition_end))
EXECUTE FUNCTION preserve_artifact_stage_ranges();

CREATE TRIGGER artifact_coalesced_page_not_reactivated
BEFORE UPDATE OR DELETE ON artifact_build_pages
FOR EACH ROW WHEN (OLD.state = 'COALESCED')
EXECUTE FUNCTION preserve_artifact_exchange_evidence();

CREATE OR REPLACE FUNCTION verify_artifact_exchange_producer_cohort()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected INTEGER := (convert_from(NEW.finalization_bytes, 'UTF8')::jsonb ->> 'producerCount')::integer;
    actual BIGINT;
    physical_count INTEGER;
    first_partition INTEGER;
    last_partition INTEGER;
    all_succeeded BOOLEAN;
BEGIN
    SELECT count(*), max(e.physical_producer_count), min(p.partition_id), max(p.partition_id),
           bool_and(p.state = 'SUCCEEDED' AND p.page_kind = 'RELATIONAL_COMPUTE')
      INTO actual, physical_count, first_partition, last_partition, all_succeeded
      FROM artifact_stage_exchanges e
      JOIN artifact_build_pages p
        ON p.tenant_id = e.tenant_id AND p.job_id = e.job_id AND p.stage_id = e.producer_stage_id
     WHERE e.tenant_id = NEW.tenant_id AND e.job_id = NEW.job_id AND e.exchange_id = NEW.exchange_id
       AND (e.physical_producer_count IS NULL OR p.state <> 'COALESCED');

    IF expected IS NULL OR expected <= 0 OR actual <> expected
            OR (physical_count IS NOT NULL AND physical_count <> expected)
            OR first_partition IS DISTINCT FROM 0 OR last_partition IS DISTINCT FROM expected::bigint - 1
            OR all_succeeded IS NOT TRUE THEN
        RAISE EXCEPTION USING ERRCODE = '23514', CONSTRAINT = 'artifact_exchange_producer_cohort',
            MESSAGE = 'External finalize lacks exact, immutable producer winners';
    END IF;
    RETURN NULL;
END;
$$;

-- Verify each report when it becomes a successful relational producer. A later
-- finalize statement only needs to prove the exact cohort of successful pages.
ALTER TABLE artifact_build_pages
    ADD CONSTRAINT artifact_relational_producer_report_digest
    CHECK (page_kind <> 'RELATIONAL_COMPUTE' OR state <> 'SUCCEEDED' OR (
        report_bytes IS NOT NULL AND octet_length(report_bytes) > 0
        AND report_sha256 IS NOT NULL AND encode(sha256(report_bytes), 'hex') = report_sha256
    ));

CREATE FUNCTION preserve_artifact_exchange_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        -- The top-level job delete still owns normal cascade retirement.
        IF NOT EXISTS (
            SELECT 1 FROM artifact_build_jobs WHERE tenant_id = OLD.tenant_id AND job_id = OLD.job_id
        ) THEN
            RETURN OLD;
        END IF;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'Committed artifact exchange evidence is immutable';
END;
$$;

CREATE TRIGGER artifact_relational_producer_immutable
BEFORE UPDATE OR DELETE ON artifact_build_pages
FOR EACH ROW WHEN (OLD.page_kind = 'RELATIONAL_COMPUTE' AND OLD.state = 'SUCCEEDED')
EXECUTE FUNCTION preserve_artifact_exchange_evidence();

CREATE TRIGGER artifact_exchange_finalization_immutable
BEFORE UPDATE OR DELETE ON artifact_exchange_finalizations
FOR EACH ROW EXECUTE FUNCTION preserve_artifact_exchange_evidence();

CREATE TRIGGER artifact_producer_evidence_no_truncate
BEFORE TRUNCATE ON artifact_build_pages
FOR EACH STATEMENT EXECUTE FUNCTION preserve_artifact_exchange_evidence();

CREATE TRIGGER artifact_exchange_finalization_no_truncate
BEFORE TRUNCATE ON artifact_exchange_finalizations
FOR EACH STATEMENT EXECUTE FUNCTION preserve_artifact_exchange_evidence();

CREATE FUNCTION verify_artifact_exchange_producer_cohort()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected INTEGER := (convert_from(NEW.finalization_bytes, 'UTF8')::jsonb ->> 'producerCount')::integer;
    actual BIGINT;
    first_partition INTEGER;
    last_partition INTEGER;
    all_succeeded BOOLEAN;
BEGIN
    SELECT count(*), min(p.partition_id), max(p.partition_id),
           bool_and(p.state = 'SUCCEEDED' AND p.page_kind = 'RELATIONAL_COMPUTE')
      INTO actual, first_partition, last_partition, all_succeeded
      FROM artifact_stage_exchanges e
      JOIN artifact_build_pages p
        ON p.tenant_id = e.tenant_id AND p.job_id = e.job_id AND p.stage_id = e.producer_stage_id
     WHERE e.tenant_id = NEW.tenant_id AND e.job_id = NEW.job_id AND e.exchange_id = NEW.exchange_id;
    -- Unique stage partition coordinates make count and endpoints prove contiguous coverage.
    IF expected IS NULL OR expected <= 0 OR actual <> expected
            OR first_partition IS DISTINCT FROM 0 OR last_partition IS DISTINCT FROM expected::bigint - 1
            OR all_succeeded IS NOT TRUE THEN
        RAISE EXCEPTION USING ERRCODE = '23514', CONSTRAINT = 'artifact_exchange_producer_cohort',
            MESSAGE = 'External finalize lacks exact, immutable producer winners';
    END IF;
    RETURN NULL;
END;
$$;

-- Only a new receipt executes the cohort check. An ON CONFLICT DO NOTHING
-- replay leaves the original receipt and proof intact.
CREATE TRIGGER artifact_exchange_finalize_producer_cohort
AFTER INSERT ON artifact_exchange_finalizations
FOR EACH ROW EXECUTE FUNCTION verify_artifact_exchange_producer_cohort();

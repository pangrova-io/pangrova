-- Keep the published-attempt guard valid while a rename cascades.
--
-- The logical name lives on attempts, generations, and objects, and a rename
-- updates all three through cascading foreign keys. PostgreSQL does not order
-- those cascades, so the published-attempt check must not join the two child
-- rows by object name: it must rely on the generation identity alone, which a
-- rename never changes.
--
-- The check only belongs to the transition into PUBLISHED. PUBLISHED is
-- terminal in the domain, and a later publication retires the previous head
-- generation without touching its attempt row. Re-validating already published
-- attempts on every update would therefore reject any subsequent rename.

CREATE OR REPLACE FUNCTION materialization.validate_attempt()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    header materialization.objects%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
    leader distributed_lease%ROWTYPE;
    base materialization.generations%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'PREPARING' THEN
            RAISE EXCEPTION 'a materialization attempt must begin in PREPARING';
        END IF;
        SELECT * INTO header FROM materialization.objects o
            WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.object_name FOR SHARE;
        IF NOT FOUND OR header.incarnation_id <> NEW.incarnation_id
                OR header.definition_revision <> NEW.definition_revision
                OR header.revision <> NEW.base_object_revision
                OR header.state NOT IN ('CREATING', 'ACTIVE', 'SUSPENDED', 'FAILED') THEN
            RAISE EXCEPTION 'attempt does not match the current materialization head';
        END IF;
        IF NEW.refresh_kind = 'FULL' AND NEW.base_generation_id IS NOT NULL THEN
            RAISE EXCEPTION 'full refresh must not name a base generation';
        END IF;
        IF NEW.refresh_kind <> 'FULL' THEN
            SELECT * INTO base FROM materialization.generations g
                WHERE g.generation_id = NEW.base_generation_id;
            IF NOT FOUND OR (base.tenant_id, base.object_name, base.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR base.state <> 'PUBLISHED'
                    OR header.published_generation_id IS DISTINCT FROM base.generation_id THEN
                RAISE EXCEPTION 'incremental attempt requires the exact published base generation';
            END IF;
        END IF;
        SELECT * INTO anchor FROM execution_anchor a
            WHERE a.execution_id = NEW.execution_anchor FOR SHARE;
        IF NOT FOUND OR anchor.fencing_token <= 0
                OR anchor.expires_at < NEW.lease_expires_at
                OR anchor.expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'attempt requires a live execution anchor covering its lease';
        END IF;
        SELECT * INTO leader FROM distributed_lease
            WHERE lease_key = 'coordinator_leader' FOR SHARE;
        IF NOT FOUND OR leader.epoch IS DISTINCT FROM NEW.control_epoch
                OR leader.expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'attempt requires the current live coordinator control epoch';
        END IF;
        IF materialization.source_vector_hash(NEW.source_vector)
                IS DISTINCT FROM NEW.source_vector_hash THEN
            RAISE EXCEPTION 'attempt source vector hash does not match its canonical content';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization attempts are immutable history';
    END IF;
    IF (NEW.attempt_id, NEW.tenant_id, NEW.incarnation_id,
            NEW.definition_revision, NEW.base_object_revision, NEW.base_generation_id,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.trigger_kind,
            NEW.requesting_principal, NEW.definer_principal, NEW.execution_anchor,
            NEW.control_epoch, NEW.lease_expires_at, NEW.candidate_catalog,
            NEW.candidate_schema, NEW.candidate_table, NEW.created_at)
            IS DISTINCT FROM
       (OLD.attempt_id, OLD.tenant_id, OLD.incarnation_id,
            OLD.definition_revision, OLD.base_object_revision, OLD.base_generation_id,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.trigger_kind,
            OLD.requesting_principal, OLD.definer_principal, OLD.execution_anchor,
            OLD.control_epoch, OLD.lease_expires_at, OLD.candidate_catalog,
            OLD.candidate_schema, OLD.candidate_table, OLD.created_at)
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization attempt identity and candidate are immutable';
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'PREPARING' THEN NEW.state IN ('BUILDING', 'FAILED', 'ABANDONED')
            WHEN 'BUILDING' THEN NEW.state IN ('COMMITTED', 'FAILED', 'ABANDONED', 'QUARANTINED')
            WHEN 'COMMITTED' THEN NEW.state IN ('PUBLISHED', 'QUARANTINED', 'SUPERSEDED', 'FAILED')
            WHEN 'QUARANTINED' THEN NEW.state IN ('COMMITTED', 'ABANDONED')
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization attempt transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.state IN ('COMMITTED', 'PUBLISHED')
            AND NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                WHERE e.attempt_id = NEW.attempt_id) THEN
        RAISE EXCEPTION 'committed or published attempt requires exact commit evidence';
    END IF;
    IF NEW.state = 'PUBLISHED' AND OLD.state IS DISTINCT FROM 'PUBLISHED' THEN
        IF NOT EXISTS (SELECT 1 FROM materialization.generations g
                JOIN materialization.objects o
                    ON o.published_generation_id = g.generation_id
                WHERE g.producing_attempt_id = NEW.attempt_id
                    AND g.state = 'PUBLISHED'
                    AND o.tenant_id = NEW.tenant_id) THEN
            RAISE EXCEPTION 'published attempt must own the published generation of its head';
        END IF;
    END IF;
    IF NEW.state = 'FAILED' AND NEW.error_code IS NULL THEN
        RAISE EXCEPTION 'failed attempt requires an error code';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'attempt update instant must not move backwards';
    END IF;
    RETURN NEW;
END $$;

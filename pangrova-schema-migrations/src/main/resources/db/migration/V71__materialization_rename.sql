-- Rename support for materialization objects.
--
-- The logical name participates in every durable foreign key, so a rename is a
-- single CAS update of materialization.objects that cascades through the
-- definition, attempt, generation, dependency, and schedule rows. Historical
-- events and command receipts keep the name they were written under.
--
-- Append-only and identity triggers continue to reject every other mutation.
-- The repository opens the narrow rename window with a transaction-local
-- `pangrova.materialization_rename` marker, and each trigger below accepts only
-- an object_name change that leaves every identity column untouched.

-- ── cascade the logical name ────────────────────────────────────────────────

ALTER TABLE materialization.definition_revisions
    DROP CONSTRAINT IF EXISTS definition_revisions_tenant_id_object_name_fkey;
ALTER TABLE materialization.definition_revisions
    DROP CONSTRAINT IF EXISTS definition_revisions_object_fk;
ALTER TABLE materialization.definition_revisions
    ADD CONSTRAINT definition_revisions_object_fk
    FOREIGN KEY (tenant_id, object_name)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

ALTER TABLE materialization.generations
    DROP CONSTRAINT IF EXISTS generations_tenant_id_object_name_fkey;
ALTER TABLE materialization.generations
    DROP CONSTRAINT IF EXISTS generations_object_fk;
ALTER TABLE materialization.generations
    ADD CONSTRAINT generations_object_fk
    FOREIGN KEY (tenant_id, object_name)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

ALTER TABLE materialization.attempts
    DROP CONSTRAINT IF EXISTS attempts_tenant_id_object_name_fkey;
ALTER TABLE materialization.attempts
    DROP CONSTRAINT IF EXISTS attempts_object_fk;
ALTER TABLE materialization.attempts
    ADD CONSTRAINT attempts_object_fk
    FOREIGN KEY (tenant_id, object_name)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

ALTER TABLE materialization.dependencies
    DROP CONSTRAINT IF EXISTS dependencies_tenant_id_downstream_object_fkey;
ALTER TABLE materialization.dependencies
    DROP CONSTRAINT IF EXISTS dependencies_downstream_object_fk;
ALTER TABLE materialization.dependencies
    ADD CONSTRAINT dependencies_downstream_object_fk
    FOREIGN KEY (tenant_id, downstream_object)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

ALTER TABLE materialization.dependencies
    DROP CONSTRAINT IF EXISTS dependencies_tenant_id_upstream_object_fkey;
ALTER TABLE materialization.dependencies
    DROP CONSTRAINT IF EXISTS dependencies_upstream_object_fk;
ALTER TABLE materialization.dependencies
    ADD CONSTRAINT dependencies_upstream_object_fk
    FOREIGN KEY (tenant_id, upstream_object)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

ALTER TABLE materialization.schedules
    DROP CONSTRAINT IF EXISTS schedules_tenant_id_object_name_fkey;
ALTER TABLE materialization.schedules
    DROP CONSTRAINT IF EXISTS schedules_object_fk;
ALTER TABLE materialization.schedules
    ADD CONSTRAINT schedules_object_fk
    FOREIGN KEY (tenant_id, object_name)
    REFERENCES materialization.objects (tenant_id, object_name) ON UPDATE CASCADE;

-- ── definition immutability, opened only for a rename ───────────────────────

CREATE OR REPLACE FUNCTION materialization.reject_definition_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
            AND current_setting('pangrova.materialization_rename', true) = 'on'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.revision = OLD.revision
            AND NEW.definition_sql = OLD.definition_sql
            AND NEW.canonical_sql = OLD.canonical_sql
            AND NEW.plan_fingerprint = OLD.plan_fingerprint
            AND NEW.resolved = OLD.resolved
            AND NEW.output_schema IS NOT DISTINCT FROM OLD.output_schema
            AND NEW.output_schema_hash IS NOT DISTINCT FROM OLD.output_schema_hash
            AND NEW.sources IS NOT DISTINCT FROM OLD.sources
            AND NEW.policy_snapshot IS NOT DISTINCT FROM OLD.policy_snapshot
            AND NEW.security_mode = OLD.security_mode
            AND NEW.definer_principal = OLD.definer_principal
            AND NEW.session_properties IS NOT DISTINCT FROM OLD.session_properties
            AND NEW.created_at = OLD.created_at THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'materialization definitions are immutable';
END $$;

-- ── object header identity ──────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_object_header()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    generation materialization.generations%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.revision <> 0 OR NEW.definition_revision <> 1
                OR NEW.published_generation_id IS NOT NULL
                OR NEW.state NOT IN ('CREATING', 'SUSPENDED')
                OR NEW.security_mode <> 'DEFINER' THEN
            RAISE EXCEPTION 'a materialization object must be created without a published generation';
        END IF;
        INSERT INTO materialization.registry_epochs (tenant_id, epoch) VALUES (NEW.tenant_id, 1)
            ON CONFLICT (tenant_id) DO UPDATE SET epoch = materialization.registry_epochs.epoch + 1;
        RETURN NEW;
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
            OR NEW.object_kind IS DISTINCT FROM OLD.object_kind
            OR NEW.incarnation_id IS DISTINCT FROM OLD.incarnation_id
            OR NEW.created_at IS DISTINCT FROM OLD.created_at
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization object identity is immutable';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'materialization object revision must advance exactly one';
    END IF;
    IF NEW.definition_revision NOT IN (OLD.definition_revision, OLD.definition_revision + 1) THEN
        RAISE EXCEPTION 'materialization definition revision must stay or advance exactly one';
    END IF;
    IF NEW.definition_revision = OLD.definition_revision + 1 AND NOT EXISTS (
            SELECT 1 FROM materialization.definition_revisions d
            WHERE d.tenant_id = NEW.tenant_id AND d.object_name = NEW.object_name
                AND d.revision = NEW.definition_revision) THEN
        RAISE EXCEPTION 'materialization definition revision % is not recorded', NEW.definition_revision;
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'CREATING' THEN NEW.state IN ('ACTIVE', 'FAILED', 'DROPPING')
            WHEN 'ACTIVE' THEN NEW.state IN ('SUSPENDED', 'FAILED', 'DROPPING')
            WHEN 'SUSPENDED' THEN NEW.state IN ('ACTIVE', 'DROPPING')
            WHEN 'FAILED' THEN NEW.state IN ('ACTIVE', 'DROPPING')
            WHEN 'DROPPING' THEN NEW.state = 'DROPPED'
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization object transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.published_generation_id IS DISTINCT FROM OLD.published_generation_id THEN
        IF NEW.published_generation_id IS NULL THEN
            IF NEW.state NOT IN ('DROPPING', 'DROPPED') THEN
                RAISE EXCEPTION 'published generation can only be cleared while dropping';
            END IF;
        ELSE
            SELECT * INTO generation FROM materialization.generations g
                WHERE g.generation_id = NEW.published_generation_id;
            IF NOT FOUND OR (generation.tenant_id, generation.object_name, generation.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR generation.definition_revision <> NEW.definition_revision
                    OR generation.state NOT IN ('COMMITTED', 'PUBLISHED')
                    OR NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                        WHERE e.attempt_id = generation.producing_attempt_id) THEN
                RAISE EXCEPTION 'published generation must be the exact committed generation of this head';
            END IF;
            IF NEW.state <> 'ACTIVE' THEN
                RAISE EXCEPTION 'publication must leave the object ACTIVE';
            END IF;
        END IF;
    END IF;
    IF NEW.state = 'ACTIVE' AND NEW.published_generation_id IS NULL THEN
        RAISE EXCEPTION 'an active materialization requires a published generation';
    END IF;
    IF NEW.refresh_mode = 'FULL' AND NEW.incremental_strategy IS NOT NULL THEN
        RAISE EXCEPTION 'full refresh mode cannot carry an incremental strategy';
    END IF;
    INSERT INTO materialization.registry_epochs (tenant_id, epoch) VALUES (NEW.tenant_id, 1)
        ON CONFLICT (tenant_id) DO UPDATE SET epoch = materialization.registry_epochs.epoch + 1;
    RETURN NEW;
END $$;

-- ── attempt identity ────────────────────────────────────────────────────────

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
    IF NEW.state = 'PUBLISHED' THEN
        IF NOT EXISTS (SELECT 1 FROM materialization.generations g
                JOIN materialization.objects o
                    ON o.tenant_id = g.tenant_id AND o.object_name = g.object_name
                WHERE g.producing_attempt_id = NEW.attempt_id
                    AND g.generation_id = o.published_generation_id
                    AND g.state = 'PUBLISHED') THEN
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

-- ── generation identity ─────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_generation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    attempt materialization.attempts%ROWTYPE;
    base materialization.generations%ROWTYPE;
    header materialization.objects%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'CANDIDATE' THEN
            RAISE EXCEPTION 'a generation must begin as a candidate';
        END IF;
        SELECT * INTO attempt FROM materialization.attempts a
            WHERE a.attempt_id = NEW.producing_attempt_id FOR SHARE;
        IF NOT FOUND OR (attempt.tenant_id, attempt.object_name, attempt.incarnation_id,
                attempt.definition_revision, attempt.candidate_catalog, attempt.candidate_schema,
                attempt.candidate_table)
                IS DISTINCT FROM
               (NEW.tenant_id, NEW.object_name, NEW.incarnation_id, NEW.definition_revision,
                NEW.physical_catalog, NEW.physical_schema, NEW.physical_table)
                OR attempt.state NOT IN ('PREPARING', 'BUILDING') THEN
            RAISE EXCEPTION 'generation must be the candidate of its exact reserved attempt';
        END IF;
        IF NEW.refresh_kind = 'FULL' AND NEW.base_generation_id IS NOT NULL THEN
            RAISE EXCEPTION 'full generation must not name a base generation';
        END IF;
        IF NEW.refresh_kind <> 'FULL' THEN
            SELECT * INTO base FROM materialization.generations g
                WHERE g.generation_id = NEW.base_generation_id;
            IF NOT FOUND OR (base.tenant_id, base.object_name, base.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR base.state <> 'PUBLISHED' THEN
                RAISE EXCEPTION 'incremental generation requires a published base generation';
            END IF;
        END IF;
        IF materialization.source_vector_hash(NEW.source_vector)
                IS DISTINCT FROM NEW.source_vector_hash THEN
            RAISE EXCEPTION 'generation source vector hash does not match its canonical content';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization generations are retained until collected';
    END IF;
    IF (NEW.generation_id, NEW.tenant_id, NEW.incarnation_id,
            NEW.definition_revision, NEW.producing_attempt_id, NEW.base_generation_id,
            NEW.physical_catalog, NEW.physical_schema, NEW.physical_table,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.created_at)
            IS DISTINCT FROM
       (OLD.generation_id, OLD.tenant_id, OLD.incarnation_id,
            OLD.definition_revision, OLD.producing_attempt_id, OLD.base_generation_id,
            OLD.physical_catalog, OLD.physical_schema, OLD.physical_table,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.created_at)
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization generation identity is immutable';
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'CANDIDATE' THEN NEW.state IN ('COMMITTED', 'QUARANTINED', 'COLLECTED')
            WHEN 'COMMITTED' THEN NEW.state IN ('PUBLISHED', 'QUARANTINED', 'COLLECTED')
            WHEN 'PUBLISHED' THEN NEW.state = 'RETIRED'
            WHEN 'RETIRED' THEN NEW.state = 'COLLECTED'
            WHEN 'QUARANTINED' THEN NEW.state IN ('COMMITTED', 'COLLECTED')
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization generation transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.state = 'COLLECTED' AND OLD.state IN ('CANDIDATE', 'COMMITTED')
            AND NOT EXISTS (SELECT 1 FROM materialization.attempts a
                WHERE a.attempt_id = NEW.producing_attempt_id
                AND a.state IN ('FAILED', 'ABANDONED', 'SUPERSEDED')) THEN
        RAISE EXCEPTION 'unpublished generation can only be collected after its attempt is terminal';
    END IF;
    IF NEW.state IN ('COMMITTED', 'PUBLISHED')
            AND NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                WHERE e.attempt_id = NEW.producing_attempt_id) THEN
        RAISE EXCEPTION 'committed or published generation requires exact commit evidence';
    END IF;
    IF NEW.state = 'PUBLISHED' THEN
        IF NEW.published_at IS NULL THEN
            RAISE EXCEPTION 'published generation requires a publication instant';
        END IF;
        SELECT * INTO header FROM materialization.objects o
            WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.object_name FOR SHARE;
        IF NOT FOUND OR header.published_generation_id IS DISTINCT FROM NEW.generation_id
                OR header.state <> 'ACTIVE' THEN
            RAISE EXCEPTION 'generation can only be published through its exact object head';
        END IF;
    END IF;
    IF NEW.state = 'COLLECTED' AND EXISTS (
            SELECT 1 FROM materialization.reader_pins p
            WHERE p.generation_id = NEW.generation_id
              AND p.expires_at > clock_timestamp()) THEN
        RAISE EXCEPTION 'generation with live reader pins cannot be collected';
    END IF;
    RETURN NEW;
END $$;

-- ── dependency graph ────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_dependency()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF current_setting('pangrova.materialization_rename', true) = 'on'
                AND NEW.tenant_id = OLD.tenant_id
                AND NEW.downstream_definition_revision = OLD.downstream_definition_revision
                AND NEW.dependency_kind = OLD.dependency_kind
                AND (NEW.downstream_object IS NOT DISTINCT FROM OLD.downstream_object
                    OR NEW.upstream_object IS NOT DISTINCT FROM OLD.upstream_object) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'materialization dependencies are rebuilt, never updated';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM materialization.definition_revisions d
            WHERE d.tenant_id = NEW.tenant_id AND d.object_name = NEW.downstream_object
                AND d.revision = NEW.downstream_definition_revision) THEN
        RAISE EXCEPTION 'dependency must name an existing downstream definition revision';
    END IF;
    PERFORM 1 FROM materialization.objects o
        WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.upstream_object
            AND o.state NOT IN ('DROPPING', 'DROPPED')
        FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dependency must name a live upstream materialization';
    END IF;
    RETURN NEW;
END $$;

-- ── schedules ───────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_schedule()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 0 THEN
            RAISE EXCEPTION 'schedule must be created at version zero';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'schedules are disabled and retained with their object';
    END IF;
    IF current_setting('pangrova.materialization_rename', true) = 'on'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.target_lag_ms IS NOT DISTINCT FROM OLD.target_lag_ms
            AND NEW.next_due_at IS NOT DISTINCT FROM OLD.next_due_at
            AND NEW.last_success_at IS NOT DISTINCT FROM OLD.last_success_at
            AND NEW.consecutive_failures = OLD.consecutive_failures
            AND NEW.state = OLD.state
            AND NEW.lease_owner IS NOT DISTINCT FROM OLD.lease_owner
            AND NEW.lease_expires_at IS NOT DISTINCT FROM OLD.lease_expires_at
            AND NEW.version = OLD.version THEN
        RETURN NEW;
    END IF;
    IF (NEW.tenant_id, NEW.object_name) IS DISTINCT FROM (OLD.tenant_id, OLD.object_name) THEN
        RAISE EXCEPTION 'schedule identity is immutable';
    END IF;
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'schedule version must advance exactly one';
    END IF;
    IF NEW.state = 'ACTIVE' AND (NEW.target_lag_ms IS NULL OR NEW.next_due_at IS NULL) THEN
        RAISE EXCEPTION 'active schedule requires a target lag and next due instant';
    END IF;
    IF NEW.state = 'DISABLED' AND NEW.target_lag_ms IS NOT NULL THEN
        RAISE EXCEPTION 'disabled schedule cannot carry a target lag';
    END IF;
    IF NEW.state = 'SUSPENDED' AND NEW.target_lag_ms IS NULL THEN
        RAISE EXCEPTION 'suspended schedule must retain its target lag';
    END IF;
    RETURN NEW;
END $$;

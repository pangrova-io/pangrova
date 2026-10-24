-- One publication authority for Dynamic Tables and Materialized Views.
--
-- The kernel owns logical definitions, immutable attempt/generation identity,
-- exact commit evidence, source pins, scheduling, reader pins, and the
-- transactional outbox. Physical generations live in the reserved catalog
-- schema __pangrova_materializations; this schema never stores row data.
--
-- This migration is intentionally one-shot: legacy Dynamic Table tables,
-- functions, and triggers are removed in the same transaction. There is no
-- dual-write window and no compatibility view.

CREATE SCHEMA IF NOT EXISTS materialization;

CREATE TABLE IF NOT EXISTS materialization.schema_version (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO materialization.schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;

-- ── logical headers ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.objects (
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    object_kind VARCHAR(32) NOT NULL
        CHECK (object_kind IN ('DYNAMIC_TABLE', 'MATERIALIZED_VIEW')),
    incarnation_id UUID NOT NULL,
    definition_revision BIGINT NOT NULL CHECK (definition_revision >= 1),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    published_generation_id UUID,
    state VARCHAR(32) NOT NULL
        CHECK (state IN ('CREATING', 'ACTIVE', 'SUSPENDED', 'FAILED', 'DROPPING', 'DROPPED')),
    definer_principal VARCHAR(512) NOT NULL CHECK (btrim(definer_principal) <> ''),
    security_mode VARCHAR(32) NOT NULL CHECK (security_mode IN ('DEFINER', 'INVOKER')),
    refresh_mode VARCHAR(32) NOT NULL CHECK (refresh_mode IN ('FULL', 'INCREMENTAL')),
    incremental_strategy VARCHAR(64),
    created_at TIMESTAMPTZ(6) NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, object_name),
    UNIQUE (incarnation_id),
    CHECK (updated_at >= created_at),
    CHECK (state <> 'ACTIVE' OR published_generation_id IS NOT NULL),
    CHECK (refresh_mode <> 'FULL' OR incremental_strategy IS NULL)
);
CREATE INDEX IF NOT EXISTS materialization_objects_kind_state
    ON materialization.objects (tenant_id, object_kind, state);

-- Monotonic registry epochs invalidate plan/result caches without a full scan.
CREATE TABLE IF NOT EXISTS materialization.registry_epochs (
    tenant_id VARCHAR(255) PRIMARY KEY,
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0)
);

-- ── immutable definitions ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.definition_revisions (
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    definition_sql TEXT NOT NULL CHECK (btrim(definition_sql) <> ''),
    canonical_sql TEXT NOT NULL CHECK (btrim(canonical_sql) <> ''),
    plan_fingerprint CHAR(64) NOT NULL CHECK (plan_fingerprint ~ '^[0-9a-f]{64}$'),
    resolved BOOLEAN NOT NULL,
    output_schema JSONB,
    output_schema_hash CHAR(64),
    sources JSONB NOT NULL,
    policy_snapshot JSONB NOT NULL,
    security_mode VARCHAR(32) NOT NULL CHECK (security_mode = 'DEFINER'),
    definer_principal VARCHAR(512) NOT NULL CHECK (btrim(definer_principal) <> ''),
    created_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, object_name, revision),
    FOREIGN KEY (tenant_id, object_name)
        REFERENCES materialization.objects (tenant_id, object_name),
    CHECK ((resolved AND output_schema IS NOT NULL AND output_schema_hash IS NOT NULL)
        OR (NOT resolved AND output_schema IS NULL AND output_schema_hash IS NULL)),
    CHECK (jsonb_typeof(sources) = 'array'),
    CHECK (jsonb_typeof(policy_snapshot) = 'object')
);
CREATE INDEX IF NOT EXISTS materialization_definitions_sources
    ON materialization.definition_revisions USING GIN (sources);

-- ── exact commit evidence ───────────────────────────────────────────────────
--
-- The write-commit journal is a redo log: the coordinator purges a COMMITTED
-- intent as soon as the query retires. Materialization evidence must outlive
-- that purge, so a committed intent that belongs to a reserved attempt is
-- captured atomically inside the journal's own transaction.

CREATE TABLE IF NOT EXISTS materialization.native_commit_capture (
    query_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(1024) NOT NULL,
    commit_fingerprint VARCHAR(64) NOT NULL CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'),
    principal VARCHAR(255),
    owner_id VARCHAR(255),
    owner_incarnation VARCHAR(255),
    execution_fencing_token BIGINT NOT NULL,
    published_version BIGINT NOT NULL CHECK (published_version > 0),
    manifest_sha256 BYTEA NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    committed_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION materialization.capture_native_commit()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
            WHERE a.execution_anchor = NEW.query_id) THEN
        RETURN NEW;
    END IF;
    IF NEW.published_version IS NULL OR NEW.manifest_sha256 IS NULL
            OR NEW.tenant_id IS NULL OR NEW.principal IS NULL THEN
        RAISE EXCEPTION 'materialization commit requires authenticated publication evidence';
    END IF;
    INSERT INTO materialization.native_commit_capture (query_id, tenant_id, catalog_name,
        table_name, commit_fingerprint, principal, owner_id, owner_incarnation,
        execution_fencing_token, published_version, manifest_sha256, committed_at)
    VALUES (NEW.query_id, NEW.tenant_id, NEW.catalog_name, NEW.table_name,
        NEW.commit_fingerprint, NEW.principal, NEW.owner_id, NEW.owner_incarnation,
        NEW.execution_fencing_token, NEW.published_version, NEW.manifest_sha256,
        clock_timestamp())
    ON CONFLICT (query_id) DO NOTHING;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_native_commit_capture ON write_commit_journal;
CREATE TRIGGER materialization_native_commit_capture
    AFTER INSERT OR UPDATE OF state, published_version, manifest_sha256
    ON write_commit_journal
    FOR EACH ROW WHEN (NEW.state = 'COMMITTED')
    EXECUTE FUNCTION materialization.capture_native_commit();

CREATE TABLE IF NOT EXISTS materialization.commit_evidence (
    attempt_id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    connector_id VARCHAR(64) NOT NULL CHECK (btrim(connector_id) <> ''),
    commit_fingerprint VARCHAR(255) NOT NULL CHECK (btrim(commit_fingerprint) <> ''),
    manifest_hash VARCHAR(255) NOT NULL CHECK (btrim(manifest_hash) <> ''),
    format_version VARCHAR(64) NOT NULL CHECK (btrim(format_version) <> ''),
    committed_at TIMESTAMPTZ(6) NOT NULL,
    recorded_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp()
);

-- ── generations ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.generations (
    generation_id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    incarnation_id UUID NOT NULL,
    definition_revision BIGINT NOT NULL CHECK (definition_revision >= 1),
    producing_attempt_id UUID NOT NULL UNIQUE,
    base_generation_id UUID,
    physical_catalog VARCHAR(255) NOT NULL,
    physical_schema VARCHAR(255) NOT NULL,
    physical_table VARCHAR(255) NOT NULL,
    refresh_kind VARCHAR(64) NOT NULL
        CHECK (refresh_kind IN ('FULL', 'INCREMENTAL_APPEND_ONLY', 'INCREMENTAL_PARTITION_REPLACE',
                'INCREMENTAL_AFFECTED_KEY', 'INCREMENTAL_CDC')),
    source_vector JSONB NOT NULL,
    source_vector_hash CHAR(64) NOT NULL CHECK (source_vector_hash ~ '^[0-9a-f]{64}$'),
    state VARCHAR(32) NOT NULL
        CHECK (state IN ('CANDIDATE', 'COMMITTED', 'PUBLISHED', 'RETIRED', 'QUARANTINED', 'COLLECTED')),
    created_at TIMESTAMPTZ(6) NOT NULL,
    published_at TIMESTAMPTZ(6),
    FOREIGN KEY (tenant_id, object_name)
        REFERENCES materialization.objects (tenant_id, object_name),
    FOREIGN KEY (base_generation_id)
        REFERENCES materialization.generations (generation_id),
    UNIQUE (physical_catalog, physical_schema, physical_table),
    CHECK (physical_schema = '__pangrova_materializations'),
    CHECK (physical_table ~ '^__pg_mat_[0-9a-f]{32}$'),
    CHECK (refresh_kind = 'FULL' OR base_generation_id IS NOT NULL),
    CHECK (refresh_kind <> 'FULL' OR base_generation_id IS NULL),
    CHECK (published_at IS NULL OR state IN ('PUBLISHED', 'RETIRED', 'COLLECTED'))
);
CREATE INDEX IF NOT EXISTS materialization_generations_object
    ON materialization.generations (tenant_id, object_name, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS materialization_generations_published
    ON materialization.generations (tenant_id, object_name, incarnation_id)
    WHERE state = 'PUBLISHED';
CREATE INDEX IF NOT EXISTS materialization_generations_retirable
    ON materialization.generations (state, published_at)
    WHERE state = 'RETIRED';

ALTER TABLE materialization.objects
    DROP CONSTRAINT IF EXISTS fk_materialization_published_generation;
ALTER TABLE materialization.objects
    ADD CONSTRAINT fk_materialization_published_generation
    FOREIGN KEY (published_generation_id)
    REFERENCES materialization.generations (generation_id)
    DEFERRABLE INITIALLY DEFERRED;

-- ── attempts ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.attempts (
    attempt_id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    incarnation_id UUID NOT NULL,
    definition_revision BIGINT NOT NULL CHECK (definition_revision >= 1),
    base_object_revision BIGINT NOT NULL CHECK (base_object_revision >= 0),
    base_generation_id UUID,
    refresh_kind VARCHAR(64) NOT NULL
        CHECK (refresh_kind IN ('FULL', 'INCREMENTAL_APPEND_ONLY', 'INCREMENTAL_PARTITION_REPLACE',
                'INCREMENTAL_AFFECTED_KEY', 'INCREMENTAL_CDC')),
    source_vector JSONB NOT NULL,
    source_vector_hash CHAR(64) NOT NULL CHECK (source_vector_hash ~ '^[0-9a-f]{64}$'),
    trigger_kind VARCHAR(32) NOT NULL
        CHECK (trigger_kind IN ('MANUAL', 'SCHEDULED', 'CREATION', 'REPAIR')),
    requesting_principal VARCHAR(512) NOT NULL CHECK (btrim(requesting_principal) <> ''),
    definer_principal VARCHAR(512) NOT NULL CHECK (btrim(definer_principal) <> ''),
    execution_anchor VARCHAR(128) NOT NULL,
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    lease_expires_at TIMESTAMPTZ(6) NOT NULL,
    candidate_catalog VARCHAR(255) NOT NULL,
    candidate_schema VARCHAR(255) NOT NULL,
    candidate_table VARCHAR(255) NOT NULL,
    state VARCHAR(32) NOT NULL
        CHECK (state IN ('PREPARING', 'BUILDING', 'COMMITTED', 'PUBLISHED',
                'FAILED', 'ABANDONED', 'QUARANTINED', 'SUPERSEDED')),
    error_code VARCHAR(128),
    created_at TIMESTAMPTZ(6) NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL,
    FOREIGN KEY (tenant_id, object_name)
        REFERENCES materialization.objects (tenant_id, object_name),
    FOREIGN KEY (base_generation_id)
        REFERENCES materialization.generations (generation_id),
    UNIQUE (candidate_catalog, candidate_schema, candidate_table),
    CHECK (candidate_schema = '__pangrova_materializations'),
    CHECK (candidate_table ~ '^__pg_mat_[0-9a-f]{32}$'),
    CHECK (updated_at >= created_at),
    CHECK (refresh_kind = 'FULL' OR base_generation_id IS NOT NULL),
    CHECK (refresh_kind <> 'FULL' OR base_generation_id IS NULL),
    CHECK (state <> 'FAILED' OR error_code IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS materialization_attempts_active
    ON materialization.attempts (tenant_id, object_name)
    WHERE state IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED');
CREATE INDEX IF NOT EXISTS materialization_attempts_unsettled
    ON materialization.attempts (state, updated_at)
    WHERE state IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED');

CREATE TABLE IF NOT EXISTS materialization.write_capabilities (
    attempt_id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    incarnation_id UUID NOT NULL,
    definition_revision BIGINT NOT NULL CHECK (definition_revision >= 1),
    generation_id UUID NOT NULL,
    candidate_catalog VARCHAR(255) NOT NULL,
    candidate_schema VARCHAR(255) NOT NULL,
    candidate_table VARCHAR(255) NOT NULL,
    source_vector_hash CHAR(64) NOT NULL CHECK (source_vector_hash ~ '^[0-9a-f]{64}$'),
    definer_principal VARCHAR(512) NOT NULL CHECK (btrim(definer_principal) <> ''),
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    issued_at TIMESTAMPTZ(6) NOT NULL,
    expires_at TIMESTAMPTZ(6) NOT NULL,
    FOREIGN KEY (attempt_id) REFERENCES materialization.attempts (attempt_id),
    FOREIGN KEY (generation_id) REFERENCES materialization.generations (generation_id),
    CHECK (expires_at > issued_at)
);

CREATE OR REPLACE FUNCTION materialization.validate_write_capability()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'materialization write capabilities are immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
            JOIN materialization.generations g ON g.generation_id = NEW.generation_id
            WHERE a.attempt_id = NEW.attempt_id
                AND a.state IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED')
                AND (a.tenant_id, a.object_name, a.incarnation_id, a.definition_revision,
                    a.candidate_catalog, a.candidate_schema, a.candidate_table,
                    a.source_vector_hash)
                    = (NEW.tenant_id, NEW.object_name, NEW.incarnation_id,
                    NEW.definition_revision, NEW.candidate_catalog, NEW.candidate_schema,
                    NEW.candidate_table, NEW.source_vector_hash)
                AND g.producing_attempt_id = NEW.attempt_id
                AND (g.physical_catalog, g.physical_schema, g.physical_table)
                    = (NEW.candidate_catalog, NEW.candidate_schema, NEW.candidate_table)) THEN
        RAISE EXCEPTION 'write capability must describe the exact reserved attempt and candidate';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_write_capability_identity
    ON materialization.write_capabilities;
CREATE TRIGGER materialization_write_capability_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.write_capabilities
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_write_capability();

CREATE TABLE IF NOT EXISTS materialization.source_pins (
    attempt_id UUID NOT NULL,
    connector_id VARCHAR(64) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    snapshot_token VARCHAR(512) NOT NULL,
    schema_fingerprint VARCHAR(255) NOT NULL,
    policy_fingerprint VARCHAR(255) NOT NULL,
    observed_at TIMESTAMPTZ(6) NOT NULL,
    expires_at TIMESTAMPTZ(6) NOT NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('ACQUIRED', 'RELEASED', 'EXPIRED')),
    PRIMARY KEY (attempt_id, connector_id, catalog_name, schema_name, table_name),
    FOREIGN KEY (attempt_id) REFERENCES materialization.attempts (attempt_id)
);
CREATE INDEX IF NOT EXISTS materialization_source_pins_expiry
    ON materialization.source_pins (state, expires_at);

CREATE TABLE IF NOT EXISTS materialization.dependencies (
    tenant_id VARCHAR(255) NOT NULL,
    downstream_object VARCHAR(512) NOT NULL,
    upstream_object VARCHAR(512) NOT NULL,
    downstream_definition_revision BIGINT NOT NULL CHECK (downstream_definition_revision >= 1),
    dependency_kind VARCHAR(32) NOT NULL
        CHECK (dependency_kind IN ('DIRECT', 'INDIRECT')),
    PRIMARY KEY (tenant_id, downstream_object, downstream_definition_revision, upstream_object),
    FOREIGN KEY (tenant_id, downstream_object)
        REFERENCES materialization.objects (tenant_id, object_name),
    FOREIGN KEY (tenant_id, upstream_object)
        REFERENCES materialization.objects (tenant_id, object_name),
    CHECK (downstream_object <> upstream_object)
);
CREATE INDEX IF NOT EXISTS materialization_dependencies_upstream
    ON materialization.dependencies (tenant_id, upstream_object);

-- ── schedules ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.schedules (
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    target_lag_ms BIGINT CHECK (target_lag_ms IS NULL OR target_lag_ms > 0),
    next_due_at TIMESTAMPTZ(6),
    last_success_at TIMESTAMPTZ(6),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    state VARCHAR(32) NOT NULL CHECK (state IN ('ACTIVE', 'SUSPENDED', 'DISABLED')),
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMPTZ(6),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, object_name),
    FOREIGN KEY (tenant_id, object_name)
        REFERENCES materialization.objects (tenant_id, object_name),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    CHECK (state <> 'ACTIVE' OR (target_lag_ms IS NOT NULL AND next_due_at IS NOT NULL)),
    CHECK (state <> 'DISABLED' OR target_lag_ms IS NULL),
    CHECK (state <> 'SUSPENDED' OR target_lag_ms IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS materialization_schedules_due
    ON materialization.schedules (next_due_at)
    WHERE state = 'ACTIVE';

-- ── reader pins, GC, commands, events ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS materialization.reader_pins (
    generation_id UUID NOT NULL,
    query_id VARCHAR(128) NOT NULL,
    owner VARCHAR(255) NOT NULL CHECK (btrim(owner) <> ''),
    expires_at TIMESTAMPTZ(6) NOT NULL,
    heartbeat_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (generation_id, query_id),
    FOREIGN KEY (generation_id) REFERENCES materialization.generations (generation_id),
    CHECK (expires_at >= heartbeat_at)
);
CREATE INDEX IF NOT EXISTS materialization_reader_pins_expiry
    ON materialization.reader_pins (expires_at);
CREATE INDEX IF NOT EXISTS materialization_reader_pins_query
    ON materialization.reader_pins (query_id);

CREATE TABLE IF NOT EXISTS materialization.gc_ledger (
    entry_id UUID PRIMARY KEY,
    generation_id UUID NOT NULL,
    decision VARCHAR(32) NOT NULL CHECK (decision IN ('RETAIN', 'COLLECT', 'DEFER')),
    evidence TEXT NOT NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('PENDING', 'APPLIED', 'FAILED')),
    created_at TIMESTAMPTZ(6) NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL,
    FOREIGN KEY (generation_id) REFERENCES materialization.generations (generation_id),
    CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS materialization.commands (
    tenant_id VARCHAR(255) NOT NULL,
    command_id VARCHAR(128) NOT NULL,
    result_reference TEXT NOT NULL CHECK (btrim(result_reference) <> ''),
    created_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, command_id)
);

CREATE TABLE IF NOT EXISTS materialization.events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    object_name VARCHAR(512) NOT NULL,
    command_id VARCHAR(128),
    attempt_id UUID,
    generation_id UUID,
    query_id VARCHAR(128),
    requesting_principal VARCHAR(512) NOT NULL,
    effective_principal VARCHAR(512) NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ(6) NOT NULL,
    published_at TIMESTAMPTZ(6)
);
CREATE INDEX IF NOT EXISTS materialization_events_outbox
    ON materialization.events (occurred_at)
    WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS materialization_events_object
    ON materialization.events (tenant_id, object_name, occurred_at);

-- ── canonical source-vector hash ────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.source_vector_hash(vector JSONB)
RETURNS CHAR(64) LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    canonical TEXT;
    entry JSONB;
    connector TEXT;
    source_name TEXT;
    entry_count INTEGER;
    distinct_count INTEGER;
BEGIN
    IF vector IS NULL OR jsonb_typeof(vector) <> 'object'
            OR jsonb_typeof(vector->'entries') <> 'array'
            OR (vector->>'canonicalVersion') IS NULL
            OR (vector->>'canonicalVersion') !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'source vector must carry canonicalVersion and an entries array';
    END IF;
    FOR entry IN SELECT value FROM jsonb_array_elements(vector->'entries') LOOP
        connector := entry->'source'->>'connectorId';
        source_name := connector || ':' || (entry->'source'->>'catalog') || '.'
            || (entry->'source'->>'schema') || '.' || (entry->'source'->>'table');
        IF connector IS NULL OR connector = '' OR (entry->'source'->>'catalog') IS NULL
                OR (entry->'source'->>'schema') IS NULL OR (entry->'source'->>'table') IS NULL
                OR (entry->'snapshot'->>'snapshotToken') IS NULL
                OR (entry->'snapshot'->>'schemaFingerprint') IS NULL
                OR (entry->'snapshot'->>'policyFingerprint') IS NULL THEN
            RAISE EXCEPTION 'source vector entry is incomplete';
        END IF;
        IF (entry->'snapshot'->>'connectorId') IS DISTINCT FROM connector
                OR COALESCE((entry->'snapshot'->>'pinnable')::BOOLEAN, FALSE) IS NOT TRUE THEN
            RAISE EXCEPTION 'source vector snapshot must match its connector and be pinnable';
        END IF;
    END LOOP;
    SELECT count(*), count(DISTINCT src.name) INTO entry_count, distinct_count
    FROM (
        SELECT (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
            || (e->'source'->>'schema') || '.' || (e->'source'->>'table') AS name
        FROM jsonb_array_elements(vector->'entries') e
    ) src;
    IF entry_count <> distinct_count THEN
        RAISE EXCEPTION 'source vector entries must be unique';
    END IF;
    canonical := 'vector:' || (vector->>'canonicalVersion');
    IF entry_count > 0 THEN
        SELECT canonical || '|' || string_agg(src.fragment, '|'
                ORDER BY src.name COLLATE "C")
        INTO canonical
        FROM (
            SELECT (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
                       || (e->'source'->>'schema') || '.' || (e->'source'->>'table') AS name,
                   (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
                       || (e->'source'->>'schema') || '.' || (e->'source'->>'table') || '@'
                       || (e->'snapshot'->>'snapshotToken') || ':'
                       || (e->'snapshot'->>'schemaFingerprint') || ':'
                       || (e->'snapshot'->>'policyFingerprint') AS fragment
            FROM jsonb_array_elements(vector->'entries') e
        ) src;
    END IF;
    RETURN encode(sha256(convert_to(canonical, 'UTF8')), 'hex');
END $$;

-- ── definition immutability ─────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.reject_definition_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'materialization definitions are immutable';
END $$;
DROP TRIGGER IF EXISTS materialization_definitions_immutable ON materialization.definition_revisions;
CREATE TRIGGER materialization_definitions_immutable
    BEFORE UPDATE OR DELETE ON materialization.definition_revisions
    FOR EACH ROW EXECUTE FUNCTION materialization.reject_definition_mutation();

-- ── object header CAS and lifecycle ─────────────────────────────────────────

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
    IF (NEW.tenant_id, NEW.object_name, NEW.object_kind, NEW.incarnation_id, NEW.created_at)
            IS DISTINCT FROM
       (OLD.tenant_id, OLD.object_name, OLD.object_kind, OLD.incarnation_id, OLD.created_at) THEN
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
DROP TRIGGER IF EXISTS materialization_object_header ON materialization.objects;
CREATE TRIGGER materialization_object_header
    BEFORE INSERT OR UPDATE ON materialization.objects
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_object_header();

-- ── attempt reservation and state machine ───────────────────────────────────

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
    IF (NEW.attempt_id, NEW.tenant_id, NEW.object_name, NEW.incarnation_id,
            NEW.definition_revision, NEW.base_object_revision, NEW.base_generation_id,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.trigger_kind,
            NEW.requesting_principal, NEW.definer_principal, NEW.execution_anchor,
            NEW.control_epoch, NEW.lease_expires_at, NEW.candidate_catalog,
            NEW.candidate_schema, NEW.candidate_table, NEW.created_at)
            IS DISTINCT FROM
       (OLD.attempt_id, OLD.tenant_id, OLD.object_name, OLD.incarnation_id,
            OLD.definition_revision, OLD.base_object_revision, OLD.base_generation_id,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.trigger_kind,
            OLD.requesting_principal, OLD.definer_principal, OLD.execution_anchor,
            OLD.control_epoch, OLD.lease_expires_at, OLD.candidate_catalog,
            OLD.candidate_schema, OLD.candidate_table, OLD.created_at) THEN
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
DROP TRIGGER IF EXISTS materialization_attempt_identity ON materialization.attempts;
CREATE TRIGGER materialization_attempt_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.attempts
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_attempt();

-- ── exact commit evidence ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_commit_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    attempt materialization.attempts%ROWTYPE;
    captured materialization.native_commit_capture%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'commit evidence is immutable';
    END IF;
    SELECT * INTO attempt FROM materialization.attempts a
        WHERE a.attempt_id = NEW.attempt_id FOR SHARE;
    IF NOT FOUND OR attempt.tenant_id <> NEW.tenant_id
            OR attempt.state NOT IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED') THEN
        RAISE EXCEPTION 'commit evidence must belong to an unsettled attempt';
    END IF;
    SELECT * INTO anchor FROM execution_anchor a
        WHERE a.execution_id = attempt.execution_anchor FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'commit evidence requires the attempt execution anchor';
    END IF;
    SELECT * INTO captured FROM materialization.native_commit_capture c
        WHERE c.query_id = attempt.execution_anchor FOR SHARE;
    IF NOT FOUND
            OR captured.tenant_id <> attempt.tenant_id
            OR captured.principal IS DISTINCT FROM attempt.requesting_principal
            OR captured.owner_id IS DISTINCT FROM anchor.owner_id
            OR captured.owner_incarnation IS DISTINCT FROM anchor.owner_incarnation
            OR captured.execution_fencing_token IS DISTINCT FROM anchor.fencing_token
            OR captured.commit_fingerprint IS DISTINCT FROM NEW.commit_fingerprint
            OR captured.catalog_name || '.' || captured.table_name
                <> attempt.candidate_catalog || '.' || attempt.candidate_schema || '.' || attempt.candidate_table
            OR encode(captured.manifest_sha256, 'hex') <> NEW.manifest_hash THEN
        RAISE EXCEPTION 'commit evidence must match the exact committed Native write intent';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_commit_evidence_identity ON materialization.commit_evidence;
CREATE TRIGGER materialization_commit_evidence_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.commit_evidence
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_commit_evidence();

-- ── generation identity, lifecycle, and publication ────────────────────────

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
    IF (NEW.generation_id, NEW.tenant_id, NEW.object_name, NEW.incarnation_id,
            NEW.definition_revision, NEW.producing_attempt_id, NEW.base_generation_id,
            NEW.physical_catalog, NEW.physical_schema, NEW.physical_table,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.created_at)
            IS DISTINCT FROM
       (OLD.generation_id, OLD.tenant_id, OLD.object_name, OLD.incarnation_id,
            OLD.definition_revision, OLD.producing_attempt_id, OLD.base_generation_id,
            OLD.physical_catalog, OLD.physical_schema, OLD.physical_table,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.created_at) THEN
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
DROP TRIGGER IF EXISTS materialization_generation_identity ON materialization.generations;
CREATE TRIGGER materialization_generation_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.generations
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_generation();

-- ── source pins ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_source_pin()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'ACQUIRED' THEN
            RAISE EXCEPTION 'source pins must be acquired before release';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
                WHERE a.attempt_id = NEW.attempt_id AND a.state IN ('PREPARING', 'BUILDING')) THEN
            RAISE EXCEPTION 'source pin must belong to an active attempt';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'source pins are retained as attempt history';
    END IF;
    IF (NEW.attempt_id, NEW.connector_id, NEW.catalog_name, NEW.schema_name, NEW.table_name,
            NEW.snapshot_token, NEW.schema_fingerprint, NEW.policy_fingerprint,
            NEW.observed_at, NEW.expires_at) IS DISTINCT FROM
       (OLD.attempt_id, OLD.connector_id, OLD.catalog_name, OLD.schema_name, OLD.table_name,
            OLD.snapshot_token, OLD.schema_fingerprint, OLD.policy_fingerprint,
            OLD.observed_at, OLD.expires_at) THEN
        RAISE EXCEPTION 'source pin identity is immutable';
    END IF;
    IF OLD.state <> 'ACQUIRED' OR NEW.state NOT IN ('RELEASED', 'EXPIRED') THEN
        RAISE EXCEPTION 'source pin can only move from ACQUIRED to RELEASED or EXPIRED';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_source_pin_identity ON materialization.source_pins;
CREATE TRIGGER materialization_source_pin_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.source_pins
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_source_pin();

-- ── dependency graph ────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.validate_dependency()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
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
    IF NOT EXISTS (SELECT 1 FROM materialization.objects o
            WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.upstream_object
                AND o.state <> 'DROPPED') THEN
        RAISE EXCEPTION 'dependency must name a live upstream materialization';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_dependency_identity ON materialization.dependencies;
CREATE TRIGGER materialization_dependency_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.dependencies
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_dependency();

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
DROP TRIGGER IF EXISTS materialization_schedule_identity ON materialization.schedules;
CREATE TRIGGER materialization_schedule_identity
    BEFORE INSERT OR UPDATE OR DELETE ON materialization.schedules
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_schedule();

-- ── commands, events, GC, reader pins ──────────────────────────────────────

CREATE OR REPLACE FUNCTION materialization.reject_append_only_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'materialization row is append-only';
END $$;
DROP TRIGGER IF EXISTS materialization_commands_immutable ON materialization.commands;
CREATE TRIGGER materialization_commands_immutable
    BEFORE UPDATE OR DELETE ON materialization.commands
    FOR EACH ROW EXECUTE FUNCTION materialization.reject_append_only_mutation();
DROP TRIGGER IF EXISTS materialization_gc_immutable ON materialization.gc_ledger;
CREATE TRIGGER materialization_gc_immutable
    BEFORE DELETE ON materialization.gc_ledger
    FOR EACH ROW EXECUTE FUNCTION materialization.reject_append_only_mutation();
DROP TRIGGER IF EXISTS materialization_schema_version_immutable ON materialization.schema_version;
CREATE TRIGGER materialization_schema_version_immutable
    BEFORE UPDATE OR DELETE ON materialization.schema_version
    FOR EACH ROW EXECUTE FUNCTION materialization.reject_append_only_mutation();

CREATE OR REPLACE FUNCTION materialization.validate_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization events are append-only';
    END IF;
    IF (NEW.event_id, NEW.event_type, NEW.tenant_id, NEW.object_name, NEW.command_id,
            NEW.attempt_id, NEW.generation_id, NEW.query_id, NEW.requesting_principal,
            NEW.effective_principal, NEW.detail, NEW.occurred_at) IS DISTINCT FROM
       (OLD.event_id, OLD.event_type, OLD.tenant_id, OLD.object_name, OLD.command_id,
            OLD.attempt_id, OLD.generation_id, OLD.query_id, OLD.requesting_principal,
            OLD.effective_principal, OLD.detail, OLD.occurred_at)
            OR OLD.published_at IS NOT NULL OR NEW.published_at IS NULL THEN
        RAISE EXCEPTION 'materialization events can only be marked delivered';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_event_identity ON materialization.events;
CREATE TRIGGER materialization_event_identity
    BEFORE UPDATE OR DELETE ON materialization.events
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_event_mutation();

CREATE OR REPLACE FUNCTION materialization.validate_reader_pin()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (NEW.generation_id, NEW.query_id, NEW.owner) IS DISTINCT FROM
           (OLD.generation_id, OLD.query_id, OLD.owner) THEN
            RAISE EXCEPTION 'reader pin identity is immutable';
        END IF;
        IF NEW.heartbeat_at < OLD.heartbeat_at OR NEW.expires_at < OLD.expires_at THEN
            RAISE EXCEPTION 'reader pin renewal must not move backwards';
        END IF;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS materialization_reader_pin_identity ON materialization.reader_pins;
CREATE TRIGGER materialization_reader_pin_identity
    BEFORE UPDATE ON materialization.reader_pins
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_reader_pin();

-- ── legacy backfill ─────────────────────────────────────────────────────────
--
-- Legacy physical generations live in the retired __pg_dt_ namespace and have
-- no exact evidence that the new kernel can verify. Importing them as
-- published heads would silently bless unverifiable data. The migration
-- therefore preserves every logical definition and suspends the object until a
-- full refresh creates a verifiable generation under __pg_mat_.

CREATE TEMPORARY TABLE migration_dynamic_table_heads ON COMMIT DROP AS
SELECT tenant_id, logical_name, incarnation_id, definition_sql, published_table,
       revision, lifecycle_state, updated_at
FROM dynamic_table_publications;

DO $$
BEGIN
    IF EXISTS (
            SELECT 1 FROM dynamic_table_state s
            WHERE NOT EXISTS (
                SELECT 1 FROM migration_dynamic_table_heads h
                WHERE h.logical_name = s.dt_name)) THEN
        RAISE EXCEPTION 'Dynamic Table state without a durable publication cannot be migrated';
    END IF;
END $$;

INSERT INTO materialization.objects
    (tenant_id, object_name, object_kind, incarnation_id, definition_revision, revision,
     published_generation_id, state, definer_principal, security_mode, refresh_mode,
     incremental_strategy, created_at, updated_at)
SELECT h.tenant_id, h.logical_name, 'DYNAMIC_TABLE', h.incarnation_id, 1, 0, NULL,
       CASE WHEN h.lifecycle_state = 'ACTIVE' THEN 'SUSPENDED' ELSE 'CREATING' END,
       'system:migration', 'DEFINER', 'FULL', NULL, h.updated_at, h.updated_at
FROM migration_dynamic_table_heads h
ON CONFLICT DO NOTHING;

INSERT INTO materialization.definition_revisions
    (tenant_id, object_name, revision, definition_sql, canonical_sql, plan_fingerprint,
     resolved, output_schema, output_schema_hash, sources, policy_snapshot,
     security_mode, definer_principal, created_at)
SELECT h.tenant_id, h.logical_name, 1, h.definition_sql, h.definition_sql,
       encode(sha256(convert_to(h.definition_sql, 'UTF8')), 'hex'),
       FALSE, NULL, NULL, '[]'::jsonb, '{}'::jsonb, 'DEFINER', 'system:migration', h.updated_at
FROM migration_dynamic_table_heads h
ON CONFLICT DO NOTHING;

INSERT INTO materialization.schedules
    (tenant_id, object_name, target_lag_ms, next_due_at, last_success_at,
     consecutive_failures, state, lease_owner, lease_expires_at, version)
SELECT h.tenant_id, h.logical_name, NULL, NULL, NULL, 0, 'DISABLED', NULL, NULL, 0
FROM migration_dynamic_table_heads h
ON CONFLICT DO NOTHING;

INSERT INTO materialization.events
    (event_id, event_type, tenant_id, object_name, command_id, attempt_id, generation_id,
     query_id, requesting_principal, effective_principal, detail, occurred_at)
SELECT gen_random_uuid(),
       'MIGRATED', h.tenant_id, h.logical_name, NULL, NULL, NULL, NULL,
       'system:migration', 'system:migration',
       'legacy_revision=' || h.revision || ';legacy_table=' || COALESCE(h.published_table, '')
           || ';rebuild_required=true', h.updated_at
FROM migration_dynamic_table_heads h
ON CONFLICT DO NOTHING;

DO $$
DECLARE
    mismatched INTEGER;
BEGIN
    SELECT count(*) INTO mismatched
    FROM dynamic_table_publications p
    LEFT JOIN materialization.objects o
        ON o.tenant_id = p.tenant_id AND o.object_name = p.logical_name
    LEFT JOIN materialization.definition_revisions d
        ON d.tenant_id = o.tenant_id AND d.object_name = o.object_name AND d.revision = 1
    WHERE o.object_name IS NULL
        OR o.incarnation_id <> p.incarnation_id
        OR o.object_kind <> 'DYNAMIC_TABLE'
        OR d.definition_sql IS DISTINCT FROM p.definition_sql;
    IF mismatched > 0 THEN
        RAISE EXCEPTION 'materialization backfill mismatch for % Dynamic Tables', mismatched;
    END IF;
    IF EXISTS (
            SELECT 1 FROM materialization.objects o
            JOIN migration_dynamic_table_heads h
                ON h.tenant_id = o.tenant_id AND h.logical_name = o.object_name
            WHERE o.state NOT IN ('CREATING', 'SUSPENDED')) THEN
        RAISE EXCEPTION 'migrated Dynamic Tables must require a verified rebuild';
    END IF;
END $$;

DROP TABLE dynamic_table_refresh_native_commits CASCADE;
DROP TABLE dynamic_table_refresh_attempts CASCADE;
DROP TABLE dynamic_table_publications CASCADE;
DROP TABLE dynamic_table_refresh_history CASCADE;
DROP TABLE dynamic_table_state CASCADE;

DROP FUNCTION IF EXISTS validate_dynamic_refresh_attempt() CASCADE;
DROP FUNCTION IF EXISTS validate_dynamic_refresh_native_commit() CASCADE;
DROP FUNCTION IF EXISTS capture_dynamic_refresh_native_commit() CASCADE;
DROP FUNCTION IF EXISTS validate_dynamic_table_publication() CASCADE;

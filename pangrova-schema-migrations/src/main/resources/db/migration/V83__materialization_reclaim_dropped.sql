-- Reclaim a dropped logical name for a fresh incarnation.
--
-- DROP keeps a tombstone row so history stays auditable. Creating the same
-- name again must start a new incarnation instead of rewriting the tombstone,
-- which the header and definition triggers reject unconditionally. The
-- repository opens exactly that one operation behind a session marker, the
-- same pattern V71 uses for renames.

CREATE OR REPLACE FUNCTION materialization.reject_definition_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE'
            AND current_setting('pangrova.materialization_reclaim', true) = 'on' THEN
        RETURN OLD;
    END IF;
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
        IF current_setting('pangrova.materialization_reclaim', true) = 'on' THEN
            RETURN OLD;
        END IF;
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
    IF current_setting('pangrova.materialization_reclaim', true) = 'on'
            AND OLD.state = 'DROPPED'
            AND NEW.state = 'CREATING'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.object_name = OLD.object_name
            AND NEW.revision = 0
            AND NEW.definition_revision = 1
            AND NEW.published_generation_id IS NULL
            AND NEW.incarnation_id IS DISTINCT FROM OLD.incarnation_id THEN
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

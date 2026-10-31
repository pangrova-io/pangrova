-- Fail-closed guards for the durable materialization dependency graph.
--
-- A live definition must never reference a dropped upstream, and dropping an
-- upstream must never leave live dependents behind. Both directions are
-- enforced in the database so a race between DROP and CREATE/ALTER cannot
-- bypass the use-case checks.

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
    -- Lock the upstream row so a concurrent DROP either waits for this edge or
    -- is rejected by the drop guard after the edge commits.
    PERFORM 1 FROM materialization.objects o
        WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.upstream_object
            AND o.state NOT IN ('DROPPING', 'DROPPED')
        FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dependency must name a live upstream materialization';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION materialization.validate_materialization_drop()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('DROPPING', 'DROPPED') AND NEW.state IS DISTINCT FROM OLD.state THEN
        IF EXISTS (
            SELECT 1 FROM materialization.dependencies d
            JOIN materialization.objects o
                ON o.tenant_id = d.tenant_id AND o.object_name = d.downstream_object
            WHERE d.tenant_id = NEW.tenant_id AND d.upstream_object = NEW.object_name
                AND d.downstream_definition_revision = o.definition_revision
                AND o.state NOT IN ('DROPPING', 'DROPPED')
        ) THEN
            RAISE EXCEPTION 'materialization % still has live dependents', NEW.object_name;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS materialization_object_drop_guard ON materialization.objects;
CREATE TRIGGER materialization_object_drop_guard
    BEFORE UPDATE ON materialization.objects
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_materialization_drop();

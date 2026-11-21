-- A materialization command receipt is written when the command is admitted and
-- upgraded once, in the publication transaction, to the terminal publication
-- receipt. Identity and admission time stay immutable; an existing publication
-- receipt is final.

CREATE OR REPLACE FUNCTION materialization.is_publication_reference(reference TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF reference IS NULL OR btrim(reference) NOT LIKE '{%' THEN
        RETURN FALSE;
    END IF;
    RETURN (reference::jsonb) ? 'generation';
EXCEPTION WHEN others THEN
    RETURN FALSE;
END $$;

CREATE OR REPLACE FUNCTION materialization.validate_command_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization commands are append-only';
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
            OR NEW.command_id IS DISTINCT FROM OLD.command_id
            OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'materialization command identity is immutable';
    END IF;
    IF materialization.is_publication_reference(OLD.result_reference) THEN
        RAISE EXCEPTION 'a publication receipt is terminal';
    END IF;
    IF NOT materialization.is_publication_reference(NEW.result_reference) THEN
        RAISE EXCEPTION 'only a publication receipt may replace an admission receipt';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS materialization_commands_immutable ON materialization.commands;
CREATE TRIGGER materialization_commands_immutable
    BEFORE UPDATE OR DELETE ON materialization.commands
    FOR EACH ROW EXECUTE FUNCTION materialization.validate_command_mutation();

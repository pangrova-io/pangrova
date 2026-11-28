-- Existing intents may have reached an external publisher without a start marker.
ALTER TABLE write_commit_journal ADD COLUMN publication_started BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE write_commit_journal ALTER COLUMN publication_started SET DEFAULT FALSE;

CREATE FUNCTION prevent_write_publication_start_reset()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.publication_started AND NOT NEW.publication_started THEN
        RAISE EXCEPTION 'Write publication start cannot be reversed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER write_publication_start_is_irreversible
BEFORE UPDATE OF publication_started ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION prevent_write_publication_start_reset();

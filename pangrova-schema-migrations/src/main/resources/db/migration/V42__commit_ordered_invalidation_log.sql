-- The migration chain is the only owner of the invalidation schema.
ALTER TABLE invalidation_log ADD COLUMN updated_at TIMESTAMP;
UPDATE invalidation_log SET updated_at = created_at;
ALTER TABLE invalidation_log ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE invalidation_log ALTER COLUMN updated_at SET NOT NULL;

-- Identity allocation alone is not commit ordering. Take this transaction lock
-- BEFORE row defaults (including identity allocation) are evaluated. All writers,
-- including direct SQL, then publish committed ids in increasing order. Rolled
-- back transactions may leave harmless sequence gaps. Schema-qualified keys keep
-- independent metadata installations from blocking one another.
CREATE FUNCTION fence_invalidation_append() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        current_database() || ':' || TG_TABLE_SCHEMA || ':' || TG_TABLE_NAME, 0));
    RETURN NULL;
END;
$$;

CREATE TRIGGER invalidation_append_commit_gate
BEFORE INSERT ON invalidation_log
FOR EACH STATEMENT EXECUTE FUNCTION fence_invalidation_append();

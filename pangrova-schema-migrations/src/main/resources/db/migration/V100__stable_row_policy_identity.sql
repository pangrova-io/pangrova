-- Existing name-only policies have no provable stable owner. Preserve them as unbound;
-- controlled policy reads fail closed until an operator explicitly binds their table_id.
DO $$
DECLARE old_constraint TEXT;
BEGIN
    IF to_regclass('row_policy') IS NOT NULL THEN
        ALTER TABLE row_policy ADD COLUMN IF NOT EXISTS table_id UUID;
        FOR old_constraint IN
            SELECT conname FROM pg_constraint
            WHERE conrelid='row_policy'::regclass AND contype='u'
                AND ARRAY(SELECT attname FROM pg_attribute
                    WHERE attrelid=conrelid AND attnum=ANY(conkey) ORDER BY attname)
                    = ARRAY['catalog_name','policy_name','schema_name','table_name']::name[]
        LOOP
            EXECUTE format('ALTER TABLE row_policy DROP CONSTRAINT %I', old_constraint);
        END LOOP;
        CREATE UNIQUE INDEX IF NOT EXISTS row_policy_scoped_identity ON row_policy (table_id,policy_name)
            WHERE table_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS row_policy_unbound_name
            ON row_policy (catalog_name,schema_name,table_name,policy_name) WHERE table_id IS NULL;
    END IF;
END;
$$;

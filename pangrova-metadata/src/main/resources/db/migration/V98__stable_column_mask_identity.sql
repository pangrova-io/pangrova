-- Security-only fresh schemas create the same index in security-policy-schema.sql.
-- Metadata bootstrap upgrades an existing policy store before secured traffic starts.
DO $$
DECLARE old_constraint TEXT;
BEGIN
    IF to_regclass('column_mask') IS NOT NULL THEN
        ALTER TABLE column_mask ADD COLUMN IF NOT EXISTS table_id UUID;
        FOR old_constraint IN
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'column_mask'::regclass AND contype = 'u' AND array_length(conkey, 1) = 5
        LOOP
            EXECUTE format('ALTER TABLE column_mask DROP CONSTRAINT %I', old_constraint);
        END LOOP;
        CREATE UNIQUE INDEX IF NOT EXISTS column_mask_scoped_identity
            ON column_mask (catalog_name, schema_name, table_name, column_name, mask_name, table_id)
            NULLS NOT DISTINCT;

        -- Only a matching stable tag binding proves ownership of an old derived mask.
        UPDATE column_mask mask SET table_id = control.table_id
        FROM table_control_registry control, table_control_governance_binding binding
        WHERE binding.table_id = control.table_id AND control.state <> 'DROPPED'
          AND control.catalog_name = mask.catalog_name AND control.schema_name = mask.schema_name
          AND control.table_name = mask.table_name AND binding.binding_type = 'COLUMN_TAG'
          AND binding.config ->> 'column' = mask.column_name
          AND binding.config ->> 'tagKey' = 'pii_type'
          AND mask.mask_name = 'auto_pii_' || mask.column_name
          AND mask.mask_expression = CASE binding.config ->> 'value'
              WHEN 'PII_EMAIL' THEN 'mask_email' WHEN 'PII_PHONE' THEN 'mask_phone'
              WHEN 'PII_SSN' THEN 'partial' WHEN 'PII_NAME' THEN 'mask_name' END
          AND mask.table_id IS NULL;
    END IF;
END;
$$;

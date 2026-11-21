-- Tags now live only in the stable-identity governance authority. A historical
-- name-keyed value may migrate only when the old binding proves its ownership.
ALTER TABLE table_control_governance_binding ALTER COLUMN binding_ref TYPE VARCHAR(1024);

CREATE TABLE IF NOT EXISTS column_tags (
    store_key VARCHAR(1024) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (store_key, tag_key)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM column_tags tag
        WHERE cardinality(string_to_array(tag.store_key, '|')) <> 4
           OR NOT EXISTS (
               SELECT 1 FROM table_control_registry control
               JOIN table_control_governance_binding binding USING (table_id)
               WHERE control.catalog_name = split_part(tag.store_key, '|', 1)
                 AND control.schema_name = split_part(tag.store_key, '|', 2)
                 AND control.table_name = split_part(tag.store_key, '|', 3)
                 AND control.state <> 'DROPPED'
                 AND binding.binding_type = 'COLUMN_TAG'
                 AND binding.binding_ref = split_part(tag.store_key, '|', 4) || ':' || tag.tag_key
                 AND binding.config ->> 'value' = tag.tag_value
           )
    ) THEN
        RAISE EXCEPTION 'Column tag ownership is unproven; reconcile the historical tags and stable bindings before migration';
    END IF;
    IF EXISTS (
        SELECT binding.binding_id
        FROM table_control_governance_binding binding
        JOIN table_control_registry control USING (table_id)
        LEFT JOIN column_tags tag ON control.catalog_name = split_part(tag.store_key, '|', 1)
          AND control.schema_name = split_part(tag.store_key, '|', 2)
          AND control.table_name = split_part(tag.store_key, '|', 3)
          AND binding.binding_ref = split_part(tag.store_key, '|', 4) || ':' || tag.tag_key
          AND binding.config ->> 'value' = tag.tag_value
        WHERE control.state <> 'DROPPED' AND binding.binding_type = 'COLUMN_TAG'
        GROUP BY binding.binding_id HAVING count(tag.store_key) <> 1
    ) THEN
        RAISE EXCEPTION 'Column tag ownership is unproven or ambiguous; reconcile each historical stable binding before migration';
    END IF;

END;
$$;

DO $backfill$
BEGIN
    IF EXISTS (SELECT 1 FROM column_tags) THEN
        UPDATE table_control_governance_binding binding
        SET binding_ref = char_length(split_part(tag.store_key, '|', 4))::text || ':'
                || split_part(tag.store_key, '|', 4) || tag.tag_key,
            config = binding.config || jsonb_build_object(
                'column', split_part(tag.store_key, '|', 4), 'tagKey', tag.tag_key)
        FROM column_tags tag, table_control_registry control
        WHERE binding.table_id = control.table_id
          AND control.catalog_name = split_part(tag.store_key, '|', 1)
          AND control.schema_name = split_part(tag.store_key, '|', 2)
          AND control.table_name = split_part(tag.store_key, '|', 3)
          AND control.state <> 'DROPPED'
          AND binding.binding_type = 'COLUMN_TAG'
          AND binding.binding_ref = split_part(tag.store_key, '|', 4) || ':' || tag.tag_key
          AND binding.config ->> 'value' = tag.tag_value;
    END IF;
END
$backfill$;

DROP TABLE column_tags;

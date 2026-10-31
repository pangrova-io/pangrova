-- Both names remain protected while physical RENAME has an uncertain outcome.
-- Historical pending renames lack a provable source; require operator reconciliation before upgrade.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM table_control_registry WHERE state='RENAMING'
            AND NOT (properties ? 'control.rename.operation')) THEN
        RAISE EXCEPTION 'Pending legacy rename requires reconciliation before name reservation migration';
    END IF;
END $$;

CREATE TABLE table_control_name_reservation (
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_id UUID NOT NULL REFERENCES table_control_registry(table_id),
    PRIMARY KEY (catalog_name, schema_name, table_name)
);
CREATE INDEX idx_table_control_name_reservation_id ON table_control_name_reservation(table_id);
INSERT INTO table_control_name_reservation
SELECT catalog_name,schema_name,table_name,table_id FROM table_control_registry WHERE state<>'DROPPED';
INSERT INTO table_control_name_reservation
SELECT properties->>'control.rename.catalog',properties->>'control.rename.schema',
       properties->>'control.rename.table',table_id
FROM table_control_registry WHERE state='RENAMING'
AND (catalog_name,schema_name,table_name) IS DISTINCT FROM
    (properties->>'control.rename.catalog',properties->>'control.rename.schema',properties->>'control.rename.table');

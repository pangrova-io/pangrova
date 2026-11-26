-- One durable owner for each physical logical table name. Do not retain a SQL
-- transaction across connector operations. Existing V122 pending operations
-- have no reliable outcome receipt and are conservatively UNKNOWN.
ALTER TABLE index_schema_evolution_outbox
    DROP CONSTRAINT chk_index_schema_evolution_state,
    ADD CONSTRAINT chk_index_schema_evolution_state
        CHECK (state IN ('INTENDED','COMMITTED','APPLIED','ABORTED','UNKNOWN')),
    ADD COLUMN source_identity_format TEXT,
    ADD COLUMN source_identity_uuid TEXT,
    ADD COLUMN source_identity_location TEXT,
    ADD COLUMN source_identity_schema TEXT,
    ADD COLUMN target_identity_format TEXT,
    ADD COLUMN target_identity_uuid TEXT,
    ADD COLUMN target_identity_location TEXT,
    ADD COLUMN target_identity_schema TEXT;

CREATE TABLE index_schema_scope_locks (
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (catalog_name,schema_name,table_name)
);

CREATE TABLE index_schema_operation_claims (
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    operation_id UUID NOT NULL REFERENCES index_schema_evolution_outbox(operation_id),
    PRIMARY KEY (catalog_name,schema_name,table_name,operation_id),
    FOREIGN KEY (catalog_name,schema_name,table_name)
        REFERENCES index_schema_scope_locks(catalog_name,schema_name,table_name)
);
CREATE INDEX idx_index_schema_operation_claims_operation ON index_schema_operation_claims(operation_id);
CREATE INDEX idx_index_schema_evolution_retry ON index_schema_evolution_outbox(state,updated_at,operation_id);

CREATE TABLE index_schema_operation_targets (
    operation_id UUID NOT NULL REFERENCES index_schema_evolution_outbox(operation_id),
    index_id VARCHAR(255) NOT NULL,
    fenced_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (operation_id,index_id)
);

UPDATE index_schema_evolution_outbox SET state='UNKNOWN',updated_at=clock_timestamp()
WHERE state IN ('INTENDED','COMMITTED');

-- Keep every pre-existing uncertain claim, including overlapping operations.
WITH scopes AS (
    SELECT operation_id,catalog_name,schema_name,
        CASE WHEN event_type='RENAME_TABLE' THEN old_table_name ELSE table_name END AS table_name
    FROM index_schema_evolution_outbox WHERE state='UNKNOWN'
    UNION
    SELECT operation_id,catalog_name,COALESCE(new_schema_name,schema_name),table_name
    FROM index_schema_evolution_outbox WHERE state='UNKNOWN' AND event_type='RENAME_TABLE'
)
INSERT INTO index_schema_scope_locks(catalog_name,schema_name,table_name)
SELECT DISTINCT catalog_name,schema_name,table_name FROM scopes
ON CONFLICT DO NOTHING;

WITH scopes AS (
    SELECT operation_id,catalog_name,schema_name,
        CASE WHEN event_type='RENAME_TABLE' THEN old_table_name ELSE table_name END AS table_name
    FROM index_schema_evolution_outbox WHERE state='UNKNOWN'
    UNION
    SELECT operation_id,catalog_name,COALESCE(new_schema_name,schema_name),table_name
    FROM index_schema_evolution_outbox WHERE state='UNKNOWN' AND event_type='RENAME_TABLE'
)
INSERT INTO index_schema_operation_claims(catalog_name,schema_name,table_name,operation_id)
SELECT catalog_name,schema_name,table_name,operation_id FROM scopes;

UPDATE index_descriptors d SET state='STALE',stale_reason='schema_operation_outcome_unknown',updated_at=clock_timestamp()
WHERE d.state<>'DROPPED' AND EXISTS (
    SELECT 1 FROM index_schema_operation_claims c
    WHERE c.catalog_name=d.catalog_name AND c.schema_name=d.schema_name AND c.table_name=d.table_name
);

UPDATE index_generation_builds b SET state='FAILED',failure_reason='schema_operation_outcome_unknown',
    updated_at=clock_timestamp()
WHERE b.state='BUILDING' AND EXISTS (
    SELECT 1 FROM index_descriptors d JOIN index_schema_operation_claims c
      ON c.catalog_name=d.catalog_name AND c.schema_name=d.schema_name AND c.table_name=d.table_name
    WHERE d.index_id=b.index_id
);

-- Shared lock ordering: table scope row first, descriptor row second. All
-- index admissions/publications participate in this transaction-local lock.
CREATE FUNCTION pangrova_lock_index_schema_scope(catalog text, schema_name_arg text, table_name_arg text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO index_schema_scope_locks(catalog_name,schema_name,table_name)
        VALUES(catalog,schema_name_arg,table_name_arg) ON CONFLICT DO NOTHING;
    PERFORM 1 FROM index_schema_scope_locks l
        WHERE l.catalog_name=catalog AND l.schema_name=schema_name_arg AND l.table_name=table_name_arg
        FOR UPDATE;
END;
$$;

CREATE FUNCTION pangrova_assert_index_schema_scope_available(catalog text, schema_name_arg text, table_name_arg text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE owner_id uuid;
BEGIN
    PERFORM pangrova_lock_index_schema_scope(catalog,schema_name_arg,table_name_arg);
    SELECT c.operation_id INTO owner_id FROM index_schema_operation_claims c
        WHERE c.catalog_name=catalog AND c.schema_name=schema_name_arg AND c.table_name=table_name_arg
        ORDER BY c.operation_id LIMIT 1;
    IF owner_id IS NOT NULL THEN
        RAISE EXCEPTION 'Index schema operation % is unresolved for %.%.%', owner_id,catalog,schema_name_arg,table_name_arg
            USING ERRCODE='55000';
    END IF;
END;
$$;

CREATE FUNCTION pangrova_assert_index_schema_descriptor_available(index_id_arg text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE original_catalog text; original_schema text; original_table text;
BEGIN
    SELECT d.catalog_name,d.schema_name,d.table_name INTO original_catalog,original_schema,original_table
        FROM index_descriptors d WHERE d.index_id=index_id_arg;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM pangrova_assert_index_schema_scope_available(original_catalog,original_schema,original_table);
    PERFORM 1 FROM index_descriptors d WHERE d.index_id=index_id_arg
        AND d.catalog_name=original_catalog AND d.schema_name=original_schema AND d.table_name=original_table FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Index scope changed during schema lock acquisition: %',index_id_arg USING ERRCODE='40001';
    END IF;
END;
$$;

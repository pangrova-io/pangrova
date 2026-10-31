-- Index descriptors are tenant-owned catalog objects. Persist that identity at
-- the catalog boundary and make table/name discovery tenant scoped.
ALTER TABLE index_descriptors
    ADD COLUMN tenant_id VARCHAR(255) NOT NULL DEFAULT 'default';

ALTER TABLE index_descriptors
    ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE index_descriptors
    DROP CONSTRAINT IF EXISTS uq_index_name_per_table;

ALTER TABLE index_descriptors
    ADD CONSTRAINT uq_index_name_per_tenant_table
        UNIQUE (tenant_id, catalog_name, schema_name, table_name, index_name);

DROP INDEX IF EXISTS idx_index_descriptors_table_state;

CREATE INDEX idx_index_descriptors_tenant_table_state
    ON index_descriptors
        (tenant_id, catalog_name, schema_name, table_name, state);

-- Lease and descriptor timestamps are absolute instants. Older migrations used
-- timestamp-without-time-zone, whose interpretation depended on the JDBC session.
ALTER TABLE distributed_lease
    ALTER COLUMN acquired_at TYPE TIMESTAMPTZ(6)
        USING acquired_at AT TIME ZONE 'UTC',
    ALTER COLUMN expires_at TYPE TIMESTAMPTZ(6)
        USING expires_at AT TIME ZONE 'UTC';

ALTER TABLE index_maintenance_lease
    ALTER COLUMN acquired_at TYPE TIMESTAMPTZ(6)
        USING acquired_at AT TIME ZONE 'UTC',
    ALTER COLUMN expires_at TYPE TIMESTAMPTZ(6)
        USING expires_at AT TIME ZONE 'UTC';

ALTER TABLE index_descriptors
    ALTER COLUMN created_at TYPE TIMESTAMPTZ(6)
        USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ(6)
        USING updated_at AT TIME ZONE 'UTC';

ALTER TABLE index_commit_log
    ALTER COLUMN created_at TYPE TIMESTAMPTZ(6)
        USING created_at AT TIME ZONE 'UTC';

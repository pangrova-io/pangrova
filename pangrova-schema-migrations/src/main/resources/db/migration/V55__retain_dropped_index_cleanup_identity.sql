-- A dropped index remains addressable by its immutable ID until GC has released
-- every external snapshot reference and deleted its index files. Reusing its
-- visible name must not erase that cleanup identity.
ALTER TABLE index_descriptors
    DROP CONSTRAINT IF EXISTS uq_index_name_per_tenant_table;

CREATE UNIQUE INDEX uq_active_index_name_per_tenant_table
    ON index_descriptors (tenant_id, catalog_name, schema_name, table_name, index_name)
    WHERE state <> 'DROPPED';

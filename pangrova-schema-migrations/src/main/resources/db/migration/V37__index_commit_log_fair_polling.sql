-- Keep each table's unprocessed events in commit order while allowing the
-- maintenance leader to poll other tables past one table's failed head event.
CREATE INDEX idx_icl_unprocessed_table_order
    ON index_commit_log (
        tenant_id, catalog_name, schema_name, table_name, created_at, event_id)
    WHERE processed = FALSE;

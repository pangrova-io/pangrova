ALTER TABLE shuffle_query_scope
    ADD COLUMN cleanup_lease_owner VARCHAR(128),
    ADD COLUMN cleanup_lease_expires_at_ms BIGINT;

ALTER TABLE shuffle_query_scope
    ADD CONSTRAINT shuffle_query_scope_cleanup_lease_check CHECK (
        (cleanup_lease_owner IS NULL AND cleanup_lease_expires_at_ms IS NULL)
        OR (cleanup_lease_owner IS NOT NULL AND cleanup_lease_expires_at_ms > 0)
    );

CREATE INDEX shuffle_query_scope_cleanup_scan_idx
    ON shuffle_query_scope (state, cleanup_expires_at_ms, cleanup_lease_expires_at_ms);

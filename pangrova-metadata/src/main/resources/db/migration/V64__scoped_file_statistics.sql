-- Keep pre-existing unscoped rows for explicit reconciliation. They are not
-- readable by the tenant/snapshot-bound statistics API; never invent an owner.
ALTER TABLE file_column_statistics ADD COLUMN tenant_id VARCHAR(255);

ALTER TABLE file_column_statistics ADD CONSTRAINT chk_file_statistics_tenant CHECK (
    tenant_id IS NULL OR (btrim(tenant_id) <> '' AND snapshot_id IS NOT NULL AND btrim(snapshot_id) <> ''));

CREATE INDEX idx_file_statistics_tenant_snapshot
    ON file_column_statistics (tenant_id, snapshot_id) WHERE tenant_id IS NOT NULL;

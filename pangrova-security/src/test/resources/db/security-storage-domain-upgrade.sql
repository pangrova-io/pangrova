-- Managed-root upgrade over the legacy shape: backfills existing rows in place,
-- then locks the invariant. New namespaces afterwards receive an independent
-- root outside the legacy volumes/ and stages/ trees.
ALTER TABLE file_space_registry ADD COLUMN managed_root TEXT;

UPDATE file_space_registry SET managed_root = CASE WHEN is_external THEN '' ELSE
    lower(space_type) || 's/' || catalog_name || '/' || schema_name || '/' || space_name END
WHERE managed_root IS NULL;

ALTER TABLE file_space_registry ALTER COLUMN managed_root SET NOT NULL;
ALTER TABLE file_space_registry ADD CONSTRAINT chk_file_space_managed_root
    CHECK ((is_external AND managed_root = '') OR
           (NOT is_external AND length(managed_root) > 0));

CREATE UNIQUE INDEX ux_file_space_managed_root ON file_space_registry (managed_root)
    WHERE NOT is_external;

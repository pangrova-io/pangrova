-- Preserve existing bytes in place. New namespaces receive an independent root
-- outside the legacy volumes/ and stages/ trees, so neither rename nor name reuse
-- redirects a managed namespace to another namespace's files.
ALTER TABLE file_space_registry ADD COLUMN managed_root TEXT;

-- A statement-level authority gate advances the revision even for a zero-row
-- UPDATE. An empty registry needs no backfill and must not revoke unrelated
-- frozen COPY admissions during an upgrade.
DO $backfill$
BEGIN
    IF EXISTS (SELECT 1 FROM file_space_registry) THEN
        UPDATE file_space_registry SET managed_root = CASE WHEN is_external THEN '' ELSE
            lower(space_type) || 's/' || catalog_name || '/' || schema_name || '/' || space_name END;
    END IF;
END
$backfill$;

ALTER TABLE file_space_registry ALTER COLUMN managed_root SET NOT NULL;
ALTER TABLE file_space_registry ADD CONSTRAINT chk_file_space_managed_root
    CHECK ((is_external AND managed_root = '') OR
           (NOT is_external AND length(managed_root) > 0));

CREATE UNIQUE INDEX ux_file_space_managed_root ON file_space_registry (managed_root)
    WHERE NOT is_external;

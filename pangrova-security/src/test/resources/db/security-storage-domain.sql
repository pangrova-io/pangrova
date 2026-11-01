-- Clean-cut baseline for the security-owned storage domains. A fresh isolated
-- schema receives the tables at their current shape; no legacy migration chain.
-- Authority gates, the storage-profile grant guard and the file-space grant
-- binding are installed afterwards by AuthorizationTransactionGate.initialize.

-- Tenant registry and membership as used by JdbcTenantStore.
CREATE TABLE tenants (
    tenant_id                  VARCHAR(255) NOT NULL PRIMARY KEY,
    name                       VARCHAR(512) NOT NULL,
    status                     VARCHAR(32)  NOT NULL,
    max_memory_bytes           BIGINT       NOT NULL DEFAULT 0,
    max_concurrent_queries     INT          NOT NULL DEFAULT 0,
    max_queued_queries         INT          NOT NULL DEFAULT 0,
    max_cpu_time_per_query_ms  BIGINT       NOT NULL DEFAULT 0,
    max_scan_bytes_per_query   BIGINT       NOT NULL DEFAULT 0,
    max_daily_query_count      BIGINT       NOT NULL DEFAULT 0,
    max_daily_scan_bytes       BIGINT       NOT NULL DEFAULT 0,
    max_storage_bytes          BIGINT       NOT NULL DEFAULT 0,
    allowed_catalogs           TEXT,
    allowed_schemas            TEXT,
    resource_pool              VARCHAR(255),
    properties                 TEXT,
    created_at                 TIMESTAMP    NOT NULL,
    updated_at                 TIMESTAMP    NOT NULL,
    created_by                 VARCHAR(255)
);

CREATE TABLE tenant_user_memberships (
    user_principal VARCHAR(255) NOT NULL PRIMARY KEY,
    tenant_id      VARCHAR(255) NOT NULL,
    role           VARCHAR(32)  NOT NULL DEFAULT 'MEMBER'
);

-- File space registry (Volume + Stage surfaces) with the managed-root
-- identity folded into the baseline shape.
CREATE TABLE file_space_registry (
    file_space_id     UUID          PRIMARY KEY,
    space_type        VARCHAR(16)   NOT NULL,
    catalog_name      VARCHAR(255)  NOT NULL,
    schema_name       VARCHAR(255)  NOT NULL,
    space_name        VARCHAR(255)  NOT NULL,
    is_external       BOOLEAN       NOT NULL,
    storage_profile   VARCHAR(255)  NOT NULL DEFAULT '',
    location          VARCHAR(2048) NOT NULL DEFAULT '',
    directory_enabled BOOLEAN       NOT NULL DEFAULT FALSE,
    comment           TEXT          NOT NULL DEFAULT '',
    owner_name        VARCHAR(255)  NOT NULL,
    created_at        TIMESTAMPTZ   NOT NULL,
    updated_at        TIMESTAMPTZ   NOT NULL,
    managed_root      TEXT          NOT NULL,
    CONSTRAINT chk_file_space_registry_type
        CHECK (space_type IN ('VOLUME', 'STAGE')),
    CONSTRAINT chk_file_space_registry_name
        CHECK (length(catalog_name) > 0 AND length(schema_name) > 0 AND length(space_name) > 0),
    CONSTRAINT chk_file_space_registry_location
        CHECK ((is_external AND length(location) > 0) OR (NOT is_external AND length(location) = 0)),
    CONSTRAINT chk_file_space_registry_directory
        CHECK (space_type = 'STAGE' OR NOT directory_enabled),
    CONSTRAINT chk_file_space_managed_root
        CHECK ((is_external AND managed_root = '') OR (NOT is_external AND length(managed_root) > 0))
);

CREATE UNIQUE INDEX ux_file_space_registry_name
    ON file_space_registry (space_type, catalog_name, schema_name, space_name);

CREATE INDEX ix_file_space_registry_schema
    ON file_space_registry (space_type, catalog_name, schema_name);

CREATE UNIQUE INDEX ux_file_space_managed_root ON file_space_registry (managed_root)
    WHERE NOT is_external;

-- Current directory observations, separate from the namespace registry and file bytes.
CREATE TABLE file_directory_index (
    file_space_id UUID NOT NULL REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE,
    relative_path TEXT COLLATE "C" NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    last_modified TIMESTAMPTZ NOT NULL,
    etag TEXT,
    content_type TEXT,
    provider_version TEXT,
    md5 TEXT,
    PRIMARY KEY (file_space_id, relative_path)
);

CREATE TABLE file_directory_refresh (
    file_space_id UUID PRIMARY KEY REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE,
    refresh_id UUID NOT NULL,
    space_updated_at TIMESTAMPTZ NOT NULL,
    subpath TEXT NOT NULL,
    next_page_token TEXT,
    page_number BIGINT NOT NULL DEFAULT 0,
    scan_complete BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE file_directory_pending (
    file_space_id UUID NOT NULL REFERENCES file_directory_refresh(file_space_id) ON DELETE CASCADE,
    relative_path TEXT COLLATE "C" NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    last_modified TIMESTAMPTZ NOT NULL,
    etag TEXT,
    content_type TEXT,
    provider_version TEXT,
    md5 TEXT,
    PRIMARY KEY (file_space_id, relative_path)
);

-- Storage profiles: current names may be re-used after DROP, but every
-- incarnation and revision remains addressable for an admitted execution.
CREATE TABLE storage_profile_definitions (
    name VARCHAR(255) PRIMARY KEY,
    incarnation_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    storage_type VARCHAR(32) NOT NULL,
    bucket TEXT NOT NULL,
    object_prefix TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT NOT NULL,
    options JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE storage_profile_revisions (
    name VARCHAR(255) NOT NULL,
    incarnation_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    storage_type VARCHAR(32) NOT NULL,
    bucket TEXT NOT NULL,
    object_prefix TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT NOT NULL,
    options JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, incarnation_id, revision)
);

CREATE FUNCTION reject_storage_profile_revision_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'storage profile revisions are immutable';
END;
$$;

CREATE TRIGGER storage_profile_revisions_no_update_or_delete
BEFORE UPDATE OR DELETE ON storage_profile_revisions
FOR EACH ROW EXECUTE FUNCTION reject_storage_profile_revision_mutation();

CREATE TRIGGER storage_profile_revisions_no_truncate
BEFORE TRUNCATE ON storage_profile_revisions
FOR EACH STATEMENT EXECUTE FUNCTION reject_storage_profile_revision_mutation();

CREATE FUNCTION validate_storage_profile_revision_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM storage_profile_definitions current
        WHERE current.name = NEW.name
          AND current.incarnation_id = NEW.incarnation_id
          AND current.revision = NEW.revision
          AND current.storage_type = NEW.storage_type
          AND current.bucket = NEW.bucket
          AND current.object_prefix = NEW.object_prefix
          AND current.endpoint = NEW.endpoint
          AND current.region = NEW.region
          AND current.options = NEW.options
    ) THEN
        RAISE EXCEPTION 'storage profile revision must match the current definition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER storage_profile_revisions_validate_insert
BEFORE INSERT ON storage_profile_revisions
FOR EACH ROW EXECUTE FUNCTION validate_storage_profile_revision_insert();

CREATE FUNCTION record_storage_profile_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO storage_profile_revisions (
        name, incarnation_id, revision, storage_type, bucket,
        object_prefix, endpoint, region, options)
    VALUES (
        NEW.name, NEW.incarnation_id, NEW.revision, NEW.storage_type, NEW.bucket,
        NEW.object_prefix, NEW.endpoint, NEW.region, NEW.options);
    RETURN NEW;
END;
$$;

CREATE TRIGGER storage_profile_record_revision
AFTER INSERT ON storage_profile_definitions
FOR EACH ROW EXECUTE FUNCTION record_storage_profile_revision();

CREATE FUNCTION reject_storage_profile_current_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'storage profile definitions cannot be updated or truncated';
END;
$$;

CREATE TRIGGER storage_profile_current_no_update
BEFORE UPDATE ON storage_profile_definitions
FOR EACH ROW EXECUTE FUNCTION reject_storage_profile_current_mutation();

CREATE TRIGGER storage_profile_current_no_truncate
BEFORE TRUNCATE ON storage_profile_definitions
FOR EACH STATEMENT EXECUTE FUNCTION reject_storage_profile_current_mutation();

-- Drop revokes exact-name data and metadata read grants in the same transaction.
-- A direct SQL DELETE gets the same cleanup as Pangrova DROP. The grant guard on
-- security_grants itself is installed by AuthorizationTransactionGate.initialize.
CREATE FUNCTION revoke_dropped_storage_profile_grants()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM security_grants
    WHERE upper(object_type)='STORAGE_PROFILE' AND catalog_name=OLD.name
      AND upper(privilege) IN ('READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER');
    RETURN OLD;
END;
$$;

CREATE TRIGGER storage_profile_drop_revoke_grants
AFTER DELETE ON storage_profile_definitions
FOR EACH ROW EXECUTE FUNCTION revoke_dropped_storage_profile_grants();

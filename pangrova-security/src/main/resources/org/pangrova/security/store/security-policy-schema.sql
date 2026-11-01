-- Shared-state schema for security/governance policy stores (SI-4).
-- Applied by PersistentStoreSupport.initSchema against the shared PostgreSQL DataSource.
-- Statements are idempotent (CREATE TABLE IF NOT EXISTS).

-- Row-level security policies (RowPolicy).
CREATE TABLE IF NOT EXISTS row_policy (
    policy_id         BIGINT       GENERATED ALWAYS AS IDENTITY,
    policy_name       VARCHAR(255) NOT NULL,
    catalog_name      VARCHAR(255) NOT NULL,
    schema_name       VARCHAR(255) NOT NULL,
    table_name        VARCHAR(255) NOT NULL,
    filter_expression TEXT         NOT NULL,
    apply_to_roles    TEXT         NOT NULL,      -- comma-separated role list ('' = all)
    enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
    priority          INT          NOT NULL DEFAULT 0,
    table_id          UUID,
    PRIMARY KEY (policy_id)
);
ALTER TABLE row_policy ADD COLUMN IF NOT EXISTS table_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS row_policy_scoped_identity ON row_policy (table_id, policy_name)
    WHERE table_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS row_policy_unbound_name
    ON row_policy (catalog_name, schema_name, table_name, policy_name) WHERE table_id IS NULL;
CREATE INDEX idx_row_policy_table ON row_policy (catalog_name, schema_name, table_name);

-- Column masking policies (ColumnMask).
CREATE TABLE IF NOT EXISTS column_mask (
    mask_id         BIGINT       GENERATED ALWAYS AS IDENTITY,
    mask_name       VARCHAR(255) NOT NULL,
    catalog_name    VARCHAR(255) NOT NULL,
    schema_name     VARCHAR(255) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    column_name     VARCHAR(255) NOT NULL,
    mask_expression TEXT         NOT NULL,
    apply_to_roles  TEXT         NOT NULL,        -- comma-separated role list ('' = all)
    exempt_roles    TEXT         NOT NULL,        -- comma-separated role list
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    table_id        UUID,
    PRIMARY KEY (mask_id)
);
ALTER TABLE column_mask ADD COLUMN IF NOT EXISTS table_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS column_mask_scoped_identity
    ON column_mask (catalog_name, schema_name, table_name, column_name, mask_name, table_id) NULLS NOT DISTINCT;
CREATE INDEX idx_column_mask_column ON column_mask (catalog_name, schema_name, table_name, column_name);

-- Privilege grants (PrivilegeGrant) — backing store for dynamic RBAC GRANT/REVOKE.
CREATE TABLE IF NOT EXISTS security_grants (
    grant_id          BIGINT       GENERATED ALWAYS AS IDENTITY,
    role_name         VARCHAR(255) NOT NULL,
    privilege         VARCHAR(64)  NOT NULL,
    object_type       VARCHAR(64)  NOT NULL,
    catalog_name      VARCHAR(255),
    schema_name       VARCHAR(255),
    table_name        VARCHAR(255),
    column_name       VARCHAR(255),
    with_grant_option BOOLEAN      NOT NULL DEFAULT FALSE,
    cascade_revoke    BOOLEAN      NOT NULL DEFAULT FALSE,
    granted_by        VARCHAR(255),
    granted_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (grant_id),
    UNIQUE (role_name, privilege, object_type, catalog_name, schema_name, table_name, column_name)
);
CREATE INDEX idx_security_grants_role ON security_grants (role_name);

-- Canonical per-tenant quota (one model bridging the security/resource boundary, F-CHAIN-RES-03).
CREATE TABLE IF NOT EXISTS tenant_quota (
    tenant_id        VARCHAR(255) NOT NULL,
    max_memory_bytes BIGINT       NOT NULL DEFAULT 0,
    cpu_weight       DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    max_concurrent   INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id)
);

-- Role catalog (F-SEC-04). Roles are RBAC primitives owned by Pangrova and are NOT the same
-- as IdP groups. Identities reach roles through pangrova_user_roles and pangrova_group_roles below.
CREATE TABLE IF NOT EXISTS pangrova_roles (
    role_name   VARCHAR(255) NOT NULL,
    description VARCHAR(1024),
    created_by  VARCHAR(255),
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (role_name)
);

-- Direct user → role bindings.
CREATE TABLE IF NOT EXISTS pangrova_user_roles (
    user_id    VARCHAR(255) NOT NULL,
    role_name  VARCHAR(255) NOT NULL,
    granted_by VARCHAR(255),
    granted_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, role_name)
);
CREATE INDEX idx_pangrova_user_roles_role ON pangrova_user_roles (role_name);

-- IdP group → Pangrova role mappings (group is identity layer, role is RBAC primitive).
CREATE TABLE IF NOT EXISTS pangrova_group_roles (
    group_name VARCHAR(255) NOT NULL,
    role_name  VARCHAR(255) NOT NULL,
    mapped_by  VARCHAR(255),
    mapped_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, role_name)
);
CREATE INDEX idx_pangrova_group_roles_role ON pangrova_group_roles (role_name);

-- Extend tenant_quota with the full ResourceQuota field set (F-SEC-02).
-- ALTER ... IF NOT EXISTS is idempotent — safe to run on an already-migrated schema.
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_queued_queries INT NOT NULL DEFAULT 0;
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_cpu_time_per_query_ms BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_scan_bytes_per_query BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_daily_query_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_daily_scan_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenant_quota ADD COLUMN IF NOT EXISTS max_storage_bytes BIGINT NOT NULL DEFAULT 0;

-- Tenant registry (F-SEC-02). Allowlist columns are comma-separated (empty = unrestricted).
CREATE TABLE IF NOT EXISTS tenant (
    tenant_id        VARCHAR(255) NOT NULL,
    name             VARCHAR(512) NOT NULL,
    status           VARCHAR(32)  NOT NULL DEFAULT 'ACTIVE',
    resource_pool    VARCHAR(255),
    allowed_catalogs TEXT         NOT NULL DEFAULT '',
    allowed_schemas  TEXT         NOT NULL DEFAULT '',
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by       VARCHAR(255),
    PRIMARY KEY (tenant_id)
);

-- Per-tenant arbitrary properties (F-SEC-02).
CREATE TABLE IF NOT EXISTS tenant_property (
    tenant_id  VARCHAR(255) NOT NULL,
    prop_key   VARCHAR(255) NOT NULL,
    prop_value TEXT,
    PRIMARY KEY (tenant_id, prop_key)
);

-- Tenant user membership (F-SEC-02). One principal belongs to at most one tenant.
CREATE TABLE IF NOT EXISTS tenant_membership (
    user_principal VARCHAR(255) NOT NULL,
    tenant_id      VARCHAR(255) NOT NULL,
    role           VARCHAR(32)  NOT NULL,
    PRIMARY KEY (user_principal)
);
CREATE INDEX idx_tenant_membership_tenant ON tenant_membership (tenant_id);

-- Authorization revision counter: every gated authority mutation advances one
-- transactional version, so a COPY admission cannot revive after revoke/re-grant
-- or profile recreation even when the final privilege looks identical.
CREATE TABLE IF NOT EXISTS pangrova_authorization_revision (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revision BIGINT NOT NULL CHECK (revision > 0)
);
INSERT INTO pangrova_authorization_revision (singleton, revision)
    VALUES (TRUE, 1) ON CONFLICT (singleton) DO NOTHING;

-- The revision row advances only as a nested effect (trigger depth >= 2) of a
-- protected authority table statement. Ordinary SQL cannot reset the version.
CREATE OR REPLACE FUNCTION guard_pangrova_authorization_revision()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'Authorization revision is trigger-managed';
    END IF;
    IF pg_trigger_depth() < 2 OR NEW.singleton IS DISTINCT FROM OLD.singleton
            OR NEW.revision IS DISTINCT FROM OLD.revision + 1 THEN
        RAISE EXCEPTION 'Authorization revision is trigger-managed';
    END IF;
    RETURN NEW;
END
$guard$;

CREATE TRIGGER pangrova_authorization_revision_update_guard
BEFORE INSERT OR UPDATE OR DELETE ON pangrova_authorization_revision
FOR EACH ROW EXECUTE FUNCTION guard_pangrova_authorization_revision();

CREATE TRIGGER pangrova_authorization_revision_truncate_guard
BEFORE TRUNCATE ON pangrova_authorization_revision
FOR EACH STATEMENT EXECUTE FUNCTION guard_pangrova_authorization_revision();

-- Storage profile / file space / tenant domain (clean-cut folded from the V1-V101 chain, 2026-11-01).
-- Authority-gate triggers and grant-guard functions are installed at runtime by
-- AuthorizationTransactionGate.initialize; they are intentionally absent here.
-- ALTER TABLE statements below re-apply safely: duplicate-constraint errors are
-- tolerated by PersistentStoreSupport.initSchema savepoint handling.

CREATE TABLE IF NOT EXISTS file_directory_index (
    file_space_id uuid NOT NULL,
    relative_path text NOT NULL COLLATE pg_catalog."C",
    size_bytes bigint NOT NULL,
    last_modified timestamp with time zone NOT NULL,
    etag text,
    content_type text,
    provider_version text,
    md5 text,
    CONSTRAINT file_directory_index_size_bytes_check CHECK ((size_bytes >= 0))
);

CREATE TABLE IF NOT EXISTS file_directory_pending (
    file_space_id uuid NOT NULL,
    relative_path text NOT NULL COLLATE pg_catalog."C",
    size_bytes bigint NOT NULL,
    last_modified timestamp with time zone NOT NULL,
    etag text,
    content_type text,
    provider_version text,
    md5 text,
    CONSTRAINT file_directory_pending_size_bytes_check CHECK ((size_bytes >= 0))
);

CREATE TABLE IF NOT EXISTS file_directory_refresh (
    file_space_id uuid NOT NULL,
    refresh_id uuid NOT NULL,
    space_updated_at timestamp with time zone NOT NULL,
    subpath text NOT NULL,
    next_page_token text,
    page_number bigint DEFAULT 0 NOT NULL,
    scan_complete boolean DEFAULT false NOT NULL
);

CREATE TABLE IF NOT EXISTS file_space_registry (
    file_space_id uuid NOT NULL,
    space_type character varying(16) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    space_name character varying(255) NOT NULL,
    is_external boolean NOT NULL,
    storage_profile character varying(255) DEFAULT ''::character varying NOT NULL,
    location character varying(2048) DEFAULT ''::character varying NOT NULL,
    directory_enabled boolean DEFAULT false NOT NULL,
    comment text DEFAULT ''::text NOT NULL,
    owner_name character varying(255) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    managed_root text NOT NULL,
    CONSTRAINT chk_file_space_managed_root CHECK (((is_external AND (managed_root = ''::text)) OR ((NOT is_external) AND (length(managed_root) > 0)))),
    CONSTRAINT chk_file_space_registry_directory CHECK ((((space_type)::text = 'STAGE'::text) OR (NOT directory_enabled))),
    CONSTRAINT chk_file_space_registry_location CHECK (((is_external AND (length((location)::text) > 0)) OR ((NOT is_external) AND (length((location)::text) = 0)))),
    CONSTRAINT chk_file_space_registry_name CHECK (((length((catalog_name)::text) > 0) AND (length((schema_name)::text) > 0) AND (length((space_name)::text) > 0))),
    CONSTRAINT chk_file_space_registry_type CHECK (((space_type)::text = ANY ((ARRAY['VOLUME'::character varying, 'STAGE'::character varying])::text[])))
);

CREATE TABLE IF NOT EXISTS storage_profile_definitions (
    name character varying(255) NOT NULL,
    incarnation_id uuid NOT NULL,
    revision bigint NOT NULL,
    storage_type character varying(32) NOT NULL,
    bucket text NOT NULL,
    object_prefix text NOT NULL,
    endpoint text NOT NULL,
    region text NOT NULL,
    options jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT storage_profile_definitions_revision_check CHECK ((revision > 0))
);

CREATE TABLE IF NOT EXISTS storage_profile_revisions (
    name character varying(255) NOT NULL,
    incarnation_id uuid NOT NULL,
    revision bigint NOT NULL,
    storage_type character varying(32) NOT NULL,
    bucket text NOT NULL,
    object_prefix text NOT NULL,
    endpoint text NOT NULL,
    region text NOT NULL,
    options jsonb NOT NULL,
    recorded_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT storage_profile_revisions_revision_check CHECK ((revision > 0))
);

CREATE TABLE IF NOT EXISTS tenant_user_memberships (
    user_principal character varying(255) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    role character varying(32) DEFAULT 'MEMBER'::character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id character varying(255) NOT NULL,
    name character varying(512) NOT NULL,
    status character varying(32) NOT NULL,
    max_memory_bytes bigint DEFAULT 0 NOT NULL,
    max_concurrent_queries integer DEFAULT 0 NOT NULL,
    max_queued_queries integer DEFAULT 0 NOT NULL,
    max_cpu_time_per_query_ms bigint DEFAULT 0 NOT NULL,
    max_scan_bytes_per_query bigint DEFAULT 0 NOT NULL,
    allowed_catalogs text,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    max_daily_query_count bigint DEFAULT 0 NOT NULL,
    max_daily_scan_bytes bigint DEFAULT 0 NOT NULL,
    max_storage_bytes bigint DEFAULT 0 NOT NULL,
    allowed_schemas text,
    resource_pool character varying(255),
    properties text,
    created_by character varying(255)
);

ALTER TABLE ONLY file_directory_index
    ADD CONSTRAINT file_directory_index_pkey PRIMARY KEY (file_space_id, relative_path);

ALTER TABLE ONLY file_directory_pending
    ADD CONSTRAINT file_directory_pending_pkey PRIMARY KEY (file_space_id, relative_path);

ALTER TABLE ONLY file_directory_refresh
    ADD CONSTRAINT file_directory_refresh_pkey PRIMARY KEY (file_space_id);

ALTER TABLE ONLY file_space_registry
    ADD CONSTRAINT file_space_registry_pkey PRIMARY KEY (file_space_id);

ALTER TABLE ONLY storage_profile_definitions
    ADD CONSTRAINT storage_profile_definitions_pkey PRIMARY KEY (name);

ALTER TABLE ONLY storage_profile_revisions
    ADD CONSTRAINT storage_profile_revisions_pkey PRIMARY KEY (name, incarnation_id, revision);

ALTER TABLE ONLY tenant_user_memberships
    ADD CONSTRAINT tenant_user_memberships_pkey PRIMARY KEY (user_principal);

ALTER TABLE ONLY tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (tenant_id);

CREATE INDEX IF NOT EXISTS ix_file_space_registry_schema ON file_space_registry USING btree (space_type, catalog_name, schema_name);

CREATE UNIQUE INDEX IF NOT EXISTS ux_file_space_managed_root ON file_space_registry USING btree (managed_root) WHERE (NOT is_external);

CREATE UNIQUE INDEX IF NOT EXISTS ux_file_space_registry_name ON file_space_registry USING btree (space_type, catalog_name, schema_name, space_name);

ALTER TABLE ONLY file_directory_index
    ADD CONSTRAINT file_directory_index_file_space_id_fkey FOREIGN KEY (file_space_id) REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE;

ALTER TABLE ONLY file_directory_pending
    ADD CONSTRAINT file_directory_pending_file_space_id_fkey FOREIGN KEY (file_space_id) REFERENCES file_directory_refresh(file_space_id) ON DELETE CASCADE;

ALTER TABLE ONLY file_directory_refresh
    ADD CONSTRAINT file_directory_refresh_file_space_id_fkey FOREIGN KEY (file_space_id) REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE;

CREATE OR REPLACE FUNCTION record_storage_profile_revision()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO storage_profile_revisions (
        name, incarnation_id, revision, storage_type, bucket,
        object_prefix, endpoint, region, options)
    VALUES (
        NEW.name, NEW.incarnation_id, NEW.revision, NEW.storage_type, NEW.bucket,
        NEW.object_prefix, NEW.endpoint, NEW.region, NEW.options);
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION reject_storage_profile_current_mutation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'storage profile definitions cannot be updated or truncated';
END;
$function$;

CREATE OR REPLACE FUNCTION reject_storage_profile_revision_mutation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'storage profile revisions are immutable';
END;
$function$;

CREATE OR REPLACE FUNCTION revoke_dropped_storage_profile_grants()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    DELETE FROM security_grants
    WHERE upper(object_type)='STORAGE_PROFILE' AND catalog_name=OLD.name
      AND upper(privilege) IN ('READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER');
    RETURN OLD;
END;
$function$;

CREATE OR REPLACE FUNCTION validate_storage_profile_revision_insert()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;

CREATE OR REPLACE TRIGGER storage_profile_current_no_truncate BEFORE TRUNCATE ON storage_profile_definitions FOR EACH STATEMENT EXECUTE FUNCTION reject_storage_profile_current_mutation();

CREATE OR REPLACE TRIGGER storage_profile_current_no_update BEFORE UPDATE ON storage_profile_definitions FOR EACH ROW EXECUTE FUNCTION reject_storage_profile_current_mutation();

CREATE OR REPLACE TRIGGER storage_profile_drop_revoke_grants AFTER DELETE ON storage_profile_definitions FOR EACH ROW EXECUTE FUNCTION revoke_dropped_storage_profile_grants();

CREATE OR REPLACE TRIGGER storage_profile_record_revision AFTER INSERT ON storage_profile_definitions FOR EACH ROW EXECUTE FUNCTION record_storage_profile_revision();

CREATE OR REPLACE TRIGGER storage_profile_revisions_no_truncate BEFORE TRUNCATE ON storage_profile_revisions FOR EACH STATEMENT EXECUTE FUNCTION reject_storage_profile_revision_mutation();

CREATE OR REPLACE TRIGGER storage_profile_revisions_no_update_or_delete BEFORE DELETE OR UPDATE ON storage_profile_revisions FOR EACH ROW EXECUTE FUNCTION reject_storage_profile_revision_mutation();

CREATE OR REPLACE TRIGGER storage_profile_revisions_validate_insert BEFORE INSERT ON storage_profile_revisions FOR EACH ROW EXECUTE FUNCTION validate_storage_profile_revision_insert();

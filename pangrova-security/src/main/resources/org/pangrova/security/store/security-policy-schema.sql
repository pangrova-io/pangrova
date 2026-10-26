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

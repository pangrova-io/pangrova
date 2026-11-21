-- Single initialization owner for policy and audit stores. No provider performs DDL.
SELECT pg_advisory_xact_lock(207578817, 1);

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
CREATE INDEX IF NOT EXISTS idx_row_policy_table ON row_policy (catalog_name, schema_name, table_name);

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
CREATE INDEX IF NOT EXISTS idx_column_mask_column ON column_mask (catalog_name, schema_name, table_name, column_name);

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
CREATE INDEX IF NOT EXISTS idx_security_grants_role ON security_grants (role_name);

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
CREATE INDEX IF NOT EXISTS idx_pangrova_user_roles_role ON pangrova_user_roles (role_name);

-- IdP group → Pangrova role mappings (group is identity layer, role is RBAC primitive).
CREATE TABLE IF NOT EXISTS pangrova_group_roles (
    group_name VARCHAR(255) NOT NULL,
    role_name  VARCHAR(255) NOT NULL,
    mapped_by  VARCHAR(255),
    mapped_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_name, role_name)
);
CREATE INDEX IF NOT EXISTS idx_pangrova_group_roles_role ON pangrova_group_roles (role_name);

CREATE TABLE IF NOT EXISTS audit_logs (
    event_id        VARCHAR(64) PRIMARY KEY,
    event_type      VARCHAR(64) NOT NULL,
    event_time      TIMESTAMP NOT NULL,
    principal       VARCHAR(256),
    client_ip       VARCHAR(64),
    trace_id        VARCHAR(128),
    span_id         VARCHAR(128),
    query_id        VARCHAR(128),
    sql_text        TEXT,
    catalog_name    VARCHAR(256),
    schema_name     VARCHAR(256),
    resource        VARCHAR(1024),
    success         BOOLEAN,
    error_message   TEXT,
    duration_ms     BIGINT,
    rows_read       BIGINT,
    bytes_read      BIGINT,
    accessed_objects TEXT,
    properties      TEXT
);

CREATE OR REPLACE FUNCTION pangrova_authority_mutation_gate()
RETURNS trigger LANGUAGE plpgsql AS $gate$
BEGIN
    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'Authority tables cannot be truncated; use gated DELETE';
    END IF;
    PERFORM pg_advisory_xact_lock(207578817, 1);
    IF to_regclass('pangrova_authorization_revision') IS NOT NULL THEN
        UPDATE pangrova_authorization_revision
        SET revision = revision + 1 WHERE singleton = TRUE;
    END IF;
    RETURN NULL;
END
$gate$;

CREATE OR REPLACE FUNCTION revoke_dropped_file_space_grants()
RETURNS trigger LANGUAGE plpgsql AS $cleanup$
BEGIN
    DELETE FROM security_grants
    WHERE upper(object_type) = OLD.space_type
      AND catalog_name = OLD.catalog_name
      AND schema_name = OLD.schema_name
      AND table_name = OLD.space_name
      AND column_name IS NULL;
    RETURN OLD;
END
$cleanup$;

CREATE OR REPLACE FUNCTION move_renamed_file_space_grants()
RETURNS trigger LANGUAGE plpgsql AS $cleanup$
BEGIN
    IF NEW.space_name IS DISTINCT FROM OLD.space_name THEN
        UPDATE security_grants
        SET table_name = NEW.space_name
        WHERE upper(object_type) = NEW.space_type
          AND catalog_name = NEW.catalog_name
          AND schema_name = NEW.schema_name
          AND table_name = OLD.space_name
          AND column_name IS NULL;
    END IF;
    RETURN NEW;
END
$cleanup$;

CREATE OR REPLACE FUNCTION pangrova_storage_profile_grant_guard()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF upper(NEW.object_type) = 'STORAGE_PROFILE'
            AND upper(NEW.privilege) IN (
                'READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER')
            AND NEW.catalog_name IS DISTINCT FROM '*' THEN
        IF NEW.catalog_name IS NULL OR NEW.schema_name IS NOT NULL
                OR NEW.table_name IS NOT NULL OR NEW.column_name IS NOT NULL
                OR to_regclass('storage_profile_definitions') IS NULL THEN
            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
        END IF;
        PERFORM 1 FROM storage_profile_definitions
        WHERE name = NEW.catalog_name FOR KEY SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
        END IF;
    END IF;
    RETURN NEW;
END
$guard$;

CREATE OR REPLACE FUNCTION pangrova_role_drop_cleanup()
RETURNS trigger LANGUAGE plpgsql AS $cleanup$
BEGIN
    DELETE FROM security_grants WHERE role_name=OLD.role_name;
    DELETE FROM pangrova_user_roles WHERE role_name=OLD.role_name;
    DELETE FROM pangrova_group_roles WHERE role_name=OLD.role_name;
    IF to_regclass('pangrova_embedding_models') IS NOT NULL THEN
        UPDATE pangrova_embedding_models
        SET admin_role=CASE WHEN admin_role=OLD.role_name THEN NULL ELSE admin_role END,
            usage_role=CASE WHEN usage_role=OLD.role_name THEN NULL ELSE usage_role END
        WHERE admin_role=OLD.role_name OR usage_role=OLD.role_name;
    END IF;
    RETURN OLD;
END
$cleanup$;

CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pangrova_roles
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pangrova_user_roles
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pangrova_group_roles
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON security_grants
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON row_policy
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON column_mask
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON tenants
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON tenant_user_memberships
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pangrova_embedding_models
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON storage_profile_definitions
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON file_space_registry
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON table_control_governance_binding
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();

CREATE OR REPLACE TRIGGER pangrova_role_drop_cleanup
AFTER DELETE ON pangrova_roles FOR EACH ROW EXECUTE FUNCTION pangrova_role_drop_cleanup();
CREATE OR REPLACE TRIGGER pangrova_storage_profile_grant_guard
BEFORE INSERT OR UPDATE ON security_grants FOR EACH ROW EXECUTE FUNCTION pangrova_storage_profile_grant_guard();
CREATE OR REPLACE TRIGGER file_space_drop_revoke_grants
AFTER DELETE ON file_space_registry FOR EACH ROW EXECUTE FUNCTION revoke_dropped_file_space_grants();
CREATE OR REPLACE TRIGGER file_space_rename_move_grants
AFTER UPDATE ON file_space_registry FOR EACH ROW EXECUTE FUNCTION move_renamed_file_space_grants();

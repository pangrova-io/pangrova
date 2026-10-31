-- Current profile names may be re-used after DROP, but every incarnation and
-- revision remains addressable for an already admitted execution.
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
-- MANAGE is a namespace permission and can be granted before CREATE.
-- Wildcard grants deliberately authorize all current and future profile names.
-- A direct SQL DELETE gets the same cleanup as Pangrova DROP.
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

-- Install the write guard at migration time for existing security databases.
-- Security bootstrap installs the same trigger when grants are created later.
CREATE OR REPLACE FUNCTION pangrova_storage_profile_grant_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF upper(NEW.object_type) = 'STORAGE_PROFILE'
            AND upper(NEW.privilege) IN (
                'READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER')
            AND NEW.catalog_name IS DISTINCT FROM '*' THEN
        IF NEW.catalog_name IS NULL OR NEW.schema_name IS NOT NULL
                OR NEW.table_name IS NOT NULL OR NEW.column_name IS NOT NULL THEN
            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
        END IF;
        PERFORM 1 FROM storage_profile_definitions
        WHERE name = NEW.catalog_name FOR KEY SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF to_regclass('security_grants') IS NOT NULL THEN
        EXECUTE 'CREATE OR REPLACE TRIGGER pangrova_storage_profile_grant_guard '
             || 'BEFORE INSERT OR UPDATE ON security_grants FOR EACH ROW '
             || 'EXECUTE FUNCTION pangrova_storage_profile_grant_guard()';
    END IF;
END;
$$;

-- Before this migration profiles were coordinator-local. Old exact-name read
-- grants cannot be retained as latent authority over a newly created name.
DO $$
BEGIN
    IF to_regclass('security_grants') IS NOT NULL THEN
        DELETE FROM security_grants
        WHERE upper(object_type) = 'STORAGE_PROFILE'
          AND upper(privilege) IN ('READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER')
          AND catalog_name IS DISTINCT FROM '*';
    END IF;
END;
$$;

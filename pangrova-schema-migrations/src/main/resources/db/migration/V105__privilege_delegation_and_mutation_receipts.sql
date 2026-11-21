-- SQL privilege delegation shares the existing authority gate and revision.
-- No provider initializes tables; this also initializes a new empty deployment.
SELECT pg_advisory_xact_lock(207578817, 1);

ALTER TABLE security_grants ADD COLUMN parent_grant_id BIGINT;
ALTER TABLE security_grants ADD CONSTRAINT security_grant_parent
    FOREIGN KEY (parent_grant_id) REFERENCES security_grants(grant_id) ON DELETE CASCADE;
ALTER TABLE security_grants ADD CONSTRAINT security_grant_acyclic
    CHECK (parent_grant_id IS NULL OR parent_grant_id < grant_id);
CREATE INDEX security_grant_parent_idx ON security_grants(parent_grant_id);

-- Check the completed statement, not each row while a coordinated rename is
-- still updating its children. Otherwise a BEFORE-row delete both loses valid
-- grants and conflicts with rows being updated by the same command.
CREATE FUNCTION pangrova_invalidate_changed_grant_children()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    DELETE FROM security_grants child
    USING new_grants parent, old_grants previous
    WHERE parent.grant_id = previous.grant_id
      AND child.parent_grant_id = parent.grant_id
      AND (
          (previous.with_grant_option AND NOT parent.with_grant_option AND parent.privilege <> 'OWNER')
          OR (parent.role_name, parent.privilege, parent.object_type, parent.parent_grant_id)
             IS DISTINCT FROM
             (previous.role_name, previous.privilege, previous.object_type, previous.parent_grant_id)
          OR (
              (parent.catalog_name, parent.schema_name, parent.table_name, parent.column_name)
              IS DISTINCT FROM
              (previous.catalog_name, previous.schema_name, previous.table_name, previous.column_name)
              AND NOT (
                  (parent.catalog_name IS NULL OR parent.catalog_name = '*'
                      OR parent.catalog_name IS NOT DISTINCT FROM child.catalog_name)
                  AND (parent.schema_name IS NULL OR parent.schema_name = '*'
                      OR parent.schema_name IS NOT DISTINCT FROM child.schema_name)
                  AND (parent.table_name IS NULL OR parent.table_name = '*'
                      OR parent.table_name IS NOT DISTINCT FROM child.table_name)
                  AND (parent.column_name IS NULL OR parent.column_name = '*'
                      OR parent.column_name IS NOT DISTINCT FROM child.column_name)
              )
          )
      );
    RETURN NULL;
END
$guard$;
CREATE TRIGGER pangrova_grant_children_guard AFTER UPDATE ON security_grants
    REFERENCING OLD TABLE AS old_grants NEW TABLE AS new_grants
    FOR EACH STATEMENT EXECUTE FUNCTION pangrova_invalidate_changed_grant_children();

-- The receipt is also the durable audit record. It commits with the grant changes.
-- Retention may not erase idempotency keys while their clients can replay requests.
CREATE TABLE security_privilege_mutations (
    request_id UUID PRIMARY KEY,
    principal VARCHAR(255) NOT NULL,
    request_body BYTEA NOT NULL,
    request_description TEXT NOT NULL,
    authorization_revision BIGINT NOT NULL CHECK (authorization_revision >= 0),
    changed_grants INT NOT NULL CHECK (changed_grants >= 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

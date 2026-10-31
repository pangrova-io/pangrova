-- A Worker must retrieve the exact descriptor named by an in-flight canonical
-- TaskRequest even after a newer DDL revision replaces catalog_definitions.
-- No FK to the current-name row: DROP/re-CREATE has a different incarnation,
-- and already dispatched work can still need its historical snapshot.
CREATE TABLE catalog_descriptor_revisions (
    name             VARCHAR(255) NOT NULL,
    incarnation_id   VARCHAR(36)  NOT NULL,
    revision         BIGINT       NOT NULL CHECK (revision > 0),
    type             VARCHAR(50)  NOT NULL,
    provider         VARCHAR(50)  NOT NULL,
    state            VARCHAR(20)  NOT NULL,
    properties       JSONB        NOT NULL,
    comment          TEXT,
    allowed_tenants  JSONB,
    recorded_at      TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (name, incarnation_id, revision)
);

-- Existing installations have only the current descriptor at upgrade time.
-- Earlier overwritten revisions cannot be reconstructed and must fail closed.
INSERT INTO catalog_descriptor_revisions
    (name, incarnation_id, revision, type, provider, state,
     properties, comment, allowed_tenants)
SELECT name, incarnation_id, revision, type, provider, state,
       properties, comment, allowed_tenants
FROM catalog_definitions;

-- Revision payloads cannot be rewritten in place. Future lease-aware GC may
-- delete rows, but no automatic deletion is installed by this migration.
CREATE FUNCTION reject_catalog_descriptor_revision_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'catalog descriptor revisions are immutable';
END;
$$;

CREATE TRIGGER catalog_descriptor_revisions_no_update
BEFORE UPDATE ON catalog_descriptor_revisions
FOR EACH ROW EXECUTE FUNCTION reject_catalog_descriptor_revision_update();

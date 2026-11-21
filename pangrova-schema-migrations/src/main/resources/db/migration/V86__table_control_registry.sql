-- Durable management state for tables Pangrova owns or has explicitly adopted.
--
-- OBSERVED tables are never stored here: they are a live view of the connector
-- catalog. Only LINKED (adopted external) and MANAGED (created/written by
-- Pangrova) tables have a control record.
--
-- table_id is the stable identity used by writes, maintenance, and governance.
-- It is intentionally different from catalog.schema.table: dropping and
-- recreating a table under the same name must not inherit the old identity.

CREATE TABLE IF NOT EXISTS table_control_registry (
    table_id              UUID         PRIMARY KEY,
    catalog_name          VARCHAR(255) NOT NULL,
    schema_name           VARCHAR(255) NOT NULL,
    table_name            VARCHAR(255) NOT NULL,
    format                VARCHAR(32)  NOT NULL,
    format_table_uuid     VARCHAR(255),
    format_location       TEXT,
    ownership_mode        VARCHAR(16)  NOT NULL,
    state                 VARCHAR(32)  NOT NULL,
    provenance            VARCHAR(16)  NOT NULL,
    catalog_incarnation_id VARCHAR(64),
    catalog_revision      BIGINT,
    metadata_revision     BIGINT       NOT NULL DEFAULT 0,
    version_token         VARCHAR(1024),
    version_observed_at   TIMESTAMPTZ,
    owner_principal       VARCHAR(255),
    properties            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    source_signature      VARCHAR(512),
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_synced_at        TIMESTAMPTZ,
    last_maintained_at    TIMESTAMPTZ,
    CONSTRAINT table_control_registry_ownership_mode_check
        CHECK (ownership_mode IN ('LINKED', 'MANAGED')),
    CONSTRAINT table_control_registry_name_not_blank
        CHECK (catalog_name <> '' AND schema_name <> '' AND table_name <> '')
);

-- DROPPED rows are retained for provenance, so the name is unique only among
-- live control records.
CREATE UNIQUE INDEX IF NOT EXISTS uq_table_control_registry_live_name
    ON table_control_registry (catalog_name, schema_name, table_name)
    WHERE state <> 'DROPPED';

CREATE INDEX IF NOT EXISTS idx_table_control_registry_catalog
    ON table_control_registry (catalog_name, ownership_mode, state);

CREATE INDEX IF NOT EXISTS idx_table_control_registry_format_uuid
    ON table_control_registry (format_table_uuid);

CREATE INDEX IF NOT EXISTS idx_table_control_registry_revision
    ON table_control_registry (metadata_revision);

-- Durable registry shared by the Volume (Databricks-style) and Stage
-- (Snowflake-style) namespace surfaces.
--
-- The registry stores namespace metadata only. File bytes, provider versions,
-- and directory listings live in the storage layer; asset/version/retention
-- rows are intentionally not modeled here.

CREATE TABLE IF NOT EXISTS file_space_registry (
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
    CONSTRAINT chk_file_space_registry_type
        CHECK (space_type IN ('VOLUME', 'STAGE')),
    CONSTRAINT chk_file_space_registry_name
        CHECK (length(catalog_name) > 0 AND length(schema_name) > 0 AND length(space_name) > 0),
    CONSTRAINT chk_file_space_registry_location
        CHECK ((is_external AND length(location) > 0) OR (NOT is_external AND length(location) = 0)),
    CONSTRAINT chk_file_space_registry_directory
        CHECK (space_type = 'STAGE' OR NOT directory_enabled)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_file_space_registry_name
    ON file_space_registry (space_type, catalog_name, schema_name, space_name);

CREATE INDEX IF NOT EXISTS ix_file_space_registry_schema
    ON file_space_registry (space_type, catalog_name, schema_name);

-- History of version transitions observed for LINKED and MANAGED tables.
--
-- OBSERVED tables are never recorded here: they are a live catalog view and have
-- no persisted identity. An observation proves that a new format version was seen;
-- it is not a Pangrova write commit and must not enter the write commit journal.

CREATE TABLE IF NOT EXISTS table_control_version_observation (
    observation_id        BIGSERIAL    PRIMARY KEY,
    table_id              UUID         NOT NULL,
    version_token         VARCHAR(1024) NOT NULL,
    previous_version_token VARCHAR(1024),
    source                VARCHAR(32)  NOT NULL,
    observed_at           TIMESTAMPTZ  NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_table_control_version_observation_table
    ON table_control_version_observation (table_id, observed_at DESC);

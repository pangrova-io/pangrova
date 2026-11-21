-- Persisted table statistics collected by ANALYZE TABLE.
--
-- A row is bound to the controlled table's stable table_id and the version token
-- observed when it was collected. Readers treat it as usable only while the
-- control record still carries that token; a later external or Pangrova commit
-- therefore makes it stale without a background writer touching this table.
-- OBSERVED tables have no stable id and are never stored here.

CREATE TABLE IF NOT EXISTS table_control_statistics (
    table_id          UUID         PRIMARY KEY,
    stats_version     BIGINT       NOT NULL,
    version_token     VARCHAR(1024) NOT NULL,
    row_count         BIGINT       NOT NULL,
    data_size         BIGINT       NOT NULL,
    source_work_units BIGINT       NOT NULL DEFAULT -1,
    column_stats      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    source            VARCHAR(64)  NOT NULL,
    computed_at       TIMESTAMPTZ  NOT NULL,
    CONSTRAINT chk_table_control_statistics_version CHECK (stats_version >= 0),
    CONSTRAINT chk_table_control_statistics_counts
        CHECK (row_count >= -1 AND data_size >= -1 AND source_work_units >= -1)
);

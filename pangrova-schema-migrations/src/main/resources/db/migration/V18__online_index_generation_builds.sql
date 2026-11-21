-- A refresh builds beside the active immutable generation. The descriptor and
-- active manifest stay queryable until publication commits the replacement.
CREATE TABLE IF NOT EXISTS index_generation_builds (
    index_id          VARCHAR(255) NOT NULL,
    generation        INT          NOT NULL,
    base_generation   INT          NOT NULL,
    source_snapshot   VARCHAR(255) NOT NULL DEFAULT '',
    state             VARCHAR(32)  NOT NULL,
    failure_reason    VARCHAR(1024),
    started_at        TIMESTAMPTZ(6) NOT NULL,
    updated_at        TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (index_id, generation),
    CONSTRAINT fk_index_generation_build
        FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_index_generation_building
    ON index_generation_builds (index_id) WHERE state = 'BUILDING';

CREATE INDEX IF NOT EXISTS idx_index_generation_build_state
    ON index_generation_builds (state, updated_at);

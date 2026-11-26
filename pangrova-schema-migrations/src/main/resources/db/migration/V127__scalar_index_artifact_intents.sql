-- Pre-mapping scalar object writes: uncertain writes remain quarantined.
CREATE TABLE scalar_index_artifact_intents (
    artifact_digest   CHAR(64)      PRIMARY KEY,
    index_id          VARCHAR(255)  NOT NULL REFERENCES index_descriptors(index_id) ON DELETE RESTRICT,
    generation        INT           NOT NULL CHECK (generation >= 0),
    attempt_owner     VARCHAR(255)  NOT NULL,
    data_file_path    VARCHAR(2048) NOT NULL,
    artifact_path     VARCHAR(2048) NOT NULL,
    state             VARCHAR(20)   NOT NULL CHECK (state IN ('WRITING', 'DRAINED', 'DELETE_PENDING')),
    created_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    drained_at        TIMESTAMP,
    CHECK ((state = 'WRITING' AND drained_at IS NULL)
        OR (state IN ('DRAINED', 'DELETE_PENDING') AND drained_at IS NOT NULL))
);

CREATE INDEX idx_scalar_artifact_intents_index ON scalar_index_artifact_intents(index_id);
CREATE INDEX idx_scalar_artifact_intents_reclaim ON scalar_index_artifact_intents(drained_at, artifact_digest)
    WHERE drained_at IS NOT NULL;

-- Hash is only a bounded lookup key; reclaim checks exact path equality as well.
CREATE INDEX idx_index_file_mappings_artifact_path_digest ON index_file_mappings (md5(index_file_path));

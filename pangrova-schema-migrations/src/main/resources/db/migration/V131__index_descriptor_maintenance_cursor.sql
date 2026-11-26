-- One durable keyset sweep per maintenance state. The revision also fences ABA after wrapping.
CREATE TABLE index_descriptor_maintenance_cursors (
    state VARCHAR(20) PRIMARY KEY CHECK (state IN ('READY', 'STALE', 'DROPPED')),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    last_index_id VARCHAR(255),
    upper_index_id VARCHAR(255),
    CHECK ((last_index_id IS NULL) = (upper_index_id IS NULL)),
    CHECK (last_index_id <= upper_index_id)
);
INSERT INTO index_descriptor_maintenance_cursors(state) VALUES ('READY'), ('STALE'), ('DROPPED');
CREATE INDEX idx_index_descriptors_state_id ON index_descriptors(state, index_id);

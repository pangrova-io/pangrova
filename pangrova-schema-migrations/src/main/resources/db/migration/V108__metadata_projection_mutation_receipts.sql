-- Deployment fence, projection, invalidation and this receipt commit together.
-- Keep receipts independent of catalog deletion so a lost commit acknowledgement remains confirmable.
CREATE TABLE metadata_projection_mutations (
    operation_id UUID PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    incarnation_id VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    request_json JSONB NOT NULL,
    receipt_json JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_metadata_projection_mutations_deployment
    ON metadata_projection_mutations(catalog_name, incarnation_id, revision);

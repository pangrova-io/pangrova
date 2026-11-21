-- Durable, immutable projection requests survive process loss before receipt publication.
-- No catalog FK: deployment replacement must explicitly fence and reject obsolete pending work.
CREATE TABLE metadata_projection_intents (
    operation_id UUID PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    incarnation_id VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    request_json JSONB NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('PENDING', 'APPLIED', 'REJECTED')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX uq_metadata_projection_pending_catalog
    ON metadata_projection_intents(catalog_name) WHERE state = 'PENDING';

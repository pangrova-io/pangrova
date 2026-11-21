-- Live query reservations are owned by the canonical control-plane schema.
-- No historical data conversion or provider-local DDL is performed.
CREATE TABLE tenant_concurrency_slots (
    tenant_id VARCHAR(255) PRIMARY KEY,
    active_count INT NOT NULL CHECK (active_count >= 0)
);
CREATE TABLE tenant_concurrency_queries (
    query_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL REFERENCES tenant_concurrency_slots(tenant_id)
);

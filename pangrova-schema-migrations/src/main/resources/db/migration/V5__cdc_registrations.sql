-- CDC registration store: persists CDC stream registrations across coordinator restarts.
-- Replaces InMemoryCdcRegistrationStore with durable JDBC-backed storage.

CREATE TABLE IF NOT EXISTS cdc_registrations (
    stream_id              VARCHAR(255)  NOT NULL PRIMARY KEY,
    consumer_id            VARCHAR(255)  NOT NULL,
    tenant_id              VARCHAR(255),
    state                  VARCHAR(32)   NOT NULL,
    max_concurrent_writes  INT           NOT NULL,
    max_pending_bytes      BIGINT        NOT NULL,
    last_heartbeat         TIMESTAMP     NOT NULL,
    created_at             TIMESTAMP     NOT NULL
);

CREATE INDEX idx_cdc_registrations_state ON cdc_registrations (state);
CREATE INDEX idx_cdc_registrations_tenant ON cdc_registrations (tenant_id);

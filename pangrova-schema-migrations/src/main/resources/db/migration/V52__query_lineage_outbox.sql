-- Retained independently of query journal purge; an acknowledged fact is retained for exact replay reconciliation.
CREATE TABLE query_lineage_outbox (
    event_id UUID PRIMARY KEY,
    namespace TEXT NOT NULL,
    query_id TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('START', 'TERMINAL')),
    payload JSONB NOT NULL,
    created_at_ms BIGINT NOT NULL,
    delivery_token BIGINT NOT NULL DEFAULT 0 CHECK (delivery_token >= 0),
    lease_until TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    UNIQUE (namespace, query_id, category)
);
CREATE INDEX query_lineage_outbox_pending ON query_lineage_outbox (created_at_ms, event_id)
    WHERE delivered_at IS NULL;
ALTER TABLE query_state_journal ADD COLUMN lineage_payload JSONB;

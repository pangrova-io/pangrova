-- Upgrade requires all old commit-log pollers and writers to be stopped. The
-- backfill establishes one durable order before the new single-path poller starts.
-- Fail deployment instead of waiting indefinitely on a still-active old writer.
SET LOCAL lock_timeout = '10s';
LOCK TABLE index_commit_log IN ACCESS EXCLUSIVE MODE;

ALTER TABLE index_commit_log
    ADD COLUMN table_sequence BIGINT;

WITH ordered AS (
    SELECT event_id,
           ROW_NUMBER() OVER (
               PARTITION BY tenant_id, catalog_name, schema_name, table_name
               ORDER BY created_at, event_id) AS sequence
    FROM index_commit_log
    WHERE processed = FALSE
)
UPDATE index_commit_log AS log
SET table_sequence = ordered.sequence
FROM ordered
WHERE log.event_id = ordered.event_id;

ALTER TABLE index_commit_log
    ADD CONSTRAINT chk_index_commit_unprocessed_sequence
    CHECK (processed OR table_sequence IS NOT NULL);

CREATE UNIQUE INDEX idx_icl_table_sequence
    ON index_commit_log (
        tenant_id, catalog_name, schema_name, table_name, table_sequence)
    WHERE table_sequence IS NOT NULL;

CREATE INDEX idx_icl_pending_table_sequence
    ON index_commit_log (
        tenant_id, catalog_name, schema_name, table_name, table_sequence)
    WHERE processed = FALSE;

CREATE TABLE index_commit_table_heads (
    tenant_id VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    head_event_id BIGINT,
    head_sequence BIGINT,
    next_sequence BIGINT NOT NULL,
    last_polled_at TIMESTAMPTZ(6) NOT NULL DEFAULT '-infinity',
    claim_epoch BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, table_name),
    CHECK ((head_event_id IS NULL) = (head_sequence IS NULL)),
    CHECK (next_sequence > 0),
    CHECK (claim_epoch >= 0)
);

WITH first_pending AS (
    SELECT DISTINCT ON (tenant_id, catalog_name, schema_name, table_name)
           tenant_id, catalog_name, schema_name, table_name,
           event_id, table_sequence
    FROM index_commit_log
    WHERE processed = FALSE
    ORDER BY tenant_id, catalog_name, schema_name, table_name, table_sequence
), tails AS (
    SELECT tenant_id, catalog_name, schema_name, table_name,
           MAX(table_sequence) + 1 AS next_sequence
    FROM index_commit_log
    WHERE table_sequence IS NOT NULL
    GROUP BY tenant_id, catalog_name, schema_name, table_name
)
INSERT INTO index_commit_table_heads (
    tenant_id, catalog_name, schema_name, table_name,
    head_event_id, head_sequence, next_sequence)
SELECT first_pending.tenant_id, first_pending.catalog_name,
       first_pending.schema_name, first_pending.table_name,
       first_pending.event_id, first_pending.table_sequence, tails.next_sequence
FROM first_pending
JOIN tails USING (tenant_id, catalog_name, schema_name, table_name);

CREATE INDEX idx_ich_ready_fairness
    ON index_commit_table_heads (
        last_polled_at, tenant_id, catalog_name, schema_name, table_name)
    WHERE head_event_id IS NOT NULL;

DROP INDEX IF EXISTS idx_icl_unprocessed_table_order;
DROP INDEX IF EXISTS idx_icl_unprocessed;

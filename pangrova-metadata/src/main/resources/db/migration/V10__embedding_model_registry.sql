-- ============================================================================
-- V10: Embedding model registry table.
--
-- Persists embedding model configurations registered via
-- CREATE EMBEDDING MODEL DDL. Survives coordinator restarts and supports
-- multi-active HA scenarios (all coordinators share this table).
--
-- Key design points:
--   - version column enables optimistic concurrency for ALTER operations
--   - has_dependents tracks whether any auto-embedding columns reference
--     this model, blocking non-CASCADE DROP
--   - api_key_secret stores an opaque reference resolved at runtime,
--     never the actual credential value
--
-- Idempotent. PostgreSQL 15+ target; H2 (MODE=PostgreSQL) compatible.
-- ============================================================================

CREATE TABLE IF NOT EXISTS pangrova_embedding_models (
    model_name      VARCHAR(256) NOT NULL,
    provider        VARCHAR(128) NOT NULL,
    endpoint        VARCHAR(512) NOT NULL,
    dimension       INT          NOT NULL,
    api_key_secret  VARCHAR(512) NOT NULL,
    max_batch_size  INT          NOT NULL,
    max_rps         INT          NOT NULL,
    query_prefix    TEXT         NOT NULL,
    document_prefix TEXT         NOT NULL,
    version         INT          NOT NULL DEFAULT 0,
    has_dependents  BOOLEAN      NOT NULL DEFAULT FALSE,
    PRIMARY KEY (model_name)
);

-- Lookup index for dependency checks (has_dependents flag queries).
CREATE INDEX IF NOT EXISTS idx_emb_models_dependents
    ON pangrova_embedding_models (model_name, has_dependents);

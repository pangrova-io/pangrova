-- Immutable incarnation fence for embedding model DROP/re-CREATE races.
-- Existing rows share the legacy value safely because model_name remains part
-- of every comparison. Every new CREATE receives a random UUID in application code.
ALTER TABLE pangrova_embedding_models
    ADD COLUMN IF NOT EXISTS incarnation_id VARCHAR(36) NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';

-- Catalog identity is part of every Worker deployment/cache key. DROP followed
-- by CREATE therefore cannot reuse a stale connector, even when all visible
-- catalog properties are identical.
ALTER TABLE catalog_definitions
    ADD COLUMN IF NOT EXISTS incarnation_id VARCHAR(36) NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000';

ALTER TABLE catalog_definitions
    ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

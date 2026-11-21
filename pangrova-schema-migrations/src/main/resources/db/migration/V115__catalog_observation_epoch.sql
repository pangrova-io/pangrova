-- Issued under the same catalog row lock as projection and checkpoint reservations.
ALTER TABLE catalog_definitions ADD COLUMN observation_epoch BIGINT NOT NULL DEFAULT 0
    CHECK (observation_epoch >= 0);

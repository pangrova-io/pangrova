-- Persist the definer's identity groups with the definition revision.
--
-- Row filters, column masks, and role grants can all depend on group
-- membership. Refreshes execute under the persisted definer, including
-- scheduled refreshes that have no interactive session, so the groups observed
-- when the definition was planned must be durable and bound into the revision
-- fingerprint.

ALTER TABLE materialization.definition_revisions
    ADD COLUMN IF NOT EXISTS definer_groups JSONB NOT NULL DEFAULT '[]'::jsonb;

-- A definition binds the planning session, not only its SQL.
--
-- Scheduled and manual refreshes replay the definition exactly; replaying it
-- with different session defaults could change catalog/schema resolution or
-- connector options while the stored plan fingerprint still matched.
ALTER TABLE materialization.definition_revisions
    ADD COLUMN IF NOT EXISTS session_properties JSONB NOT NULL DEFAULT '{}'::jsonb;

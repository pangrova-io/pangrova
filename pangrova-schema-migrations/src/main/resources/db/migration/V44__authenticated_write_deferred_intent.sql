-- Existing unscoped intents are retained but cannot be rehydrated as authenticated writes.
-- Reconcile them explicitly rather than inventing a principal, roles or generated definition.
ALTER TABLE write_commit_journal
    ADD COLUMN principal VARCHAR(255),
    ADD COLUMN deferred_authorization_scope BYTEA,
    ADD COLUMN deferred_work_acknowledged BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE write_commit_journal ADD CONSTRAINT chk_write_commit_authenticated_authority CHECK (
    (principal IS NULL AND deferred_authorization_scope IS NULL) OR
    (principal IS NOT NULL AND btrim(principal) <> '' AND deferred_authorization_scope IS NOT NULL));

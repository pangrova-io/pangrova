-- Canonical generation task reports use namespaced SHA-256 identities.
ALTER TABLE task_state_journal
    ALTER COLUMN query_id TYPE VARCHAR(256),
    ALTER COLUMN task_id TYPE VARCHAR(256);

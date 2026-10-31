-- Keep the ordered row IDs while identifying immutable facts across uncertain COMMIT outcomes.
ALTER TABLE query_state_journal ADD COLUMN event_id UUID;
UPDATE query_state_journal SET event_id = gen_random_uuid();
ALTER TABLE query_state_journal ALTER COLUMN event_id SET NOT NULL;
ALTER TABLE query_state_journal ADD CONSTRAINT query_state_journal_event_identity UNIQUE (event_id);

ALTER TABLE task_state_journal ADD COLUMN event_id UUID;
UPDATE task_state_journal SET event_id = gen_random_uuid();
ALTER TABLE task_state_journal ALTER COLUMN event_id SET NOT NULL;
ALTER TABLE task_state_journal ADD CONSTRAINT task_state_journal_event_identity UNIQUE (event_id);

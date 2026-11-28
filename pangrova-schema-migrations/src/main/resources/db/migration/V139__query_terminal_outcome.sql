ALTER TABLE query_state_journal ADD COLUMN terminal_outcome JSONB;
ALTER TABLE query_state_journal ADD CONSTRAINT query_state_journal_outcome_is_terminal
    CHECK (terminal_outcome IS NULL OR state IN ('FINISHED', 'FAILED', 'CANCELLED'));
CREATE UNIQUE INDEX query_state_journal_recoverable_terminal
    ON query_state_journal (query_id) WHERE terminal_outcome IS NOT NULL;

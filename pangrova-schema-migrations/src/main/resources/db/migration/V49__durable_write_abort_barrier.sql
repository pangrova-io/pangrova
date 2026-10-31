-- Confirmed aborts retain their immutable identity instead of allowing replay
-- to recreate a PENDING write after the original intent has been deleted.
ALTER TABLE write_commit_journal
    ADD CONSTRAINT chk_write_commit_terminal_state CHECK (
        state IN ('PENDING', 'COMMITTED', 'ABORTED')
        AND (state <> 'ABORTED'
            OR (published_version IS NULL AND manifest_sha256 IS NULL))
    );

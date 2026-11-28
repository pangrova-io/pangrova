-- NULL: no cleanup obligation. Zero: fenced abort, awaiting writer exit.
-- A positive token identifies the execution grant that proved physical writer exit.
ALTER TABLE write_commit_journal
    ADD COLUMN staging_drain_token BIGINT,
    ADD CONSTRAINT write_staging_drain_requires_unpublished_abort CHECK (
        staging_drain_token IS NULL OR (
            state = 'ABORTED' AND NOT publication_started
            AND published_version IS NULL AND manifest_sha256 IS NULL
            AND staging_drain_token >= 0
            AND (staging_drain_token = 0 OR staging_drain_token >= execution_fencing_token)
        )
    );

ALTER TABLE write_commit_journal
    ADD COLUMN published_version BIGINT,
    ADD COLUMN manifest_sha256 BYTEA,
    ADD CONSTRAINT chk_write_commit_publication CHECK (
        (published_version IS NULL AND manifest_sha256 IS NULL)
        OR (published_version IS NOT NULL AND published_version > 0
            AND manifest_sha256 IS NOT NULL AND octet_length(manifest_sha256) = 32)
    );

ALTER TABLE write_commit_journal
    ADD COLUMN staging_observed_claim_token BIGINT,
    ADD COLUMN staging_observed_reclaimed_at TIMESTAMPTZ,
    ADD CONSTRAINT write_staging_observed_cleanup_requires_drain CHECK (
        (staging_observed_claim_token IS NULL AND staging_observed_reclaimed_at IS NULL)
        OR (staging_observed_claim_token IS NOT NULL AND staging_drain_token IS NOT NULL
            AND staging_drain_token > 0 AND staging_observed_claim_token >= staging_drain_token)
    );

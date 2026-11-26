-- Queue priority is independent of physical drain evidence and the wall clock.
CREATE SEQUENCE scalar_artifact_reclaim_poll_seq AS BIGINT;

-- Pre-existing V127 rows retain null priority and are considered before rotated/new work.
ALTER TABLE scalar_index_artifact_intents
    ADD COLUMN reclaim_poll_order BIGINT CHECK (reclaim_poll_order > 0);

ALTER TABLE scalar_index_artifact_intents
    ALTER COLUMN reclaim_poll_order SET DEFAULT nextval('scalar_artifact_reclaim_poll_seq');

ALTER SEQUENCE scalar_artifact_reclaim_poll_seq OWNED BY scalar_index_artifact_intents.reclaim_poll_order;

CREATE INDEX idx_scalar_artifact_intents_retry
    ON scalar_index_artifact_intents (reclaim_poll_order NULLS FIRST, drained_at, artifact_digest)
    WHERE drained_at IS NOT NULL;

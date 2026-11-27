ALTER TABLE shuffle_cleanup_intent
    ADD COLUMN revocation_generation BIGINT NOT NULL CHECK (revocation_generation > 0),
    ADD COLUMN converged_at TIMESTAMPTZ,
    ADD COLUMN uncertain_write_handoff BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT shuffle_cleanup_execution_anchor
        FOREIGN KEY (query_id) REFERENCES public.execution_anchor(execution_id) ON DELETE CASCADE;

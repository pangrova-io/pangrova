ALTER TABLE shuffle_cleanup_intent
    ADD COLUMN converged_at TIMESTAMPTZ,
    ADD CONSTRAINT shuffle_cleanup_execution_anchor
        FOREIGN KEY (query_id) REFERENCES public.execution_anchor(execution_id) ON DELETE CASCADE;

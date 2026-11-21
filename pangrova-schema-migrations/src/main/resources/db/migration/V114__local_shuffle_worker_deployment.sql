ALTER TABLE worker_registry
    DROP CONSTRAINT chk_worker_shuffle_deployment,
    ADD CONSTRAINT chk_worker_shuffle_deployment CHECK (
        (shuffle_mode IS NULL AND shuffle_provider IS NULL)
        OR (shuffle_mode IS NOT NULL AND shuffle_mode IN ('NATIVE', 'LOCAL') AND shuffle_provider IS NULL)
        OR (shuffle_mode IS NOT NULL AND shuffle_mode = 'REMOTE' AND shuffle_provider IS NOT NULL
            AND shuffle_provider ~ '^[a-z][a-z0-9-]*$')
    );

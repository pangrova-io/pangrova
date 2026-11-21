-- A zero port preserves fail-closed dispatch for workers without a private mTLS listener.
ALTER TABLE worker_registry
    ADD COLUMN copy_task_tls_port INTEGER NOT NULL DEFAULT 0;

ALTER TABLE worker_registry
    ADD CONSTRAINT chk_worker_copy_task_tls_port
    CHECK (copy_task_tls_port BETWEEN 0 AND 65535);

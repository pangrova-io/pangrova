ALTER TABLE task_state_journal
    ADD COLUMN worker_host TEXT,
    ADD COLUMN worker_http_port INTEGER,
    ADD COLUMN owner_incarnation TEXT,
    ADD COLUMN owner_fencing_token BIGINT,
    ADD COLUMN admission_seal BYTEA,
    ADD CONSTRAINT task_state_journal_issuance_shape CHECK (
        (worker_host IS NULL AND worker_http_port IS NULL AND owner_incarnation IS NULL
            AND owner_fencing_token IS NULL AND admission_seal IS NULL)
        OR
        (state = 'SCHEDULED' AND report_sequence = 0 AND worker_id IS NOT NULL AND coordinator_id IS NOT NULL
            AND worker_host IS NOT NULL AND worker_host <> ''
            AND worker_http_port IS NOT NULL AND worker_http_port BETWEEN 1 AND 65535
            AND owner_incarnation IS NOT NULL AND owner_incarnation <> ''
            AND owner_fencing_token IS NOT NULL AND owner_fencing_token > 0
            AND admission_seal IS NOT NULL AND octet_length(admission_seal) BETWEEN 1 AND 65536)
    );

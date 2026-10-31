-- SI-6: leadership-gated, ownership-scoped journal recovery + zombie fencing.
--
-- Adds a coordinator ownership column and a monotonic epoch to the state journals so recovery and
-- purge operate ONLY on rows owned by the recovering coordinator (never the live leader's queries),
-- and so a promoted coordinator can fence stale (zombie) instructions by epoch. Idempotent column
-- adds; safe to re-run.

ALTER TABLE query_state_journal ADD COLUMN IF NOT EXISTS coordinator_id VARCHAR(128);
ALTER TABLE query_state_journal ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;

ALTER TABLE task_state_journal ADD COLUMN IF NOT EXISTS coordinator_id VARCHAR(128);
ALTER TABLE task_state_journal ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;

-- Recovery scans by owning coordinator, so it can replay/purge only its own rows.
CREATE INDEX IF NOT EXISTS idx_qsj_owner ON query_state_journal (coordinator_id);
CREATE INDEX IF NOT EXISTS idx_tsj_owner ON task_state_journal (coordinator_id);

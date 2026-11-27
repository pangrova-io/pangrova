-- The exact write participants outlive allocation removal until acknowledged.
ALTER TABLE shuffle_write_intent
    DROP CONSTRAINT shuffle_write_intent_shuffle_id_fkey;

-- A NULL next sweep leaves the identity fence in place and retires compensation.
-- Existing fences lack a complete participant set and keep their sweep schedule.
ALTER TABLE shuffle_uncertain_write_tombstone
    ALTER COLUMN next_sweep_at DROP NOT NULL,
    ADD COLUMN reservations_retained BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN final_sweep BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT shuffle_final_sweep_has_lease CHECK (NOT final_sweep OR lease_owner IS NOT NULL);

DROP INDEX shuffle_uncertain_write_tombstone_due_idx;
CREATE INDEX shuffle_uncertain_write_tombstone_due_idx
    ON shuffle_uncertain_write_tombstone (next_sweep_at, lease_expires_at)
    WHERE next_sweep_at IS NOT NULL;

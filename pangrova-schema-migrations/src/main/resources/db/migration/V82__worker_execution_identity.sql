-- Old registrations stay undispatchable until the Worker registers its durable identity.
-- A Worker address, heartbeat, or prior worker_id cannot establish physical incarnation.
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS admission_domain_id VARCHAR(36);
ALTER TABLE worker_registry ADD COLUMN IF NOT EXISTS incarnation_id VARCHAR(36);

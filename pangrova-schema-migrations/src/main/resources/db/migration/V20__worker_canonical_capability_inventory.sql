-- Preserve the exact canonical v2 Native capability inventory across
-- Coordinator failover. The JSON registration API is only the transport
-- envelope; this column stores the Base64-encoded CapabilityInventory proto.
-- Also retain the result data-plane endpoint, which must survive leader change.
ALTER TABLE worker_registry
    ADD COLUMN IF NOT EXISTS result_server_port INT NOT NULL DEFAULT 0;

ALTER TABLE worker_registry
    ADD COLUMN IF NOT EXISTS capability_inventory_proto TEXT DEFAULT NULL;

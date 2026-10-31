-- Worker admission and scheduling use one canonical v2 capability inventory.
-- Do not reconstruct the inventory from the former string projection. Worker
-- registrations are renewable leases, not durable user metadata: registrations
-- without an exact inventory are removed so those workers must complete the
-- canonical handshake again instead of blocking the cluster migration.
DELETE FROM worker_registry
WHERE protocol_version <> 2
   OR capability_inventory_proto IS NULL
   OR BTRIM(capability_inventory_proto) = '';

ALTER TABLE worker_registry
    ALTER COLUMN protocol_version SET DEFAULT 2,
    ALTER COLUMN capability_inventory_proto SET NOT NULL,
    DROP COLUMN capabilities;

ALTER TABLE worker_registry
    ADD CONSTRAINT chk_worker_registry_protocol_v2
        CHECK (protocol_version = 2);

package org.pangrova.table.maintenance;

import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.enums.MaintenanceOperation;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.ownership.TableControlRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/** Ownership-dependent maintenance defaults, selected by the domain service. */
final class TableMaintenanceDefaults {
    private TableMaintenanceDefaults() {}

    /** Defaults for a table Pangrova owns: maintenance is expected and safe. */
    public static TableMaintenancePolicy managedDefaults(UUID tableId, Instant now) {
        return new TableMaintenancePolicy(tableId,
                EnumSet.of(MaintenanceOperation.COMPACT, MaintenanceOperation.OPTIMIZE,
                        MaintenanceOperation.VACUUM, MaintenanceOperation.EXPIRE_SNAPSHOTS,
                        MaintenanceOperation.REMOVE_ORPHAN_FILES),
                10, Duration.ofDays(3), Duration.ofDays(7), false, "", now);
    }

    /**
     * Defaults for an adopted external table: nothing enabled, and the version
     * must be proven stable before touching the table.
     */
    public static TableMaintenancePolicy linkedDefaults(UUID tableId, Instant now) {
        return new TableMaintenancePolicy(tableId, Set.of(), 10,
                Duration.ofDays(3), Duration.ofDays(7), true, "", now);
    }

    /** Chooses the ownership-appropriate default when no policy was configured. */
    public static TableMaintenancePolicy defaultsFor(
            TableControlRecord record, Instant now) {
        return defaultsFor(record.ownershipMode(), record.tableId(), now);
    }

    /** Chooses the ownership-appropriate default for a table not yet registered. */
    public static TableMaintenancePolicy defaultsFor(
            OwnershipMode mode, UUID tableId, Instant now) {
        return mode == OwnershipMode.LINKED
                ? linkedDefaults(tableId, now) : managedDefaults(tableId, now);
    }

}

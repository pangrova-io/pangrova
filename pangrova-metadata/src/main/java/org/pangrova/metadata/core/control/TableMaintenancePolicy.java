package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Authorization and safety limits for maintenance of one controlled table.
 *
 * <p>An absent policy row is not the same as "allow nothing everywhere": the
 * defaults depend on ownership. MANAGED tables start with the plan-approved
 * operations enabled; LINKED tables start with none, because adopting an external
 * table must never imply the right to rewrite its files.</p>
 */
public record TableMaintenancePolicy(
        UUID tableId,
        Set<MaintenanceOperation> enabledOperations,
        int retainLast,
        Duration orphanFileAge,
        Duration snapshotRetention,
        boolean requireStableExternalVersion,
        String updatedBy,
        Instant updatedAt) {

    /** Orphans younger than this may belong to an in-flight write. */
    public static final Duration MIN_ORPHAN_FILE_AGE = Duration.ofDays(1);
    /** Snapshot expiration below this window would erase recovery history. */
    public static final Duration MIN_SNAPSHOT_RETENTION = Duration.ofDays(1);
    /** Keeping a single snapshot removes the history recovery depends on. */
    public static final int MIN_RETAIN_LAST = 1;

    public TableMaintenancePolicy {
        Objects.requireNonNull(tableId, "tableId");
        EnumSet<MaintenanceOperation> operations = EnumSet.noneOf(MaintenanceOperation.class);
        if (enabledOperations != null) {
            operations.addAll(enabledOperations);
        }
        enabledOperations = Collections.unmodifiableSet(operations);
        Objects.requireNonNull(orphanFileAge, "orphanFileAge");
        Objects.requireNonNull(snapshotRetention, "snapshotRetention");
        if (retainLast < MIN_RETAIN_LAST) {
            throw new IllegalArgumentException(
                    "retainLast must be at least " + MIN_RETAIN_LAST + ", got " + retainLast);
        }
        if (orphanFileAge.compareTo(MIN_ORPHAN_FILE_AGE) < 0) {
            throw new IllegalArgumentException("orphanFileAge must be at least "
                    + MIN_ORPHAN_FILE_AGE + ", got " + orphanFileAge);
        }
        if (snapshotRetention.compareTo(MIN_SNAPSHOT_RETENTION) < 0) {
            throw new IllegalArgumentException("snapshotRetention must be at least "
                    + MIN_SNAPSHOT_RETENTION + ", got " + snapshotRetention);
        }
        updatedBy = updatedBy == null ? "" : updatedBy;
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

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

    /**
     * Returns whether the operation is authorized. Compaction aliases and orphan
     * cleanup aliases are treated as one action so a policy cannot be bypassed by
     * spelling the same physical operation differently.
     */
    public boolean allows(MaintenanceOperation operation) {
        Objects.requireNonNull(operation, "operation");
        if (enabledOperations.contains(operation)) {
            return true;
        }
        for (MaintenanceOperation enabled : enabledOperations) {
            if (operation.isCompaction() && enabled.isCompaction()) {
                return true;
            }
            if (operation.isOrphanCleanup() && enabled.isOrphanCleanup()) {
                return true;
            }
        }
        return false;
    }

    public TableMaintenancePolicy withUpdatedBy(String actor, Instant at) {
        return new TableMaintenancePolicy(tableId, enabledOperations, retainLast, orphanFileAge,
                snapshotRetention, requireStableExternalVersion, actor, at);
    }

    public TableMaintenancePolicy withOperations(Set<MaintenanceOperation> operations,
            String actor, Instant at) {
        return new TableMaintenancePolicy(tableId, operations, retainLast, orphanFileAge,
                snapshotRetention, requireStableExternalVersion, actor, at);
    }

    public TableMaintenancePolicy withLimits(int newRetainLast, Duration newOrphanFileAge,
            Duration newSnapshotRetention, boolean requireStable, String actor, Instant at) {
        return new TableMaintenancePolicy(tableId, enabledOperations, newRetainLast,
                newOrphanFileAge, newSnapshotRetention, requireStable, actor, at);
    }
}

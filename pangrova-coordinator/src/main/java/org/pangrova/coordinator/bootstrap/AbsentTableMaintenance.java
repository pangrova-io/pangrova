package org.pangrova.coordinator.bootstrap;

import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Shared no-op table maintenance implementation used by both
 * {@link ModuleIntegrationBootstrap} and {@link ModuleIntegrationWiring}.
 *
 * <p>This class provides a default no-op maintenance SPI implementation and
 * a factory method for creating a default {@link MaintenanceScheduler}.
 * Real compaction/cleanup is connector-specific and gets injected at runtime
 * when connectors register their maintenance implementations.</p>
 *
 * <p>Extracted to eliminate duplication between the two bootstrap classes
 * (F-COORD-26).</p>
 */
final class AbsentTableMaintenance implements TableMaintenanceSpi {

    private static final AbsentTableMaintenance INSTANCE = new AbsentTableMaintenance();

    private AbsentTableMaintenance() {}

    static AbsentTableMaintenance instance() {
        return INSTANCE;
    }

    /**
     * Creates a default MaintenanceScheduler with no-op table maintenance.
     */
    static MaintenanceScheduler createDefaultScheduler() {
        return new MaintenanceScheduler(INSTANCE, MaintenanceConfig.defaults());
    }

    @Override
    public CompletableFuture<CompactionResult> compactAsync(String tableName) {
        return unsupported("compact", tableName);
    }

    @Override
    public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(
            String tableName, Duration olderThan) {
        return unsupported("remove_orphan_files", tableName);
    }

    @Override
    public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(
            String tableName, Duration olderThan) {
        return unsupported("expire_snapshots", tableName);
    }

    /**
     * Fails the operation instead of reporting a successful no-op. A scheduler
     * without a connector-backed maintenance implementation must surface that
     * absence rather than advance its bookkeeping as if work had run.
     */
    private static <T> CompletableFuture<T> unsupported(String operation, String tableName) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "No table maintenance implementation is configured; cannot run " + operation
                        + " on " + tableName));
    }

    @Override
    public MaintenanceStatus getMaintenanceStatus(String tableName) {
        return new MaintenanceStatus(
                tableName, 0, 0, 0L, 0, 0, 0L, 0L, null, null, null);
    }
}

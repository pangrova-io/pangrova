package org.pangrova.coordinator.query.statement;

import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Executes table maintenance statements: COMPACT, VACUUM, OPTIMIZE.
 * Delegates to the {@link TableMaintenanceSpi} provided by the streaming module.
 *
 * <p>Mapping: COMPACT → {@code compactAsync}, VACUUM → {@code cleanOrphansAsync},
 * OPTIMIZE → {@code compactAsync} (optimize = compact in lakehouse semantics).</p>
 */
public final class MaintenanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceExecutor.class);
    private static final Duration DEFAULT_VACUUM_OLDER_THAN = Duration.ofHours(24);

    private final TableMaintenanceSpi maintenance;

    public MaintenanceExecutor(TableMaintenanceSpi maintenance) {
        this.maintenance = maintenance != null ? maintenance : NoOpMaintenance.INSTANCE;
    }

    /** Executes a table compaction statement. */
    public List<TypedRow> executeCompact(CompactStatement stmt) {
        String tableName = String.join(".", stmt.tableName().parts());
        LOG.info("event=compact_table table={}", tableName);
        var result = awaitQuietly(maintenance.compactAsync(tableName));
        return List.of(TypedRow.builder()
                .putString("result", "COMPACT TABLE succeeded")
                .putString("table", tableName)
                .putInt("files_compacted", result != null ? result.filesCompacted() : 0)
                .putInt("files_produced", result != null ? result.filesProduced() : 0)
                .putLong("bytes_rewritten", result != null ? result.bytesRewritten() : 0L)
                .build());
    }

    /** Executes a table vacuum statement. */
    public List<TypedRow> executeVacuum(VacuumStatement stmt) {
        String tableName = String.join(".", stmt.tableName().parts());
        LOG.info("event=vacuum_table table={}", tableName);
        var result = awaitQuietly(maintenance.cleanOrphansAsync(tableName, DEFAULT_VACUUM_OLDER_THAN));
        return List.of(TypedRow.builder()
                .putString("result", "VACUUM TABLE succeeded")
                .putString("table", tableName)
                .putInt("files_removed", result != null ? result.filesRemoved() : 0)
                .putLong("bytes_reclaimed", result != null ? result.bytesReclaimed() : 0L)
                .build());
    }

    /** Executes a table optimization statement. */
    public List<TypedRow> executeOptimize(OptimizeStatement stmt) {
        String tableName = String.join(".", stmt.tableName().parts());
        LOG.info("event=optimize_table table={}", tableName);
        var result = awaitQuietly(maintenance.compactAsync(tableName));
        return List.of(TypedRow.builder()
                .putString("result", "OPTIMIZE TABLE succeeded")
                .putString("table", tableName)
                .putInt("files_compacted", result != null ? result.filesCompacted() : 0)
                .putInt("files_produced", result != null ? result.filesProduced() : 0)
                .putLong("bytes_rewritten", result != null ? result.bytesRewritten() : 0L)
                .build());
    }

    /** Maximum time to wait for a maintenance operation before timing out. */
    private static final Duration MAINTENANCE_TIMEOUT = Duration.ofMinutes(30);

    private static <T> T awaitQuietly(CompletableFuture<T> future) {
        try {
            return future.get(MAINTENANCE_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(
                    "Table maintenance operation timed out after " + MAINTENANCE_TIMEOUT, e);
        } catch (java.util.concurrent.ExecutionException e) {
            LOG.error("event=maintenance_failed error={}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException(
                    "Table maintenance operation failed: " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Table maintenance operation interrupted", e);
        }
    }

    /** Fallback when no TableMaintenanceSpi is configured. Returns zero-result responses. */
    private enum NoOpMaintenance implements TableMaintenanceSpi {
        INSTANCE;

        @Override
        public CompletableFuture<org.pangrova.streaming.maintenance.CompactionResult> compactAsync(String tableName) {
            return CompletableFuture.completedFuture(
                    new org.pangrova.streaming.maintenance.CompactionResult(tableName, 0, 0, 0L));
        }

        @Override
        public CompletableFuture<org.pangrova.streaming.maintenance.OrphanCleanupResult> cleanOrphansAsync(
                String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(
                    new org.pangrova.streaming.maintenance.OrphanCleanupResult(tableName, 0, 0L));
        }

        @Override
        public CompletableFuture<org.pangrova.streaming.maintenance.SnapshotExpirationResult> expireSnapshotsAsync(
                String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(
                    new org.pangrova.streaming.maintenance.SnapshotExpirationResult(tableName, 0));
        }

        @Override
        public org.pangrova.streaming.maintenance.MaintenanceStatus getMaintenanceStatus(String tableName) {
            return new org.pangrova.streaming.maintenance.MaintenanceStatus(
                    tableName, 0, 0, 0L, 0, 0, 0L, 0L, null, null, null);
        }
    }
}

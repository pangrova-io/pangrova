package org.pangrova.coordinator.maintenance;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.statement.ControlledTableMaintenance;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.core.control.MaintenanceOperation;
import org.pangrova.metadata.core.control.MaintenanceRunState;
import org.pangrova.metadata.core.control.TableControlMaintenanceService;
import org.pangrova.metadata.core.control.TableControlRecord;
import org.pangrova.metadata.core.control.TableControlState;
import org.pangrova.metadata.core.control.TableMaintenancePolicy;
import org.pangrova.metadata.core.control.TableMaintenanceRun;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Connector-backed maintenance SPI for truly owned tables.
 *
 * <p>The scheduler may only visit records that exist in the control registry.
 * OBSERVED tables are invisible here, LINKED tables run only operations their
 * policy enables, and every operation goes through
 * {@link ControlledTableMaintenance} so it gets the lease, identity fence,
 * version-stability check, and durable run history. A table whose catalog record
 * is quarantined, stale, or missing is skipped instead of being maintained.</p>
 */
public final class ControlledTableMaintenanceSpi implements TableMaintenanceSpi {

    private static final Logger LOG =
            LoggerFactory.getLogger(ControlledTableMaintenanceSpi.class);
    private static final String SYSTEM_ACTOR = "system:maintenance";
    private static final int RUN_HISTORY_LIMIT = 100;

    private final CatalogRegistry catalogs;
    private final TableControlMaintenanceService policies;
    private final ControlledTableMaintenance executor;
    private final MaintenanceConfig config;
    private final Clock clock;

    public ControlledTableMaintenanceSpi(CatalogRegistry catalogs,
            TableControlMaintenanceService policies, ControlledTableMaintenance executor,
            MaintenanceConfig config) {
        this(catalogs, policies, executor, config, Clock.systemUTC());
    }

    /** Creates the SPI with an explicit clock for deterministic scheduling tests. */
    public ControlledTableMaintenanceSpi(CatalogRegistry catalogs,
            TableControlMaintenanceService policies, ControlledTableMaintenance executor,
            MaintenanceConfig config, Clock clock) {
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.policies = java.util.Objects.requireNonNull(policies, "policies");
        this.executor = java.util.Objects.requireNonNull(executor, "executor");
        this.config = java.util.Objects.requireNonNull(config, "config");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<CompactionResult> compactAsync(String tableName) {
        try {
            return CompletableFuture.completedFuture(compact(tableName));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(
            String tableName, Duration olderThan) {
        try {
            return CompletableFuture.completedFuture(cleanOrphans(tableName, olderThan));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(
            String tableName, Duration olderThan) {
        try {
            return CompletableFuture.completedFuture(expireSnapshots(tableName));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public MaintenanceStatus getMaintenanceStatus(String tableName) {
        QualifiedTableName ref = parse(tableName);
        Optional<TableControlRecord> record = controlled(ref);
        if (record.isEmpty()) {
            return emptyStatus(tableName);
        }
        TableStatistics statistics = statistics(ref).orElse(TableStatistics.UNKNOWN);
        int totalFiles = statistics.sourceWorkUnits() < 0
                ? 0 : (int) Math.min(Integer.MAX_VALUE, statistics.sourceWorkUnits());
        UUID tableId = record.orElseThrow().tableId();
        return new MaintenanceStatus(tableName, 0, totalFiles,
                statistics.dataSize(), 0, 0, 0, 0,
                lastAttempt(tableId, MaintenanceOperation.COMPACT).orElse(null),
                lastAttempt(tableId, MaintenanceOperation.REMOVE_ORPHAN_FILES).orElse(null),
                lastAttempt(tableId, MaintenanceOperation.EXPIRE_SNAPSHOTS).orElse(null));
    }

    @Override
    public boolean isScheduledMaintenanceDue(String tableName) {
        QualifiedTableName ref = parse(tableName);
        return controlled(ref).map(record -> {
            TableMaintenancePolicy policy = policies.policyFor(record);
            return (policy.allows(MaintenanceOperation.COMPACT)
                        && due(record, MaintenanceOperation.COMPACT, config.compactionInterval()))
                    || (policy.allows(MaintenanceOperation.EXPIRE_SNAPSHOTS)
                        && due(record, MaintenanceOperation.EXPIRE_SNAPSHOTS,
                                config.snapshotExpirationInterval()))
                    || (policy.allows(MaintenanceOperation.REMOVE_ORPHAN_FILES)
                        && due(record, MaintenanceOperation.REMOVE_ORPHAN_FILES,
                                config.orphanCleanupInterval()));
        }).orElse(false);
    }

    @Override
    public CompletableFuture<Void> runScheduledMaintenanceAsync(String tableName) {
        try {
            runScheduled(tableName);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void runScheduled(String tableName) {
        QualifiedTableName ref = parse(tableName);
        Optional<TableControlRecord> found = controlled(ref);
        if (found.isEmpty()) {
            return;
        }
        TableControlRecord record = found.orElseThrow();
        TableMaintenancePolicy policy = policies.policyFor(record);
        for (MaintenanceOperation operation : List.of(
                MaintenanceOperation.COMPACT,
                MaintenanceOperation.EXPIRE_SNAPSHOTS,
                MaintenanceOperation.REMOVE_ORPHAN_FILES)) {
            if (!policy.allows(operation) || !due(record, operation, intervalFor(operation))) {
                continue;
            }
            try {
                executor.executeAutomatic(operation, ref);
            } catch (AccessDeniedException denied) {
                // An active external writer or a state transition between the due
                // check and execution aborts this operation; the run is recorded as
                // skipped and the next cycle re-evaluates.
                LOG.warn("event=scheduled_maintenance_skipped table={} operation={} reason={}",
                        record.qualifiedName(), operation, denied.getMessage());
            }
        }
    }

    private CompactionResult compact(String tableName) {
        QualifiedTableName ref = parse(tableName);
        Optional<TableControlRecord> found = controlled(ref);
        if (found.isEmpty()) {
            return new CompactionResult(tableName, 0, 0, 0);
        }
        TableControlRecord record = found.orElseThrow();
        if (!policies.policyFor(record).allows(MaintenanceOperation.COMPACT)) {
            LOG.debug("event=maintenance_not_enabled table={} operation=COMPACT",
                    record.qualifiedName());
            return new CompactionResult(tableName, 0, 0, 0);
        }
        try {
            TypedRow row = firstRow(executor.executeAutomatic(MaintenanceOperation.COMPACT, ref));
            return new CompactionResult(tableName,
                    intValue(row, "files_compacted"),
                    intValue(row, "files_produced"),
                    longValue(row, "bytes_rewritten"));
        } catch (AccessDeniedException denied) {
            LOG.warn("event=scheduled_maintenance_skipped table={} operation=COMPACT reason={}",
                    record.qualifiedName(), denied.getMessage());
            return new CompactionResult(tableName, 0, 0, 0);
        }
    }

    private OrphanCleanupResult cleanOrphans(String tableName, Duration olderThan) {
        QualifiedTableName ref = parse(tableName);
        Optional<TableControlRecord> found = controlled(ref);
        if (found.isEmpty()) {
            return new OrphanCleanupResult(tableName, 0, 0);
        }
        TableControlRecord record = found.orElseThrow();
        if (!policies.policyFor(record).allows(MaintenanceOperation.REMOVE_ORPHAN_FILES)) {
            return new OrphanCleanupResult(tableName, 0, 0);
        }
        try {
            TypedRow row = firstRow(executor.executeAutomatic(
                    MaintenanceOperation.REMOVE_ORPHAN_FILES, ref));
            return new OrphanCleanupResult(tableName,
                    intValue(row, "deleted_file_count"),
                    longValue(row, "deleted_bytes"));
        } catch (AccessDeniedException denied) {
            LOG.warn("event=scheduled_maintenance_skipped table={} operation=VACUUM reason={}",
                    record.qualifiedName(), denied.getMessage());
            return new OrphanCleanupResult(tableName, 0, 0);
        }
    }

    private SnapshotExpirationResult expireSnapshots(String tableName) {
        QualifiedTableName ref = parse(tableName);
        Optional<TableControlRecord> found = controlled(ref);
        if (found.isEmpty()) {
            return new SnapshotExpirationResult(tableName, 0);
        }
        TableControlRecord record = found.orElseThrow();
        if (!policies.policyFor(record).allows(MaintenanceOperation.EXPIRE_SNAPSHOTS)) {
            return new SnapshotExpirationResult(tableName, 0);
        }
        try {
            TypedRow row = firstRow(executor.executeAutomatic(
                    MaintenanceOperation.EXPIRE_SNAPSHOTS, ref));
            return new SnapshotExpirationResult(tableName,
                    intValue(row, "expired_snapshots"));
        } catch (AccessDeniedException denied) {
            LOG.warn("event=scheduled_maintenance_skipped table={} operation=EXPIRE_SNAPSHOTS "
                    + "reason={}", record.qualifiedName(), denied.getMessage());
            return new SnapshotExpirationResult(tableName, 0);
        }
    }

    private Optional<TableControlRecord> controlled(QualifiedTableName ref) {
        if (!catalogs.tableControl().enabled()) {
            return Optional.empty();
        }
        return catalogs.tableControl().find(ref.catalog(), ref.schema(), ref.table())
                .filter(record -> record.state() == TableControlState.ACTIVE);
    }

    private Optional<TableStatistics> statistics(QualifiedTableName ref) {
        try {
            ConnectorSession session = SessionContextConverter.toConnectorSession(
                    systemContext(ref), ref.catalog(), ref.schema());
            ConnectorMetadata metadata =
                    catalogs.getCatalog(ref.catalog()).getMetadata(session);
            ConnectorTableHandle handle = metadata.getTableHandle(
                    session, new SchemaTableName(ref.schema(), ref.table()));
            if (handle == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(metadata.getTableStatistics(session, handle));
        } catch (RuntimeException failure) {
            LOG.debug("event=maintenance_status_unavailable table={} error={}",
                    ref, failure.getMessage());
            return Optional.empty();
        }
    }

    private boolean due(TableControlRecord record, MaintenanceOperation operation,
            Duration interval) {
        return lastAttempt(record.tableId(), operation)
                .map(finishedAt -> !clock.instant().isBefore(finishedAt.plus(interval)))
                .orElse(true);
    }

    private Optional<Instant> lastAttempt(UUID tableId, MaintenanceOperation operation) {
        return policies.listRuns(tableId, RUN_HISTORY_LIMIT).stream()
                .filter(run -> run.state() != MaintenanceRunState.RUNNING)
                .filter(run -> sameAction(run.operation(), operation))
                .map(TableMaintenanceRun::finishedAt)
                .findFirst();
    }

    private static boolean sameAction(MaintenanceOperation left, MaintenanceOperation right) {
        if (left == right) {
            return true;
        }
        if (right.isCompaction()) {
            return left.isCompaction();
        }
        if (right.isOrphanCleanup()) {
            return left.isOrphanCleanup();
        }
        return false;
    }

    private Duration intervalFor(MaintenanceOperation operation) {
        if (operation == MaintenanceOperation.EXPIRE_SNAPSHOTS) {
            return config.snapshotExpirationInterval();
        }
        if (operation.isOrphanCleanup()) {
            return config.orphanCleanupInterval();
        }
        return config.compactionInterval();
    }

    private CoordinatorSessionContext systemContext(QualifiedTableName ref) {
        return new CoordinatorSessionContext(
                "maintenance-status-" + UUID.randomUUID(),
                Map.of("catalog", ref.catalog(), "schema", ref.schema()),
                new UserIdentity(SYSTEM_ACTOR, Set.of(), Map.of(), clock.instant()),
                "system");
    }

    private static MaintenanceStatus emptyStatus(String tableName) {
        return new MaintenanceStatus(tableName, 0, 0, 0L, 0, 0, 0L, 0L,
                null, null, null);
    }

    private static TypedRow firstRow(List<TypedRow> rows) {
        return rows == null || rows.isEmpty() ? null : rows.get(0);
    }

    private static int intValue(TypedRow row, String column) {
        Long value = row == null ? null : row.getLong(column);
        return value == null ? 0 : (int) Math.min(Integer.MAX_VALUE, value);
    }

    private static long longValue(TypedRow row, String column) {
        Long value = row == null ? null : row.getLong(column);
        return value == null ? 0L : value;
    }

    static QualifiedTableName parse(String tableName) {
        String[] parts = tableName.split("\\.");
        return switch (parts.length) {
            case 3 -> new QualifiedTableName(parts[0], parts[1], parts[2]);
            case 2 -> new QualifiedTableName("default", parts[0], parts[1]);
            case 1 -> new QualifiedTableName("default", "default", parts[0]);
            default -> throw new IllegalArgumentException(
                    "Invalid maintenance table name: " + tableName);
        };
    }
}

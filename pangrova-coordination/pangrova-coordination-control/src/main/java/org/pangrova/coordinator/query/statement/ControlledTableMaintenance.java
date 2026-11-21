package org.pangrova.coordinator.query.statement;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.coordinator.clustering.WorkerReclusterDispatcher;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.session.SessionTableNames;
import org.pangrova.metadata.resolution.ConnectorTableLookup;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.enums.MaintenanceOperation;
import org.pangrova.table.api.enums.MaintenanceRunState;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.maintenance.TableControlMaintenanceService;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.clustering.ClusteringMaintenance;
import org.pangrova.connector.api.clustering.ClusteringPublicationException;
import org.pangrova.connector.api.enums.ClusteringMode;
import org.pangrova.connector.api.enums.ClusteringState;
import org.pangrova.parser.api.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.api.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.api.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Executes maintenance only for controlled tables, under a policy and a
 * distributed lease.
 *
 * <p>OBSERVED tables are rejected without touching the connector. LINKED tables
 * execute only operations their policy explicitly enables, and only after the
 * format version is observed twice and found stable: an external writer that
 * commits between those observations aborts the run. MANAGED tables use the
 * ownership defaults. Every attempt that reaches execution is recorded in
 * {@code table_control_maintenance_run} with the version observed beforehand.</p>
 */
public final class ControlledTableMaintenance {
    private final ExecutionTraceContextFactory traceContexts;

    private static final Logger LOG = LoggerFactory.getLogger(ControlledTableMaintenance.class);
    private static final Duration DEFAULT_LEASE_TTL = Duration.ofMinutes(30);
    private static final String SOURCE = "MAINTENANCE";
    private static final String CLUSTERING_REVISION = "clustering_revision";
    private static final String SYSTEM_ACTOR = "system:maintenance";
    private static final String MODE_MANUAL = "MANUAL";
    private static final String MODE_AUTOMATIC = "AUTOMATIC";

    private final CatalogRegistry catalogs;
    private final TableControlMaintenanceService policyService;
    private final DistributedLease lease;
    private final String holderId;
    private final Duration leaseTtl;
    private final Clock clock;
    private volatile WorkerReclusterDispatcher reclusterDispatcher;
    private volatile org.pangrova.coordinator.query.audit.TableControlAuditEmitter audit;

    public ControlledTableMaintenance(CatalogRegistry catalogs,
            TableControlMaintenanceService policyService, DistributedLease lease,
            String holderId, ExecutionTraceContextFactory traceContexts) {
        this(catalogs, policyService, lease, holderId, DEFAULT_LEASE_TTL, Clock.systemUTC(), traceContexts);
    }

    /** Creates the executor with explicit lease TTL and clock for deterministic tests. */
    public ControlledTableMaintenance(CatalogRegistry catalogs,
            TableControlMaintenanceService policyService, DistributedLease lease,
            String holderId, Duration leaseTtl, Clock clock, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.policyService = java.util.Objects.requireNonNull(policyService, "policyService");
        this.lease = lease;
        this.holderId = holderId == null || holderId.isBlank() ? "coordinator" : holderId;
        this.leaseTtl = leaseTtl == null ? DEFAULT_LEASE_TTL : leaseTtl;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    /** Records every maintenance attempt in the durable audit trail. */
    public void setTableControlAudit(
            org.pangrova.coordinator.query.audit.TableControlAuditEmitter emitter) {
        this.audit = emitter;
    }

    /** Provides staging for SQL RECLUSTER on a process with authenticated Workers. */
    public void setReclusterDispatcher(WorkerReclusterDispatcher dispatcher) {
        reclusterDispatcher = java.util.Objects.requireNonNull(dispatcher, "dispatcher");
    }

    /** Runs the operation, returning the connector's result rows. */
    public List<TypedRow> execute(MaintenanceOperation operation, Identifier tableName,
            QuerySession context) {
        return execute(operation, SessionTableNames.resolveTableRef(tableName.parts(), context),
                context, MODE_MANUAL, Map.of());
    }

    /** Sets the clustering declaration under the table maintenance lease. */
    public List<TypedRow> setClusteringOrder(SetClusteringOrderStatement statement, QuerySession context) {
        var target = clusteringTarget(statement.tableName(), context, true);
        var mode = ClusteringMode.valueOf(statement.mode() == null ? "INTERLEAVE"
                : statement.mode().toUpperCase(Locale.ROOT));
        return withClusteringLease(target, authority -> {
            requireConfirmedClustering(target, context, authority);
            target.connector().setOrder(statement.columnNames(), mode, authority);
            return List.of(TypedRow.builder().putString("table", target.name().toString())
                    .putString("result", "CLUSTERING ORDER SET").putString("mode", mode.name()).build());
        });
    }

    /** Runs a journaled clustering rewrite and returns the confirmed snapshot. */
    public List<TypedRow> recluster(ReclusterTableStatement statement, QuerySession context) {
        if (statement.whereCondition().isPresent()) {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    "RECLUSTER WHERE requires a partition-scoped connector rewrite");
        }
        var target = clusteringTarget(statement.tableName(), context, true);
        var dispatcher = reclusterDispatcher;
        if (dispatcher == null) {
            throw new IllegalStateException("RECLUSTER requires authenticated Worker staging");
        }
        return withClusteringLease(target, authority -> {
            requireConfirmedClustering(target, context, authority);
            String actor = context.userIdentity() == null ? "" : context.userIdentity().principal();
            String revision = target.connector().revision();
            TableMaintenanceRun run = policyService.beginRun(target.record(), MaintenanceOperation.RECLUSTER,
                    MODE_MANUAL, observeVersion(target.metadata(), target.session(), target.handle()), actor,
                    Map.of(CLUSTERING_REVISION, revision));
            OptionalLong snapshot;
            try {
                var catalog = catalogs.getCatalog(target.name().catalog()).getDescriptor();
                var stage = dispatcher.bind(catalog, target.name(),
                        target.session().getProperties().get("tenant_id"), target.session().getUser(), run.runId());
                snapshot = target.connector().rewrite(run.runId(), revision, stage, () -> {
                    authority.run();
                    var current = catalogs.getCatalog(target.name().catalog()).getDescriptor();
                    if (!catalog.identity().equals(current.identity())) {
                        throw new IllegalStateException("RECLUSTER catalog deployment changed after admission");
                    }
                });
            } catch (ClusteringPublicationException unconfirmed) {
                throw unconfirmed;
            } catch (RuntimeException failure) {
                var failed = policyService.finishRun(run, MaintenanceRunState.FAILED, "",
                        Map.of("reason", String.valueOf(failure.getMessage())));
                audit(context, target.name(), target.record(), failed, failure.getMessage());
                throw failure;
            }
            authority.run();
            var completed = finishClustering(target, context, run, snapshot);
            return List.of(TypedRow.builder().putString("result", "RECLUSTER SUCCEEDED")
                    .putString("table", target.name().toString()).putString("task_id", run.runId().toString())
                    .putString("snapshot_id", completed.versionAfter()).build());
        });
    }

    /** Reports the current declaration, file coverage and the latest durable rewrite state. */
    public List<TypedRow> showClusteringStatus(ShowClusteringStatusStatement statement, QuerySession context) {
        var target = clusteringTarget(statement.tableName(), context, false);
        Optional<TableMaintenanceRun> lastRun = latestClusteringRun(target);
        if (lastRun.filter(run -> run.state() == MaintenanceRunState.RUNNING).isPresent()) {
            var acquired = acquireClusteringLease(target);
            if (acquired.isPresent()) {
                var held = acquired.orElseThrow();
                try (var guard = new MaintenanceLeaseGuard(lease, held, leaseTtl)) {
                    lastRun = recoverClustering(target, context, guardedClustering(target, guard));
                } finally {
                    lease.release(held.leaseKey(), held.holderId(), held.epoch());
                }
            }
        }
        var status = target.connector().status();
        boolean restoring = lastRun.filter(run -> run.state() == MaintenanceRunState.RUNNING).isPresent();
        double ratio = status.totalFiles() == 0 ? 0d : status.clusteredFiles() * 100d / status.totalFiles();
        return List.of(TypedRow.builder().putString("table", target.name().toString())
                .putString("mode", status.mode() == null ? "NONE" : status.mode().name())
                .putString("columns", String.join(", ", status.columns()))
                .putString("clustering_ratio", String.format(Locale.ROOT, "%.2f%%", ratio))
                .putInt("total_files", status.totalFiles()).putInt("clustered_files", status.clusteredFiles())
                .putString("state", clusteringState(status, restoring).name())
                .putString("task_id", lastRun.map(run -> run.runId().toString()).orElse(""))
                .putString("task_state", lastRun.map(run -> run.state().name()).orElse(""))
                .build());
    }

    private static ClusteringState clusteringState(ClusteringMaintenance.Status status, boolean running) {
        if (running) return ClusteringState.RECLUSTERING;
        if (status.mode() == null) return ClusteringState.NONE;
        if (status.clusteredFiles() == 0) return ClusteringState.PENDING;
        return status.clusteredFiles() == status.totalFiles() ? ClusteringState.CLUSTERED : ClusteringState.STALE;
    }

    private ClusteringTarget clusteringTarget(Identifier tableName, QuerySession context, boolean mutate) {
        var ref = SessionTableNames.resolveTableRef(tableName.parts(), context);
        var control = catalogs.tableControl();
        var record = requireControlled(control, ref, MaintenanceOperation.RECLUSTER);
        if (mutate && (record.ownershipMode() != OwnershipMode.MANAGED
                || !policyService.policyFor(record).allows(MaintenanceOperation.RECLUSTER))) {
            throw new AccessDeniedException("Clustering mutation requires a MANAGED table with RECLUSTER enabled");
        }
        var session = SessionContextConverter.toConnectorSession(context, ref.catalog(), ref.schema());
        var metadata = ConnectorTableLookup.getMetadata(catalogs, ref.catalog(), session);
        var handle = ConnectorTableLookup.requireTableHandle(metadata, session, ref);
        control.requireActiveIdentity(record, metadata.getTableIdentity(session, handle), "CLUSTERING");
        var connector = metadata.clusteringMaintenance(session, handle).orElseThrow(() ->
                new PangrovaException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Clustering is unavailable for " + ref));
        return new ClusteringTarget(ref, record, session, metadata, handle, connector);
    }

    private record ClusteringTarget(QualifiedTableName name, TableControlRecord record, ConnectorSession session,
            ConnectorMetadata metadata, ConnectorTableHandle handle, ClusteringMaintenance connector) {}

    @FunctionalInterface
    private interface ClusteringAction {
        List<TypedRow> execute(Runnable authority);
    }

    private List<TypedRow> withClusteringLease(ClusteringTarget target, ClusteringAction action) {
        var held = acquireClusteringLease(target).orElseThrow(() ->
                new AccessDeniedException("Table maintenance is already in progress for " + target.name()));
        try (var guard = new MaintenanceLeaseGuard(lease, held, leaseTtl)) {
            var authority = guardedClustering(target, guard);
            authority.run();
            return action.execute(authority);
        } finally {
            lease.release(held.leaseKey(), held.holderId(), held.epoch());
        }
    }

    private Optional<LeaseEntry> acquireClusteringLease(ClusteringTarget target) {
        if (lease == null) throw new AccessDeniedException("Clustering requires durable coordination");
        return lease.tryAcquire("table-maintenance:" + target.record().tableId(), holderId + ":" + UUID.randomUUID(),
                leaseTtl);
    }

    private Runnable guardedClustering(ClusteringTarget target, MaintenanceLeaseGuard guard) {
        return () -> {
            var fresh = ConnectorTableLookup.requireTableHandle(target.metadata(), target.session(), target.name());
            catalogs.tableControl().requireActiveIdentity(target.record(),
                    target.metadata().getTableIdentity(target.session(), fresh), "CLUSTERING");
            guard.requireHeld();
        };
    }

    private Optional<TableMaintenanceRun> latestClusteringRun(ClusteringTarget target) {
        return policyService.findLatestRun(target.record().tableId(), MaintenanceOperation.RECLUSTER);
    }

    private void requireConfirmedClustering(ClusteringTarget target, QuerySession context, Runnable authority) {
        if (recoverClustering(target, context, authority)
                .filter(run -> run.state() == MaintenanceRunState.RUNNING).isPresent()) {
            throw new IllegalStateException("A previous RECLUSTER still requires publication confirmation");
        }
    }

    private Optional<TableMaintenanceRun> recoverClustering(
            ClusteringTarget target, QuerySession context, Runnable authority) {
        var lastRun = latestClusteringRun(target);
        if (lastRun.isEmpty() || lastRun.orElseThrow().state() != MaintenanceRunState.RUNNING) return lastRun;
        authority.run();
        var run = lastRun.orElseThrow();
        var evidence = target.connector().reconcile(run.runId(),
                run.details().get(CLUSTERING_REVISION), authority);
        authority.run();
        if (evidence.aborted()) {
            String reason = "Unpublished rewrite fenced at the catalog";
            var stopped = policyService.finishRun(run, MaintenanceRunState.FAILED, "",
                    Map.of("reason", reason));
            audit(context, target.name(), target.record(), stopped, reason);
            return Optional.of(stopped);
        }
        return evidence.snapshot().isPresent()
                ? Optional.of(finishClustering(target, context, run, evidence.snapshot())) : lastRun;
    }

    private TableMaintenanceRun finishClustering(
            ClusteringTarget target, QuerySession context, TableMaintenanceRun run, OptionalLong snapshot) {
        String version = snapshot.isPresent() ? Long.toString(snapshot.getAsLong()) : run.versionBefore();
        var completed = policyService.finishRun(run, MaintenanceRunState.SUCCEEDED, version,
                Map.of("published", Boolean.toString(snapshot.isPresent())));
        audit(context, target.name(), target.record(), completed, null);
        return completed;
    }

    /** CALL uses the same ownership, policy, lease and identity boundary as maintenance SQL. */
    public List<TypedRow> executeProcedure(String procedure, QualifiedTableName ref,
            Map<String, String> arguments, QuerySession context) {
        MaintenanceOperation operation = MaintenanceOperation.parse(procedure)
                .orElseThrow(() -> new AccessDeniedException("Procedure " + procedure
                        + " has no table-control maintenance policy; execution is not authorized"));
        if (operation == MaintenanceOperation.RECLUSTER) {
            throw new AccessDeniedException("Use RECLUSTER TABLE for governed clustering");
        }
        return execute(operation, ref, context, MODE_MANUAL, arguments);
    }

    /**
     * Runs a leader-owned maintenance operation without a client statement.
     *
     * <p>The run uses a synthetic system session but the same ownership, policy,
     * lease, and version-stability checks as a manual statement. Its history is
     * recorded as {@code AUTOMATIC}.</p>
     */
    public List<TypedRow> executeAutomatic(MaintenanceOperation operation,
            QualifiedTableName ref) {
        return execute(operation, ref, systemContext(ref), MODE_AUTOMATIC, Map.of());
    }

    private List<TypedRow> execute(MaintenanceOperation operation, QualifiedTableName ref,
            QuerySession context, String mode, Map<String, String> arguments) {
        TableControlService control = catalogs.tableControl();
        TableControlRecord record = requireControlled(control, ref, operation);
        TableMaintenancePolicy policy = policyService.policyFor(record);
        if (!policy.allows(operation)) {
            throw new AccessDeniedException(operation + " is not enabled for "
                    + record.qualifiedName() + " (" + record.ownershipMode()
                    + "); enabled operations: " + policy.enabledOperations());
        }
        Map<String, String> connectorArguments = connectorArguments(ref, record, operation, policy, arguments);

        ConnectorSession session = SessionContextConverter.toConnectorSession(
                context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = ConnectorTableLookup.getMetadata(
                catalogs, ref.catalog(), session);
        ConnectorTableHandle handle = ConnectorTableLookup.requireTableHandle(metadata, session, ref);
        String actor = context.userIdentity() == null ? "" : context.userIdentity().principal();

        String leaseKey = "table-maintenance:" + record.tableId();
        if (lease == null) {
            throw new AccessDeniedException(operation + " requires a distributed lease; "
                    + "coordination is not configured, so two coordinators could run it at once");
        }
        // The lease SPI is reentrant for a holder. Each attempt must have its own
        // holder, otherwise two requests on one coordinator both acquire it.
        String attemptHolder = holderId + ":" + UUID.randomUUID();
        Optional<LeaseEntry> acquired = lease.tryAcquire(leaseKey, attemptHolder, leaseTtl);
        if (acquired.isEmpty()) {
            TableMaintenanceRun skipped = policyService.beginRun(
                    record, operation, mode, "", actor);
            String reason = "maintenance lease held by another coordinator";
            TableMaintenanceRun finished = policyService.finishRun(
                    skipped, MaintenanceRunState.SKIPPED, "", Map.of("reason", reason));
            audit(context, ref, record, finished, reason);
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Maintenance already in progress for " + record.qualifiedName());
        }

        TableMaintenanceRun run = null;
        boolean executionStarted = false;
        try (var ownership = new MaintenanceLeaseGuard(lease, acquired.orElseThrow(), leaseTtl)) {
            String versionBefore = observeVersion(metadata, session, handle);
            run = policyService.beginRun(record, operation, mode, versionBefore, actor);
            requireStableVersion(record, policy, metadata, session, ref, versionBefore);
            control.requireActiveIdentity(record, metadata.getTableIdentity(session, handle),
                    operation.name());
            ownership.requireHeld();

            executionStarted = true;
            List<TypedRow> rows = executeConnector(
                    metadata, session, operation, connectorArguments);
            String versionAfter = observeVersionFresh(metadata, session, ref);
            control.requireActiveIdentity(record, metadata.getTableIdentity(session, handle),
                    operation.name());
            ownership.requireHeld();
            TableMaintenanceRun finished = policyService.finishRun(
                    run, MaintenanceRunState.SUCCEEDED, versionAfter,
                    Map.of("rows", String.valueOf(rows.size())));
            audit(context, ref, record, finished, null);
            refreshControlVersion(control, record, versionAfter);
            LOG.info("event=table_maintenance_succeeded table={} tableId={} operation={} "
                            + "versionBefore={} versionAfter={}",
                    record.qualifiedName(), record.tableId(), operation,
                    versionBefore, versionAfter);
            return rows;
        } catch (RuntimeException failure) {
            if (run != null) {
                boolean rejected = !executionStarted && failure instanceof AccessDeniedException;
                TableMaintenanceRun finished = policyService.finishRun(run,
                        rejected ? MaintenanceRunState.SKIPPED : MaintenanceRunState.FAILED, "",
                        Map.of("error", failure.getClass().getSimpleName(),
                                "reason", String.valueOf(failure.getMessage())));
                audit(context, ref, record, finished, failure.getMessage());
            }
            LOG.warn("event=table_maintenance_failed table={} tableId={} operation={} error={}",
                    record.qualifiedName(), record.tableId(), operation, failure.getMessage());
            throw failure;
        } finally {
            acquired.ifPresent(entry -> lease.release(leaseKey, attemptHolder, entry.epoch()));
        }
    }

    private TableControlRecord requireControlled(TableControlService control,
            QualifiedTableName ref, MaintenanceOperation operation) {
        if (!control.enabled()) {
            throw new AccessDeniedException(operation + " requires the table control plane: "
                    + "without ownership records no table can be proven safe to maintain");
        }
        TableControlRecord record = control.find(ref.catalog(), ref.schema(), ref.table())
                .orElseThrow(() -> new AccessDeniedException(operation + " rejected: "
                        + ref + " is OBSERVED; run ALTER TABLE ... ADOPT before maintenance"));
        if (record.state() != TableControlState.ACTIVE) {
            throw new AccessDeniedException(operation + " rejected: the control record for "
                    + ref + " is " + record.state() + "; maintenance requires ACTIVE");
        }
        if (record.ownershipMode() == OwnershipMode.OBSERVED) {
            throw new AccessDeniedException(operation + " rejected: " + ref + " is OBSERVED");
        }
        return record;
    }

    /**
     * Detects an external writer between planning and execution. The version is
     * read from a fresh handle so a cached handle cannot mask a newer snapshot.
     * LINKED tables fail closed when the connector cannot prove stability.
     */
    private void requireStableVersion(TableControlRecord record, TableMaintenancePolicy policy,
            ConnectorMetadata metadata, ConnectorSession session, QualifiedTableName ref,
            String versionBefore) {
        if (!policy.requireStableExternalVersion()) {
            return;
        }
        if (versionBefore.isEmpty()) {
            throw new AccessDeniedException("Maintenance rejected: " + record.qualifiedName()
                    + " is LINKED but the connector cannot report a version token; "
                    + "active external writers cannot be detected");
        }
        String current = observeVersionFresh(metadata, session, ref);
        if (!versionBefore.equals(current)) {
            throw new AccessDeniedException("Maintenance abandoned: the version of "
                    + record.qualifiedName() + " changed between observations ("
                    + versionBefore + " -> " + current + "); an external writer is active");
        }
    }

    private Map<String, String> connectorArguments(QualifiedTableName ref,
            TableControlRecord record, MaintenanceOperation operation,
            TableMaintenancePolicy policy, Map<String, String> requested) {
        Map<String, String> args = new LinkedHashMap<>();
        args.put("arg0", ref.schema() + "." + ref.table());
        int maximumArguments = 1;
        if (operation.isOrphanCleanup()) {
            Instant cutoff = clock.instant().minus(policy.orphanFileAge());
            args.put("arg1", String.valueOf(cutoff.toEpochMilli()));
            args.put("arg2", "false");
            maximumArguments = 3;
        } else if (operation == MaintenanceOperation.EXPIRE_SNAPSHOTS) {
            appendExpirationArguments(args, record, policy);
            maximumArguments = 4;
        }
        for (String key : requested.keySet()) {
            if (!args.containsKey(key)) {
                throw new IllegalArgumentException(operation + " accepts at most " + maximumArguments + " arguments");
            }
        }
        for (int index = 1; index < maximumArguments; index++) {
            String key = "arg" + index;
            if (!requested.containsKey(key)) {
                continue;
            }
            String value = requested.get(key);
            validateRetentionArgument(operation, record.format(), index,
                    index == maximumArguments - 1, value, args.get(key));
            args.put(key, value);
        }
        if (operation == MaintenanceOperation.EXPIRE_SNAPSHOTS && "PAIMON".equalsIgnoreCase(record.format())
                && Long.parseLong(args.get("arg1")) < Long.parseLong(args.get("arg2"))) {
            throw new IllegalArgumentException("retain_max must be at least retain_min");
        }
        return args;
    }

    private static void validateRetentionArgument(MaintenanceOperation operation, String format,
            int index, boolean dryRun, String value, String policyLimit) {
        if (dryRun) {
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException("dry_run must be true or false");
            }
            return;
        }
        long number = Long.parseLong(value);
        long limit = Long.parseLong(policyLimit);
        boolean count = operation == MaintenanceOperation.EXPIRE_SNAPSHOTS
                && (index == 1 || "PAIMON".equalsIgnoreCase(format));
        if (count ? number < limit || number > Integer.MAX_VALUE : number > limit) {
            throw new AccessDeniedException(operation + " argument " + index
                    + " violates the table maintenance retention policy");
        }
    }

    private List<TypedRow> executeConnector(ConnectorMetadata metadata, ConnectorSession session,
            MaintenanceOperation operation, Map<String, String> args) {
        List<TypedRow> rows;
        if (operation.isCompaction()) {
            rows = metadata.executeProcedure(session, "compact", args);
        } else if (operation.isOrphanCleanup()) {
            rows = metadata.executeProcedure(session, "remove_orphan_files", args);
        } else if (operation == MaintenanceOperation.EXPIRE_SNAPSHOTS) {
            rows = metadata.executeProcedure(session, "expire_snapshots", args);
        } else if (operation == MaintenanceOperation.REWRITE_MANIFESTS) {
            rows = metadata.executeProcedure(session, "rewrite_manifests", args);
        } else {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    operation + " is authorized but has no executor yet; refusing to report "
                            + "success without running it");
        }
        return rows == null ? List.of() : rows;
    }

    /**
     * Both connectors expose {@code expire_snapshots}, but their arguments differ:
     * Iceberg accepts a retain count plus a time horizon, while Paimon retains a
     * snapshot-count window. Sending one connector's arguments to the other would
     * either fail validation or expire more history than the policy allows.
     */
    private void appendExpirationArguments(Map<String, String> args,
            TableControlRecord record, TableMaintenancePolicy policy) {
        String format = record.format() == null
                ? "" : record.format().toUpperCase(java.util.Locale.ROOT);
        if ("PAIMON".equals(format)) {
            args.put("arg1", String.valueOf(policy.retainLast()));
            args.put("arg2", String.valueOf(policy.retainLast()));
            args.put("arg3", "false");
            return;
        }
        if ("ICEBERG".equals(format)) {
            long olderThan = clock.instant().minus(policy.snapshotRetention()).toEpochMilli();
            args.put("arg1", String.valueOf(policy.retainLast()));
            args.put("arg2", String.valueOf(olderThan));
            args.put("arg3", "false");
            return;
        }
        throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                "expire_snapshots is not supported for format " + record.format()
                        + " on " + record.qualifiedName());
    }

    private QuerySession systemContext(QualifiedTableName ref) {
        return new QuerySession(
                "maintenance-" + UUID.randomUUID(),
                Map.of("catalog", ref.catalog(), "schema", ref.schema()),
                new UserIdentity(SYSTEM_ACTOR, Set.of(), Map.of(), clock.instant()),
                "system", java.util.Optional.empty(), traceContexts.capture());
    }

    private void audit(QuerySession context, QualifiedTableName ref,
            TableControlRecord record, TableMaintenanceRun run, String error) {
        var sink = audit;
        if (sink != null) {
            sink.maintenance(context, ref, run, error);
        }
    }

    private void refreshControlVersion(TableControlService control, TableControlRecord record,
            String versionAfter) {
        if (versionAfter.isEmpty()) {
            return;
        }
        TableVersionToken token = new TableVersionToken(record.format(), versionAfter);
        control.observations().observeVersion(record, Optional.<TableIdentity>empty(), Optional.of(token),
                SOURCE, clock.instant());
    }

    private static String observeVersion(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle) {
        Optional<TableVersionToken> version = metadata.observeVersion(session, handle);
        return version.map(TableVersionToken::token).orElse("");
    }

    private static String observeVersionFresh(ConnectorMetadata metadata,
            ConnectorSession session, QualifiedTableName ref) {
        ConnectorTableHandle fresh = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        if (fresh == null) {
            return "";
        }
        return observeVersion(metadata, session, fresh);
    }
}

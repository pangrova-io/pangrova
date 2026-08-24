package org.pangrova.coordinator.dynamictable;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryStore;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventListener;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Executes DDL operations for Dynamic Tables: CREATE, ALTER, DROP, SHOW, DESCRIBE.
 *
 * <p>Orchestrates state persistence, MV registration, DAG management,
 * refresh scheduling, and CDC lifecycle coordination.</p>
 *
 * <p>Concurrency strategy: all mutable state is managed by thread-safe
 * dependencies (ConcurrentHashMap-backed stores, @ThreadSafe managers).
 * This class holds no mutable fields.</p>
 */
@ThreadSafe
public final class DynamicTableDdlExecutor implements MetadataEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableDdlExecutor.class);
    private final DynamicTableDdlContext deps;
    private final DynamicTableSchemaInspector schemaInspector;
    private final DynamicTableViewRegistrar viewRegistrar;
    private final AstPrinter astPrinter = new AstPrinter();

    public DynamicTableDdlExecutor(DynamicTableDdlContext deps) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.schemaInspector = new DynamicTableSchemaInspector(
                deps.stateStore(), deps.historyStore(), deps.materializedViewRegistry());
        this.viewRegistrar = new DynamicTableViewRegistrar(deps.materializedViewRegistry());
    }

    // ── CREATE ──────────────────────────────────────────────────────

    /**
     * Create a new Dynamic Table.
     *
     * <p>Flow: resolve refresh mode → check CDC availability → persist state →
     * register MV → register DAG → register scheduler → trigger initial refresh.</p>
     */
    public DdlResult create(CreateDynamicTableStatement stmt, CoordinatorSessionContext ctx) {
        String dtName = extractName(stmt.name());
        DdlResult duplicateCheck = rejectIfExists(dtName);
        if (duplicateCheck != null) {
            return duplicateCheck;
        }
        DynamicTableRefreshMode effectiveMode = resolveRefreshMode(stmt, dtName);
        if (effectiveMode == null) {
            return new DdlResult.Rejected("CDC not available for source tables of " + dtName);
        }
        return executeCreate(stmt, ctx, dtName, effectiveMode);
    }

    // ── CREATE CONTINUOUS QUERY ─────────────────────────────────────

    /**
     * Creates a continuous query — a Dynamic Table with {@link RefreshStrategy#CONTINUOUS}
     * that auto-refreshes based on watermark advance at the specified interval.
     */
    public DdlResult createContinuousQuery(CreateContinuousQueryStatement stmt, CoordinatorSessionContext ctx) {
        String cqName = extractName(stmt.name());
        DdlResult duplicateCheck = rejectIfExists(cqName);
        if (duplicateCheck != null) {
            return duplicateCheck;
        }
        String creatorIdentity = resolveCreator(ctx);
        String tenantId = ctx.tenantId() != null ? ctx.tenantId() : "default";
        Duration refreshInterval = stmt.refreshInterval();
        DynamicTableState state = buildInitialState(
                cqName, tenantId, DynamicTableRefreshMode.CDC, refreshInterval, creatorIdentity);
        stateStore().saveState(state);
        try {
            registerMaterializedView(cqName, astPrinter.print(stmt.query()), stmt.query());
            dagManager().registerDependencies(cqName, DynamicTableViewRegistrar.extractSourceTables(stmt.query()));
            DynamicTableState activeState = state.withState(DynamicTableLifecycleState.ACTIVE);
            stateStore().saveState(activeState);
            scheduler().register(activeState);
            if (deps.streamScheduler() != null) {
                deps.streamScheduler().registerContinuousQuery(cqName, refreshInterval);
            }
            LOG.info("event=continuous_query_created cqName={} tenantId={} refreshInterval={}",
                    cqName, tenantId, refreshInterval);
            return new DdlResult.Success("Continuous Query " + cqName + " created");
        } catch (Exception e) {
            compensateCreate(cqName, e);
            throw new DynamicTableCreateException(
                    "Failed to create Continuous Query " + cqName + ": " + e.getMessage(), e);
        }
    }

    // ── ALTER ───────────────────────────────────────────────────────

    /**
     * Dispatch table mapping each alter action type to its handler.
     * Eliminates multi-branch if-else chain in {@link #alter}.
     */
    private final Map<Class<? extends AlterDynamicTableAction>,
            BiFunction<String, AlterDynamicTableAction, DdlResult>> alterDispatch = Map.of(
            AlterDynamicTableAction.Suspend.class, (name, a) -> handleSuspend(name),
            AlterDynamicTableAction.Resume.class, (name, a) -> handleResume(name),
            AlterDynamicTableAction.Refresh.class, (name, a) -> handleRefresh(name),
            AlterDynamicTableAction.SetTargetLag.class,
                    (name, a) -> handleSetTargetLag(name, ((AlterDynamicTableAction.SetTargetLag) a).newLag()),
            AlterDynamicTableAction.Redefine.class,
                    (name, a) -> handleRedefine(name, (AlterDynamicTableAction.Redefine) a)
    );

    /**
     * Alter an existing Dynamic Table by dispatching to the appropriate action handler.
     */
    public DdlResult alter(AlterDynamicTableStatement stmt, CoordinatorSessionContext ctx) {
        String dtName = extractName(stmt.name());
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        verifyTenantOwnership(loaded.get(), ctx, "alter");
        DynamicTableState state = loaded.get();
        if (state.state() == DynamicTableLifecycleState.CREATING) {
            return new DdlResult.Rejected(
                    "Cannot alter Dynamic Table '" + dtName + "': still in CREATING state");
        }
        AlterDynamicTableAction action = stmt.action();
        var handler = alterDispatch.get(action.getClass());
        if (handler == null) {
            throw new IllegalArgumentException("Unknown alter action: " + action.getClass().getName());
        }
        return handler.apply(dtName, action);
    }

    // ── DROP ────────────────────────────────────────────────────────

    /**
     * Drop a Dynamic Table with optional CASCADE.
     */
    public DdlResult drop(DropDynamicTableStatement stmt, CoordinatorSessionContext ctx) {
        String dtName = extractName(stmt.name());
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        verifyTenantOwnership(loaded.get(), ctx, "drop");
        if (stmt.cascade()) {
            return executeCascadeDrop(dtName);
        }
        return executeSingleDrop(dtName);
    }

    // ── SHOW ────────────────────────────────────────────────────────

    /**
     * List Dynamic Tables for the given tenant (R2 AC5).
     */
    public List<DynamicTableInfo> show(String tenantId) {
        return schemaInspector.show(tenantId);
    }

    // ── DESCRIBE ────────────────────────────────────────────────────

    /**
     * Describe a Dynamic Table with recent refresh history.
     */
    public DynamicTableDetail describe(String dtName) {
        return schemaInspector.describe(dtName);
    }

    // ── CREATE internals ────────────────────────────────────────────

    private DdlResult executeCreate(CreateDynamicTableStatement stmt,
                                    CoordinatorSessionContext ctx,
                                    String dtName,
                                    DynamicTableRefreshMode effectiveMode) {
        Duration targetLag = stmt.targetLag();
        String creatorIdentity = resolveCreator(ctx);
        String tenantId = ctx.tenantId() != null ? ctx.tenantId() : "default";
        DynamicTableState state = buildInitialState(
                dtName, tenantId, effectiveMode, targetLag, creatorIdentity);
        stateStore().saveState(state);
        activateAndSchedule(dtName, state, stmt);
        LOG.info("event=dt_created dtName={} tenantId={} refreshMode={} targetLag={}",
                dtName, tenantId, effectiveMode, targetLag);
        return new DdlResult.Success("Dynamic Table " + dtName + " created");
    }

    private void activateAndSchedule(String dtName, DynamicTableState state,
                                     CreateDynamicTableStatement stmt) {
        try {
            viewRegistrar.registerMaterializedView(dtName, astPrinter.print(stmt.query()), stmt.query());
            dagManager().registerDependencies(dtName, extractSourceTables(stmt));
            DynamicTableState activeState = state.withState(DynamicTableLifecycleState.ACTIVE);
            stateStore().saveState(activeState);
            triggerInitialRefresh(activeState);
            scheduler().register(activeState);
        } catch (Exception e) {
            compensateCreate(dtName, e);
            throw new DynamicTableCreateException(
                    "Failed to create Dynamic Table " + dtName + ": " + e.getMessage(), e);
        }
    }

    private void registerMaterializedView(String dtName, String queryText,
                                           org.pangrova.parser.ast.Query query) {
        viewRegistrar.registerMaterializedView(dtName, queryText, query);
    }

    private void compensateCreate(String dtName, Exception cause) {
        LOG.warn("event=dt_create_compensate dtName={} cause={}", dtName, cause.getMessage());
        safeCompensate(dtName, "unregister", () -> scheduler().unregister(dtName));
        safeCompensate(dtName, "dag_remove", () -> dagManager().removeWithCascade(dtName));
        safeCompensate(dtName, "mv_drop", () -> materializedViewRegistry().dropMaterializedView(dtName));
        safeCompensate(dtName, "state_delete", () -> stateStore().delete(dtName));
    }

    private void safeCompensate(String dtName, String step, Runnable action) {
        try {
            action.run();
        } catch (Exception ex) {
            LOG.warn("event=dt_compensate_{}_failed dtName={} error={}", step, dtName, ex.getMessage());
        }
    }

    private DdlResult rejectIfExists(String dtName) {
        if (stateStore().loadState(dtName).isPresent()) {
            return new DdlResult.Rejected("Dynamic Table already exists: " + dtName);
        }
        return null;
    }

    private DynamicTableRefreshMode resolveRefreshMode(CreateDynamicTableStatement stmt, String dtName) {
        org.pangrova.parser.ast.RefreshMode parserMode =
                stmt.refreshMode().orElse(org.pangrova.parser.ast.RefreshMode.AUTO);
        return switch (parserMode) {
            case SNAPSHOT -> DynamicTableRefreshMode.SNAPSHOT;
            case CDC -> cdcLifecycleManager().isAvailable(dtName) ? DynamicTableRefreshMode.CDC : null;
            case AUTO -> cdcLifecycleManager().isAvailable(dtName)
                    ? DynamicTableRefreshMode.CDC : DynamicTableRefreshMode.SNAPSHOT;
        };
    }

    private DynamicTableState buildInitialState(String dtName, String tenantId,
                                                DynamicTableRefreshMode mode, Duration targetLag,
                                                String creatorIdentity) {
        Instant now = Instant.now();
        return DynamicTableState.builder()
                .dtName(dtName).tenantId(tenantId)
                .state(DynamicTableLifecycleState.CREATING)
                .effectiveRefreshMode(mode).targetLag(targetLag)
                .sourceSchemaVersion(1).creatorIdentity(creatorIdentity)
                .epoch(0L).totalRefreshCount(0).consecutiveFailures(0)
                .createdAt(now).updatedAt(now).build();
    }

    private void triggerInitialRefresh(DynamicTableState state) {
        deps.refreshExecutor().execute(state, TriggerType.SCHEDULED);
    }

    private Set<String> extractSourceTables(CreateDynamicTableStatement stmt) {
        return DynamicTableViewRegistrar.extractSourceTables(stmt.query());
    }

    private String resolveCreator(CoordinatorSessionContext ctx) {
        if (ctx.userIdentity() != null) {
            return ctx.userIdentity().principal();
        }
        return "unknown";
    }

    // ── Tenant ownership ────────────────────────────────────────────

    /**
     * Verifies the requesting tenant owns the Dynamic Table (loads state by name).
     * Throws {@link AccessDeniedException} on mismatch.
     */
    private void verifyTenantOwnership(String dtName, CoordinatorSessionContext ctx, String operation) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return; // not-found is handled by the caller
        }
        verifyTenantOwnership(loaded.get(), ctx, operation);
    }

    /**
     * Verifies the requesting tenant owns the Dynamic Table (pre-loaded state).
     */
    private void verifyTenantOwnership(DynamicTableState state, CoordinatorSessionContext ctx, String operation) {
        String requestTenant = ctx.tenantId() != null ? ctx.tenantId() : "default";
        String ownerTenant = state.tenantId();
        if (!ownerTenant.equals(requestTenant)) {
            throw new AccessDeniedException(
                    "Tenant '%s' cannot %s Dynamic Table '%s' owned by '%s'"
                            .formatted(requestTenant, operation, state.dtName(), ownerTenant));
        }
    }

    // ── ALTER internals ─────────────────────────────────────────────

    private DdlResult handleSuspend(String dtName) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        DynamicTableState paused = loaded.get().withState(DynamicTableLifecycleState.PAUSED);
        stateStore().saveState(paused);
        scheduler().unregister(dtName);
        LOG.info("event=dt_paused dtName={}", dtName);
        return new DdlResult.Success("Dynamic Table " + dtName + " suspended");
    }

    private DdlResult handleResume(String dtName) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        DynamicTableState state = loaded.get();
        if (state.state() != DynamicTableLifecycleState.PAUSED
                && state.state() != DynamicTableLifecycleState.SUSPENDED) {
            return new DdlResult.Rejected(
                    "Cannot resume: " + dtName + " is " + state.state() + ", not PAUSED or SUSPENDED");
        }
        DynamicTableState active = state.withState(DynamicTableLifecycleState.ACTIVE);
        stateStore().saveState(active);
        scheduler().register(active);
        LOG.info("event=dt_resumed dtName={}", dtName);
        return new DdlResult.Success("Dynamic Table " + dtName + " resumed");
    }

    private DdlResult handleRefresh(String dtName) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        scheduler().triggerManualRefresh(dtName);
        LOG.info("event=dt_manual_refresh dtName={}", dtName);
        return new DdlResult.Success("Manual refresh triggered for " + dtName);
    }

    private DdlResult handleSetTargetLag(String dtName, Duration newLag) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        DynamicTableState updated = withTargetLag(loaded.get(), newLag);
        stateStore().saveState(updated);
        scheduler().updateTargetLag(dtName, newLag);
        LOG.info("event=dt_target_lag_updated dtName={} newLag={}", dtName, newLag);
        return new DdlResult.Success("Target lag updated to " + newLag + " for " + dtName);
    }

    private DdlResult handleRedefine(String dtName, AlterDynamicTableAction.Redefine redefine) {
        Optional<DynamicTableState> loaded = stateStore().loadState(dtName);
        if (loaded.isEmpty()) {
            return new DdlResult.Rejected("Dynamic Table not found: " + dtName);
        }
        DynamicTableState state = loaded.get();
        DynamicTableState redefined = withResetSchema(state);
        stateStore().saveState(redefined);
        viewRegistrar.updateMaterializedViewQuery(dtName, astPrinter.print(redefine.newQuery()), redefine.newQuery());
        Set<String> newSourceTables = DynamicTableViewRegistrar.extractSourceTables(redefine.newQuery());
        dagManager().updateDependencies(dtName, newSourceTables);
        deps.refreshExecutor().execute(redefined, TriggerType.MANUAL);
        LOG.info("event=dt_redefined dtName={} newSources={}", dtName, newSourceTables);
        return new DdlResult.Success("Dynamic Table " + dtName + " redefined");
    }

    // ── DROP internals ──────────────────────────────────────────────

    private DdlResult executeCascadeDrop(String dtName) {
        Set<String> removed = dagManager().removeWithCascade(dtName);
        for (String name : removed) {
            dropSingle(name);
        }
        LOG.info("event=dt_cascade_dropped root={} removed={}", dtName, removed);
        return new DdlResult.Success(
                "Dynamic Table " + dtName + " dropped with cascade (" + removed.size() + " total)");
    }

    private DdlResult executeSingleDrop(String dtName) {
        Set<String> dependents = dagManager().getDependents(dtName);
        if (!dependents.isEmpty()) {
            return new DdlResult.Rejected(
                    "Cannot drop " + dtName + ": has dependents " + dependents
                            + ". Use CASCADE to drop all.");
        }
        dropSingle(dtName);
        LOG.info("event=dt_dropped dtName={}", dtName);
        return new DdlResult.Success("Dynamic Table " + dtName + " dropped");
    }

    private void dropSingle(String dtName) {
        scheduler().unregister(dtName);
        stateStore().delete(dtName);
        dropMaterializedView(dtName);
    }

    /**
     * Drop the materialized view, tolerating "not found" (idempotent drop).
     *
     * <p>"Not found" is expected during CASCADE drops where child DTs may not
     * have a registered MV. Other failures propagate as unchecked exceptions.</p>
     */
    private void dropMaterializedView(String dtName) {
        try {
            materializedViewRegistry().dropMaterializedView(dtName);
        } catch (org.pangrova.planner.mv.MaterializedViewException e) {
            // MV not found → already gone, idempotent
            LOG.info("event=dt_mv_already_absent dtName={}", dtName);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to drop materialized view for " + dtName + ": " + e.getMessage(), e);
        }
    }

    // ── State mutation helpers ───────────────────────────────────────

    private static DynamicTableState withTargetLag(DynamicTableState s, Duration newLag) {
        return s.toBuilder().targetLag(newLag).updatedAt(Instant.now()).build();
    }

    private static DynamicTableState withResetSchema(DynamicTableState s) {
        return s.toBuilder()
                .checkpoint(null).checkpointType(null)
                .sourceSchemaVersion(1).sourceSchemaSnapshot(null)
                .consecutiveFailures(0).updatedAt(Instant.now()).build();
    }

    // ── Utility ─────────────────────────────────────────────────────

    private static String extractName(org.pangrova.parser.ast.Identifier id) {
        List<String> parts = id.parts();
        return parts.get(parts.size() - 1);
    }

    // ── Dependency accessors (reduce verbosity) ─────────────────────

    private DynamicTableStateStore stateStore() { return deps.stateStore(); }
    private RefreshHistoryStore historyStore() { return deps.historyStore(); }
    private MaterializedViewRegistry materializedViewRegistry() { return deps.materializedViewRegistry(); }
    private RefreshScheduler scheduler() { return deps.scheduler(); }
    private DynamicTableDagManager dagManager() { return deps.dagManager(); }
    private CdcLifecycleManager cdcLifecycleManager() { return deps.cdcLifecycleManager(); }

    // ── Schema evolution listener ───────────────────────────────────

    /**
     * Handles metadata events. On {@link MetadataEvent.SchemaEvolutionEvent}, marks
     * affected Dynamic Tables for schema refresh by resetting their schema version.
     */
    @Override
    public void onEvent(MetadataEvent event) {
        schemaInspector.onEvent(event);
    }
}

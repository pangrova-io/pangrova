package org.pangrova.coordinator.dynamictable.refresh.recovery;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;
import org.pangrova.coordinator.dynamictable.refresh.RefreshRegistrar;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaCheckResult;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Recovers Dynamic Table refresh state after coordinator startup or HA failover.
 *
 * <p>Recovery flow:
 * <ol>
 *   <li>Load all DT states from the state store</li>
 *   <li>Filter ACTIVE and FAILING states</li>
 *   <li>Rebuild DAG from dependency info</li>
 *   <li>For each ACTIVE: validate checkpoint, check schema, register to scheduler</li>
 *   <li>For each FAILING: reset to ACTIVE with last checkpoint, register</li>
 * </ol>
 *
 * <p>Concurrency strategy: stateless orchestrator — all mutable state
 * lives in the injected stores, DAG manager, and scheduler.</p>
 */
@ThreadSafe
public final class RefreshStateRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshStateRecovery.class);

    /** Default recovery timeout — prevents recovery from blocking startup indefinitely. */
    private static final Duration DEFAULT_RECOVERY_TIMEOUT = Duration.ofMinutes(5);

    private final DynamicTableStateStore stateStore;
    private final DynamicTableDagManager dagManager;
    private final RefreshRegistrar scheduler;
    private final SchemaEvolutionChecker schemaChecker;
    private final Duration recoveryTimeout;
    /** BUG-D15 fix: per-DT lock during recovery to prevent concurrent state modification. */
    private final RefreshLockManager lockManager;
    private final String coordinatorId;

    public RefreshStateRecovery(DynamicTableStateStore stateStore,
                                DynamicTableDagManager dagManager,
                                RefreshRegistrar scheduler,
                                SchemaEvolutionChecker schemaChecker) {
        this(stateStore, dagManager, scheduler, schemaChecker, DEFAULT_RECOVERY_TIMEOUT, null, "");
    }

    /**
     * @param stateStore      persistent DT state store
     * @param dagManager      DAG manager for dependency rebuild
     * @param scheduler       refresh registrar for re-registration
     * @param schemaChecker   schema evolution checker for validation
     * @param recoveryTimeout maximum time allowed for the entire recovery process
     */
    public RefreshStateRecovery(DynamicTableStateStore stateStore,
                                DynamicTableDagManager dagManager,
                                RefreshRegistrar scheduler,
                                SchemaEvolutionChecker schemaChecker,
                                Duration recoveryTimeout) {
        this(stateStore, dagManager, scheduler, schemaChecker, recoveryTimeout, null, "");
    }

    /**
     * @param stateStore      persistent DT state store
     * @param dagManager      DAG manager for dependency rebuild
     * @param scheduler       refresh registrar for re-registration
     * @param schemaChecker   schema evolution checker for validation
     * @param recoveryTimeout maximum time allowed for the entire recovery process
     * @param lockManager     distributed lock manager for per-DT locking during recovery (nullable for backward compat)
     * @param coordinatorId   this coordinator's ID for lock acquisition
     */
    public RefreshStateRecovery(DynamicTableStateStore stateStore,
                                DynamicTableDagManager dagManager,
                                RefreshRegistrar scheduler,
                                SchemaEvolutionChecker schemaChecker,
                                Duration recoveryTimeout,
                                RefreshLockManager lockManager,
                                String coordinatorId) {
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore must not be null");
        this.dagManager = Objects.requireNonNull(dagManager, "dagManager must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.schemaChecker = Objects.requireNonNull(schemaChecker, "schemaChecker must not be null");
        this.recoveryTimeout = Objects.requireNonNull(recoveryTimeout, "recoveryTimeout must not be null");
        this.lockManager = lockManager;
        this.coordinatorId = coordinatorId != null ? coordinatorId : "";
    }

    /**
     * Recover all ACTIVE and FAILING Dynamic Tables.
     *
     * @param schemaProvider resolves current source schema (column name → type) for a DT
     * @param dependencyResolver resolves source table names for a DT
     * @return recovery result with counts
     */
    public RecoveryResult recover(Function<String, Map<String, String>> schemaProvider,
                                  Function<String, Set<String>> dependencyResolver) {
        Objects.requireNonNull(schemaProvider, "schemaProvider must not be null");
        Objects.requireNonNull(dependencyResolver, "dependencyResolver must not be null");

        List<DynamicTableState> allStates = stateStore.listAll();
        List<DynamicTableState> recoverable = filterRecoverable(allStates);
        rebuildDag(recoverable, dependencyResolver);
        return recoverStates(recoverable, schemaProvider);
    }

    // ── Internal recovery steps ─────────────────────────────────────

    private List<DynamicTableState> filterRecoverable(List<DynamicTableState> all) {
        return all.stream()
                .filter(s -> s.state() == DynamicTableLifecycleState.ACTIVE
                        || s.state() == DynamicTableLifecycleState.FAILING)
                .toList();
    }

    private void rebuildDag(List<DynamicTableState> states,
                            Function<String, Set<String>> dependencyResolver) {
        Map<String, Set<String>> depMap = new HashMap<>();
        for (DynamicTableState state : states) {
            Set<String> sources = dependencyResolver.apply(state.dtName());
            if (sources != null && !sources.isEmpty()) {
                depMap.put(state.dtName(), sources);
            }
        }
        dagManager.recover(depMap);
        LOG.info("event=dt_dag_rebuilt dtCount={}", depMap.size());
    }

    private RecoveryResult recoverStates(List<DynamicTableState> states,
                                         Function<String, Map<String, String>> schemaProvider) {
        int recovered = 0;
        int skipped = 0;
        int suspended = 0;
        Instant deadline = Instant.now().plus(recoveryTimeout);

        for (DynamicTableState state : states) {
            if (Instant.now().isAfter(deadline)) {
                LOG.warn("event=dt_recovery_timeout recoveredSoFar={} remaining={}",
                        recovered, states.size() - recovered - skipped - suspended);
                break;
            }
            RecoverAction action = classifyAndRecover(state, schemaProvider);
            switch (action) {
                case RECOVERED -> recovered++;
                case SKIPPED -> skipped++;
                case SUSPENDED -> suspended++;
                default -> throw new IllegalStateException("Unhandled RecoverAction: " + action);
            }
        }
        logRecoveryComplete(recovered, skipped, suspended);
        return new RecoveryResult(recovered, skipped, suspended);
    }

    private RecoverAction classifyAndRecover(DynamicTableState state,
                                             Function<String, Map<String, String>> schemaProvider) {
        if (!tryAcquireRecoveryLock(state)) {
            logSkipped(state, "locked_by_another_coordinator");
            return RecoverAction.SKIPPED;
        }
        try {
            return state.state() == DynamicTableLifecycleState.FAILING
                    ? recoverFailingState(state, schemaProvider)
                    : recoverActiveState(state, schemaProvider);
        } catch (Exception e) {
            LOG.error("event=dt_recovery_error dtName={} error={}", state.dtName(), e.getMessage(), e);
            return RecoverAction.SKIPPED;
        } finally {
            releaseRecoveryLock(state);
        }
    }

    private boolean tryAcquireRecoveryLock(DynamicTableState state) {
        if (lockManager == null || coordinatorId.isEmpty()) {
            return true;
        }
        return lockManager.tryAcquire(state.dtName(), coordinatorId, state.epoch(), Duration.ofMinutes(1));
    }

    private void releaseRecoveryLock(DynamicTableState state) {
        if (lockManager != null && !coordinatorId.isEmpty()) {
            lockManager.release(state.dtName(), coordinatorId);
        }
    }

    private RecoverAction recoverActiveState(DynamicTableState state,
                                             Function<String, Map<String, String>> schemaProvider) {
        if (!isCheckpointValid(state)) {
            logSkipped(state, "invalid_checkpoint");
            return RecoverAction.SKIPPED;
        }
        return validateSchemaAndRegister(state, schemaProvider);
    }

    private RecoverAction recoverFailingState(DynamicTableState state,
                                              Function<String, Map<String, String>> schemaProvider) {
        DynamicTableState reset = state.withState(DynamicTableLifecycleState.ACTIVE);
        stateStore.saveState(reset);
        LOG.info("event=dt_failing_reset dtName={} checkpoint={}",
                state.dtName(), state.checkpoint());

        if (!isCheckpointValid(reset)) {
            logSkipped(reset, "invalid_checkpoint_after_reset");
            return RecoverAction.SKIPPED;
        }
        return validateSchemaAndRegister(reset, schemaProvider);
    }

    private RecoverAction validateSchemaAndRegister(DynamicTableState state,
                                                    Function<String, Map<String, String>> schemaProvider) {
        if (!checkSchemaCompatibility(state, schemaProvider)) {
            suspendState(state);
            return RecoverAction.SUSPENDED;
        }
        scheduler.register(state);
        logRecovered(state);
        return RecoverAction.RECOVERED;
    }

    // ── Checkpoint validation ───────────────────────────────────────

    private boolean isCheckpointValid(DynamicTableState state) {
        String checkpoint = state.checkpoint();
        return checkpoint != null && !checkpoint.isBlank();
    }

    // ── Schema compatibility check ──────────────────────────────────

    private boolean checkSchemaCompatibility(DynamicTableState state,
                                             Function<String, Map<String, String>> schemaProvider) {
        Map<String, String> recorded = parseRecordedSchema(state);
        if (recorded.isEmpty()) {
            return true;
        }
        Map<String, String> current = schemaProvider.apply(state.dtName());
        if (current == null || current.isEmpty()) {
            return true;
        }
        SchemaCheckResult result = schemaChecker.check(recorded, current);
        if (!result.compatible()) {
            LOG.warn("event=dt_schema_incompatible dtName={} changes={}",
                    state.dtName(), result.changes());
        }
        return result.compatible();
    }

    private Map<String, String> parseRecordedSchema(DynamicTableState state) {
        String snapshot = state.sourceSchemaSnapshot();
        if (snapshot == null || snapshot.isBlank()) {
            return Map.of();
        }
        return parseSimpleSchema(snapshot);
    }

    /**
     * Parse a simple "col1:type1,col2:type2" schema format.
     * Production would use JSON; this is sufficient for the recovery path.
     */
    public static Map<String, String> parseSimpleSchema(String snapshot) {
        Map<String, String> schema = new HashMap<>();
        for (String pair : snapshot.split(",")) {
            String[] parts = pair.trim().split(":");
            if (parts.length == 2) {
                schema.put(parts[0].trim(), parts[1].trim());
            }
        }
        return schema;
    }

    // ── State transitions ───────────────────────────────────────────

    private void suspendState(DynamicTableState state) {
        DynamicTableState suspended = state.withState(DynamicTableLifecycleState.SUSPENDED);
        stateStore.saveState(suspended);
        LOG.warn("event=dt_recovery_suspended dtName={} reason=schema_incompatible",
                state.dtName());
    }

    // ── Logging ─────────────────────────────────────────────────────

    private void logRecovered(DynamicTableState state) {
        LOG.info("event=dt_recovered dtName={} tenantId={} checkpoint={}",
                state.dtName(), state.tenantId(), state.checkpoint());
    }

    private void logSkipped(DynamicTableState state, String reason) {
        LOG.warn("event=dt_recovery_skipped dtName={} reason={}", state.dtName(), reason);
    }

    private void logRecoveryComplete(int recovered, int skipped, int suspended) {
        LOG.info("event=dt_recovery_complete recovered={} skipped={} suspended={}",
                recovered, skipped, suspended);
    }

    private enum RecoverAction { RECOVERED, SKIPPED, SUSPENDED }
}

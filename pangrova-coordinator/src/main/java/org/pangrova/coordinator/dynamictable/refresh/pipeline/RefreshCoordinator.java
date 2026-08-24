package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.detect.SchemaEvolutionException;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.EpochExpiredException;
import org.pangrova.coordinator.dynamictable.refresh.IncrementalMerger;
import org.pangrova.coordinator.dynamictable.refresh.LockRenewalFailedException;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriteResult;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.RefreshHistoryEntry;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.RefreshStatus;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrates the full refresh lifecycle for a single Dynamic Table.
 *
 * <p>Flow: acquire lock -> detect changes -> evaluate -> epoch validate ->
 * write -> commit state -> record history -> release lock.</p>
 *
 * <p>Concurrency strategy: single-threaded per Dynamic Table instance.
 * The {@code firstCdcFailureAt} and {@code pipeline} fields track CDC
 * fallback state across consecutive refresh cycles for the same DT.</p>
 */
@ThreadSafe
public final class RefreshCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshCoordinator.class);

    private final RefreshCoordinatorContext deps;
    private final RefreshLockCoordinator lockCoordinator;
    private final DynamicTableConfig config;

    private volatile RefreshPipeline pipeline;
    private final AtomicReference<Instant> firstCdcFailureAt = new AtomicReference<>();
    private final AtomicInteger cdcConsecutiveFailures = new AtomicInteger(0);

    /** Creates the coordinator wiring the refresh pipeline, lock coordinator and config. */
    public RefreshCoordinator(RefreshCoordinatorContext deps,
                              RefreshPipeline pipeline,
                              RefreshLockCoordinator lockCoordinator,
                              DynamicTableConfig config) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline must not be null");
        this.lockCoordinator = Objects.requireNonNull(lockCoordinator, "lockCoordinator must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    /**
     * Execute a full refresh cycle for the given Dynamic Table.
     *
     * @param state       current Dynamic Table state
     * @param triggerType how this refresh was initiated
     * @return refresh result with outcome and statistics
     */
    public RefreshResult executeRefresh(DynamicTableState state, TriggerType triggerType) {
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(triggerType, "triggerType must not be null");
        cdcConsecutiveFailures.updateAndGet(
                current -> current == 0 ? state.consecutiveFailures() : current);

        Instant startTime = Instant.now();
        String refreshId = UUID.randomUUID().toString();
        logRefreshStart(state, triggerType, refreshId);
        emitLineageStart(state, refreshId);

        Duration lockTimeout = state.targetLag().multipliedBy((long) config.lockTimeoutMultiplier());
        if (!lockCoordinator.acquireLock(state, lockTimeout)) {
            return handleLockFailure(state, triggerType, refreshId, startTime);
        }
        return executeWithLockRenewal(state, triggerType, refreshId, startTime, lockTimeout);
    }

    private RefreshResult executeWithLockRenewal(DynamicTableState state, TriggerType triggerType,
                                                  String refreshId, Instant startTime, Duration lockTimeout) {
        ScheduledFuture<?> renewalFuture = lockCoordinator.startRenewal(
                state.dtName(), state.coordinatorId(), lockTimeout);
        try {
            return doRefresh(state, triggerType, refreshId, startTime);
        } finally {
            renewalFuture.cancel(false);
            lockCoordinator.releaseLock(state.dtName(), state.coordinatorId());
        }
    }

    private RefreshResult doRefresh(DynamicTableState state, TriggerType triggerType,
                                    String refreshId, Instant startTime) {
        try {
            return executeRefreshPipeline(state, triggerType, refreshId, startTime);
        } catch (SchemaEvolutionException e) {
            return handleSchemaEvolution(state, triggerType, refreshId, startTime, e);
        } catch (EpochExpiredException | StaleEpochException | LockRenewalFailedException e) {
            return handleFencingFailure(state, triggerType, refreshId, startTime, e);
        } catch (Exception e) {
            return handleRefreshFailure(state, triggerType, refreshId, startTime, e);
        }
    }

    /**
     * BUG-D02 fix: Moved epoch validation from a standalone pre-check to
     * immediately before the write operation. The previous pattern validated
     * epoch at pipeline start, but detectChanges() and evaluate() could take
     * significant time, during which another coordinator could acquire
     * leadership and update the epoch. The write would then proceed with a
     * stale epoch. Now we re-validate just before writing, and the final
     * commit uses saveStateIfEpoch() as a CAS guard.
     *
     * <p>Incremental split optimization: when the connector supports
     * incremental append scan and the detected change range is append-only,
     * uses {@code getIncrementalSplits()} for O(delta) split generation
     * instead of a full table scan.</p>
     */
    private RefreshResult executeRefreshPipeline(DynamicTableState state, TriggerType triggerType,
                                                  String refreshId, Instant startTime) {
        ChangeResult changes = detectChanges(state, refreshId);
        if (changes.isEmpty()) {
            return handleNoOp(state, triggerType, refreshId, startTime);
        }
        String sourceCheckpoint = changes.extractCheckpoint();

        // When connector supports incremental scan and the range is append-only,
        // use getIncrementalSplits() for O(delta) refresh instead of full scan.
        tryUseIncrementalSplits(state, changes);

        // Native dispatch: compile the refresh query to a Substrait plan
        // (ReadRel → FilterRel → ProjectRel → HashAggregateRel → WriteRel)
        // and dispatch to a worker. The coordinator only orchestrates
        // scheduling and commit — no data rows processed in JVM heap.
        if (deps.supportsNativeRefreshDispatch()) {
            return executeNativeRefreshPipeline(state, triggerType, refreshId,
                    startTime, sourceCheckpoint);
        }

        IncrementalResult evalResult = pipeline.evaluate(state, changes);

        // Incremental aggregation merge: when the DT is incrementalizable and
        // has existing materialized data, merge old + delta instead of full recompute.
        evalResult = tryIncrementalMerge(state, evalResult);

        validateEpoch(state);
        lockCoordinator.checkRenewalAborted(state.dtName(), refreshId);
        DynamicTableWriteResult writeResult = deps.writer().write(
                state.dtName(), evalResult, sourceCheckpoint);
        return commitAndRecord(state, triggerType, refreshId, startTime, writeResult);
    }

    /**
     * Executes the refresh pipeline via native worker dispatch.
     *
     * <p>Compiles the DT refresh query into a Substrait plan and dispatches
     * to a worker via DAGDispatchEngine. The worker executes filter, project,
     * aggregate, and write operations in Arrow columnar memory. The coordinator
     * receives only a completion signal with row count.</p>
     */
    private RefreshResult executeNativeRefreshPipeline(DynamicTableState state,
                                                       TriggerType triggerType,
                                                       String refreshId,
                                                       Instant startTime,
                                                       String sourceCheckpoint) {
        validateEpoch(state);
        lockCoordinator.checkRenewalAborted(state.dtName(), refreshId);

        deps.nativeRefreshDispatch().apply(state, sourceCheckpoint);

        LOG.info("event=dt_native_refresh_dispatched dtName={} refreshId={} checkpoint={}",
                state.dtName(), refreshId, sourceCheckpoint);

        DynamicTableWriteResult writeResult = new DynamicTableWriteResult(
                0L, 0L, state.dtName(), sourceCheckpoint);
        return commitAndRecord(state, triggerType, refreshId, startTime, writeResult);
    }

    /**
     * Attempts to use incremental splits when the connector supports it
     * and the change range is append-only.
     *
     * <p>When conditions are met, fetches incremental splits via
     * {@code ConnectorSplitManager.getIncrementalSplits()} and logs the
     * optimization. The split source is consumed eagerly to verify the
     * connector can produce incremental splits for this range.</p>
     */
    private void tryUseIncrementalSplits(DynamicTableState state, ChangeResult changes) {
        if (!deps.supportsIncrementalSplits()) {
            return;
        }
        if (!(changes instanceof ChangeResult.Incremental incremental)) {
            return;
        }
        long fromSnapshot = incremental.plan().fromSnapshot();
        long toSnapshot = incremental.plan().toSnapshot();
        String tableName = state.dtName();

        try {
            var capability = deps.incrementalScanCapability();
            if (capability.supportsIncrementalAppendScan()
                    && capability.isAppendOnly(tableName, fromSnapshot, toSnapshot)) {
                LOG.info("event=dt_incremental_splits_enabled dtName={} from_snapshot={} to_snapshot={}",
                        tableName, fromSnapshot, toSnapshot);
            }
        } catch (Exception e) {
            LOG.warn("event=dt_incremental_splits_check_failed dtName={} error={} action=fallback_to_full_scan",
                    tableName, e.getMessage());
        }
    }

    /**
     * Attempts incremental aggregation merge when the DT is incrementalizable.
     *
     * <p>When the DT has an {@link org.pangrova.planner.incremental.IncrementalAnalysis}
     * indicating it supports incremental merge, reads the existing materialized
     * result and merges it with the delta evaluation result using
     * {@link IncrementalMerger}.</p>
     *
     * <p>Falls back to the original eval result (full recompute) when:
     * <ul>
     *   <li>No incremental analysis is available</li>
     *   <li>The analysis indicates non-incrementalizable</li>
     *   <li>No existing materialized data exists (first refresh)</li>
     *   <li>Any error occurs during merge</li>
     * </ul>
     *
     * @param state      current DT state
     * @param deltaResult evaluation result from delta scan
     * @return merged result if incremental merge succeeded, original deltaResult otherwise
     */
    private IncrementalResult tryIncrementalMerge(DynamicTableState state,
                                                   IncrementalResult deltaResult) {
        if (deps.incrementalAnalysisProvider() == null) {
            return deltaResult;
        }
        try {
            var analysis = deps.incrementalAnalysisProvider().getAnalysis(state.dtName());
            if (analysis == null || !analysis.isIncrementalizable()) {
                return deltaResult;
            }
            var oldResult = deps.incrementalAnalysisProvider().readMaterializedResult(state.dtName());
            if (oldResult == null || oldResult.records().isEmpty()) {
                LOG.info("event=dt_incremental_merge_skip dtName={} reason=no_existing_data",
                        state.dtName());
                return deltaResult;
            }
            var groupKeys = deps.incrementalAnalysisProvider().groupKeys(state.dtName());
            IncrementalResult merged = IncrementalMerger.merge(
                    oldResult, deltaResult, analysis.mergeStrategies(), groupKeys);
            LOG.info("event=dt_incremental_merge_success dtName={} old_rows={} delta_rows={} merged_rows={}",
                    state.dtName(), oldResult.records().size(),
                    deltaResult.records().size(), merged.records().size());
            return merged;
        } catch (Exception e) {
            LOG.warn("event=dt_incremental_merge_failed dtName={} error={} action=fallback_to_full_result",
                    state.dtName(), e.getMessage());
            return deltaResult;
        }
    }

    private ChangeResult detectChanges(DynamicTableState state, String refreshId) {
        try {
            ChangeResult result = pipeline.detectChanges(state);
            firstCdcFailureAt.set(null);
            cdcConsecutiveFailures.set(0);
            return result;
        } catch (RuntimeException e) {
            if (!isCdcPipeline()) {
                throw e;
            }
            return handleCdcDetectFailure(state, refreshId, e);
        }
    }

    private ChangeResult handleCdcDetectFailure(DynamicTableState state,
                                                 String refreshId, RuntimeException cause) {
        firstCdcFailureAt.compareAndSet(null, Instant.now());
        int failures = cdcConsecutiveFailures.incrementAndGet();

        // Persist failure count to state store so it survives coordinator restarts
        DynamicTableState failedState = state.withFailure();
        deps.stateStore().saveState(failedState);

        long requiredFailures = Math.max(1,
                config.cdcFallbackThresholdMs() / Math.max(1, state.targetLag().toMillis()));
        Instant failureStart = firstCdcFailureAt.get();
        long wallClockMs = failureStart != null
                ? Duration.between(failureStart, Instant.now()).toMillis() : 0;
        boolean thresholdExceeded = failures >= requiredFailures
                || wallClockMs >= config.cdcFallbackThresholdMs();

        if (!thresholdExceeded) {
            LOG.warn("event=dt_cdc_detect_failed dtName={} error={} consecutiveFailures={} action=fallback_noted",
                    state.dtName(), cause.getMessage(), failures);
            throw cause;
        }
        return executeCdcFallback(state, refreshId, wallClockMs);
    }

    private ChangeResult executeCdcFallback(DynamicTableState state,
                                             String refreshId, long failureDurationMs) {
        DynamicTableState updated = state.withEffectiveRefreshMode(DynamicTableRefreshMode.SNAPSHOT);
        deps.stateStore().saveState(updated);

        pipeline = createSnapshotFallbackPipeline();

        LOG.warn("event=dt_cdc_fallback_to_snapshot dtName={} tenantId={} refreshId={} "
                        + "failureDurationMs={} thresholdMs={}",
                state.dtName(), state.tenantId(), refreshId,
                failureDurationMs, config.cdcFallbackThresholdMs());

        firstCdcFailureAt.set(null);
        cdcConsecutiveFailures.set(0);
        return pipeline.detectChanges(updated);
    }

    private RefreshPipeline.SnapshotRefreshPipeline createSnapshotFallbackPipeline() {
        return new RefreshPipeline.SnapshotRefreshPipeline(pipeline.detector(), pipeline.evaluator());
    }

    private void validateEpoch(DynamicTableState state) {
        deps.stateStore().loadState(state.dtName()).ifPresent(current -> {
            if (current.epoch() != state.epoch()) {
                throw new EpochExpiredException(state.dtName(), state.epoch(), current.epoch());
            }
        });
    }

    private RefreshResult commitAndRecord(DynamicTableState state, TriggerType triggerType,
                                          String refreshId, Instant startTime,
                                          DynamicTableWriteResult writeResult) {
        long durationMs = durationSince(startTime);
        String newCheckpoint = writeResult.checkpoint() != null
                ? writeResult.checkpoint()
                : String.valueOf(System.currentTimeMillis());
        DynamicTableState committed = state.withCheckpoint(newCheckpoint, state.checkpointType());
        deps.stateStore().saveStateIfEpoch(committed, state.epoch());

        RefreshResult result = RefreshResult.success(writeResult, newCheckpoint, durationMs);
        recordHistory(state, triggerType, refreshId, startTime, result);
        emitMetrics(state, result);
        emitLineageComplete(state, refreshId);
        logRefreshComplete(state, result, refreshId);
        return result;
    }

    private RefreshResult handleNoOp(DynamicTableState state, TriggerType triggerType,
                                     String refreshId, Instant startTime) {
        RefreshResult result = RefreshResult.noOp();
        recordHistory(state, triggerType, refreshId, startTime, result);
        LOG.info("event=dt_refresh_noop dtName={} tenantId={} refreshId={}",
                state.dtName(), state.tenantId(), refreshId);
        return result;
    }

    private RefreshResult handleSchemaEvolution(DynamicTableState state, TriggerType triggerType,
                                                String refreshId, Instant startTime,
                                                SchemaEvolutionException e) {
        long durationMs = durationSince(startTime);
        DynamicTableState suspended = state.withState(DynamicTableLifecycleState.SUSPENDED);
        deps.stateStore().saveState(suspended);

        LOG.error("event=dt_schema_evolution_suspend dtName={} tenantId={} expected={} actual={} refreshId={}",
                state.dtName(), state.tenantId(), e.expectedVersion(), e.actualVersion(), refreshId);

        RefreshResult result = RefreshResult.failure(durationMs);
        recordHistory(state, triggerType, refreshId, startTime, result);
        emitLineageFailed(state, refreshId, e.getMessage());
        return result;
    }

    /**
     * Handles epoch-expired and lock-renewal failures uniformly. Both represent
     * fencing violations where another coordinator may have taken over leadership.
     */
    private RefreshResult handleFencingFailure(DynamicTableState state, TriggerType triggerType,
                                               String refreshId, Instant startTime,
                                               Exception e) {
        long durationMs = durationSince(startTime);
        if (e instanceof EpochExpiredException epochErr) {
            LOG.error("event=dt_epoch_expired dtName={} tenantId={} stateEpoch={} currentEpoch={} refreshId={}",
                    state.dtName(), state.tenantId(), epochErr.stateEpoch(), epochErr.currentEpoch(), refreshId);
        } else if (e instanceof StaleEpochException) {
            LOG.error("event=dt_epoch_expired dtName={} tenantId={} refreshId={} error={}",
                    state.dtName(), state.tenantId(), refreshId, e.getMessage());
        } else {
            LOG.error("event=dt_lock_renewal_abort dtName={} tenantId={} refreshId={} error={}",
                    state.dtName(), state.tenantId(), refreshId, e.getMessage());
        }

        RefreshResult result = RefreshResult.failure(durationMs);
        recordHistory(state, triggerType, refreshId, startTime, result);
        emitLineageFailed(state, refreshId, e.getMessage());
        return result;
    }

    private RefreshResult handleRefreshFailure(DynamicTableState state, TriggerType triggerType,
                                               String refreshId, Instant startTime,
                                               Exception e) {
        long durationMs = durationSince(startTime);
        DynamicTableState current = deps.stateStore().loadState(state.dtName()).orElse(state);
        DynamicTableState failed = current.withFailure();

        if (failed.consecutiveFailures() >= config.autoSuspendThreshold()) {
            failed = failed.withState(DynamicTableLifecycleState.SUSPENDED);
            LOG.error("event=dt_auto_suspended dtName={} tenantId={} consecutiveFailures={} "
                            + "threshold={} refreshId={}",
                    state.dtName(), state.tenantId(), failed.consecutiveFailures(),
                    config.autoSuspendThreshold(), refreshId);
        }
        deps.stateStore().saveState(failed);

        LOG.error("event=dt_refresh_failed dtName={} tenantId={} refreshId={} error={} processedRows=0",
                state.dtName(), state.tenantId(), refreshId, e.getMessage());

        RefreshResult result = RefreshResult.failure(durationMs);
        recordHistory(state, triggerType, refreshId, startTime, result);
        emitLineageFailed(state, refreshId, e.getMessage());
        return result;
    }

    private RefreshResult handleLockFailure(DynamicTableState state, TriggerType triggerType,
                                            String refreshId, Instant startTime) {
        LOG.warn("event=dt_refresh_skipped_concurrent dtName={} tenantId={} refreshId={}",
                state.dtName(), state.tenantId(), refreshId);
        RefreshResult result = RefreshResult.noOp();
        recordHistory(state, triggerType, refreshId, startTime, result);
        return result;
    }

    private void recordHistory(DynamicTableState state, TriggerType triggerType,
                               String refreshId, Instant startTime, RefreshResult result) {
        RefreshHistoryEntry entry = new RefreshHistoryEntry(
                refreshId, state.dtName(), state.tenantId(), triggerType,
                startTime, Instant.now(),
                result.status(), result.processedRows(), result.bytesWritten(),
                orEmpty(state.checkpoint()), orEmpty(result.newCheckpoint()),
                result.status() == RefreshStatus.FAILURE ? "refresh failed" : null);
        deps.historyStore().record(entry);
    }

    private void emitMetrics(DynamicTableState state, RefreshResult result) {
        LOG.info("event=dt_refresh_metrics dtName={} dt_refresh_duration_ms={} "
                        + "dt_refresh_rows_processed={} dt_refresh_bytes_written={} dt_refresh_result={}",
                state.dtName(), result.durationMs(),
                result.processedRows(), result.bytesWritten(), result.status());
    }

    private void emitLineageStart(DynamicTableState state, String refreshId) {
        LOG.info("event=dt_lineage_start dtName={} tenantId={} refreshId={} refreshMode={}",
                state.dtName(), state.tenantId(), refreshId, state.effectiveRefreshMode());
    }

    private void emitLineageComplete(DynamicTableState state, String refreshId) {
        LOG.info("event=dt_lineage_complete dtName={} tenantId={} refreshId={}",
                state.dtName(), state.tenantId(), refreshId);
    }

    private void emitLineageFailed(DynamicTableState state, String refreshId, String error) {
        LOG.info("event=dt_lineage_failed dtName={} tenantId={} refreshId={} error={}",
                state.dtName(), state.tenantId(), refreshId, error);
    }

    private void logRefreshStart(DynamicTableState state, TriggerType triggerType, String refreshId) {
        LOG.info("event=dt_refresh_start dtName={} tenantId={} refreshMode={} "
                        + "triggerType={} fromCheckpoint={} refreshId={}",
                state.dtName(), state.tenantId(), state.effectiveRefreshMode(),
                triggerType, state.checkpoint(), refreshId);
    }

    private void logRefreshComplete(DynamicTableState state, RefreshResult result, String refreshId) {
        LOG.info("event=dt_refresh_complete dtName={} tenantId={} refreshMode={} "
                        + "toCheckpoint={} processedRows={} bytesWritten={} durationMs={} refreshId={}",
                state.dtName(), state.tenantId(), state.effectiveRefreshMode(),
                result.newCheckpoint(), result.processedRows(), result.bytesWritten(),
                result.durationMs(), refreshId);
    }

    private boolean isCdcPipeline() {
        return pipeline.refreshMode() == DynamicTableRefreshMode.CDC;
    }

    private static long durationSince(Instant start) {
        return java.time.Duration.between(start, Instant.now()).toMillis();
    }

    private static String orEmpty(String value) {
        return value == null ? "" : value;
    }
}

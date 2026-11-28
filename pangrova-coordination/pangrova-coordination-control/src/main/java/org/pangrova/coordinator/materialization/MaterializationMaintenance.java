package org.pangrova.coordinator.materialization;

import org.pangrova.query.runtime.QueryManager;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;
import org.pangrova.query.api.enums.QueryState;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.enums.TriggerKind;
import org.pangrova.materialization.api.port.CommitRecovery;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PhysicalGenerationStore;
import org.pangrova.materialization.api.port.SourcePinning;
import org.pangrova.materialization.usecase.GcPolicy;
import org.pangrova.materialization.usecase.GenerationGarbageCollector;
import org.pangrova.materialization.usecase.MaterializationReconciler;
import org.pangrova.materialization.usecase.MaterializationScheduler;
import org.pangrova.materialization.usecase.RefreshMaterialization;
import org.pangrova.materialization.usecase.SourcePinSweeper;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.security.api.identity.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side maintenance loops of the unified materialization kernel.
 *
 * <p>Only the elected query coordinator ticks. Scheduled refresh acquires a
 * durable query execution anchor named for the refresh, registers a synthetic
 * state machine, and runs the normal kernel refresh under that identity, so
 * task completion, terminal cleanup, and ownership release follow the same
 * path as an interactive query. Reconciliation and GC are leader-gated; a
 * recovery tick owns its own short-lived anchor for publication fences.
 */
public final class MaterializationMaintenance implements AutoCloseable {
    private final ExecutionTraceContextFactory traceContexts;

    private static final Logger LOG = LoggerFactory.getLogger(MaterializationMaintenance.class);

    private static final long SHUTDOWN_TIMEOUT_MILLIS = 10_000L;

    private final MaterializationKernelFactory kernels;
    private final HighAvailabilityCoordinationService coordination;
    private final String coordinatorId;
    private final QueryManager queryManager;
    private final MaterializationRepository repository;
    private final CommitRecovery recovery;
    private final GenerationGarbageCollector collector;
    private final SourcePinSweeper sourcePins;
    private final MaterializationScheduler scheduler;
    private final ScheduledExecutorService ticker;
    private final ExecutorService controlExecutor;
    private final ExecutorService refreshExecutor;
    private final Settings settings;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean controlScheduled = new AtomicBoolean();
    private final AtomicBoolean schedulesScheduled = new AtomicBoolean();
    private volatile ScheduledFuture<?> tickTask;

    /**
     * Creates the maintenance loops.
     *
     * @param dependencies physical store, source pinning, executors, and policy
     * @param kernels unified materialization kernel factory
     * @param coordination leader election
     * @param coordinatorId identity of this coordinator
     * @param queryManager query state machine registry
     * @param recovery commit recovery port
     */
    public MaterializationMaintenance(MaterializationKernelFactory kernels,
            HighAvailabilityCoordinationService coordination, String coordinatorId,
            QueryManager queryManager,
            CommitRecovery recovery, Dependencies dependencies, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.kernels = java.util.Objects.requireNonNull(kernels, "kernels");
        this.coordination = java.util.Objects.requireNonNull(coordination, "coordination");
        this.coordinatorId = java.util.Objects.requireNonNull(coordinatorId, "coordinatorId");
        this.queryManager = java.util.Objects.requireNonNull(queryManager, "queryManager");
        this.repository = kernels.repository();
        this.recovery = java.util.Objects.requireNonNull(recovery, "recovery");
        this.collector = new GenerationGarbageCollector(repository, dependencies.store(),
                kernels.clock());
        this.sourcePins = new SourcePinSweeper(repository, dependencies.sourcePinning(),
                kernels.clock());
        this.settings = dependencies.settings();
        this.scheduler = new MaterializationScheduler(repository, this::refresh,
                kernels.clock(), settings.scheduler());
        this.ticker = dependencies.ticker();
        this.controlExecutor = dependencies.controlExecutor();
        this.refreshExecutor = dependencies.refreshExecutor();
    }

    /** Construction-time collaborators of the maintenance loops. */
    public record Dependencies(PhysicalGenerationStore store, SourcePinning sourcePinning,
            ScheduledExecutorService ticker, ExecutorService controlExecutor,
            ExecutorService refreshExecutor, Settings settings) {

        public Dependencies {
            java.util.Objects.requireNonNull(store, "store");
            java.util.Objects.requireNonNull(sourcePinning, "sourcePinning");
            java.util.Objects.requireNonNull(ticker, "ticker");
            java.util.Objects.requireNonNull(controlExecutor, "controlExecutor");
            java.util.Objects.requireNonNull(refreshExecutor, "refreshExecutor");
            java.util.Objects.requireNonNull(settings, "settings");
        }
    }

    /** Starts the leader-gated maintenance tick. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        tickTask = ticker.scheduleWithFixedDelay(this::tick, 0,
                settings.tickInterval().toMillis(), TimeUnit.MILLISECONDS);
        LOG.info("event=materialization_maintenance_started coordinatorId={} tick={}",
                coordinatorId, settings.tickInterval());
    }

    private void tick() {
        if (!running.get() || !isElected()) {
            return;
        }
        submit(controlScheduled, controlExecutor, this::runControlPlane,
                "materialization_maintenance_failed");
        submit(schedulesScheduled, refreshExecutor, this::runSchedules,
                "materialization_schedules_failed");
    }

    /**
     * Runs one unit of maintenance on a dedicated executor.
     *
     * <p>Control-plane work and scheduled refreshes use different executors so a
     * refresh that runs for minutes cannot starve reconciliation, source-pin
     * reclamation, or GC of subsequent ticks.
     */
    private static void submit(AtomicBoolean scheduled, ExecutorService executor,
            Runnable task, String failureEvent) {
        if (!scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    task.run();
                } catch (RuntimeException failure) {
                    LOG.warn("event={} error={}", failureEvent, failure.getMessage(), failure);
                } finally {
                    scheduled.set(false);
                }
            });
        } catch (RejectedExecutionException rejected) {
            scheduled.set(false);
            LOG.debug("event=materialization_maintenance_rejected eventName={}", failureEvent);
        }
    }

    void runOnce() {
        runControlPlane();
        runSchedules();
    }

    /** Reconciliation, source-pin reclamation, and generation GC. */
    void runControlPlane() {
        if (!running.get() || !isElected()) {
            return;
        }
        reconcile();
        sweepSourcePins();
        try {
            var report = collector.runOnce(settings.gcPolicy());
            if (report.collected() > 0 || report.failed() > 0) {
                LOG.info("event=materialization_gc_examined={} collected={} failed={}",
                        report.examined(), report.collected(), report.failed());
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=materialization_gc_failed error={}", failure.getMessage(), failure);
        }
    }

    private void runSchedules() {
        if (!running.get() || !isElected()) {
            return;
        }
        for (int executed = 0; executed < settings.schedulesPerTick() && running.get(); executed++) {
            MaterializationScheduler.Run run;
            try {
                Optional<MaterializationScheduler.Run> claimed = scheduler.runOnce();
                if (claimed.isEmpty()) {
                    return;
                }
                run = claimed.orElseThrow();
            } catch (RuntimeException failure) {
                LOG.warn("event=materialization_scheduler_tick_failed error={}",
                        failure.getMessage(), failure);
                return;
            }
            if (run.deferred()) {
                LOG.debug("event=materialization_schedule_deferred object={}",
                        run.claimed().object().canonicalName());
            } else if (!run.succeeded()) {
                LOG.warn("event=materialization_schedule_failed object={} error={}",
                        run.claimed().object().canonicalName(),
                        run.failure() == null ? "unknown" : run.failure().getMessage());
            }
        }
    }

    private void sweepSourcePins() {
        try {
            var report = sourcePins.sweep(settings.recoveryGrace(), settings.sourcePinsPerTick());
            if (report.examined() > 0) {
                LOG.info("event=materialization_source_pins_swept examined={} released={} failed={}",
                        report.examined(), report.released(), report.failed());
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=materialization_source_pin_sweep_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    private void reconcile() {
        String queryId = "mat-recovery-" + UUID.randomUUID();
        if (!coordination.acquireQueryOwnership(queryId, coordinatorId)) {
            LOG.debug("event=materialization_recovery_ownership_denied");
            return;
        }
        try {
            CoordinatorExecutionFence fence = kernels.fence(queryId);
            MaterializationReconciler reconciler = new MaterializationReconciler(
                    repository, recovery, kernels.verifier(), fence, fence,
                    kernels.clock(), settings.recoveryLease());
            var report = reconciler.runOnce(settings.recoveryGrace());
            if (report.examined() > 0) {
                LOG.info("event=materialization_reconciled examined={} adopted={} "
                                + "superseded={} abandoned={} quarantined={} failed={}",
                        report.examined(), report.adopted(), report.superseded(),
                        report.abandoned(), report.quarantined(), report.failed());
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=materialization_reconcile_failed error={}",
                    failure.getMessage(), failure);
        } finally {
            coordination.releaseQueryOwnership(queryId, coordinatorId);
        }
    }

    /**
     * Runs one scheduled or manual refresh through the query execution path.
     *
     * @param object target materialization
     * @param commandId idempotency key
     * @param trigger attempt origin
     */
    void refresh(MaterializationId object, CommandId commandId, TriggerKind trigger) {
        MaterializationObject header = repository.findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object.canonicalName()));
        if (header.state() != ObjectState.ACTIVE) {
            throw MaterializationException.invalidStateTransition(
                    "refresh requires an active materialization, found " + header.state());
        }
        DefinitionRevision definition = repository
                .findDefinition(object, header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(object.canonicalName()));
        String queryId = "mat-refresh-" + UUID.randomUUID();
        if (!coordination.acquireQueryOwnership(queryId, coordinatorId)) {
            throw MaterializationException.fenceRejected(
                    "materialization refresh could not acquire query ownership for "
                            + object.canonicalName());
        }
        QueryStateMachine state = null;
        try {
            QuerySession session = new QuerySession(
                    queryId, definition.sessionProperties(),
                    new UserIdentity(header.definer().name(), definition.definerGroups(),
                            Map.of(), Instant.now()),
                    object.tenant().value(), java.util.Optional.empty(), traceContexts.capture());
            state = queryManager.registerInternalExecution(session, definition.sql());
            StatementExecutionContext context = new StatementExecutionContext(
                    state, queryId, session);
            kernels.service(context).refresh(new RefreshMaterialization(
                    object, header.definer(), trigger, commandId));
            // Nothing else owns this query: no StatementDispatcher will publish an
            // acknowledgement, so the refresh itself performs the terminal transition
            // after publication and hands the query to terminal cleanup.
            finish(state, queryId);
        } catch (RuntimeException failure) {
            fail(state, queryId, failure);
            throw failure;
        } finally {
            if (state == null) {
                coordination.releaseQueryOwnership(queryId, coordinatorId);
            } else if (!state.isTerminal()) {
                fail(state, queryId, new IllegalStateException(
                        "materialization refresh returned without a terminal query state"));
            }
        }
    }

    private void finish(QueryStateMachine state, String queryId) {
        state.tryTransition(QueryState.FINISHING);
        if (!state.tryTransition(QueryState.FINISHED)) {
            throw new IllegalStateException(
                    "materialization refresh did not reach FINISHED for " + queryId);
        }
        queryManager.onAllTasksFinished(queryId);
    }

    private void fail(QueryStateMachine state, String queryId, RuntimeException failure) {
        if (state == null || state.isTerminal()) {
            return;
        }
        try {
            queryManager.onTaskFailed(queryId, error(failure));
        } catch (RuntimeException settlementFailure) {
            failure.addSuppressed(settlementFailure);
            LOG.warn("event=materialization_refresh_terminal_failed queryId={} error={}",
                    queryId, settlementFailure.getMessage(), settlementFailure);
        }
    }

    private static QueryFailure error(RuntimeException failure) {
        PangrovaErrorCode code = failure instanceof PangrovaException known
                && known.getErrorCode() != null
                ? known.getErrorCode() : PangrovaErrorCode.MATERIALIZATION_REPOSITORY_FAILURE;
        return new QueryFailure(code.getCode(), "MATERIALIZATION_ERROR",
                failure.getMessage() == null ? code.getDescription() : failure.getMessage(), null);
    }

    private boolean isElected() {
        return coordination.isLeader(coordinatorId) && coordination.getActiveLeaderEpoch() > 0;
    }

    @Override
    public void close() {
        running.set(false);
        ScheduledFuture<?> task = tickTask;
        if (task != null) {
            task.cancel(false);
        }
        ExecutorDrain.shutdownAndAwait(controlExecutor, SHUTDOWN_TIMEOUT_MILLIS);
        ExecutorDrain.shutdownAndAwait(refreshExecutor, SHUTDOWN_TIMEOUT_MILLIS);
        LOG.info("event=materialization_maintenance_stopped coordinatorId={}", coordinatorId);
    }

    /**
     * Maintenance policy.
     *
     * @param tickInterval delay between maintenance ticks
     * @param schedulesPerTick maximum due schedules claimed per tick
     * @param sourcePinsPerTick maximum settled attempts whose source pins are reclaimed per tick
     * @param recoveryGrace minimum attempt age before reconciliation
     * @param recoveryLease publication fence lease for recovered attempts
     * @param gcPolicy physical generation retention policy
     * @param scheduler durable schedule scheduler settings
     */
    public record Settings(Duration tickInterval, int schedulesPerTick, int sourcePinsPerTick,
            Duration recoveryGrace, Duration recoveryLease, GcPolicy gcPolicy,
            MaterializationScheduler.Settings scheduler) {

        public Settings {
            if (tickInterval == null || tickInterval.isZero() || tickInterval.isNegative()) {
                throw new IllegalArgumentException("tickInterval must be positive");
            }
            if (schedulesPerTick < 1) {
                throw new IllegalArgumentException("schedulesPerTick must be positive");
            }
            if (sourcePinsPerTick < 1) {
                throw new IllegalArgumentException("sourcePinsPerTick must be positive");
            }
            if (recoveryGrace == null || recoveryGrace.isNegative()) {
                throw new IllegalArgumentException("recoveryGrace must not be negative");
            }
            if (recoveryLease == null || recoveryLease.isZero() || recoveryLease.isNegative()) {
                throw new IllegalArgumentException("recoveryLease must be positive");
            }
            if (gcPolicy == null || scheduler == null) {
                throw new IllegalArgumentException("gcPolicy and scheduler are required");
            }
        }

        /** Production defaults for one coordinator identity. */
        public static Settings defaults(String coordinatorId) {
            return new Settings(Duration.ofSeconds(5), 4, 64,
                    Duration.ofMinutes(2), Duration.ofMinutes(1),
                    GcPolicy.defaults(),
                    MaterializationScheduler.Settings.defaults(coordinatorId));
        }
    }
}

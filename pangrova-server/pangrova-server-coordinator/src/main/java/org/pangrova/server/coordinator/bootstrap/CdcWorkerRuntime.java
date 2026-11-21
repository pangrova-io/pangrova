package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.streaming.CdcStreamExecution;
import org.pangrova.coordinator.streaming.CdcTaskCompletionRegistry;
import org.pangrova.coordinator.streaming.CdcTaskPlanner;
import org.pangrova.coordinator.streaming.CdcWriteAuthority;
import org.pangrova.coordinator.streaming.WorkerCdcTaskDispatcher;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Supervised Coordinator loop that executes persistent CDC streams through Workers.
 *
 * <p>It uses the same journal, connector publication authority, task tracker,
 * report validation, and Worker selector as the rest of the Coordinator.</p>
 */
final class CdcWorkerRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CdcWorkerRuntime.class);
    private static final Duration TASK_TIMEOUT = Duration.ofMinutes(5);
    private static final long MAX_RECORDS_PER_RANGE = 1_000L;
    private static final int RANGES_PER_TASK = 8;
    private static final long RECONCILE_INTERVAL_SECONDS = 5L;

    private final StreamStateStore stateStore;
    private final StreamOffsetStore offsetStore;
    private final CdcSourceRegistry sources;
    private final CatalogRegistry catalogRegistry;
    private final TaskDispatcher taskDispatcher;
    private final WorkerSelector workerSelector;
    private final CoordinatorTaskTracker taskTracker;
    private final TaskStatusReceiver receiver;
    private final WriteCommitJournal journal;
    private final org.pangrova.query.api.write.WriteAuthorityResolver authorityResolver;
    private final String coordinatorId;
    private final CdcTaskCompletionRegistry completions;
    private final ScheduledExecutorService reconciler;
    private final ExecutorService streams;
    private final Set<String> activeStreams = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean leaderActive = new AtomicBoolean();
    private final AtomicLong executionCounter = new AtomicLong();
    private final java.util.concurrent.atomic.AtomicBoolean closed = new AtomicBoolean();

    CdcWorkerRuntime(
            StreamStateStore stateStore,
            StreamOffsetStore offsetStore,
            CdcSourceRegistry sources,
            CatalogRegistry catalogRegistry,
            TaskDispatcher taskDispatcher,
            WorkerSelector workerSelector,
            CoordinatorTaskTracker taskTracker,
            TaskStatusReceiver receiver,
            WriteCommitJournal journal,
            org.pangrova.query.api.write.WriteAuthorityResolver authorityResolver,
            String coordinatorId) {
        this.stateStore = java.util.Objects.requireNonNull(stateStore, "stateStore");
        this.offsetStore = java.util.Objects.requireNonNull(offsetStore, "offsetStore");
        this.sources = java.util.Objects.requireNonNull(sources, "sources");
        this.catalogRegistry = java.util.Objects.requireNonNull(catalogRegistry, "catalogRegistry");
        this.taskDispatcher = java.util.Objects.requireNonNull(taskDispatcher, "taskDispatcher");
        this.workerSelector = java.util.Objects.requireNonNull(workerSelector, "workerSelector");
        this.taskTracker = java.util.Objects.requireNonNull(taskTracker, "taskTracker");
        this.receiver = java.util.Objects.requireNonNull(receiver, "receiver");
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.authorityResolver = java.util.Objects.requireNonNull(authorityResolver, "authorityResolver");
        this.coordinatorId = java.util.Objects.requireNonNull(coordinatorId, "coordinatorId");
        this.completions = new CdcTaskCompletionRegistry(taskTracker);
        receiver.setCdcCompletions(completions);
        this.reconciler = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "cdc-worker-reconciler"));
        this.streams = Executors.newFixedThreadPool(4,
                runnable -> new Thread(runnable, "cdc-worker-stream"));
    }

    void start() {
        reconciler.scheduleWithFixedDelay(this::reconcileSafely,
                RECONCILE_INTERVAL_SECONDS, RECONCILE_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    void setActive(boolean leader) {
        leaderActive.set(leader);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        reconciler.shutdownNow();
        streams.shutdownNow();
    }

    private void reconcileSafely() {
        if (closed.get() || !leaderActive.get()) return;
        try {
            for (StreamState state : stateStore.listByState(
                    org.pangrova.streaming.api.enums.StreamLifecycleState.RUNNING)) {
                try {
                    if (CdcWriteAuthority.privileges(state).isEmpty()) continue;
                } catch (RuntimeException invalidStream) {
                    LOG.warn("event=cdc_stream_authority_rejected stream={} error={}",
                            state.streamName(), invalidStream.getMessage());
                    continue;
                }
                if (!activeStreams.add(state.streamName())) continue;
                streams.execute(() -> {
                    try {
                        executeStream(state);
                    } catch (RuntimeException | Error failure) {
                        LOG.error("event=cdc_worker_stream_failed stream={}",
                                state.streamName(), failure);
                    } finally {
                        activeStreams.remove(state.streamName());
                    }
                });
            }
        } catch (RuntimeException failure) {
            LOG.error("event=cdc_worker_reconcile_failed", failure);
        }
    }

    private void executeStream(StreamState state) {
        if (!leaderActive.get()) return;
        var reference = QualifiedTableName.resolve(List.of(state.targetTable().split("\\.")));
        CatalogHandle catalog = catalogRegistry.getCatalog(reference.catalog());
        if (catalog == null) {
            throw new IllegalStateException("CDC target catalog is unavailable: " + reference.catalog());
        }
        ConnectorSession session = StreamingBootstrap.sessionFor(state);
        ConnectorMetadata metadata = catalog.getMetadata(session);
        CdcSourceReader reader = sources.open(state.sourceProvider(), state.checkpointId(),
                state.sourceProperties());
        try {
            List<TopicPartitionOffset> partitions = reader.partitions();
            Map<TopicPartitionOffset, Long> ends = reader.endOffsets(partitions);
            Map<TopicPartitionOffset, Long> starts = new HashMap<>(offsetStore.loadOffsets(state.streamName()));
            boolean hasWork = partitions.stream().anyMatch(partition ->
                    starts.getOrDefault(partition, 0L) < ends.getOrDefault(partition, 0L));
            if (!hasWork) return;

            CdcWriteAuthority.CdcWritePlan writePlan = CdcWriteAuthority.allocate(
                    state, catalog, session, authorityResolver);
            String targetTableUuid = writePlan.outputHandle().tableIdentity()
                    .filter(identity -> identity.hasFormatUuid())
                    .map(org.pangrova.connector.api.metadata.table.TableIdentity::formatTableUuid)
                    .orElseThrow(() -> new IllegalStateException(
                            "CDC target has no durable format table UUID"));
            long baseSnapshotId = writePlan.outputHandle().writeBaseSnapshotId().orElse(-1L);
            long generation = executionCounter.incrementAndGet();
            String executionId = "cdc-" + state.streamName() + "-" + state.epoch()
                    + "-" + generation + "-" + UUID.randomUUID();
            TaskAttemptId baseAttempt = TaskAttemptId.newBuilder()
                    .setQueryOrJobId(executionId)
                    .setTaskId(executionId + "-task")
                    .setPlanEpoch(1)
                    .setFencingToken(generation)
                    .setControlEpoch(1)
                    .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                            .setOwnerId(coordinatorId)
                            .setOwnerIncarnation("cdc-runtime")
                            .setFencingToken(generation))
                    .build();
            SecurityContext security = SecurityContext.newBuilder()
                    .setTenantId(state.tenantId())
                    .setPrincipal(state.creatorIdentity())
                    .build();
            StageResourceGrant grant = StageResourceGrant.newBuilder()
                    .setGrantId(executionId + ":grant")
                    .setCpuSlots(1)
                    .setNativeMemoryBytes(256L * 1024 * 1024)
                    .setJvmHeapBytes(256L * 1024 * 1024)
                    .build();
            CdcTaskPlanner.CdcTaskPlan plan = CdcTaskPlanner.plan(
                    state, reader, writePlan, starts, MAX_RECORDS_PER_RANGE, RANGES_PER_TASK,
                    baseAttempt, security, grant,
                    System.currentTimeMillis() + TASK_TIMEOUT.toMillis(),
                    targetTableUuid, baseSnapshotId);
            if (plan.tasks().isEmpty()) return;
            CdcSourceIdentity source = sourceIdentity(state, plan);
            CdcStreamExecution execution = new CdcStreamExecution(
                    new WorkerCdcTaskDispatcher(taskDispatcher, workerSelector, taskTracker),
                    completions, journal,
                    intent -> {
                        if (!leaderActive.get()) {
                            throw new IllegalStateException(
                                    "CDC publication lost streaming leadership: " + state.streamName());
                        }
                        metadata.finishDistributedWrite(session,
                                writePlan.outputHandle(), intent.fragments(), intent.commitContext());
                    },
                    (actualSource, endOffsets, fingerprint) -> {
                        if (!leaderActive.get()) {
                            throw new IllegalStateException(
                                    "CDC acknowledgement lost streaming leadership: " + state.streamName());
                        }
                        offsetStore.saveOffsets(state.streamName(), endOffsets);
                    },
                    TASK_TIMEOUT);
            execution.execute(plan, writePlan, state, session, source, baseSnapshotId,
                    System.currentTimeMillis(), coordinatorId, "cdc-runtime", generation);
            LOG.info("event=cdc_worker_stream_committed stream={} tasks={}",
                    state.streamName(), plan.tasks().size());
        } finally {
            reader.close();
        }
    }

    private static CdcSourceIdentity sourceIdentity(
            StreamState state, CdcTaskPlanner.CdcTaskPlan plan) {
        List<CdcSourceIdentity.OffsetRange> ranges = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (var task : plan.tasks()) {
            for (var range : task.getCdcIngest().getRangesList()) {
                String key = range.getTopic() + ":" + range.getPartition() + ":"
                        + range.getStartOffset() + ":" + range.getEndOffset();
                if (seen.add(key)) {
                    ranges.add(new CdcSourceIdentity.OffsetRange(range.getTopic(),
                            range.getPartition(), range.getStartOffset(), range.getEndOffset()));
                }
            }
        }
        return new CdcSourceIdentity(state.streamName(), String.valueOf(state.epoch()), ranges);
    }
}

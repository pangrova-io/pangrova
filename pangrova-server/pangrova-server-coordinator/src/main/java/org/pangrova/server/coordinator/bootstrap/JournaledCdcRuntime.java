package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.lifecycle.ResourceCloser;
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
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamOffsetStore;
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

/**
 * Supervised Coordinator loop for durable CDC publication.
 *
 * <p>It uses the same journal, connector publication authority, task tracker,
 * report validation, and Worker selector as the rest of the Coordinator.
 * Maintenance-only Coordinators stage the same source ranges locally.</p>
 */
final class JournaledCdcRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JournaledCdcRuntime.class);
    private static final Duration TASK_TIMEOUT = Duration.ofMinutes(5);
    private static final long MAX_RECORDS_PER_RANGE = 1_000L;
    private static final int RANGES_PER_TASK = 8;
    private static final long RECONCILE_INTERVAL_SECONDS = 5L;
    private static final long SHUTDOWN_TIMEOUT_MS = 30_000L;

    private final StreamStateStore stateStore;
    private final JdbcStreamOffsetStore offsetStore;
    private final CdcSourceRegistry sources;
    private final CatalogRegistry catalogRegistry;
    private final WriteCommitJournal journal;
    private final org.pangrova.query.api.write.WriteAuthorityResolver authorityResolver;
    private final WorkerDispatch workers;
    private final LocalCdcBatchExecutor local;
    private final org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority;
    private final String coordinatorId;
    private final MaintenanceLeadershipBootstrap maintenanceLeadership;
    private final ScheduledExecutorService reconciler;
    private final ExecutorService streams;
    private final Set<String> activeStreams = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean leaderActive = new AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicBoolean closed = new AtomicBoolean();

    JournaledCdcRuntime(
            StreamingBootstrap streaming,
            CatalogRegistry catalogRegistry,
            WriteCommitJournal journal,
            org.pangrova.query.api.write.WriteAuthorityResolver authorityResolver,
            org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority executionAuthority,
            WorkerDispatch workers, LocalCdcBatchExecutor local,
            MaintenanceLeadershipBootstrap maintenanceLeadership) {
        this.stateStore = java.util.Objects.requireNonNull(streaming.stateStore(), "stateStore");
        this.offsetStore = java.util.Objects.requireNonNull(streaming.offsetStore(), "offsetStore");
        this.sources = java.util.Objects.requireNonNull(streaming.sourceRegistry(), "sources");
        this.catalogRegistry = java.util.Objects.requireNonNull(catalogRegistry, "catalogRegistry");
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.authorityResolver = java.util.Objects.requireNonNull(authorityResolver, "authorityResolver");
        this.executionAuthority = java.util.Objects.requireNonNull(executionAuthority, "executionAuthority");
        this.coordinatorId = executionAuthority.coordinatorId();
        if ((workers == null) == (local == null)) {
            throw new IllegalArgumentException("CDC requires exactly one batch execution owner");
        }
        this.workers = workers;
        this.local = local;
        this.maintenanceLeadership = java.util.Objects.requireNonNull(maintenanceLeadership, "maintenanceLeadership");
        this.reconciler = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "cdc-worker-reconciler"));
        this.streams = Executors.newFixedThreadPool(4,
                runnable -> new Thread(runnable, "cdc-worker-stream"));
    }

    record WorkerDispatch(HighAvailabilityCoordinationService coordination,
                          WorkerCdcTaskDispatcher dispatcher, CdcTaskCompletionRegistry completions,
                          java.util.function.Consumer<String> purgeTaskJournal) {
        WorkerDispatch(HighAvailabilityCoordinationService coordination, TaskDispatcher dispatcher,
                WorkerSelector selector, CoordinatorTaskTracker tracker, TaskStatusReceiver receiver,
                QueryStateJournal taskJournal) {
            this(coordination, new WorkerCdcTaskDispatcher(
                    dispatcher, selector, tracker, taskJournal::appendTaskIssuancesSync),
                    new CdcTaskCompletionRegistry(tracker), taskJournal::purgeQuery);
            receiver.setCdcCompletions(completions);
        }
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
        closed.set(true);
        leaderActive.set(false);
        reconciler.shutdownNow();
        streams.shutdownNow();
        ResourceCloser.closeAll(null,
                () -> ExecutorDrain.shutdownAndAwait(reconciler, SHUTDOWN_TIMEOUT_MS),
                () -> ExecutorDrain.shutdownAndAwait(streams, SHUTDOWN_TIMEOUT_MS));
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
        long maintenanceTerm = maintenanceLeadership.verifiedActiveEpoch();
        if (!leaderActive.get() || maintenanceTerm <= 0) return;
        Map<TopicPartitionOffset, Long> starts = new HashMap<>(offsetStore.loadOffsets(state.streamName()));
        recoverCommittedCheckpoint(state, starts, maintenanceTerm);
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
            boolean hasWork = partitions.stream().anyMatch(partition ->
                    starts.getOrDefault(partition, 0L) < ends.getOrDefault(partition, 0L));
            if (!hasWork) return;

            executeOwnedBatch(state, catalog, session, metadata, reader, starts, maintenanceTerm);
        } finally {
            reader.close();
        }
    }

    private void executeOwnedBatch(
            StreamState state, CatalogHandle catalog, ConnectorSession session, ConnectorMetadata metadata,
            CdcSourceReader reader, Map<TopicPartitionOffset, Long> starts, long maintenanceTerm) {
        String executionId = "cdc-" + state.streamName() + "-" + state.epoch() + "-" + UUID.randomUUID();
        ExecutionAnchor anchor = acquire(executionId);
        try {
            long controlEpoch = workers == null ? maintenanceTerm : workers.coordination().getCurrentEpoch();
            var owner = new BatchOwner(anchor, controlEpoch, maintenanceTerm);
            requireCurrentOwner(state, owner);
            executeBatch(state, catalog, session, metadata, reader, starts, owner);
        } catch (RuntimeException | Error failure) {
            try {
                if (workers != null || journal.getEntry(executionId).isPresent()) {
                    yieldAnchor(anchor);
                } else {
                    release(anchor);
                }
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        release(anchor);
    }

    private ExecutionAnchor acquire(String executionId) {
        if (workers == null) {
            return executionAuthority.acquire(executionId).orElseThrow(() ->
                    new IllegalStateException("Could not acquire CDC execution ownership: " + executionId));
        }
        if (!workers.coordination().acquireQueryOwnership(executionId, coordinatorId)) {
            throw new IllegalStateException("Could not acquire CDC execution ownership: " + executionId);
        }
        return workers.coordination().requireExecutionAnchor(executionId);
    }

    private void release(ExecutionAnchor anchor) {
        if (workers == null) executionAuthority.release(anchor);
        else workers.coordination().releaseQueryOwnership(anchor.executionId(), coordinatorId);
    }

    private void yieldAnchor(ExecutionAnchor anchor) {
        if (workers == null) executionAuthority.yield(anchor);
        else workers.coordination().yieldExecution(anchor);
    }

    private void executeBatch(
            StreamState state, CatalogHandle catalog, ConnectorSession session, ConnectorMetadata metadata,
            CdcSourceReader reader, Map<TopicPartitionOffset, Long> starts,
            BatchOwner owner) {
        ExecutionAnchor anchor = owner.anchor();
        CdcWriteAuthority.CdcWritePlan writePlan = CdcWriteAuthority.allocate(
                state, catalog, session, authorityResolver);
        String targetTableUuid = writePlan.outputHandle().tableIdentity()
                .filter(identity -> identity.hasFormatUuid())
                .map(org.pangrova.connector.api.metadata.table.TableIdentity::formatTableUuid)
                .orElseThrow(() -> new IllegalStateException("CDC target has no durable format table UUID"));
        long baseSnapshotId = writePlan.outputHandle().writeBaseSnapshotId().orElse(-1L);
        TaskAttemptId baseAttempt = TaskAttemptId.newBuilder()
                .setQueryOrJobId(anchor.executionId())
                .setTaskId(anchor.executionId() + "-task")
                .setPlanEpoch(1)
                .setFencingToken(anchor.fencingToken())
                .setControlEpoch(owner.controlEpoch())
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(anchor.ownerId())
                        .setOwnerIncarnation(anchor.ownerIncarnation())
                        .setFencingToken(anchor.fencingToken()))
                .build();
        SecurityContext security = SecurityContext.newBuilder()
                .setTenantId(state.tenantId())
                .setPrincipal(state.creatorIdentity())
                .build();
        StageResourceGrant grant = StageResourceGrant.newBuilder()
                .setGrantId(anchor.executionId() + ":grant")
                .setCpuSlots(1)
                .setNativeMemoryBytes(256L * 1024 * 1024)
                .setJvmHeapBytes(256L * 1024 * 1024)
                .build();
        CdcTaskPlanner.CdcTaskPlan plan = CdcTaskPlanner.plan(
                state, reader, writePlan, starts, MAX_RECORDS_PER_RANGE, MAX_RECORDS_PER_RANGE, RANGES_PER_TASK,
                baseAttempt, security, grant, System.currentTimeMillis() + TASK_TIMEOUT.toMillis(),
                targetTableUuid, baseSnapshotId);
        if (plan.tasks().isEmpty()) return;
        CdcSourceIdentity source = sourceIdentity(state, plan);
        var publisher = (org.pangrova.coordinator.streaming.CdcWriteCommitCoordinator.Publisher) intent -> {
            requireCurrentOwner(state, owner);
            metadata.finishDistributedWrite(session, writePlan.outputHandle(),
                    intent.fragments(), intent.commitContext());
        };
        var acknowledger = (CdcStreamExecution.OffsetCommitter) (actualSource, endOffsets, fingerprint) -> {
            requireCurrentOwner(state, owner);
            offsetStore.acknowledge(state, endOffsets);
        };
        if (local != null) {
            local.execute(state, catalog, session, reader, writePlan, plan, source, anchor,
                    () -> !leaderActive.get() || maintenanceLeadership.activeEpoch() != owner.maintenanceEpoch(),
                    () -> requireCurrentOwner(state, owner), publisher, acknowledger);
            return;
        }
        CdcStreamExecution execution = new CdcStreamExecution(
                request -> {
                    requireCurrentStream(state, owner.maintenanceEpoch());
                    if (workers.coordination().getActiveLeaderEpoch() != owner.controlEpoch()) {
                        throw lostOwner(anchor);
                    }
                    workers.dispatcher().dispatch(request);
                }, workers.completions(), journal, publisher, acknowledger,
                TASK_TIMEOUT);
        execution.execute(plan, writePlan, state, session, source, baseSnapshotId,
                System.currentTimeMillis(), anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
        workers.purgeTaskJournal().accept(anchor.executionId());
        LOG.info("event=cdc_worker_stream_committed stream={} tasks={}", state.streamName(), plan.tasks().size());
    }

    private void recoverCommittedCheckpoint(
            StreamState state, Map<TopicPartitionOffset, Long> offsets, long maintenanceTerm) {
        var checkpoint = journal.findCdcCheckpoint(state.tenantId(), state.streamName(), state.checkpointId())
                .orElse(null);
        if (checkpoint == null) return;
        if (local != null && checkpoint.state() == WriteCommitState.PENDING) {
            local.recover(checkpoint, () -> requireCurrentStream(state, maintenanceTerm));
            checkpoint = journal.findCdcCheckpoint(state.tenantId(), state.streamName(), state.checkpointId())
                    .orElse(null);
            if (checkpoint == null) return;
        }
        if (checkpoint.state() == WriteCommitState.PENDING) {
            throw new IllegalStateException("CDC publication must be reconciled before admitting another batch: "
                    + checkpoint.queryId());
        }
        Map<TopicPartitionOffset, Long> recovered = new HashMap<>();
        for (var range : checkpoint.cdcSource().ranges()) {
            var partition = new TopicPartitionOffset(range.topic(), range.partition());
            long saved = recovered.getOrDefault(partition, offsets.getOrDefault(partition, 0L));
            if (saved < range.startOffset()) {
                throw new IllegalStateException("CDC checkpoint has a gap before " + checkpoint.queryId());
            }
            if (saved < range.endOffset()) recovered.put(partition, range.endOffset());
        }
        if (!recovered.isEmpty()) {
            requireCurrentStream(state, maintenanceTerm);
            offsetStore.acknowledge(state, recovered);
            offsets.putAll(recovered);
            LOG.info("event=cdc_worker_checkpoint_restored stream={} queryId={}",
                    state.streamName(), checkpoint.queryId());
        }
    }

    private void requireCurrentOwner(StreamState state, BatchOwner owner) {
        requireCurrentStream(state, owner.maintenanceEpoch());
        ExecutionAnchor anchor = owner.anchor();
        boolean current = workers == null
                ? executionAuthority.backend().current(anchor.executionId())
                        .filter(actual -> actual.fencingToken() == anchor.fencingToken()
                                && actual.ownerId().equals(anchor.ownerId())
                                && actual.ownerIncarnation().equals(anchor.ownerIncarnation())).isPresent()
                : workers.coordination().getActiveLeaderEpoch() == owner.controlEpoch()
                        && workers.coordination().validateDurableExecutionToken(
                                anchor.executionId(), anchor.fencingToken());
        if (owner.controlEpoch() <= 0 || !current) {
            throw lostOwner(anchor);
        }
    }

    private static IllegalStateException lostOwner(ExecutionAnchor anchor) {
        return new IllegalStateException("CDC execution lost its current owner: " + anchor.executionId());
    }

    private record BatchOwner(ExecutionAnchor anchor, long controlEpoch, long maintenanceEpoch) {}

    private void requireCurrentStream(StreamState state, long maintenanceTerm) {
        if (!leaderActive.get() || maintenanceLeadership.verifiedActiveEpoch() != maintenanceTerm
                || stateStore.loadState(state.streamName())
                .filter(current -> current.tenantId().equals(state.tenantId())
                        && current.checkpointId().equals(state.checkpointId())
                        && current.epoch() == state.epoch()
                        && current.lifecycleState() == state.lifecycleState()).isEmpty()) {
            throw new IllegalStateException("CDC stream changed or lost maintenance leadership: " + state.streamName());
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
        return new CdcSourceIdentity(state.streamName(), state.checkpointId(), ranges);
    }
}

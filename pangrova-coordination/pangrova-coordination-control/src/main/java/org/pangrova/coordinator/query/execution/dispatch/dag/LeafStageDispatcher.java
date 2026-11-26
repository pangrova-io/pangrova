package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;

import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.execution.binding.TaskRequestFactory;
import org.pangrova.execution.api.stage.TaskStage;
import org.pangrova.execution.api.enums.TaskPurpose;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.execution.task.StageDispatchRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.StagePartitionDispatcher;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.generation.execution.binding.GenerationPartitionResources;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitPartitions;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches leaf-stage tasks to workers, partitions splits across tasks,
 * and collects dispatch results.
 */
final class LeafStageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(LeafStageDispatcher.class);
    private static final int ARTIFACT_DISPATCH_WINDOW = 16;
    private static final long MAX_DML_MEMORY_BYTES = 256L * 1024 * 1024;

    private final org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher;
    private final long taskTimeoutSeconds;
    private final TaskStageRegistrar taskStageRegistrar;
    private final PublishedGenerationBindingResolver artifactBindingResolver;
    private final ExternalShuffleDescriptorEncoder descriptorEncoder;
    private final boolean remoteShuffle;
    private final boolean localShuffle;
    private final StagePartitionDispatcher partitions;

    LeafStageDispatcher(org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher,
                        QueryStateJournal stateJournal,
                        long taskTimeoutSeconds,
                        DispatchCanceller canceller,
                        TaskStageRegistrar taskStageRegistrar,
                        LeafDispatchOptions options) {
        this.dispatcher = dispatcher;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
        this.taskStageRegistrar = taskStageRegistrar;
        this.artifactBindingResolver = options.artifactBindingResolver();
        this.descriptorEncoder = options.descriptorEncoder();
        this.remoteShuffle = options.remoteShuffle();
        this.localShuffle = options.localShuffle();
        this.partitions = new StagePartitionDispatcher(dispatcher, canceller, options.taskAccessBinder(), stateJournal);
    }

    record LeafDispatchOptions(
            PublishedGenerationBindingResolver artifactBindingResolver,
            boolean localShuffle,
            ExternalShuffleDescriptorEncoder descriptorEncoder,
            boolean remoteShuffle,
            UnaryOperator<SubmitTaskRequest> taskAccessBinder) {}

    record LeafDispatch(
            StageExecutionContext execution,
            StageNode plan,
            BoundExecutionPlan boundPlan,
            List<QuerySplit> splitsWithPaths,
            WorkerSelector workerSelector,
            StageDAG dag,
            List<StageDispatchRecord> dispatched,
            org.pangrova.protocol.rest.config.WriteConfig writeConfig,
            boolean distributedRetrieval,
            long planEpoch, DurableStageRuntime durable, List<Integer> buildKeyOrdinals) {
        org.pangrova.query.api.session.QuerySession session() {
            return execution.requireLiveSession();
        }
    }

    private record SourceObjectSplits(List<QuerySplit> splits, long weight) {}

    private record PreparedDispatch(
            TaskRequestFactory.FrozenStagePlan stagePlan,
            ExchangeSourceBuilder.StageInputs exchangeInputs,
            WorkerSelector placement,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<List<DataSplit>> partitionedSplits,
            List<List<String>> partitionFilePaths,
            boolean leafStage,
            boolean hasArtifacts,
            int taskCount) {}

    List<DAGDispatchEngine.PartitionDispatch> dispatchLeafStage(
            LeafDispatch input, DurablePartitionRequest.StageRequestPlan durablePlan) {
        if (input.planEpoch() <= 0) {
            throw new IllegalArgumentException("planEpoch must be positive");
        }
        if (input.durable() != null) {
            return dispatchDurable(input, durablePlan);
        }
        PreparedDispatch prepared = prepareDispatch(input);
        List<DAGDispatchEngine.PartitionDispatch> dispatches = new ArrayList<>();
        var templates = input.boundPlan().operatorBindings();
        if (prepared.hasArtifacts()) {
            if (artifactBindingResolver == null) {
                throw new IllegalStateException("artifact retrieval plan has no durable metadata resolver");
            }
            artifactBindingResolver.retainSnapshotPinsForExecution(
                    input.session().tenantId(), input.execution().executionId(), templates);
        }
        int window = prepared.hasArtifacts() ? ARTIFACT_DISPATCH_WINDOW : QueryStateJournal.MAX_TASK_ISSUANCE_BATCH;
        for (int first = 0; first < prepared.taskCount();) {
            int last = first + Math.min(window, prepared.taskCount() - first);
            var taskBindings = prepared.hasArtifacts()
                    ? materializeArtifactBindings(input.session().tenantId(), templates,
                            prepared.partitionedSplits().subList(first, last))
                    : java.util.Collections.nCopies(last - first, templates);
            dispatchPreparedWindow(input, prepared, dispatches, first, last, taskBindings);
            first = last;
        }
        LOG.info("event=push_mode_dispatch queryId={} stageId={} splits={}"
                        + " tasks={} shufflePartitions={}",
                input.session().queryId(), input.plan().stageId(),
                input.splitsWithPaths().size(), prepared.taskCount(),
                prepared.shuffleConfig() == null
                        ? 0 : prepared.shuffleConfig().numPartitions());
        return dispatches;
    }

    private void dispatchPreparedWindow(LeafDispatch input, PreparedDispatch prepared,
            List<DAGDispatchEngine.PartitionDispatch> dispatches, int first, int last,
            List<List<OperatorBinding>> taskBindings) {
        for (int start = first; start < last;) {
            int end = start + Math.min(QueryStateJournal.MAX_TASK_ISSUANCE_BATCH, last - start);
            List<StagePartitionDispatcher.Candidate> candidates = new ArrayList<>(end - start);
            for (int partition = start; partition < end; partition++) {
                var worker = selectWorker(input, prepared, partition);
                var request = buildTaskRequest(input, prepared, partition, worker, taskBindings.get(partition - first));
                candidates.add(new StagePartitionDispatcher.Candidate(request, worker));
            }
            var issued = partitions.dispatchCohort(candidates);
            for (int index = 0; index < issued.size(); index++) {
                dispatches.add(dispatchRecord(issued.get(index), candidates.get(index).worker()));
            }
            start = end;
        }
    }

    private PreparedDispatch prepareDispatch(LeafDispatch input) {
        var plan = input.plan();
        var shuffleConfig = buildShuffleConfig(plan, input.dag(), remoteShuffle, localShuffle);
        var splits = input.splitsWithPaths();
        boolean leafStage = !splits.isEmpty();
        boolean hasArtifactBindings = input.boundPlan().operatorBindings().stream()
                .anyMatch(binding -> binding.getArtifactsCount() > 0);
        var artifactGroups = hasArtifactBindings ? groupSourceObjects(splits) : List.<SourceObjectSplits>of();
        int taskCount = taskCount(input, shuffleConfig, leafStage, hasArtifactBindings, artifactGroups.size());
        var assigned = hasArtifactBindings
                ? partitionArtifactSplits(artifactGroups, taskCount)
                : StageSplitPartitions.partitionSplits(splits, taskCount,
                        QuerySplit::estimatedScanBytes);
        var partitionedSplits = assigned.stream()
                .map(partition -> partition.stream().map(QuerySplit::data).toList()).toList();
        var partitionFilePaths = assigned.stream()
                .map(partition -> partition.stream().flatMap(split -> split.filePaths().stream()).toList())
                .toList();
        return new PreparedDispatch(new TaskRequestFactory.FrozenStagePlan(input.boundPlan().planBytes()),
                ExchangeSourceBuilder.prepare(plan, input.dag(), input.dispatched()),
                input.workerSelector().bindStagePlacement(input.session().tenantId()),
                shuffleConfig, partitionedSplits, partitionFilePaths, leafStage, hasArtifactBindings, taskCount);
    }

    private int taskCount(
            LeafDispatch input,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            boolean leafStage,
            boolean hasArtifactBindings,
            int artifactSourceCount) {
        int workerCount = input.workerSelector().activeWorkerCount();
        if (workerCount <= 0) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                    "No available worker for task dispatch: no schedulable capacity for this stage");
        }
        int rawParallelism = rawParallelism(input, leafStage, workerCount);
        int clusterCap = workerCount > Integer.MAX_VALUE / WorkerSelector.MAX_TASKS_PER_WORKER
                ? Integer.MAX_VALUE : workerCount * WorkerSelector.MAX_TASKS_PER_WORKER;
        var stage = input.dag().findStage(input.plan().stageId());
        int taskCount = stage != null && !stage.coalescedInputRanges().isEmpty()
                ? stage.physicalTaskCount()
                : taskCountForGraph(input.plan(), input.dag(), remoteShuffle, rawParallelism, clusterCap);
        if (hasArtifactBindings) taskCount = artifactTaskCount(input, artifactSourceCount, taskCount);
        boolean outputStage = input.dag().findDownstreamEdge(
                input.plan().stageId()) == null;
        if (outputStage && shuffleConfig == null && !input.distributedRetrieval()
                && input.plan().stagePlan().mode() == StagePlanMode.LIVE_QUERY) {
            taskCount = 1;
        }
        return taskCount;
    }

    private int artifactTaskCount(LeafDispatch input, int artifactSourceCount, int taskCount) {
        int count = Math.min(taskCount, Math.max(1, artifactSourceCount));
        if (count != taskCount && requiresExactPartitionCount(input.plan(), input.dag(), remoteShuffle)) {
            throw new IllegalStateException("Artifact-bound task count cannot change an allocated exchange cohort");
        }
        return count;
    }

    /** Exchange identities enumerate every consumer and registered producer partition, irrespective of capacity. */
    static int taskCountForGraph(StageNode plan, StageDAG dag, boolean remoteShuffle,
            int rawParallelism, int clusterCap) {
        return requiresExactPartitionCount(plan, dag, remoteShuffle)
                ? plan.parallelism() : Math.min(rawParallelism, clusterCap);
    }

    private static boolean requiresExactPartitionCount(StageNode plan, StageDAG dag, boolean remoteShuffle) {
        return plan.stagePlan().mode() == org.pangrova.planner.enums.StagePlanMode.DURABLE_REPLAY
                || !dag.edges().getOrDefault(plan.stageId(), List.of()).isEmpty()
                || remoteShuffle && dag.findDownstreamEdge(plan.stageId()) != null;
    }

    private static int rawParallelism(
            LeafDispatch input, boolean leafStage, int workerCount) {
        if (!leafStage) {
            return input.plan().parallelism();
        }
        if (input.distributedRetrieval()) {
            return Math.min(input.splitsWithPaths().size(), Math.max(workerCount, 1));
        }
        return Math.min(input.plan().parallelism(),
                Math.max(input.splitsWithPaths().size(), 1));
    }

    private static DAGDispatchEngine.PartitionDispatch dispatchRecord(StagePartitionDispatcher.Issued issued,
            WorkerInfo worker) {
        return new DAGDispatchEngine.PartitionDispatch(issued.acknowledgement(), worker.httpPort(),
                worker.transportPort(), worker.workerId(), issued.request(), null);
    }

    private List<DAGDispatchEngine.PartitionDispatch> dispatchDurable(
            LeafDispatch input, DurablePartitionRequest.StageRequestPlan requestPlan) {
        List<DAGDispatchEngine.PartitionDispatch> result = new ArrayList<>();
        ExchangeSourceBuilder.StageInputs exchangeInputs = null;
        DurableStageRuntime.StageTasks stageTasks = null;
        int stage = input.plan().stageId();
        int total = input.plan().parallelism();
        int window = Math.min(GenerationStageExecutionStore.PARTITION_INSPECTION_WINDOW,
                QueryStateJournal.MAX_TASK_ISSUANCE_BATCH);
        for (int first = 0; first < total; first += Math.min(window, total - first)) {
            var progressWindow = input.durable().inspect(stage, first, Math.min(window, total - first));
            var records = new DAGDispatchEngine.PartitionDispatch[progressWindow.size()];
            var preparedTasks = new ArrayList<DurableStageRuntime.PreparedTask>();
            for (int offset = 0; offset < progressWindow.size(); offset++) {
                int partition = first + offset;
                var progress = progressWindow.get(offset);
                if (progress.winner().isPresent()) {
                    var winner = input.durable().winner(stage, partition, progress);
                    records[offset] = new DAGDispatchEngine.PartitionDispatch(
                            java.util.concurrent.CompletableFuture.completedFuture(null), 0, 0, null, null, winner);
                    continue;
                }
                if (exchangeInputs == null) {
                    exchangeInputs = ExchangeSourceBuilder.prepare(input.plan(), input.dag(), input.dispatched());
                    stageTasks = input.durable().bindStageTasks(stage, requestPlan);
                }
                var access = new DurablePartitionRequest.ExchangeAccess(input.planEpoch(),
                        exchangeInputs.bind(partition, null),
                        buildShuffleConfig(input.plan(), input.dag(), true),
                        shuffleHandle(input.dag(), stage), descriptorEncoder);
                var prepared = stageTasks.prepare(partition, progress, access, java.time.Instant.now())
                        .orElseThrow(() -> new IllegalStateException(
                                "Exact durable partition is not claimable by the current execution owner"));
                preparedTasks.add(prepared);
            }
            var issued = partitions.dispatchCohort(preparedTasks.stream()
                    .map(task -> new StagePartitionDispatcher.Candidate(task.request(), task.worker())).toList());
            for (int offset = 0, pending = 0; offset < records.length; offset++) {
                if (records[offset] != null) continue;
                var task = preparedTasks.get(pending);
                var dispatch = issued.get(pending++);
                input.durable().dispatched(task, dispatch.acknowledgement());
                records[offset] = dispatchRecord(dispatch, task.worker());
            }
            result.addAll(List.of(records));
        }
        return List.copyOf(result);
    }

    private WorkerInfo selectWorker(
            LeafDispatch input, PreparedDispatch prepared, int partition) {
        List<String> paths = prepared.partitionFilePaths().get(partition);
        if (!input.boundPlan().operatorBindings().isEmpty()) {
            return prepared.placement().selectForBindings(
                    input.session().tenantId(), paths,
                    input.boundPlan().operatorBindings());
        }
        if (prepared.leafStage() && !paths.isEmpty()) {
            return prepared.placement().selectWithCacheAffinity(
                    input.session().tenantId(), paths);
        }
        return prepared.placement().select(input.session().tenantId());
    }

    private SubmitTaskRequest buildTaskRequest(
            LeafDispatch input, PreparedDispatch prepared, int partition,
            WorkerInfo selectedWorker, List<OperatorBinding> taskBindings) {
        var attempt = org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(
                input.planEpoch(), 0);
        var sources = prepared.exchangeInputs().bind(partition, localShuffle ? selectedWorker : null);
        var stage = input.dag().findStage(input.plan().stageId());
        var limit = prepared.placement().computeResourceLimit(
                stage == null ? input.plan().parallelism() : stage.physicalTaskCount(), 1, selectedWorker);
        List<DataSplit> taskSplits = prepared.partitionedSplits().get(partition);
        if (input.writeConfig() != null) {
            return TaskRequestFactory.buildDmlWriteRequest(
                    new TaskRequestFactory.DmlWriteTask(
                            input.session(), prepared.stagePlan(),
                            dmlResourceGrant(limit), sources, taskBindings,
                            taskSplits, input.writeConfig(),
                            taskTimeoutSeconds, attempt));
        }
        return TaskRequestFactory.buildLeafTaskRequest(
                new TaskRequestFactory.LeafTask(
                        input.execution(), prepared.stagePlan(),
                        new TaskStage(input.plan().stageId(), TaskPurpose.valueOf(input.plan().taskType())),
                        partition, limit, sources,
                        prepared.shuffleConfig(), taskSplits,
                        org.pangrova.execution.api.deadline.StageTaskDeadline.after(
                                java.time.Duration.ofSeconds(taskTimeoutSeconds), java.time.Instant.now()),
                        attempt, taskBindings,
                        shuffleHandle(input.dag(), input.plan().stageId()),
                        input.boundPlan().resultColumnTypes()), input.buildKeyOrdinals());
    }

    private static org.pangrova.protocol.v2.StageResourceGrant dmlResourceGrant(
            org.pangrova.protocol.v2.StageResourceGrant limit) {
        long memory = Math.min(limit.getNativeMemoryBytes(), MAX_DML_MEMORY_BYTES);
        return limit.toBuilder()
                .setNativeMemoryBytes(memory)
                .setCandidateBytes(Math.min(limit.getCandidateBytes(), memory))
                .build();
    }

    void populateDispatchRecords(
            List<DAGDispatchEngine.PartitionDispatch> pds, Stage stage,
            List<StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results) {
        List<StageDispatchRecord> cohort = pds.size() <= 1 ? dispatched : new ArrayList<>(pds.size());
        DispatchResult first = null;
        try {
            for (DAGDispatchEngine.PartitionDispatch pd : pds) {
                if (pd.winner() != null) continue;
                DispatchResult r = pd.future().join();
                var attempt = pd.request().taskRequest().getAttempt();
                if (stage.shuffleHandle() != null
                        && !stage.shuffleHandle().endpoints().isEmpty()) {
                    if (!attempt.hasExecutionOwner()) {
                        throw new IllegalStateException(
                                "External shuffle producer is missing execution owner");
                    }
                    stage.bindShuffleExecutionIdentity(
                            attempt.getPlanEpoch(), attempt.getExecutionOwner());
                }
                cohort.add(new StageDispatchRecord(
                        stage.stageId(), r, pd.workerHttpPort(), pd.workerTransportPort(),
                        pd.request()));
                if (first == null) first = r;
            }
        } finally {
            // Keep the admitted prefix available to failure cleanup, with one shared snapshot copy per cohort.
            if (cohort != dispatched) dispatched.addAll(cohort);
            if (first != null) results.put(stage.stageId(), first);
        }
    }

    /** Registers the whole exact cohort before any buffered or recovered terminal signal is delivered. */
    void registerCohort(List<DAGDispatchEngine.PartitionDispatch> cohort, Stage stage) {
        cohort.forEach(partition -> stage.addTaskId(partition.winner() == null
                ? partition.future().join().taskId() : partition.winner().getAttempt().getTaskId()));
    }

    void restoreWinners(List<DAGDispatchEngine.PartitionDispatch> cohort, Stage stage) {
        cohort.stream().filter(partition -> partition.winner() != null)
                .forEach(partition -> stage.onTaskFinished(partition.winner().getAttempt().getTaskId()));
    }

    void registerTasks(
            List<DAGDispatchEngine.PartitionDispatch> pds, Stage stage,
            StageExecutionContext execution) {
        for (DAGDispatchEngine.PartitionDispatch pd : pds) {
            if (pd.winner() != null) {
                continue;
            }
            DispatchResult r = pd.future().join();
            taskStageRegistrar.register(execution.executionId(), r.taskId(), stage);

        }
    }

    private List<List<OperatorBinding>> materializeArtifactBindings(
            String tenantId, List<OperatorBinding> bindings, List<List<DataSplit>> taskSplits) {
        if (artifactBindingResolver == null) {
            throw new IllegalStateException("artifact retrieval plan has no durable metadata resolver");
        }
        return artifactBindingResolver.materializeForTasks(tenantId, bindings, taskSplits).stream()
                .map(GenerationPartitionResources::bind).toList();
    }

    private static List<SourceObjectSplits> groupSourceObjects(List<QuerySplit> splits) {
        var grouped = new java.util.LinkedHashMap<String, List<QuerySplit>>();
        for (var split : splits) {
            grouped.computeIfAbsent(split.data().getDataObjectId(), ignored -> new ArrayList<>()).add(split);
        }
        return grouped.values().stream()
                .map(group -> new SourceObjectSplits(group, group.stream()
                        .mapToLong(split -> Math.max(1L, split.estimatedScanBytes()))
                        .reduce(0L, (left, right) -> left + Math.min(Long.MAX_VALUE - left, right))))
                .toList();
    }

    /** Keeps every split of one immutable source object on one Native task. */
    private static List<List<QuerySplit>> partitionArtifactSplits(
            List<SourceObjectSplits> groups, int numPartitions) {
        return StageSplitPartitions.partitionSplits(groups, numPartitions, SourceObjectSplits::weight)
                .stream().map(partition -> partition.stream().flatMap(group -> group.splits().stream()).toList())
                .toList();
    }

    private static org.pangrova.shuffle.api.ShuffleHandle shuffleHandle(
            StageDAG dag, int stageId) {
        Stage stage = dag.findStage(stageId);
        return stage == null ? null : stage.shuffleHandle();
    }

    static org.pangrova.protocol.rest.config.ShuffleConfig buildShuffleConfig(
            StageNode plan, StageDAG dag) {
        return buildShuffleConfig(plan, dag, false);
    }

    static org.pangrova.protocol.rest.config.ShuffleConfig buildShuffleConfig(
            StageNode plan, StageDAG dag, boolean remoteShuffle) {
        return buildShuffleConfig(plan, dag, remoteShuffle, false);
    }

    static org.pangrova.protocol.rest.config.ShuffleConfig buildShuffleConfig(
            StageNode plan, StageDAG dag, boolean remoteShuffle, boolean localShuffle) {
        StageEdge downstreamEdge = dag.findDownstreamEdge(plan.stageId());
        if (downstreamEdge == null) {
            return null;
        }
        requireDistribution(downstreamEdge, remoteShuffle);
        StageNode downstreamNode = dag.findStageNode(downstreamEdge.downstreamStageId());
        if (downstreamNode == null) {
            return null;
        }
        int numPartitions = downstreamEdge.partitionCount();
        if (numPartitions != downstreamNode.parallelism()) {
            throw new IllegalStateException(
                    "Exchange partition count does not match downstream parallelism: exchange="
                            + downstreamEdge.exchangeId()
                            + ", partitions=" + numPartitions
                            + ", downstreamParallelism=" + downstreamNode.parallelism());
        }
        return new org.pangrova.protocol.rest.config.ShuffleConfig(
                downstreamEdge.batchSerde().name().substring("BATCH_SERDE_".length()), null, 0,
                remoteShuffle ? "MEMORY_AND_REMOTE" : localShuffle ? "MEMORY_AND_DISK" : "MEMORY_ONLY",
                numPartitions,
                downstreamEdge.distributionKeys());
    }

    private static void requireDistribution(StageEdge downstreamEdge, boolean remoteShuffle) {
        if (downstreamEdge.shuffleType() == org.pangrova.planner.enums.ShuffleType.ANY
                || downstreamEdge.shuffleType() == org.pangrova.planner.enums.ShuffleType.RANGE) {
            throw new IllegalArgumentException(
                    "Unsupported materialized shuffle distribution: "
                            + downstreamEdge.shuffleType());
        }
        if (remoteShuffle && downstreamEdge.shuffleType() == org.pangrova.planner.enums.ShuffleType.BROADCAST) {
            throw new IllegalArgumentException(
                    "External shuffle does not support distribution: "
                            + downstreamEdge.shuffleType());
        }
    }
}

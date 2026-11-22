package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;

import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.execution.binding.TaskRequestFactory;
import org.pangrova.execution.api.stage.TaskStage;
import org.pangrova.execution.api.enums.TaskPurpose;
import org.pangrova.query.api.task.QuerySplit;
import org.pangrova.execution.task.StageDispatchRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.StagePartitionDispatcher;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.generation.execution.binding.GenerationPartitionResources;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitPartitions;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.DynamicFilterBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches leaf-stage tasks to workers, partitions splits across tasks,
 * and collects dispatch results.
 */
final class LeafStageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(LeafStageDispatcher.class);
    private static final long MAX_DML_MEMORY_BYTES = 256L * 1024 * 1024;

    private final org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher;
    private final org.pangrova.planner.runtimefilter.RuntimeFilterRegistry runtimeFilterRegistry;
    private final QueryStateJournal stateJournal;
    private final long taskTimeoutSeconds;
    private final TaskStageRegistrar taskStageRegistrar;
    private final PublishedGenerationBindingResolver artifactBindingResolver;
    private final ExternalShuffleDescriptorEncoder descriptorEncoder;
    private final boolean remoteShuffle;
    private final boolean localShuffle;
    private final StagePartitionDispatcher partitions;
    private final DispatchCanceller canceller;

    LeafStageDispatcher(org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher,
                        org.pangrova.planner.runtimefilter.RuntimeFilterRegistry runtimeFilterRegistry,
                        QueryStateJournal stateJournal,
                        long taskTimeoutSeconds,
                        DispatchCanceller canceller,
                        TaskStageRegistrar taskStageRegistrar,
                        LeafDispatchOptions options) {
        this.dispatcher = dispatcher;
        this.runtimeFilterRegistry = runtimeFilterRegistry;
        this.stateJournal = stateJournal;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
        this.taskStageRegistrar = taskStageRegistrar;
        this.artifactBindingResolver = options.artifactBindingResolver();
        this.descriptorEncoder = options.descriptorEncoder();
        this.remoteShuffle = options.remoteShuffle();
        this.localShuffle = options.localShuffle();
        this.partitions = new StagePartitionDispatcher(dispatcher, canceller, options.taskAccessBinder());
        this.canceller = canceller;
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
            long planEpoch, DurableStageRuntime durable) {
        org.pangrova.query.api.session.QuerySession session() {
            return execution.requireLiveSession();
        }
    }

    private record PreparedDispatch(
            List<org.pangrova.protocol.rest.filter.DynamicFilterPayload> filters,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<List<String>> partitionedSplits,
            List<List<String>> partitionFilePaths,
            boolean leafStage,
            int taskCount) {}

    List<DAGDispatchEngine.PartitionDispatch> dispatchLeafStage(LeafDispatch input) {
        if (input.planEpoch() <= 0) {
            throw new IllegalArgumentException("planEpoch must be positive");
        }
        if (input.durable() != null) {
            return dispatchDurable(input);
        }
        PreparedDispatch prepared = prepareDispatch(input);
        List<DAGDispatchEngine.PartitionDispatch> dispatches = new ArrayList<>();
        for (int partition = 0; partition < prepared.taskCount(); partition++) {
            dispatches.add(dispatchPartition(input, prepared, partition));
        }
        LOG.info("event=push_mode_dispatch queryId={} stageId={} splits={}"
                        + " tasks={} shufflePartitions={}",
                input.session().queryId(), input.plan().stageId(),
                input.splitsWithPaths().size(), prepared.taskCount(),
                prepared.shuffleConfig() == null
                        ? 0 : prepared.shuffleConfig().numPartitions());
        return dispatches;
    }

    private PreparedDispatch prepareDispatch(LeafDispatch input) {
        var session = input.session();
        var plan = input.plan();
        var filters = DynamicFilterBinder.resolveFilters(
                session.queryId() + ":" + plan.stageId(), session.queryId(),
                runtimeFilterRegistry);
        var shuffleConfig = buildShuffleConfig(plan, input.dag(), remoteShuffle, localShuffle);
        List<String> allSplits = input.splitsWithPaths().stream()
                .map(QuerySplit::serializedData).toList();
        boolean leafStage = !input.splitsWithPaths().isEmpty();
        boolean hasArtifactBindings = input.boundPlan().operatorBindings().stream()
                .anyMatch(binding -> binding.getArtifactsCount() > 0);
        int taskCount = taskCount(input, shuffleConfig, allSplits,
                leafStage, hasArtifactBindings);
        TaskRequestFactory.validateSplitSources(
                allSplits, session.queryId(), plan.stageId());
        List<List<String>> partitionedSplits = hasArtifactBindings
                ? partitionArtifactSplits(allSplits, taskCount)
                : StageSplitPartitions.partitionSplits(allSplits, taskCount);
        List<List<String>> partitionFilePaths = StageSplitPartitions.buildPartitionFilePaths(
                input.splitsWithPaths(), partitionedSplits);
        return new PreparedDispatch(filters, shuffleConfig, partitionedSplits,
                partitionFilePaths, leafStage, taskCount);
    }

    private int taskCount(
            LeafDispatch input,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<String> splits,
            boolean leafStage,
            boolean hasArtifactBindings) {
        int workerCount = input.workerSelector().activeWorkerCount();
        if (workerCount <= 0) {
            throw new org.pangrova.scheduler.api.exception.NoAvailableWorkerException(
                    "No available worker for task dispatch: no schedulable capacity for this stage");
        }
        int rawParallelism = rawParallelism(input, leafStage, workerCount);
        int clusterCap = workerCount > Integer.MAX_VALUE / WorkerSelector.MAX_TASKS_PER_WORKER
                ? Integer.MAX_VALUE : workerCount * WorkerSelector.MAX_TASKS_PER_WORKER;
        int taskCount = taskCountForGraph(input.plan(), input.dag(), remoteShuffle, rawParallelism, clusterCap);
        if (hasArtifactBindings) taskCount = artifactTaskCount(input, splits, taskCount);
        boolean outputStage = input.dag().findDownstreamEdge(
                input.plan().stageId()) == null;
        if (outputStage && shuffleConfig == null && !input.distributedRetrieval()
                && input.plan().stagePlan().mode() == StagePlanMode.LIVE_QUERY) {
            taskCount = 1;
        }
        return taskCount;
    }

    private int artifactTaskCount(LeafDispatch input, List<String> splits, int taskCount) {
        int count = Math.min(taskCount, countSourceObjects(splits));
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

    private DAGDispatchEngine.PartitionDispatch dispatchPartition(
            LeafDispatch input, PreparedDispatch prepared, int partition) {
        var worker = selectWorker(input, prepared, partition);
        var request = buildTaskRequest(input, prepared, partition, worker);
        var issued = partitions.dispatch(request, worker);
        return new DAGDispatchEngine.PartitionDispatch(issued.acknowledgement(), worker.httpPort(),
                worker.transportPort(), worker.workerId(), issued.request(), null);
    }

    private List<DAGDispatchEngine.PartitionDispatch> dispatchDurable(LeafDispatch input) {
        List<DAGDispatchEngine.PartitionDispatch> result = new ArrayList<>();
        for (int partition = 0; partition < input.plan().parallelism(); partition++) {
            var progress = input.durable().inspect(input.plan().stageId(), partition);
            if (progress.winner().isPresent()) {
                var winner = input.durable().winner(input.plan().stageId(), partition, progress);
                result.add(new DAGDispatchEngine.PartitionDispatch(
                        java.util.concurrent.CompletableFuture.completedFuture(null), 0, 0, null, null, winner));
                continue;
            }
            var access = new DurablePartitionRequest.ExchangeAccess(input.planEpoch(),
                    ExchangeSourceBuilder.buildExchangeInputs(input.plan(), input.dag(), input.dispatched(), partition),
                    buildShuffleConfig(input.plan(), input.dag(), true),
                    shuffleHandle(input.dag(), input.plan().stageId()), descriptorEncoder);
            var prepared = input.durable().prepare(input.plan().stageId(), partition, progress, access,
                    java.time.Instant.now()).orElseThrow(() -> new IllegalStateException(
                            "Exact durable partition is not claimable by the current execution owner"));
            var issued = partitions.dispatch(prepared.request(), prepared.worker());
            input.durable().dispatched(prepared, issued.acknowledgement());
            result.add(new DAGDispatchEngine.PartitionDispatch(issued.acknowledgement(), prepared.worker().httpPort(),
                    prepared.worker().transportPort(), prepared.worker().workerId(), issued.request(), null));
        }
        return List.copyOf(result);
    }

    private WorkerInfo selectWorker(
            LeafDispatch input, PreparedDispatch prepared, int partition) {
        List<String> paths = prepared.partitionFilePaths().get(partition);
        if (!input.boundPlan().operatorBindings().isEmpty()) {
            return input.workerSelector().selectForBindings(
                    input.session().tenantId(), paths,
                    input.boundPlan().operatorBindings());
        }
        if (prepared.leafStage() && !paths.isEmpty()) {
            return input.workerSelector().selectWithCacheAffinity(
                    input.session().tenantId(), paths);
        }
        return input.workerSelector().select(input.session().tenantId());
    }

    private SubmitTaskRequest buildTaskRequest(
            LeafDispatch input, PreparedDispatch prepared, int partition,
            WorkerInfo selectedWorker) {
        var attempt = org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(
                input.planEpoch(), 0);
        var sources = ExchangeSourceBuilder.buildExchangeInputs(
                input.plan(), input.dag(), input.dispatched(), partition, localShuffle ? selectedWorker : null);
        var limit = input.workerSelector().computeResourceLimit(
                input.plan().parallelism(), 1, selectedWorker);
        List<String> taskSplits = prepared.partitionedSplits().get(partition);
        var taskBindings = materializeArtifactBindings(
                input.session().tenantId(),
                input.boundPlan().operatorBindings(), taskSplits);
        if (input.writeConfig() != null) {
            return TaskRequestFactory.buildDmlWriteRequest(
                    new TaskRequestFactory.DmlWriteTask(
                            input.session(), input.boundPlan().planBytes(),
                            dmlResourceGrant(limit), sources, taskBindings,
                            prepared.filters(), taskSplits, input.writeConfig(),
                            taskTimeoutSeconds, attempt));
        }
        return TaskRequestFactory.buildLeafTaskRequest(
                new TaskRequestFactory.LeafTask(
                        input.execution(), input.boundPlan().planBytes(),
                        new TaskStage(input.plan().stageId(), TaskPurpose.valueOf(input.plan().taskType())),
                        partition, limit, sources,
                        prepared.filters(), prepared.shuffleConfig(), taskSplits,
                        org.pangrova.execution.api.deadline.StageTaskDeadline.after(
                                java.time.Duration.ofSeconds(taskTimeoutSeconds), java.time.Instant.now()),
                        attempt, taskBindings,
                        shuffleHandle(input.dag(), input.plan().stageId()),
                        input.boundPlan().resultColumnTypes()));
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
            if (stateJournal != null) {
                canceller.journalScheduledTask(stateJournal, r.taskId());
            }
        }
    }

    private List<org.pangrova.protocol.v2.OperatorBinding> materializeArtifactBindings(
            String tenantId,
            List<org.pangrova.protocol.v2.OperatorBinding> bindings,
            List<String> taskSplits) {
        boolean requiresArtifacts = bindings.stream()
                .anyMatch(binding -> binding.getArtifactsCount() > 0);
        if (!requiresArtifacts) {
            return bindings;
        }
        if (artifactBindingResolver == null) {
            throw new IllegalStateException(
                    "artifact retrieval plan has no durable metadata resolver");
        }
        return GenerationPartitionResources.bind(artifactBindingResolver.materializeForTask(
                tenantId, bindings, taskSplits));
    }

    private static int countSourceObjects(List<String> splits) {
        return Math.max(1, (int) splits.stream()
                .map(LeafStageDispatcher::sourceObjectId)
                .distinct().count());
    }

    /** Keeps every split of one immutable source object on one Native task. */
    static List<List<String>> partitionArtifactSplits(
            List<String> splits, int numPartitions) {
        java.util.Map<String, List<String>> grouped = new java.util.LinkedHashMap<>();
        for (String split : splits) {
            grouped.computeIfAbsent(sourceObjectId(split), ignored -> new ArrayList<>())
                    .add(split);
        }
        List<List<String>> groups = new ArrayList<>(grouped.values());
        groups.sort((left, right) -> Long.compare(
                right.stream().mapToLong(String::length).sum(),
                left.stream().mapToLong(String::length).sum()));
        List<List<String>> result = new ArrayList<>(numPartitions);
        long[] loads = new long[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            result.add(new ArrayList<>());
        }
        for (List<String> group : groups) {
            int target = 0;
            for (int partition = 1; partition < numPartitions; partition++) {
                if (loads[partition] < loads[target]) {
                    target = partition;
                }
            }
            result.get(target).addAll(group);
            loads[target] += group.stream().mapToLong(String::length).sum();
        }
        return result;
    }

    private static String sourceObjectId(String encodedSplit) {
        try {
            var split = org.pangrova.protocol.v2.DataSplit.parseFrom(
                    java.util.Base64.getDecoder().decode(encodedSplit));
            if (split.getDataObjectId().isBlank()) {
                throw new IllegalArgumentException("split data_object_id is blank");
            }
            return split.getDataObjectId();
        } catch (com.google.protobuf.InvalidProtocolBufferException
                | IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "cannot partition artifact split by source object", failure);
        }
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

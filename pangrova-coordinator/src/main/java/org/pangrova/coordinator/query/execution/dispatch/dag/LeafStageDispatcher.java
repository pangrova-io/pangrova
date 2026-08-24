package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.DynamicFilterBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches leaf-stage tasks to workers, partitions splits across tasks,
 * and collects dispatch results.
 */
final class LeafStageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(LeafStageDispatcher.class);

    private final org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher;
    private final org.pangrova.planner.runtimefilter.RuntimeFilterRegistry runtimeFilterRegistry;
    private final QueryStateJournal stateJournal;
    private final DispatchCanceller canceller;
    private final TaskStageRegistrar taskStageRegistrar;

    LeafStageDispatcher(org.pangrova.coordinator.scheduler.TaskDispatcher dispatcher,
                        org.pangrova.planner.runtimefilter.RuntimeFilterRegistry runtimeFilterRegistry,
                        QueryStateJournal stateJournal,
                        DispatchCanceller canceller,
                        TaskStageRegistrar taskStageRegistrar) {
        this.dispatcher = dispatcher;
        this.runtimeFilterRegistry = runtimeFilterRegistry;
        this.stateJournal = stateJournal;
        this.canceller = canceller;
        this.taskStageRegistrar = taskStageRegistrar;
    }

    List<DAGDispatchEngine.PartitionDispatch> dispatchLeafStage(
            CoordinatorSessionContext session, StageNode plan,
            byte[] planBytes,
            List<SplitFetcher.SplitWithPaths> splitsWithPaths,
            WorkerSelector ws, StageDAG dag,
            List<DAGDispatchEngine.StageDispatchRecord> dispatched) {
        return dispatchLeafStage(session, plan, planBytes, splitsWithPaths,
                ws, dag, dispatched, null);
    }

    List<DAGDispatchEngine.PartitionDispatch> dispatchLeafStage(
            CoordinatorSessionContext session, StageNode plan,
            byte[] planBytes,
            List<SplitFetcher.SplitWithPaths> splitsWithPaths,
            WorkerSelector ws, StageDAG dag,
            List<DAGDispatchEngine.StageDispatchRecord> dispatched,
            org.pangrova.protocol.rest.config.WriteConfig writeConfig) {
        // F-AQE-02: Use queryId-namespaced key to match the registration in
        // RuntimeFilterStageHandler, preventing cross-query filter collisions.
        var filters = DynamicFilterBinder.resolveFilters(
                session.queryId() + ":" + plan.stageId(), session.queryId(),
                runtimeFilterRegistry);
        org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig =
                buildShuffleConfig(plan, dag);
        List<String> allSplits = splitsWithPaths.stream()
                .map(SplitFetcher.SplitWithPaths::serializedData).toList();
        boolean isLeafStage = !splitsWithPaths.isEmpty();
        int rawParallelism = isLeafStage
                ? Math.min(plan.parallelism(),
                        Math.max(splitsWithPaths.size(), 1))
                : plan.parallelism();
        // Cap intermediate stage parallelism to cluster capacity.
        // Leaf stages are naturally bounded by split count; intermediate stages
        // (hash join build side, aggregation) could exceed worker count without this.
        int maxTasksPerWorker = 4;
        int workerCount = ws.activeWorkerCount();
        int clusterCap = workerCount > (Integer.MAX_VALUE / maxTasksPerWorker)
                ? Integer.MAX_VALUE
                : workerCount * maxTasksPerWorker;
        int numTasks = Math.min(rawParallelism, clusterCap);
        boolean isOutputStage = dag.findDownstreamEdge(plan.stageId()) == null;
        if (isOutputStage && shuffleConfig == null) {
            numTasks = 1;
        }
        if (numTasks > 1 && plan.planFragment().containsWindowFunction()) {
            LOG.info("event=window_function_consolidation queryId={} stageId={}"
                            + " originalTasks={} reason=window_requires_single_task",
                    session.queryId(), plan.stageId(), numTasks);
            numTasks = 1;
        }
        TaskRequestFactory.validateMultiTableSplits(
                allSplits, session.queryId(), plan.stageId());
        List<List<String>> partitionedSplits = partitionSplits(allSplits, numTasks);
        List<List<String>> partitionFilePaths = buildPartitionFilePaths(
                splitsWithPaths, numTasks);
        List<DAGDispatchEngine.PartitionDispatch> dispatches = new ArrayList<>();
        for (int p = 0; p < numTasks; p++) {
            var sources = ExchangeSourceBuilder.buildExchangeSources(
                    plan, dag, dispatched, p);
            WorkerInfo worker = isLeafStage && !partitionFilePaths.get(p).isEmpty()
                    ? ws.selectWithCacheAffinity(
                            session.tenantId(), partitionFilePaths.get(p))
                    : ws.select(session.tenantId());
            var limit = ws.computeResourceLimit(plan.parallelism(), 1);
            SubmitTaskRequest req;
            if (writeConfig != null) {
                long dmlMemory = Math.min(limit.maxMemoryBytes(), 256L * 1024 * 1024);
                var dmlLimit = new org.pangrova.protocol.rest.config.ResourceLimitPayload(
                        dmlMemory, limit.maxThreads(),
                        limit.maxSpillBytes(), limit.spillDirectory());
                req = TaskRequestFactory.buildDmlWriteRequest(
                        session, planBytes, dmlLimit, filters,
                        partitionedSplits.get(p), writeConfig);
            } else {
                req = TaskRequestFactory.buildLeafTaskRequest(
                        session, planBytes, plan, p, limit, sources, filters,
                        shuffleConfig, partitionedSplits.get(p));
            }
            dispatches.add(new DAGDispatchEngine.PartitionDispatch(
                    dispatcher.dispatchAsync(req, worker),
                    worker.httpPort(), worker.transportPort(),
                    worker.workerId(), req));
        }
        LOG.info("event=push_mode_dispatch queryId={} stageId={} splits={}"
                        + " tasks={} shufflePartitions={}",
                session.queryId(), plan.stageId(), splitsWithPaths.size(),
                numTasks,
                shuffleConfig != null ? shuffleConfig.numPartitions() : 0);
        return dispatches;
    }

    void populateDispatchRecords(
            List<DAGDispatchEngine.PartitionDispatch> pds, Stage stage,
            List<DAGDispatchEngine.StageDispatchRecord> dispatched,
            ConcurrentHashMap<Integer, DispatchResult> results) {
        DispatchResult first = null;
        for (DAGDispatchEngine.PartitionDispatch pd : pds) {
            DispatchResult r = pd.future().join();
            dispatched.add(new DAGDispatchEngine.StageDispatchRecord(
                    stage.stageId(), r, pd.workerHttpPort(), pd.workerTransportPort()));
            if (first == null) {
                first = r;
                results.put(stage.stageId(), first);
            }
        }
    }

    void registerTasks(
            List<DAGDispatchEngine.PartitionDispatch> pds, Stage stage,
            CoordinatorSessionContext session) {
        for (DAGDispatchEngine.PartitionDispatch pd : pds) {
            DispatchResult r = pd.future().join();
            stage.addTaskId(r.taskId());
            canceller.registerDispatchedTask(session.queryId(), r, pd.workerId());
            canceller.storeTaskRequest(r.taskId(), pd.request());
            taskStageRegistrar.register(session.queryId(), r.taskId(), stage);
            if (stateJournal != null) {
                stateJournal.appendTaskEvent(new TaskStateEvent(
                        session.queryId(), r.taskId(), pd.workerId(),
                        "SCHEDULED", stage.stageId(), 0L,
                        System.currentTimeMillis()));
            }
        }
    }

    /**
     * Distributes splits across partitions using greedy load balancing.
     * Sorts splits by serialized size descending, then assigns each split
     * to the partition with the smallest accumulated load (LPT algorithm).
     * This reduces data skew compared to round-robin when split sizes vary.
     */
    static List<List<String>> partitionSplits(List<String> splits, int numPartitions) {
        List<List<String>> result = new ArrayList<>(numPartitions);
        long[] partitionLoad = new long[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            result.add(new ArrayList<>());
        }
        if (splits.isEmpty()) {
            return result;
        }
        // Sort indices by split serialized length descending (LPT heuristic).
        Integer[] indices = new Integer[splits.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        java.util.Arrays.sort(indices,
                (a, b) -> Integer.compare(splits.get(b).length(), splits.get(a).length()));
        for (int idx : indices) {
            int minPartition = 0;
            for (int p = 1; p < numPartitions; p++) {
                if (partitionLoad[p] < partitionLoad[minPartition]) {
                    minPartition = p;
                }
            }
            result.get(minPartition).add(splits.get(idx));
            partitionLoad[minPartition] += splits.get(idx).length();
        }
        return result;
    }

    static List<List<String>> buildPartitionFilePaths(
            List<SplitFetcher.SplitWithPaths> splitsWithPaths, int numPartitions) {
        List<List<String>> result = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            result.add(new ArrayList<>());
        }
        for (int i = 0; i < splitsWithPaths.size(); i++) {
            result.get(i % numPartitions).addAll(splitsWithPaths.get(i).filePaths());
        }
        return result;
    }

    static org.pangrova.protocol.rest.config.ShuffleConfig buildShuffleConfig(
            StageNode plan, StageDAG dag) {
        StageEdge downstreamEdge = dag.findDownstreamEdge(plan.stageId());
        if (downstreamEdge == null || !downstreamEdge.isHashShuffle()) {
            return null;
        }
        StageNode downstreamNode = dag.findStageNode(downstreamEdge.downstreamStageId());
        if (downstreamNode == null) {
            return null;
        }
        int numPartitions = downstreamNode.parallelism();
        return new org.pangrova.protocol.rest.config.ShuffleConfig(
                null, null, 0, null, numPartitions,
                downstreamEdge.distributionKeys());
    }
}

package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.Map;
import java.util.stream.Collectors;

import org.pangrova.execution.core.authority.StageExecutionContext;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.protocol.rest.shuffle.PartitionStats;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.pangrova.shuffle.api.ShuffleCommitResult;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Manages the control-plane lifecycle of blocking external shuffle outputs.
 * Row transport and partition processing remain entirely in the Worker data plane.
 */
public final class ShuffleStageHandler implements StageLifecycleHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleStageHandler.class);

    private final ShuffleLifecycleManager shuffleLifecycleManager;

    public ShuffleStageHandler(ShuffleLifecycleManager shuffleLifecycleManager) {
        this.shuffleLifecycleManager = checkNotNull(
                shuffleLifecycleManager, "shuffleLifecycleManager must not be null");
    }

    @Override
    public void onScheduling(StageLifecycleEvent event) {
        Stage stage = event.stage();
        StageEdge outputEdge = externalShuffleOutput(event);
        if (outputEdge == null) {
            return;
        }
        long planEpoch = event.execution()
                instanceof StageExecutionContext.Durable durable
                ? durable.planEpoch(stage.stageId()) : Math.addExact(stage.currentGeneration(), 1L);
        int expectedProducerCount = stage.plan().parallelism();
        long estimatedBytesPerPartition = estimatedBytesPerPartition(
                stage.plan().estimatedShuffleBytes(), outputEdge.partitionCount());
        var handle = shuffleLifecycleManager.registerShuffle(
                new ShuffleRegistration(
                        event.queryId(), stage.stageId(),
                        outputEdge.partitionCount(), estimatedBytesPerPartition, false,
                        planEpoch, expectedProducerCount,
                        ShuffleProducerSetChecksum.forContiguousPartitions(
                                event.queryId(), stage.stageId(), planEpoch,
                                expectedProducerCount)));
        if (handle.endpoints().isEmpty()) {
            throw new IllegalStateException(
                    "external shuffle registration returned no partition endpoints");
        }
        stage.bindShuffleAllocation(handle, planEpoch);
        if (event.execution() instanceof StageExecutionContext.Durable durable) {
            var owner = durable.authority().executionAnchor();
            stage.bindShuffleExecutionIdentity(planEpoch, org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                    .setOwnerId(owner.ownerId()).setOwnerIncarnation(owner.ownerIncarnation())
                    .setFencingToken(owner.fencingToken()).build());
        }
        LOG.debug("event=shuffle_registered queryId={} stageId={} shuffleId={}",
                event.queryId(), stage.stageId(), handle.shuffleId());
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        Stage stage = event.stage();
        if (stage.shuffleHandle() == null || stage.shuffleHandle().endpoints().isEmpty()) {
            return;
        }
        ShuffleCommitResult commitResult = shuffleLifecycleManager.commitShuffle(
                event.queryId(), stage.stageId());
        if (!commitResult.partitionStats().isEmpty()) {
            ShuffleStageStatsReport report = buildStatsReport(
                    event.queryId(), stage.stageId(), commitResult);
            stage.setShuffleStatsReport(report);
            LOG.info("event=shuffle_commit_stats queryId={} stageId={} totalBytes={} totalRows={} partitions={}",
                    event.queryId(), stage.stageId(),
                    commitResult.totalBytes(), commitResult.totalRows(),
                    commitResult.partitionStats().size());
        }
    }

    private static StageEdge externalShuffleOutput(StageLifecycleEvent event) {
        var outputs = event.dag().stageEdges().stream()
                .filter(edge -> edge.upstreamStageId() == event.stage().stageId())
                .filter(edge -> edge.shuffleType() == org.pangrova.planner.core.analysis.ShuffleType.HASH
                        || edge.shuffleType() == org.pangrova.planner.core.analysis.ShuffleType.ROUND_ROBIN
                        || edge.shuffleType() == org.pangrova.planner.core.analysis.ShuffleType.SINGLETON)
                .toList();
        if (outputs.isEmpty()) {
            return null;
        }
        if (outputs.size() != 1) {
            throw new IllegalStateException(
                    "one stage cannot bind multiple external shuffle outputs");
        }
        return outputs.get(0);
    }

    private static long estimatedBytesPerPartition(long estimatedBytes, int partitionCount) {
        if (estimatedBytes <= 0) {
            return 0L;
        }
        return estimatedBytes / partitionCount
                + (estimatedBytes % partitionCount == 0 ? 0 : 1);
    }

    private static ShuffleStageStatsReport buildStatsReport(
            String queryId, int stageId, ShuffleCommitResult commitResult) {
        Map<Integer, PartitionStats> protocolStats = commitResult.partitionStats().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new PartitionStats(e.getValue().dataBytes(), e.getValue().rowCount())));
        return new ShuffleStageStatsReport(
                queryId,
                String.valueOf(stageId),
                protocolStats,
                commitResult.totalBytes(),
                commitResult.totalRows(),
                System.currentTimeMillis());
    }

    @Override
    public void onFailed(StageLifecycleEvent event) {
        // Stage-level failure cleanup is handled by onQueryFailed at query level.
        // Per-stage onFailed is intentionally a no-op to avoid double-cleanup
        // when DAGDispatchEngine fires onQueryFailed after stage failure propagation.
    }

    @Override
    public void onQueryCompleted(String queryId) {
        shuffleLifecycleManager.cleanupShuffle(queryId);
        LOG.debug("event=shuffle_query_cleanup queryId={} reason=query_completed", queryId);
    }

    @Override
    public void onQueryFailed(String queryId) {
        shuffleLifecycleManager.cleanupShuffle(queryId);
        LOG.debug("event=shuffle_query_cleanup queryId={} reason=query_failed", queryId);
    }
}

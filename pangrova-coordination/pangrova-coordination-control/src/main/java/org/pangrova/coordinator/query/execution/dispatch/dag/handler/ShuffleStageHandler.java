package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.protocol.shuffle.codec.ShuffleProducerSetChecksum;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

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
        int expectedProducerCount = stage.physicalTaskCount();
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
        var publication = stage.shufflePublication(event.queryId());
        var receipt = shuffleLifecycleManager.commitShuffle(publication);
        stage.bindShuffleReceipt(publication, receipt);
        long bytes = 0;
        long rows = 0;
        for (var partition : receipt.partitions().values()) {
            bytes = Math.addExact(bytes, partition.bytes());
            rows = Math.addExact(rows, partition.rows());
        }
        LOG.info("event=shuffle_commit_stats queryId={} stageId={} totalBytes={} totalRows={} partitions={}",
                event.queryId(), stage.stageId(), bytes, rows, receipt.partitions().size());
    }

    private static StageEdge externalShuffleOutput(StageLifecycleEvent event) {
        var outputs = event.dag().outgoingEdges(event.stage().stageId()).stream()
                .filter(edge -> edge.shuffleType() == org.pangrova.planner.enums.ShuffleType.HASH
                        || edge.shuffleType() == org.pangrova.planner.enums.ShuffleType.ROUND_ROBIN
                        || edge.shuffleType() == org.pangrova.planner.enums.ShuffleType.SINGLETON)
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

    @Override
    public void onDurableQueryCompleted(String queryId) {
        shuffleLifecycleManager.cleanupShuffle(queryId);
        LOG.debug("event=shuffle_query_cleanup queryId={} reason=durable_graph_completed", queryId);
    }
}

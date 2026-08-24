package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.Map;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.protocol.rest.shuffle.PartitionStats;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Manages shuffle lifecycle during stage execution.
 * Registers shuffle on SCHEDULING, commits on FINISHED, cleans up on FAILED.
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
        var handle = shuffleLifecycleManager.registerShuffle(
                new ShuffleRegistration(
                        event.queryId(), stage.stageId(),
                        stage.plan().parallelism(), 0L, false));
        LOG.debug("event=shuffle_registered queryId={} stageId={} shuffleId={}",
                event.queryId(), stage.stageId(), handle.shuffleId());
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        Stage stage = event.stage();
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

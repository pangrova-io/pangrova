package org.pangrova.coordinator.resilience.lineage;

import org.pangrova.coordinator.enums.StageRecoveryMode;
import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks stage dependency lineage and selects recovery mode for failed stages.
 *
 * <p>Concurrency note: individual map operations are thread-safe via
 * {@link ConcurrentHashMap}, but cross-entry consistency during
 * {@link #getRecomputeChain} is not guaranteed if concurrent
 * {@link #recordLineage} calls modify the graph.</p>
 */
@ThreadSafe
public class StageLineageTracker {

    private static final Logger LOG = LoggerFactory.getLogger(StageLineageTracker.class);
    private static final Duration RECOMPUTE_THRESHOLD = Duration.ofSeconds(30);
    private static final long SHUFFLE_BYTES_THRESHOLD = 1L * 1024 * 1024 * 1024;

    private final ConcurrentHashMap<String, List<String>> lineageGraph = new ConcurrentHashMap<>();
    private final Set<String> lostOutputs = ConcurrentHashMap.newKeySet();

    public void recordLineage(String stageId, List<String> inputStageIds) {
        lineageGraph.put(stageId, List.copyOf(inputStageIds));
    }

    /** Chooses a recovery mode based on estimated recompute duration and shuffle size. */
    public StageRecoveryMode selectRecoveryMode(Duration estimatedDuration, long estimatedShuffleBytes) {
        if (estimatedDuration.compareTo(RECOMPUTE_THRESHOLD) < 0) {
            return StageRecoveryMode.RECOMPUTE;
        }
        if (estimatedShuffleBytes > SHUFFLE_BYTES_THRESHOLD) {
            return StageRecoveryMode.MEMORY_AND_REMOTE;
        }
        return selectByCost(estimatedDuration, estimatedShuffleBytes);
    }

    /** Returns the ordered chain of upstream stages that must be recomputed for the failed stage. */
    public List<String> getRecomputeChain(String failedStageId) {
        List<String> chain = new ArrayList<>();
        Set<String> visited = ConcurrentHashMap.newKeySet();
        buildChain(failedStageId, chain, visited);
        return Collections.unmodifiableList(chain);
    }

    public void markOutputLost(String stageId) {
        lostOutputs.add(stageId);
        LOG.info("Stage {} output marked as lost", stageId);
    }

    public boolean isOutputLost(String stageId) {
        return lostOutputs.contains(stageId);
    }

    private StageRecoveryMode selectByCost(Duration estimatedDuration, long estimatedShuffleBytes) {
        long recomputeCost = estimatedDuration.toMillis();
        long shuffleCost = estimatedShuffleBytes / (1024 * 1024);
        return recomputeCost < shuffleCost ? StageRecoveryMode.RECOMPUTE : StageRecoveryMode.MEMORY_AND_REMOTE;
    }

    private void buildChain(String stageId, List<String> chain, Set<String> visited) {
        if (!visited.add(stageId)) {
            throw new IllegalStateException("Cycle detected in stage lineage at stage: " + stageId);
        }
        List<String> parents = lineageGraph.getOrDefault(stageId, List.of());
        for (String parent : parents) {
            buildChain(parent, chain, visited);
        }
        chain.add(stageId);
    }
}

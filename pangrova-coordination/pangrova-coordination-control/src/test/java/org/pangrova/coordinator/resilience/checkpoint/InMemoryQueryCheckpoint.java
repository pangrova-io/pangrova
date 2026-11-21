package org.pangrova.coordinator.resilience.checkpoint;

import org.pangrova.common.resilience.api.QueryCheckpoint;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link QueryCheckpoint} for testing and single-node deployments.
 *
 * <p>Stage checkpoints are stored in a ConcurrentHashMap keyed by (queryId, stageId).
 * State is lost on process restart — production deployments should use a JDBC-backed
 * implementation.</p>
 */
public final class InMemoryQueryCheckpoint implements QueryCheckpoint {

    private final ConcurrentHashMap<String, StageCheckpoint> store = new ConcurrentHashMap<>();

    @Override
    public void checkpointStage(StageCheckpoint checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException("checkpoint must not be null");
        }
        store.put(compositeKey(checkpoint.queryId(), checkpoint.stageId()), checkpoint);
    }

    @Override
    public Optional<StageCheckpoint> getStageCheckpoint(String queryId, String stageId) {
        return Optional.ofNullable(store.get(compositeKey(queryId, stageId)));
    }

    @Override
    public List<StageCheckpoint> getQueryCheckpoints(String queryId) {
        String prefix = queryId + ":";
        return store.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .map(Map.Entry::getValue)
                .sorted(Comparator.comparingLong(StageCheckpoint::checkpointedAtMs))
                .toList();
    }

    @Override
    public void clearQueryCheckpoints(String queryId) {
        String prefix = queryId + ":";
        store.keySet().removeIf(k -> k.startsWith(prefix));
    }

    /** Returns the total number of stored checkpoints (for testing). */
    public int size() {
        return store.size();
    }

    private static String compositeKey(String queryId, String stageId) {
        return queryId + ":" + stageId;
    }
}

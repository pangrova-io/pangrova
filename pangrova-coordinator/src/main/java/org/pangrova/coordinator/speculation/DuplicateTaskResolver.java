package org.pangrova.coordinator.speculation;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * First-to-finish deduplication for shuffle outputs.
 * Uses putIfAbsent on (stageId:partitionId) key to determine the winner.
 *
 * <p>Concurrency strategy: ConcurrentHashMap with putIfAbsent for atomic winner registration.</p>
 */
@ThreadSafe
public class DuplicateTaskResolver {

    private final ConcurrentHashMap<String, String> shuffleOutputRegistry = new ConcurrentHashMap<>();

    /**
     * Registers a task completion for a given stage partition.
     * Returns true if this task is the first (winner).
     */
    public boolean onTaskComplete(String taskId, String stageId, int partitionId) {
        String key = buildKey(stageId, partitionId);
        return shuffleOutputRegistry.putIfAbsent(key, taskId) == null;
    }

    public Optional<String> getWinner(String stageId, int partitionId) {
        return Optional.ofNullable(shuffleOutputRegistry.get(buildKey(stageId, partitionId)));
    }

    public void clear(String stageId) {
        shuffleOutputRegistry.keySet().removeIf(key -> key.startsWith(stageId + ":"));
    }

    private static String buildKey(String stageId, int partitionId) {
        return stageId + ":" + partitionId;
    }
}

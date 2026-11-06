package org.pangrova.scheduler.core.speculation;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * First-to-finish deduplication for speculative attempts of one query partition.
 * Uses putIfAbsent on the full query/stage/partition identity to determine the winner.
 *
 * <p>Concurrency strategy: ConcurrentHashMap with putIfAbsent for atomic winner registration.</p>
 */
@ThreadSafe
public class DuplicateTaskResolver {

    private final ConcurrentHashMap<PartitionKey, String> shuffleOutputRegistry =
            new ConcurrentHashMap<>();

    /**
     * Registers a task completion for a given stage partition.
     * Returns true if this task is the first (winner).
     */
    public boolean onTaskComplete(
            String queryId, String taskId, String stageId, int partitionId) {
        return shuffleOutputRegistry.putIfAbsent(
                new PartitionKey(queryId, stageId, partitionId), taskId) == null;
    }

    public Optional<String> getWinner(String queryId, String stageId, int partitionId) {
        return Optional.ofNullable(shuffleOutputRegistry.get(
                new PartitionKey(queryId, stageId, partitionId)));
    }

    public void clear(String queryId, String stageId, int partitionId) {
        shuffleOutputRegistry.remove(new PartitionKey(queryId, stageId, partitionId));
    }

    /** Releases every election for a terminal query without affecting peers. */
    public void clearQuery(String queryId) {
        shuffleOutputRegistry.keySet().removeIf(key -> key.queryId().equals(queryId));
    }

    private record PartitionKey(String queryId, String stageId, int partitionId) {
        private PartitionKey {
            if (queryId == null || queryId.isBlank() || stageId == null
                    || stageId.isBlank() || partitionId < 0) {
                throw new IllegalArgumentException("speculative partition identity is invalid");
            }
        }
    }
}

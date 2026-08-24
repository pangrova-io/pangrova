package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.shuffle.ShuffleDataInventory;
import org.pangrova.shuffle.ShuffleDataRef;

/**
 * In-memory implementation of {@link ShuffleDataInventory} for the coordinator (G-RES-08).
 *
 * <p>Tracks committed shuffle data references in a ConcurrentHashMap, keyed by a composite
 * of (queryId, stageId, workerId). The coordinator populates this inventory as stages commit
 * shuffle data and cleans it up when queries complete.
 *
 * <p>Thread-safe: all mutation goes through ConcurrentHashMap atomic operations.
 */
@ThreadSafe
public final class InMemoryShuffleDataInventory implements ShuffleDataInventory {

    /**
     * Composite key for shuffle data refs.
     */
    private record RefKey(String queryId, int stageId, String workerId) {}

    private final ConcurrentHashMap<RefKey, ShuffleDataRef> refs = new ConcurrentHashMap<>();

    @Override
    public List<ShuffleDataRef> getLiveDataOnWorker(String workerId) {
        return refs.values().stream()
                .filter(ref -> ref.workerId().equals(workerId))
                .filter(ShuffleDataRef::isLive)
                .toList();
    }

    @Override
    public void register(ShuffleDataRef ref) {
        refs.put(new RefKey(ref.queryId(), ref.stageId(), ref.workerId()), ref);
    }

    @Override
    public void markConsumed(String queryId, int stageId, String workerId) {
        refs.computeIfPresent(new RefKey(queryId, stageId, workerId), (key, existing) ->
                new ShuffleDataRef(
                        existing.queryId(),
                        existing.stageId(),
                        existing.workerId(),
                        existing.retention(),
                        existing.sizeBytes(),
                        true));
    }

    @Override
    public void removeQuery(String queryId) {
        refs.entrySet().removeIf(entry -> entry.getKey().queryId().equals(queryId));
    }

    /**
     * Returns the total number of tracked shuffle data references (for testing/metrics).
     */
    public int size() {
        return refs.size();
    }

    /**
     * Returns all tracked references as an unmodifiable snapshot (for testing/metrics).
     */
    public Map<?, ShuffleDataRef> snapshot() {
        return Map.copyOf(refs);
    }
}

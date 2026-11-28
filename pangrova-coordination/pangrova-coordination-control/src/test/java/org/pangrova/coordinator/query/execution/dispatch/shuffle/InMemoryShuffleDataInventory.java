package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.shuffle.api.ShuffleDataInventory;
import org.pangrova.shuffle.api.DecommissionAssessment;
import org.pangrova.shuffle.api.ShuffleDataRef;

/**
 * Diagnostic references supplied manually, not a production-complete Worker residency view.
 * The current key has no worker incarnation or exchange generation and no admission fence.
 * Consequently this implementation cannot authorize decommission, even when its map is empty.
 * It must be replaced by an authoritative view when production removal is wired.
 */
@ThreadSafe
public final class InMemoryShuffleDataInventory implements ShuffleDataInventory {

    /**
     * Composite key for shuffle data refs.
     */
    private record RefKey(String queryId, int stageId, String workerId) {}

    private final ConcurrentHashMap<RefKey, ShuffleDataRef> refs = new ConcurrentHashMap<>();

    @Override
    public DecommissionAssessment assessDecommission(String workerId) {
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        return DecommissionAssessment.UNKNOWN;
    }

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

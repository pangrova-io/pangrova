package org.pangrova.planner.runtimefilter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Query-level registry for runtime filter propagation.
 *
 * <p>Keyed by {@code targetStageId} — the probe-side stage that the filter
 * applies to. Build-side stages register filters after completing; probe-side
 * stages retrieve them before scanning.</p>
 *
 * <p>Thread-safe: backed by {@link ConcurrentHashMap}.</p>
 */
public final class RuntimeFilterRegistry {

    private final ConcurrentHashMap<String, RuntimeFilter> filtersByTargetStage =
            new ConcurrentHashMap<>();

    /**
     * Registers a runtime filter for the given target (probe) stage.
     *
     * @param targetStageId the probe-side stage ID
     * @param filter        the runtime filter generated from build-side data
     */
    public void register(String targetStageId, RuntimeFilter filter) {
        filtersByTargetStage.put(targetStageId, filter);
    }

    /**
     * Retrieves the runtime filter for the given target stage, if available.
     *
     * @param targetStageId the probe-side stage ID
     * @return the filter, or empty if build side has not completed yet
     */
    public Optional<RuntimeFilter> getFilter(String targetStageId) {
        return Optional.ofNullable(filtersByTargetStage.get(targetStageId));
    }

    /**
     * Returns all registered target stage IDs.
     */
    public List<String> registeredStageIds() {
        return List.copyOf(filtersByTargetStage.keySet());
    }

    public void clear() {
        filtersByTargetStage.clear();
    }

    /**
     * Removes all entries whose key starts with the given prefix.
     * Used for per-query cleanup when a query completes or is cancelled.
     *
     * @param prefix the queryId prefix (e.g., "queryId:")
     */
    public void removeByPrefix(String prefix) {
        filtersByTargetStage.keySet().removeIf(key -> key.startsWith(prefix));
    }
}

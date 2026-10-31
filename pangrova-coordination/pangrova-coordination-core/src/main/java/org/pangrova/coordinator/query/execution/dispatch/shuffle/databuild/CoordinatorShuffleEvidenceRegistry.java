package org.pangrova.coordinator.query.execution.dispatch.shuffle.databuild;

import java.util.concurrent.ConcurrentHashMap;

/** Process-local hooks. Durable evidence itself is held by the authoritative metadata store. */
@org.pangrova.common.api.annotation.Internal
public final class CoordinatorShuffleEvidenceRegistry {
    private final ConcurrentHashMap<String, ShuffleLifecycleEvidence> executions = new ConcurrentHashMap<>();

    /** Binds durable shuffle evidence to an execution identity. */
    public void bind(String executionId, ShuffleLifecycleEvidence evidence) {
        if (executionId == null || executionId.isBlank() || evidence == null) {
            throw new IllegalArgumentException("Durable shuffle evidence requires exact execution identity");
        }
        var existing = executions.putIfAbsent(executionId, evidence);
        if (existing != null && existing != evidence) {
            throw new IllegalStateException("Execution already has a different durable shuffle control binding");
        }
    }

    public ShuffleLifecycleEvidence find(String executionId) { return executions.get(executionId); }
    public void remove(String executionId) { executions.remove(executionId); }
    public void clear() { executions.clear(); }
}

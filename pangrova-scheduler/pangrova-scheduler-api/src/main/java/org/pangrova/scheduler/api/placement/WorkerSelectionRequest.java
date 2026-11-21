package org.pangrova.scheduler.api.placement;

import java.util.HashSet;
import java.util.List;

/**
 * One immutable, current candidate set. Eligibility, tenant scope, capability and
 * locality constraints belong to the caller; a strategy may only rank this set.
 * Duplicate identities are rejected because a result must map to one candidate.
 */
public record WorkerSelectionRequest(List<WorkerSelectionCandidate> candidates) {
    public WorkerSelectionRequest {
        candidates = List.copyOf(candidates);
        var ids = new HashSet<String>();
        for (var candidate : candidates) {
            if (!ids.add(candidate.workerId())) {
                throw new IllegalArgumentException("Duplicate worker identity: " + candidate.workerId());
            }
        }
    }
}

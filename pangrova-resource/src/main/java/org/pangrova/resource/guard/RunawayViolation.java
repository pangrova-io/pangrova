package org.pangrova.resource.guard;

import java.util.List;

/**
 * Describes why a query was classified as a runaway: the offending query plus one or more
 * human-readable breach reasons (one per exceeded threshold dimension).
 *
 * @param queryId the query that breached a threshold
 * @param reasons the breached dimensions, e.g. {@code "cpuTimeMs: 9000 > 5000"}; never empty
 */
public record RunawayViolation(String queryId, List<String> reasons) {

    public RunawayViolation {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (reasons == null || reasons.isEmpty()) {
            throw new IllegalArgumentException("reasons must not be null or empty");
        }
        reasons = List.copyOf(reasons);
    }
}

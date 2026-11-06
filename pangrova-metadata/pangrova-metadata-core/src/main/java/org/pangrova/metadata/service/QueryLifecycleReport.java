package org.pangrova.metadata.service;

import java.util.List;
import java.util.Map;

/**
 * Immutable report of a query lifecycle operation (BEGIN or CLEANUP) across catalogs.
 *
 * <p>Aggregates per-catalog success/failure outcomes so callers can observe partial
 * failures instead of silently swallowing exceptions.</p>
 */
public final class QueryLifecycleReport {

    public enum Operation { BEGIN, CLEANUP }

    private final String queryId;
    private final Operation operation;
    private final List<String> succeededCatalogs;
    private final Map<String, Exception> failedCatalogs;

    public QueryLifecycleReport(
            String queryId,
            Operation operation,
            List<String> succeededCatalogs,
            Map<String, Exception> failedCatalogs) {
        this.queryId = queryId;
        this.operation = operation;
        this.succeededCatalogs = List.copyOf(succeededCatalogs);
        this.failedCatalogs = Map.copyOf(failedCatalogs);
    }

    public String queryId() {
        return queryId;
    }

    public Operation operation() {
        return operation;
    }

    public List<String> succeededCatalogs() {
        return succeededCatalogs;
    }

    public Map<String, Exception> failedCatalogs() {
        return failedCatalogs;
    }

    public boolean hasFailures() {
        return !failedCatalogs.isEmpty();
    }

    public int failedCount() {
        return failedCatalogs.size();
    }

    public int succeededCount() {
        return succeededCatalogs.size();
    }

    public int totalCount() {
        return succeededCatalogs.size() + failedCatalogs.size();
    }
}

package org.pangrova.streaming.core.query;

/**
 * Executor interface for continuous query evaluation.
 * Invoked periodically by {@link ContinuousQueryConsumer} to refresh materialized views.
 */
public interface ContinuousQueryExecutor {

    /**
     * Executes a single refresh cycle for the given continuous query.
     *
     * @param queryName the registered continuous query name
     */
    void executeRefresh(String queryName);
}

package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects build-side join key values from shuffle output after a build stage completes.
 *
 * <p>Values are deposited via {@link #deposit} when task results arrive, and
 * retrieved via {@link #collectBuildSideValues} when the stage-completion hook fires.</p>
 *
 * <p>Thread-safe: backed by {@link ConcurrentHashMap}.</p>
 */
public final class ShuffleOutputCollector
        implements RuntimeFilterStageHandler.BuildSideCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleOutputCollector.class);

    /**
     * Key: "{queryId}:{stageId}:{column}" → collected values.
     */
    private final ConcurrentHashMap<String, List<? extends Comparable<?>>> valueStore =
            new ConcurrentHashMap<>();

    /**
     * Deposits build-side values collected from task execution results.
     *
     * @param queryId     the query identifier
     * @param stageId     the build-side stage identifier
     * @param column      the join key column name
     * @param values      the collected values from the build side
     */
    public void deposit(String queryId, String stageId, String column,
                        List<? extends Comparable<?>> values) {
        String key = buildKey(queryId, stageId, column);
        valueStore.put(key, List.copyOf(values));
        LOG.debug("event=build_side_values_deposited queryId={} stageId={} column={} count={}",
                queryId, stageId, column, values.size());
    }

    @Override
    public List<? extends Comparable<?>> collectBuildSideValues(
            String queryId, String stageId, String buildColumn) {
        String key = buildKey(queryId, stageId, buildColumn);
        List<? extends Comparable<?>> values = valueStore.remove(key);
        if (values == null) {
            LOG.debug("event=build_side_values_miss queryId={} stageId={} column={}",
                    queryId, stageId, buildColumn);
            return List.of();
        }
        return values;
    }

    /**
     * Clears all deposited values for a query (called on query completion/failure).
     */
    public void clearQuery(String queryId) {
        valueStore.keySet().removeIf(key -> key.startsWith(queryId + ":"));
    }

    private static String buildKey(String queryId, String stageId, String column) {
        return queryId + ":" + stageId + ":" + column;
    }
}

package org.pangrova.coordinator.query.execution.split;

import org.pangrova.query.api.task.QuerySplit;

import java.util.List;
import java.util.Map;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.planner.core.analysis.PlanFragment;

/**
 * Enumerates authenticated live-plan splits, optionally applying safe index pruning.
 * The caller retains the query authority; implementations must validate the pinned
 * read binding and preserve every row that could match the plan. Enumeration failure
 * propagates to the query; it is never converted to an empty successful result.
 * This operation does not own publication readers, dispatch state or task resources.
 */
@FunctionalInterface
public interface StageSplitSource {
    List<QuerySplit> fetchAndFilterSplits(
            PlanFragment fragment, Map<String, Long> snapshotOverrides,
            String queryId, QuerySession session);
}

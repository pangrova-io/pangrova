package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.planner.incremental.IncrementalAnalysis;

import java.util.List;

/**
 * Provides incremental analysis and materialized data for DT incremental merge.
 *
 * <p>Bridges the planner's {@link IncrementalAnalysis} (computed at DT creation)
 * with the coordinator's refresh pipeline. Implementations store the analysis
 * alongside the DT definition and provide access to the existing materialized
 * result for merge operations.</p>
 */
public interface IncrementalAnalysisProvider {

    /**
     * Returns the incremental analysis for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     * @return analysis result, or null if not available
     */
    IncrementalAnalysis getAnalysis(String dtName);

    /**
     * Reads the existing materialized aggregation result for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     * @return the current materialized result, or null if no data exists
     */
    IncrementalResult readMaterializedResult(String dtName);

    /**
     * Returns the GROUP BY key column names for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     * @return list of group key column names, empty for non-grouped queries
     */
    List<String> groupKeys(String dtName);
}

package org.pangrova.planner.analysis;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

/**
 * Query-owned result of {@link PangrovaPlanner#plan(String)}, bundling the optimized
 * plan with metadata that was previously stored in volatile fields.
 *
 * <p>Eliminates the race condition where concurrent {@code plan()} calls could
 * overwrite each other's column names and dynamic table mappings. Metadata
 * collections are immutable; the Calcite tree remains confined to this query's
 * planning lifecycle and must not be cached or mutated concurrently.</p>
 *
 * @param plan                        the optimized Calcite RelNode plan
 * @param preOptimizationColumnNames  column names captured before HEP optimization
 *                                    (preserves original SQL aliases)
 * @param physicalToLogical           query-owned identity for durable Dynamic Table scans
 */
@NotThreadSafe
public record PlanResult(
        RelNode plan,
        List<String> preOptimizationColumnNames,
        Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {

    public PlanResult {
        preOptimizationColumnNames = List.copyOf(preOptimizationColumnNames);
        physicalToLogical = Map.copyOf(physicalToLogical);
    }

    /** Plan result without a durable Dynamic Table scan mapping. */
    public PlanResult(RelNode plan, List<String> preOptimizationColumnNames) {
        this(plan, preOptimizationColumnNames, Map.of());
    }

    /**
     * Wraps this result in a {@link PlannedQuery} so that coordinator code can
     * hold a Calcite-free reference to the compiled plan.
     */
    public PlannedQuery toPlannedQuery() {
        return new PlannedQuery(plan, preOptimizationColumnNames, physicalToLogical);
    }

}

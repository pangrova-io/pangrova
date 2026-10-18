package org.pangrova.planner;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

/**
 * Query-owned result of {@link PangrovaPlanner#plan(String)}, bundling the optimized
 * plan with metadata that was previously stored in volatile fields.
 *
 * <p>Eliminates the race condition where concurrent {@code plan()} calls could
 * overwrite each other's column names and runtime filter descriptors. Metadata
 * collections are immutable; the Calcite tree remains confined to this query's
 * planning lifecycle and must not be cached or mutated concurrently.</p>
 *
 * @param plan                        the optimized Calcite RelNode plan
 * @param preOptimizationColumnNames  column names captured before HEP optimization
 *                                    (preserves original SQL aliases)
 * @param runtimeFilterDescriptors    DPP runtime filter descriptors for eligible joins
 * @param mergeOnReadHints            merge-on-read resolutions for DTs with pending deltas
 * @param physicalToLogical           query-owned identity for durable Dynamic Table scans
 */
@NotThreadSafe
public record PlanResult(
        RelNode plan,
        List<String> preOptimizationColumnNames,
        List<RuntimeFilterDescriptor> runtimeFilterDescriptors,
        List<DynamicTableResolution.MergeOnRead> mergeOnReadHints,
        Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {

    public PlanResult {
        preOptimizationColumnNames = List.copyOf(preOptimizationColumnNames);
        runtimeFilterDescriptors = List.copyOf(runtimeFilterDescriptors);
        mergeOnReadHints = List.copyOf(mergeOnReadHints);
        physicalToLogical = Map.copyOf(physicalToLogical);
    }

    /** Backward-compatible constructor for plans without a durable Dynamic Table mapping. */
    public PlanResult(RelNode plan, List<String> preOptimizationColumnNames,
                      List<RuntimeFilterDescriptor> runtimeFilterDescriptors,
                      List<DynamicTableResolution.MergeOnRead> mergeOnReadHints) {
        this(plan, preOptimizationColumnNames, runtimeFilterDescriptors, mergeOnReadHints, Map.of());
    }

    /**
     * Backward-compatible constructor without merge-on-read hints.
     */
    public PlanResult(RelNode plan, List<String> preOptimizationColumnNames,
                      List<RuntimeFilterDescriptor> runtimeFilterDescriptors) {
        this(plan, preOptimizationColumnNames, runtimeFilterDescriptors, List.of());
    }

    /**
     * Wraps this result in a {@link PlannedQuery} so that coordinator code can
     * hold a Calcite-free reference to the compiled plan.
     */
    public PlannedQuery toPlannedQuery() {
        return new PlannedQuery(plan, preOptimizationColumnNames,
                runtimeFilterDescriptors, mergeOnReadHints, physicalToLogical);
    }

}

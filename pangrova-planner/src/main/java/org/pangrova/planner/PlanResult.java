package org.pangrova.planner;

import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Immutable result of {@link PangrovaPlanner#plan(String)}, bundling the optimized
 * plan with metadata that was previously stored in volatile fields.
 *
 * <p>Eliminates the race condition where concurrent {@code plan()} calls could
 * overwrite each other's column names and runtime filter descriptors.</p>
 *
 * @param plan                        the optimized Calcite RelNode plan
 * @param preOptimizationColumnNames  column names captured before HEP optimization
 *                                    (preserves original SQL aliases)
 * @param runtimeFilterDescriptors    DPP runtime filter descriptors for eligible joins
 * @param mergeOnReadHints            merge-on-read resolutions for DTs with pending deltas;
 *                                    empty when no DTs require merge-on-read
 */
public record PlanResult(
        RelNode plan,
        List<String> preOptimizationColumnNames,
        List<RuntimeFilterDescriptor> runtimeFilterDescriptors,
        List<DynamicTableResolution.MergeOnRead> mergeOnReadHints) {

    public PlanResult {
        preOptimizationColumnNames = List.copyOf(preOptimizationColumnNames);
        runtimeFilterDescriptors = List.copyOf(runtimeFilterDescriptors);
        mergeOnReadHints = List.copyOf(mergeOnReadHints);
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
                runtimeFilterDescriptors, mergeOnReadHints);
    }

    /**
     * Creates a copy of this PlanResult with the plan detached from the VolcanoPlanner's
     * shared cluster (F-PLANNER-10). This prevents data races when a cached plan's cluster
     * is reused by a subsequent query on the same thread.
     *
     * <p>The detachment is done by copying the RelNode tree with its current trait set,
     * which severs the reference chain back to the thread-local VolcanoPlanner.</p>
     */
    public PlanResult withDetachedPlan() {
        RelNode detached = plan.copy(plan.getTraitSet(), plan.getInputs());
        return new PlanResult(detached, preOptimizationColumnNames,
                runtimeFilterDescriptors, mergeOnReadHints);
    }
}

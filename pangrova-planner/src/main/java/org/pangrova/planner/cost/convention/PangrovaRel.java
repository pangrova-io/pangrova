package org.pangrova.planner.cost.convention;

import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for all Pangrova physical operators.
 * Physical operators that implement this interface participate in the Volcano
 * planner's convention-based cost comparison and can be serialized to Substrait
 * for native execution.
 *
 * <p>All physical RelNode classes (HashJoin, BroadcastJoin, NestedLoopJoin,
 * SortMergeJoin, Sort, Aggregate, Exchange) implement this interface and carry
 * {@link PangrovaConvention#INSTANCE} in their traitSet.</p>
 */
public interface PangrovaRel extends RelNode {
}

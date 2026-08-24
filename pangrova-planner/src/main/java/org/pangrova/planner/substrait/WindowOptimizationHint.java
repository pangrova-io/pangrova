package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;

/**
 * Optimization hint for window function patterns eligible for specialized
 * native operators (RowNumberNode, TopNRowNumberNode).
 *
 * <p>Attached to the Project Rel's advanced_extension when the planner detects
 * a row_number() pattern that the native engine can execute more efficiently
 * using RowNumberNode or TopNRowNumberNode instead of the general WindowNode.</p>
 *
 * <p>The native engine uses this hint to skip pattern detection and directly
 * create the specialized operator. If the hint is absent, the native engine
 * falls back to its own pattern detection logic.</p>
 *
 * @param eligibleForRowNumberNode true when the window contains only row_number()
 *                                  with PARTITION BY, no ORDER BY, and default frame
 * @param eligibleForTopNRowNumberNode true when additionally a TopN limit is detected
 * @param topnLimit the limit K for TopN pattern (0 when not applicable)
 */
public record WindowOptimizationHint(
        boolean eligibleForRowNumberNode,
        boolean eligibleForTopNRowNumberNode,
        long topnLimit) implements Extension.Optimization {

    /** Marker key for identifying window optimization hints in the plan. */
    static final String MARKER = "pangrova:window_optimization_hint";

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"window_optimization_hint\"");
        json.append(",\"eligible_for_row_number_node\":").append(eligibleForRowNumberNode);
        json.append(",\"eligible_for_topn_row_number_node\":").append(eligibleForTopNRowNumberNode);
        if (topnLimit > 0) {
            json.append(",\"topn_limit\":").append(topnLimit);
        }
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
